// SPDX-License-Identifier: Apache-2.0
use crate::{
    api::Core,
    headers::{validate_bounded_header_extension, CanonicalHeaderBoundary, SnapshotHeaderStaging},
    network::Network,
    runtime::{ensure_origin, now},
};
use anyhow::{anyhow, ensure, Context, Result};
use futures::StreamExt;
use libp2p::PeerId;
use noid_chain::storage::snapshot_staging::{
    AuthenticatedSnapshotMetadata, SnapshotStagingSession,
};
use noid_chain::storage::{verify_history_step_terminal_candidate, SnapshotSegmentDescriptor};
use noid_chain::{hash_block_header, Block};
use noid_p2p::object_protocol::{ChainPoint, ObjectId};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

const HEADER_PROBE: u16 = 64;
const MAX_REORG_BYTES: usize = 64 * 1024 * 1024;

const HEADER_WINDOW: usize = 6;
// Native full peers admit two concurrent State responses; miners admit one.
const SEGMENT_WINDOW: usize = 2;
const BODY_WINDOW: usize = 4;
const PREFETCH_BYTES: usize = 32 * 1024 * 1024;

enum FollowOutcome {
    Checked,
    NoHeaders,
}

pub async fn run(core: Arc<Core>, net: Network, data: PathBuf) -> Result<()> {
    let mut deferred = HashMap::<PeerId, Instant>::new();
    let mut cursor = 0usize;
    loop {
        let mut peers: Vec<_> = net
            .status
            .lock()
            .unwrap()
            .peers
            .iter()
            .filter(|(id, p)| {
                p.selected
                    && deferred
                        .get(id)
                        .is_none_or(|until| Instant::now() >= *until)
            })
            .map(|(id, _)| *id)
            .collect();
        if peers.is_empty() {
            net.changed(Duration::from_secs(1)).await;
            continue;
        }
        let offset = cursor % peers.len();
        peers.rotate_left(offset);
        peers.truncate(8);
        cursor = cursor.wrapping_add(1);
        let floor = core.chain.read().await.finalized_checkpoint().height;
        // Probes run independently of the one canonical State writer. One slow
        // peer cannot delay confirmations from the peers that have responded.
        let mut probes = futures::stream::iter(peers)
            .map(|peer| {
                let net = &net;
                async move { (peer, net.inventory(peer, floor, HEADER_PROBE).await) }
            })
            .buffer_unordered(4);
        let mut progressed = false;
        while let Some(first) = probes.next().await {
            let mut ready = vec![first];
            let deadline = tokio::time::Instant::now() + Duration::from_millis(75);
            while let Ok(Some(response)) = tokio::time::timeout_at(deadline, probes.next()).await {
                ready.push(response);
            }
            // This orders work only. Each candidate still needs full consensus
            // and proof validation before it can affect the selected chain.
            ready.sort_by_key(|(_, response)| {
                std::cmp::Reverse(response.as_ref().ok().map_or(0, |(records, snapshot)| {
                    records
                        .last()
                        .map_or(0, |r| r.header.height)
                        .max(snapshot.map_or(0, |p| p.height))
                }))
            });
            for (peer, response) in ready {
                let outcome = match response {
                    Ok((records, snapshot)) => {
                        follow(&core, &net, peer, &data, floor, records, snapshot).await
                    }
                    Err(error) => Err(error),
                };
                match outcome {
                    Ok(FollowOutcome::Checked) => {
                        let mut status = net.status.lock().unwrap();
                        status.stage = "following";
                        status.error = None;
                        deferred.remove(&peer);
                        progressed = true;
                    }
                    Ok(FollowOutcome::NoHeaders) => {
                        // An empty range does not confirm our tip, but it is not
                        // evidence of a conflicting chain or an invalid source.
                        if let Some(p) = net.status.lock().unwrap().peers.get_mut(&peer) {
                            p.tip = None;
                        }
                        deferred.insert(peer, Instant::now() + Duration::from_secs(10));
                    }
                    Err(error) if crate::network::is_backpressure(&error) => {
                        tracing::debug!(%peer, %error, "sync request waiting for capacity");
                        deferred.insert(peer, Instant::now() + Duration::from_secs(1));
                    }
                    Err(error) => {
                        tracing::warn!(%peer, %error, "sync source deferred");
                        let has_state = core.chain.read().await.tip_height() > 0;
                        let mut status = net.status.lock().unwrap();
                        if !progressed {
                            status.stage = if has_state { "following" } else { "retrying" };
                            status.error = Some(error.to_string());
                        }
                        if let Some(p) = status.peers.get_mut(&peer) {
                            p.tip = None;
                        }
                        deferred.insert(peer, Instant::now() + Duration::from_secs(3));
                    }
                }
            }
        }
        net.changed(Duration::from_secs(2)).await;
    }
}

async fn follow(
    core: &Arc<Core>,
    net: &Network,
    peer: PeerId,
    data: &std::path::Path,
    requested_floor: u64,
    records: Vec<noid_p2p::header_protocol::HeaderInventoryRecord>,
    snapshot: Option<ChainPoint>,
) -> Result<FollowOutcome> {
    let (local_height, local_hash, local_work, floor, store) = {
        let chain = core.chain.read().await;
        (
            chain.tip_height(),
            chain.tip_hash(),
            *chain.tip_chain_work(),
            chain.finalized_checkpoint().height,
            chain.store.clone(),
        )
    };
    ensure!(
        records.len() <= usize::from(HEADER_PROBE),
        "header inventory exceeds request"
    );
    let snapshot_ahead = snapshot.is_some_and(|point| point.height > local_height);
    if records.is_empty() && snapshot_ahead {
        snapshot_sync(core, net, peer, data).await?;
        return Ok(FollowOutcome::Checked);
    }
    if records.is_empty() {
        tracing::debug!(
            %peer,
            requested_floor,
            local_height,
            snapshot_height = snapshot.map(|point| point.height),
            "peer returned an empty header range"
        );
        return Ok(FollowOutcome::NoHeaders);
    }
    let mut ancestor = None;
    for (i, record) in records.iter().enumerate() {
        ensure!(
            record.header.height == requested_floor + i as u64,
            "noncontiguous inventory"
        );
        record.validate()?;
        if i > 0 {
            ensure!(
                record.header.prev_block_hash == hash_block_header(&records[i - 1].header),
                "inventory parent mismatch"
            );
        }
        if store.get_header(record.header.height)? == Some(record.header) {
            ancestor = Some(i);
        } else {
            break;
        }
    }
    let index = ancestor.context("peer conflicts with finalized prefix")?;
    let candidate = &records[index + 1..];
    if candidate.is_empty() {
        let last = records.last().unwrap().header;
        if last.height == local_height && hash_block_header(&last) == local_hash {
            if let Some(p) = net.status.lock().unwrap().peers.get_mut(&peer) {
                p.tip = Some((ChainPoint::new(local_height, local_hash), Instant::now()));
            }
            // Reload and authenticate a persisted fork origin after process restart.
            if noid_chain::consensus::params::v2_active(local_height) {
                let terminal = store.get_history_step_terminal_at(local_height, local_hash)?;
                if let Some(terminal) = terminal {
                    let _permit = core.verifier.clone().acquire_owned().await?;
                    ensure_origin(&core.runtime, &terminal, peer, &net.commands).await?;
                }
            }
        }
        return Ok(FollowOutcome::Checked);
    }
    // A newer finalized snapshot is also advertised by peers when we are
    // already within their retained suffix. Follow those bodies directly;
    // otherwise a moving network can make us download snapshots endlessly.
    let retained = candidate.len()
        <= noid_chain::consensus::params::RETAINED_BLOCK_SERVING_DEPTH as usize
        && candidate.iter().all(|record| record.body.is_some())
        && candidate
            .last()
            .is_some_and(|record| record.terminal.is_some());
    if !retained && snapshot_ahead {
        snapshot_sync(core, net, peer, data).await?;
        return Ok(FollowOutcome::Checked);
    }
    ensure!(
        retained,
        "peer cannot supply the bounded suffix or a newer snapshot"
    );
    let ancestor = records[index].header.height;
    ensure!(
        ancestor >= floor,
        "candidate precedes the current finalized boundary"
    );
    let headers: Vec<_> = candidate.iter().map(|r| r.header).collect();
    let final_header = *headers.last().unwrap();
    let final_hash = hash_block_header(&final_header);
    let store_for_check = store.clone();
    let validate_headers = headers.clone();
    let work = tokio::task::spawn_blocking(move || {
        validate_bounded_header_extension(&store_for_check, ancestor, &validate_headers, now())
    })
    .await??;
    if !matches!(
        noid_chain::choose_chain_by_work(&work, &final_hash, &local_work, &local_hash),
        noid_chain::consensus::fork_choice::ChainChoice::A
    ) {
        return Ok(FollowOutcome::Checked);
    }
    ensure!(
        headers.len() <= noid_chain::consensus::params::RETAINED_BLOCK_SERVING_DEPTH as usize,
        "candidate exceeds bounded retained suffix; snapshot required"
    );
    net.status.lock().unwrap().stage = "verifying";
    let tip_record = candidate.last().unwrap();
    let terminal_id = tip_record.terminal.context("tip terminal is unavailable")?;
    let (terminal, terminal_permit) = net.object(peer, ObjectId::Terminal(terminal_id)).await?;
    let permit = core.verifier.clone().acquire_owned().await?;
    ensure_origin(&core.runtime, &terminal, peer, &net.commands).await?;
    let epoch_height = noid_chain::consensus::tx_epoch_anchor_height_for_child(final_header.height);
    let epoch = if epoch_height <= ancestor {
        store
            .get_header(epoch_height)?
            .context("epoch header missing")?
    } else {
        *headers
            .iter()
            .find(|h| h.height == epoch_height)
            .context("suffix epoch header missing")?
    };
    let runtime = core.runtime.clone();
    let verified = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let _transport = terminal_permit;
        noid_miner::install_inbound_verifier_cpu(|| {
            verify_history_step_terminal_candidate(final_header, epoch, terminal, |claim| {
                runtime
                    .verify_terminal(
                        claim.terminal_bytes,
                        &claim.header,
                        &claim.epoch_anchor_header,
                    )
                    .map(|_| ())
            })
            .map_err(|e| e.to_string())
        })
        .map_err(|e| anyhow!(e.to_string()))?
        .map_err(anyhow::Error::msg)
    })
    .await??;
    let declared_bytes = candidate.iter().try_fold(0usize, |total, record| {
        total
            .checked_add(record.body.context("body unavailable")?.encoded_len as usize)
            .context("body size overflow")
    })?;
    ensure!(
        declared_bytes <= MAX_REORG_BYTES,
        "suffix bodies exceed 64 MiB limit"
    );
    let mut downloads = futures::stream::iter(candidate.to_vec())
        .map(|record| async move {
            let (body, transport) = net
                .object(
                    peer,
                    ObjectId::BlockBody(record.body.context("body unavailable")?),
                )
                .await?;
            let expected = record.header;
            let body = tokio::task::spawn_blocking(move || -> Result<_> {
                let _transport = transport;
                ensure!(
                    Block::from_bytes(&body)
                        .map_err(|e| anyhow!("{e:?}"))?
                        .header
                        == expected,
                    "body header mismatch"
                );
                // Ownership moves from the transport budget to the capped suffix.
                Ok(body)
            })
            .await??;
            Ok::<_, anyhow::Error>(body)
        })
        .buffered(BODY_WINDOW);
    let mut bodies = Vec::with_capacity(candidate.len());
    while let Some(body) = downloads.next().await {
        bodies.push(body?);
    }
    let is_reorg = ancestor < local_height;
    if is_reorg {
        let chain = core.chain.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut chain = chain.blocking_write();
            ensure!(
                chain.tip_height() == local_height && chain.tip_hash() == local_hash,
                "chain moved"
            );
            let authority = chain.authorize_preverified_reorg_suffix(ancestor, verified)?;
            chain
                .apply_verified_reorg_suffix_with_applier_indexed(
                    authority,
                    &bodies,
                    now(),
                    |block, state| {
                        noid_chain::materialize_accepted_block_state(state, block)
                            .map_err(|e| format!("{e:?}"))
                    },
                )
                .map_err(|e| anyhow!(e.to_string()))?;
            Ok(())
        })
        .await??;
    } else {
        let mut authority = core
            .chain
            .write()
            .await
            .begin_preverified_recursive_suffix(verified)?;
        for body in bodies {
            let chain = core.chain.clone();
            authority = tokio::task::spawn_blocking(move || -> Result<_> {
                chain
                    .blocking_write()
                    .apply_verified_recursive_suffix_block(
                        &mut authority,
                        &body,
                        now(),
                        |block, state| {
                            noid_chain::materialize_accepted_block_state(state, block)
                                .map_err(|e| format!("{e:?}"))
                        },
                    )?;
                Ok(authority)
            })
            .await??;
        }
        ensure!(authority.is_complete(), "incomplete recursive suffix");
    }
    if let Some(p) = net.status.lock().unwrap().peers.get_mut(&peer) {
        p.tip = Some((
            ChainPoint::new(final_header.height, final_hash),
            Instant::now(),
        ));
    }
    tracing::info!(
        height = final_header.height,
        reorg = is_reorg,
        "verified canonical State advanced"
    );
    Ok(FollowOutcome::Checked)
}

async fn snapshot_sync(
    core: &Arc<Core>,
    net: &Network,
    peer: PeerId,
    data: &std::path::Path,
) -> Result<()> {
    let started = Instant::now();
    net.status.lock().unwrap().stage = "snapshot";
    let (local_height, store, base) = {
        let chain = core.chain.read().await;
        (
            chain.tip_height(),
            chain.store.clone(),
            CanonicalHeaderBoundary::load(&chain.store, chain.finalized_checkpoint().height)?,
        )
    };
    let manifest = net.manifest(peer, local_height).await?;
    ensure!(
        manifest.tip_height > local_height && manifest.has_valid_manifest_digest(),
        "snapshot does not advance State"
    );
    let point = ChainPoint::new(manifest.tip_height, manifest.tip_hash);
    let bridge = manifest
        .bridge_tip_height
        .checked_sub(point.height)
        .context("snapshot bridge precedes boundary")?;
    ensure!(
        bridge <= noid_chain::consensus::params::RECENT_BLOCK_RETENTION_DEPTH,
        "snapshot bridge exceeds retained suffix"
    );
    if bridge == 0 {
        ensure!(
            manifest.bridge_tip_hash == point.hash
                && manifest.bridge_cumulative_chainwork == manifest.cumulative_chainwork,
            "empty snapshot bridge differs from boundary"
        );
    } else {
        ensure!(
            noid_chain::work_gt(
                &manifest.bridge_cumulative_chainwork,
                &manifest.cumulative_chainwork
            ),
            "snapshot bridge work mismatch"
        );
    }
    // Fetch only the small terminal context first. Its headers remain candidates
    // until the complete independently checked work chain agrees with them.
    let epoch_height = noid_chain::consensus::tx_epoch_anchor_height_for_child(point.height);
    let count = u16::try_from(point.height - epoch_height + 1)?;
    let context = net.headers(peer, epoch_height, count).await?;
    ensure!(
        context.len() == usize::from(count),
        "incomplete terminal header context"
    );
    let header = *context.last().context("snapshot tip missing")?;
    let epoch = context[0];
    ensure!(
        header.height == point.height
            && epoch.height == epoch_height
            && hash_block_header(&header) == point.hash,
        "snapshot terminal header identity mismatch"
    );
    ensure!(
        header.state_root == manifest.state_root
            && header.log_slots == manifest.log_slots
            && header.active_slot_count == manifest.active_slot_count
            && header.alloc_counter == manifest.alloc_counter,
        "snapshot State metadata mismatch"
    );
    noid_chain::consensus::validate_future_drift(header.timestamp, now())?;
    let temp = tempfile::Builder::new().prefix("sync-").tempdir_in(data)?;
    let headers_future = async {
        let start = Instant::now();
        let mut staging = SnapshotHeaderStaging::create(temp.path(), &store, base)?;
        let first = staging.next_height()?;
        let ranges = (first..=point.height)
            .step_by(crate::headers::MAX_STAGED_HEADER_BATCH)
            .map(|height| {
                let count = (point.height - height + 1)
                    .min(crate::headers::MAX_STAGED_HEADER_BATCH as u64)
                    as u16;
                (height, count)
            });
        let mut downloads = futures::stream::iter(ranges)
            .map(|(height, count)| async move {
                let headers = net.headers(peer, height, count).await?;
                ensure!(
                    headers.len() == usize::from(count),
                    "incomplete snapshot header range"
                );
                Ok::<_, anyhow::Error>(headers)
            })
            .buffered(HEADER_WINDOW);
        while let Some(batch) = downloads.next().await {
            let batch = batch?;
            staging = tokio::task::spawn_blocking(move || -> Result<_> {
                staging.append_batch(&batch)?;
                Ok(staging)
            })
            .await??;
        }
        let boundary =
            staging.exact_boundary(point.height, point.hash, manifest.cumulative_chainwork)?;
        ensure!(
            boundary.tip_header == header && boundary.epoch_anchor_header == epoch,
            "terminal context conflicts with work chain"
        );
        ensure!(
            noid_chain::work_gt(
                &boundary.cumulative_chainwork,
                core.chain.read().await.tip_chain_work()
            ),
            "snapshot has insufficient work"
        );
        let sealed = staging.seal(&store)?;
        tracing::info!(
            seconds = start.elapsed().as_secs_f64(),
            headers = point.height - base.header.height,
            "snapshot work chain verified"
        );
        Ok::<_, anyhow::Error>(sealed)
    };
    let (send_segments, mut receive_segments) = tokio::sync::mpsc::channel(128);
    let prefetch_budget = Arc::new(tokio::sync::Semaphore::new(PREFETCH_BYTES));
    let download_future = async {
        let download_started = Instant::now();
        let send_segments = send_segments;
        let items = manifest
            .segment_ids
            .iter()
            .copied()
            .zip(manifest.segment_lengths.iter().copied());
        let mut downloads = futures::stream::iter(items)
            .map(|(id, length)| {
                let budget = prefetch_budget.clone();
                let digest = manifest.manifest_digest;
                async move {
                    let reservation = length.checked_add(512).context("segment length overflow")?;
                    ensure!(
                        reservation as usize <= PREFETCH_BYTES,
                        "segment exceeds prefetch budget"
                    );
                    let memory = budget.acquire_many_owned(reservation).await?;
                    let response = net.segment(peer, id, point, digest).await?;
                    ensure!(
                        response
                            .data
                            .as_ref()
                            .is_some_and(|bytes| bytes.len() == length as usize),
                        "segment response length differs from its pinned descriptor"
                    );
                    Ok::<_, anyhow::Error>((id, response, memory))
                }
            })
            .buffer_unordered(SEGMENT_WINDOW);
        while let Some(item) = downloads.next().await {
            send_segments
                .send(item?)
                .await
                .map_err(|_| anyhow!("snapshot consumer stopped"))?;
        }
        tracing::info!(
            seconds = download_started.elapsed().as_secs_f64(),
            bytes = manifest
                .segment_lengths
                .iter()
                .map(|n| u64::from(*n))
                .sum::<u64>(),
            "snapshot State transfer complete"
        );
        Ok::<_, anyhow::Error>(())
    };
    let state_future = async {
        let proof_started = Instant::now();
        let (terminal, transport) = net.terminal(peer, point).await?;
        let permit = core.verifier.clone().acquire_owned().await?;
        ensure_origin(&core.runtime, &terminal, peer, &net.commands).await?;
        let runtime = core.runtime.clone();
        let chain = core.chain.clone();
        let verified = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _transport = transport;
            noid_miner::install_inbound_verifier_cpu(|| {
                chain
                    .blocking_read()
                    .verify_snapshot_boundary(header, epoch, terminal, |claim| {
                        runtime.verify_terminal(
                            claim.terminal_bytes,
                            &claim.header,
                            &claim.epoch_anchor_header,
                        )
                    })
                    .map_err(|e| e.to_string())
            })
            .map_err(|e| anyhow!(e.to_string()))?
            .map_err(anyhow::Error::msg)
        })
        .await??;
        tracing::info!(
            seconds = proof_started.elapsed().as_secs_f64(),
            "snapshot recursive proof verified"
        );
        let state_started = Instant::now();
        let metadata = AuthenticatedSnapshotMetadata::from_authenticated_header(
            header,
            point.hash,
            manifest.eff_log,
        )?;
        let descriptors = manifest
            .segment_ids
            .iter()
            .copied()
            .zip(manifest.segment_roots.iter().copied())
            .zip(manifest.segment_lengths.iter().copied())
            .map(
                |((segment_id, segment_root), encoded_len)| SnapshotSegmentDescriptor {
                    segment_id,
                    segment_root,
                    encoded_len,
                },
            )
            .collect();
        let mut state = SnapshotStagingSession::new(temp.path(), metadata, descriptors)?;
        let mut segment_checks_seconds = 0.0;
        while let Some((id, response, memory)) = receive_segments.recv().await {
            let check_started = Instant::now();
            state = tokio::task::spawn_blocking(move || -> Result<_> {
                let _memory = memory;
                ensure!(
                    response.status.busy_retry_after_ms().is_none(),
                    "segment provider busy"
                );
                state.accept_segment(
                    id,
                    response.eff_log,
                    response.data.as_deref().context("segment unavailable")?,
                )?;
                Ok(state)
            })
            .await??;
            segment_checks_seconds += check_started.elapsed().as_secs_f64();
        }
        let finalize_started = Instant::now();
        let staged = tokio::task::spawn_blocking(move || state.finalize()).await??;
        tracing::info!(
            seconds = state_started.elapsed().as_secs_f64(),
            segment_checks_seconds,
            finalize_seconds = finalize_started.elapsed().as_secs_f64(),
            segments = manifest.segment_ids.len(),
            "snapshot State verified"
        );
        Ok::<_, anyhow::Error>((staged, verified))
    };
    let (mut sealed, (state, verified), ()) =
        tokio::try_join!(headers_future, state_future, download_future)?;
    let chain = core.chain.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        chain
            .blocking_write()
            .apply_staged_state_snapshot(&state, &verified, &mut sealed, true)?;
        Ok(())
    })
    .await??;
    tracing::info!(
        height = point.height,
        seconds = started.elapsed().as_secs_f64(),
        "authenticated snapshot installed"
    );
    Ok(())
}
