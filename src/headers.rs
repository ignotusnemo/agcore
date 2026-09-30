// SPDX-License-Identifier: Apache-2.0
//! Bounded header validation for Agent Core. All consensus decisions use the
//! native v2 validators; transport and staging do not use the wallet sync loop.
use anyhow::{anyhow, ensure, Context, Result};
use noid_chain::consensus::{
    add_work, asert_anchor_height, block_work, finalized_expansion_window,
    header::validate_header_timeless_prehashed_parent,
    params::{EPOCH_LENGTH, EXPANSION_HEADER_LOOKBACK, MEDIAN_TIME_BLOCKS, TX_EPOCH_BLOCKS},
};
use noid_chain::storage::{MdbxStore, SnapshotHeaderInstallSource, VerifiedHeaderBatchRecord};
use noid_chain::wire::BLOCK_HEADER_WIRE_SIZE;
use noid_chain::{hash_block_header, BlockHeader, HeaderChainAnchor};
use rayon::prelude::*;
use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::Path,
};

pub const MAX_STAGED_HEADER_BATCH: usize = noid_p2p::header_sync_codec::MAX_HEADERS_PER_BATCH;
const RECORD_BYTES: usize = BLOCK_HEADER_WIRE_SIZE + 64;
fn window_len() -> usize {
    (EXPANSION_HEADER_LOOKBACK as usize + 1)
        .max(EPOCH_LENGTH as usize)
        .max(MEDIAN_TIME_BLOCKS)
        .max(TX_EPOCH_BLOCKS as usize + MEDIAN_TIME_BLOCKS + 1)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanonicalHeaderBoundary {
    pub header: BlockHeader,
    pub block_hash: [u8; 32],
    pub cumulative_chainwork: [u8; 32],
}
impl CanonicalHeaderBoundary {
    pub fn load(store: &MdbxStore, height: u64) -> Result<Self> {
        let header = store
            .get_header(height)?
            .context("canonical header missing")?;
        ensure!(
            header.height == height && height < u64::MAX,
            "canonical height mismatch"
        );
        let block_hash = hash_block_header(&header);
        let cumulative_chainwork = store
            .get_chain_work(height)?
            .context("canonical work missing")?;
        ensure!(
            store.get_header_anchor(height)?
                == Some(HeaderChainAnchor {
                    height,
                    block_id: block_hash,
                    state_root: header.state_root,
                    tx_root: header.tx_root,
                    miner_address: header.miner_address,
                    log_slots: header.log_slots,
                    active_slot_count: header.active_slot_count,
                    alloc_counter: header.alloc_counter,
                    cumulative_chainwork,
                }),
            "canonical anchor mismatch"
        );
        Ok(Self {
            header,
            block_hash,
            cumulative_chainwork,
        })
    }
    fn record(self) -> VerifiedHeaderBatchRecord {
        VerifiedHeaderBatchRecord {
            header: self.header,
            hash: self.block_hash,
            cumulative_chainwork: self.cumulative_chainwork,
        }
    }
}
#[derive(Clone, Copy)]
pub struct SnapshotHeaderBoundary {
    pub tip_header: BlockHeader,
    pub tip_hash: [u8; 32],
    pub cumulative_chainwork: [u8; 32],
    pub epoch_anchor_header: BlockHeader,
}

fn load_window(store: &MdbxStore, height: u64) -> Result<Vec<BlockHeader>> {
    let start = height.saturating_sub(window_len() as u64 - 1);
    (start..=height)
        .map(|h| {
            let header = store
                .get_header(h)?
                .context("canonical consensus window incomplete")?;
            ensure!(header.height == h, "canonical window height mismatch");
            Ok(header)
        })
        .collect()
}

// Every header's parent, MTP, ASERT anchor and expansion window are immutable
// inputs in this batch. They can be checked in parallel, provided every input
// header also passes and the first one extends the authenticated local prefix.
fn validate_batch(
    prefix: &[BlockHeader],
    base: CanonicalHeaderBoundary,
    headers: &[BlockHeader],
    local_time: Option<u64>,
) -> Result<Vec<VerifiedHeaderBatchRecord>> {
    ensure!(
        !headers.is_empty() && headers.len() <= MAX_STAGED_HEADER_BATCH,
        "header batch bound"
    );
    ensure!(
        prefix.last() == Some(&base.header),
        "header window boundary mismatch"
    );
    let first = prefix.first().context("empty header window")?.height;
    let mut all = Vec::with_capacity(prefix.len() + headers.len());
    all.extend_from_slice(prefix);
    all.extend_from_slice(headers);
    for (i, header) in all.iter().enumerate() {
        ensure!(
            first.checked_add(i as u64) == Some(header.height),
            "noncontiguous header batch"
        );
    }
    let checked = || -> Result<Vec<[u8; 32]>> {
        let hashes: Vec<_> = headers.par_iter().map(hash_block_header).collect();
        headers
            .par_iter()
            .enumerate()
            .try_for_each(|(i, header)| -> Result<()> {
                let at = prefix.len() + i;
                let parent = &all[at - 1];
                let parent_id = if i == 0 {
                    base.block_hash
                } else {
                    hashes[i - 1]
                };
                let previous = &all[at.saturating_sub(MEDIAN_TIME_BLOCKS)..at];
                let mut timestamps = [0; MEDIAN_TIME_BLOCKS];
                for (out, h) in timestamps.iter_mut().zip(previous) {
                    *out = h.timestamp;
                }
                let anchor_height = asert_anchor_height(parent.height);
                let anchor_index = anchor_height
                    .checked_sub(first)
                    .context("ASERT anchor outside window")?
                    as usize;
                let anchor = all.get(anchor_index).context("ASERT anchor missing")?;
                let counts: Vec<_> = match finalized_expansion_window(parent.height) {
                    None => Vec::new(),
                    Some((start, end)) => {
                        let a = start
                            .checked_sub(first)
                            .context("expansion window missing")?
                            as usize;
                        let b =
                            end.checked_sub(first).context("expansion window missing")? as usize;
                        all.get(a..=b)
                            .context("incomplete expansion window")?
                            .iter()
                            .map(|h| h.active_slot_count)
                            .collect()
                    }
                };
                validate_header_timeless_prehashed_parent(
                    header,
                    parent,
                    parent_id,
                    &timestamps[..previous.len()],
                    &counts,
                    anchor.height,
                    anchor.timestamp,
                    &anchor.difficulty_target,
                )
                .with_context(|| format!("invalid header at {}", header.height))?;
                if let Some(now) = local_time {
                    noid_chain::consensus::validate_future_drift(header.timestamp, now)?;
                }
                Ok(())
            })?;
        Ok(hashes)
    };
    let hashes =
        noid_miner::install_inbound_verifier_cpu(checked).map_err(|e| anyhow!(e.to_string()))??;
    let mut work = base.cumulative_chainwork;
    Ok(headers
        .iter()
        .zip(hashes)
        .map(|(&header, hash)| {
            work = add_work(&work, &block_work(&header.difficulty_target));
            VerifiedHeaderBatchRecord {
                header,
                hash,
                cumulative_chainwork: work,
            }
        })
        .collect())
}

pub fn validate_bounded_header_extension(
    store: &MdbxStore,
    ancestor: u64,
    headers: &[BlockHeader],
    now: u64,
) -> Result<[u8; 32]> {
    let base = CanonicalHeaderBoundary::load(store, ancestor)?;
    let records = validate_batch(&load_window(store, ancestor)?, base, headers, Some(now))?;
    Ok(records
        .last()
        .context("empty header extension")?
        .cumulative_chainwork)
}

pub struct SnapshotHeaderStaging {
    base: CanonicalHeaderBoundary,
    tip: CanonicalHeaderBoundary,
    window: Vec<BlockHeader>,
    file: BufWriter<File>,
    digest: blake3::Hasher,
    count: u64,
}
impl SnapshotHeaderStaging {
    pub fn create(
        directory: &Path,
        store: &MdbxStore,
        base: CanonicalHeaderBoundary,
    ) -> Result<Self> {
        ensure!(
            CanonicalHeaderBoundary::load(store, base.header.height)? == base,
            "canonical base moved"
        );
        // The anonymous private file has no path that can be replaced during sync.
        Ok(Self {
            base,
            tip: base,
            window: load_window(store, base.header.height)?,
            file: BufWriter::with_capacity(1024 * 1024, tempfile::tempfile_in(directory)?),
            digest: blake3::Hasher::new(),
            count: 0,
        })
    }
    pub fn next_height(&self) -> Result<u64> {
        self.tip
            .header
            .height
            .checked_add(1)
            .context("height overflow")
    }
    pub fn append_batch(&mut self, headers: &[BlockHeader]) -> Result<()> {
        let records = validate_batch(&self.window, self.tip, headers, None)?;
        let mut encoded = Vec::with_capacity(records.len() * RECORD_BYTES);
        for record in &records {
            record.header.encode(&mut encoded);
            encoded.extend_from_slice(&record.hash);
            encoded.extend_from_slice(&record.cumulative_chainwork);
        }
        // No state is published until the entire batch passes native validation.
        self.file.write_all(&encoded)?;
        self.digest.update(&encoded);
        let last = records.last().unwrap();
        self.tip = CanonicalHeaderBoundary {
            header: last.header,
            block_hash: last.hash,
            cumulative_chainwork: last.cumulative_chainwork,
        };
        self.count += records.len() as u64;
        self.window.extend_from_slice(headers);
        if self.window.len() > window_len() {
            self.window.drain(..self.window.len() - window_len());
        }
        Ok(())
    }
    pub fn exact_boundary(
        &self,
        height: u64,
        hash: [u8; 32],
        work: [u8; 32],
    ) -> Result<SnapshotHeaderBoundary> {
        ensure!(
            self.tip.header.height == height
                && self.tip.block_hash == hash
                && self.tip.cumulative_chainwork == work,
            "staged chain does not match the exact snapshot boundary and work"
        );
        let epoch = noid_chain::consensus::tx_epoch_anchor_height_for_child(height);
        let epoch_anchor_header = *self
            .window
            .iter()
            .find(|h| h.height == epoch)
            .context("snapshot epoch missing")?;
        Ok(SnapshotHeaderBoundary {
            tip_header: self.tip.header,
            tip_hash: hash,
            cumulative_chainwork: work,
            epoch_anchor_header,
        })
    }
    pub fn seal(mut self, store: &MdbxStore) -> Result<HeaderArchive> {
        ensure!(
            CanonicalHeaderBoundary::load(store, self.base.header.height)? == self.base,
            "canonical base moved"
        );
        self.file.flush()?;
        let mut file = self.file.into_inner().map_err(|e| anyhow!(e.to_string()))?;
        ensure!(
            file.metadata()?.len() == self.count * RECORD_BYTES as u64,
            "header spool length changed"
        );
        file.seek(SeekFrom::Start(0))?;
        let start = self
            .tip
            .header
            .height
            .saturating_sub(MEDIAN_TIME_BLOCKS as u64 + TX_EPOCH_BLOCKS);
        let recent: Vec<_> = self
            .window
            .into_iter()
            .filter(|h| h.height >= start)
            .collect();
        ensure!(
            recent.first().is_some_and(|h| h.height == start),
            "recent header window incomplete"
        );
        Ok(HeaderArchive {
            base: self.base.record(),
            tip: self.tip.record(),
            recent,
            file: BufReader::with_capacity(1024 * 1024, file),
            count: self.count,
            read: 0,
            expected: *self.digest.finalize().as_bytes(),
            digest: blake3::Hasher::new(),
        })
    }
}

pub struct HeaderArchive {
    base: VerifiedHeaderBatchRecord,
    tip: VerifiedHeaderBatchRecord,
    recent: Vec<BlockHeader>,
    file: BufReader<File>,
    count: u64,
    read: u64,
    expected: [u8; 32],
    digest: blake3::Hasher,
}
impl SnapshotHeaderInstallSource for HeaderArchive {
    fn base_record(&self) -> VerifiedHeaderBatchRecord {
        self.base
    }
    fn target_record(&self) -> VerifiedHeaderBatchRecord {
        self.tip
    }
    fn recent_headers(&self) -> &[BlockHeader] {
        &self.recent
    }
    fn next_record(&mut self) -> std::result::Result<Option<VerifiedHeaderBatchRecord>, String> {
        (|| -> Result<_> {
            if self.read == self.count {
                ensure!(
                    self.file.get_ref().metadata()?.len() == self.count * RECORD_BYTES as u64,
                    "header spool length changed"
                );
                ensure!(
                    self.digest.finalize().as_bytes() == &self.expected,
                    "verified header spool changed during installation"
                );
                return Ok(None);
            }
            let mut bytes = [0; RECORD_BYTES];
            self.file.read_exact(&mut bytes)?;
            self.digest.update(&bytes);
            self.read += 1;
            Ok(Some(VerifiedHeaderBatchRecord {
                header: BlockHeader::from_bytes(&bytes[..BLOCK_HEADER_WIRE_SIZE])
                    .map_err(|e| anyhow!("{e:?}"))?,
                hash: bytes[BLOCK_HEADER_WIRE_SIZE..BLOCK_HEADER_WIRE_SIZE + 32]
                    .try_into()
                    .unwrap(),
                cumulative_chainwork: bytes[BLOCK_HEADER_WIRE_SIZE + 32..].try_into().unwrap(),
            }))
        })()
        .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noid_chain::consensus::{difficulty::next_target, genesis_header, params::BLOCK_TIME};

    fn cpu() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            noid_miner::configure_process_cpu_budget_with_threads(
                noid_miner::ProcessCpuBudgetMode::ProofOnly,
                Some(2),
            )
            .unwrap();
        });
    }
    fn child() -> BlockHeader {
        let parent = genesis_header();
        let timestamp = parent.timestamp + BLOCK_TIME;
        BlockHeader {
            prev_block_hash: hash_block_header(&parent),
            state_root: [1; 32],
            tx_root: [0x41; 32],
            timestamp,
            height: 1,
            miner_address: parent.miner_address,
            nonce: 58_902,
            difficulty_target: next_target(
                0,
                parent.timestamp,
                &parent.difficulty_target,
                1,
                parent.timestamp,
                timestamp,
            ),
            log_slots: parent.log_slots,
            active_slot_count: parent.active_slot_count,
            alloc_counter: parent.alloc_counter,
        }
    }
    #[test]
    fn native_consensus_and_parallel_batch_agree() {
        cpu();
        let db = tempfile::tempdir().unwrap();
        let chain = noid_chain::storage::MdbxChainContext::open_or_create(db.path()).unwrap();
        let base = CanonicalHeaderBoundary::load(&chain.store, 0).unwrap();
        let valid = child();
        assert!(validate_batch(&[base.header], base, &[valid], None).is_ok());
        let mut mutations = vec![valid; 6];
        mutations[0].prev_block_hash = [0; 32];
        mutations[1].difficulty_target[31] ^= 1;
        mutations[2].nonce = 0;
        mutations[3].timestamp = base.header.timestamp;
        mutations[4].log_slots += 1;
        mutations[5].height += 1;
        for changed in mutations {
            assert!(noid_chain::consensus::header::validate_header_timeless(
                &changed,
                &base.header,
                &[base.header.timestamp],
                &[],
                0,
                base.header.timestamp,
                &base.header.difficulty_target
            )
            .is_err());
            assert!(validate_batch(&[base.header], base, &[changed], None).is_err());
        }
    }
    #[test]
    fn rejected_batch_does_not_publish_a_valid_prefix() {
        cpu();
        let db = tempfile::tempdir().unwrap();
        let chain = noid_chain::storage::MdbxChainContext::open_or_create(db.path()).unwrap();
        let base = CanonicalHeaderBoundary::load(&chain.store, 0).unwrap();
        let mut staging = SnapshotHeaderStaging::create(db.path(), &chain.store, base).unwrap();
        let valid = child();
        let mut bad = valid;
        bad.height = 2;
        assert!(staging.append_batch(&[valid, bad]).is_err());
        assert_eq!(staging.count, 0);
        assert_eq!(staging.next_height().unwrap(), 1);
        staging.append_batch(&[valid]).unwrap();
        assert!(staging
            .exact_boundary(1, hash_block_header(&valid), [0; 32])
            .is_err());
        let mut sealed = staging.seal(&chain.store).unwrap();
        assert_eq!(sealed.next_record().unwrap().unwrap().header, valid);
        assert!(sealed.next_record().unwrap().is_none());
        assert!(chain.store.get_header(1).unwrap().is_none());
    }
    #[test]
    fn changed_spool_cannot_complete_installation() {
        cpu();
        let db = tempfile::tempdir().unwrap();
        let chain = noid_chain::storage::MdbxChainContext::open_or_create(db.path()).unwrap();
        let base = CanonicalHeaderBoundary::load(&chain.store, 0).unwrap();
        let mut staging = SnapshotHeaderStaging::create(db.path(), &chain.store, base).unwrap();
        staging.append_batch(&[child()]).unwrap();
        let mut sealed = staging.seal(&chain.store).unwrap();
        let file = sealed.file.get_mut();
        file.seek(SeekFrom::Start((RECORD_BYTES - 1) as u64))
            .unwrap();
        file.write_all(&[0xFF]).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        sealed.next_record().unwrap();
        assert!(sealed.next_record().is_err());
        assert!(chain.store.get_header(1).unwrap().is_none());
    }
}
