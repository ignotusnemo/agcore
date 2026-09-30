// SPDX-License-Identifier: Apache-2.0
use anyhow::{anyhow, Context, Result};
use noid_miner::HistoryProtocolRuntime;
use noid_p2p::{NetworkCommand, NetworkCommandSender};
use std::{path::Path, sync::Arc, time::Duration};

pub fn load(data: &Path) -> Result<Arc<HistoryProtocolRuntime>> {
    let pack = crate::embedded::embedded_history_step_pack().context(
        "this executable has no authenticated proof artifacts; build with the release packs",
    )?;
    let metadata = noid_miner::decode_history_step_runtime_metadata_pinned(
        pack.runtime_metadata(),
        pack.runtime_metadata_digest(),
    )
    .map_err(|e| anyhow!(e.to_string()))?;
    let keys =
        crate::embedded::embedded_retirement_keys(metadata.bank()).map_err(anyhow::Error::msg)?;
    let cache = data
        .join("matrices")
        .join(hex::encode(pack.runtime_metadata_digest()));
    let source = pack
        .matrix_source(Some(cache))
        .map_err(|e| anyhow!(e.to_string()))?;
    let (bank, parts) = metadata.into_parts();
    let legacy =
        noid_recursive::acceptance::history_step::HistoryStepRuntime::new(bank, source, parts)
            .map_err(|e| anyhow!(e.to_string()))?;
    let mut runtime = HistoryProtocolRuntime::new(
        Some(Arc::new(legacy)),
        crate::embedded::embedded_v2_runtime().map_err(anyhow::Error::msg)?,
        data.join("fork-origins"),
    )
    .map_err(anyhow::Error::msg)?
    .with_legacy_matrix_cache_available(pack.has_legacy_matrices())
    .with_large_v2_mining(false);
    if let Some(keys) = keys {
        runtime = runtime
            .with_retirement_keys(keys)
            .map_err(anyhow::Error::msg)?;
    }
    Ok(Arc::new(runtime))
}

pub async fn ensure_origin(
    runtime: &Arc<HistoryProtocolRuntime>,
    terminal: &[u8],
    peer: libp2p::PeerId,
    commands: &NetworkCommandSender,
) -> Result<()> {
    let Some(origin) = runtime
        .requested_origin(terminal)
        .map_err(anyhow::Error::msg)?
    else {
        return Ok(());
    };
    if runtime
        .has_verified_origin(&origin)
        .map_err(anyhow::Error::msg)?
    {
        return Ok(());
    }
    if let Ok(Some(bytes)) = runtime.origin_bytes(origin.request_binding()) {
        let runtime = runtime.clone();
        let expected = origin.clone();
        let result = tokio::task::spawn_blocking(move || {
            noid_miner::install_inbound_verifier_cpu(|| {
                let checked = runtime.install_origin_bytes(&bytes)?;
                if checked.origin() != &expected {
                    return Err("fork origin mismatch".to_string());
                }
                Ok(())
            })
            .map_err(|e| e.to_string())?
        })
        .await?;
        if result.is_ok() {
            return Ok(());
        }
    }
    let (reply, receive) = tokio::sync::oneshot::channel();
    commands
        .send(NetworkCommand::FetchForkOrigin {
            peer,
            request: origin.request_binding(),
            reply,
        })
        .await
        .map_err(|e| anyhow!(e.to_string()))?;
    let response = tokio::time::timeout(Duration::from_secs(50), receive)
        .await??
        .map_err(anyhow::Error::msg)?;
    let runtime = runtime.clone();
    tokio::task::spawn_blocking(move || {
        noid_miner::install_inbound_verifier_cpu(|| {
            // Retain the response's byte-budget permit throughout verification.
            let bytes = response
                .bytes
                .as_deref()
                .ok_or("peer has no requested fork origin")?;
            let checked = runtime.install_origin_bytes(bytes)?;
            if checked.origin() != &origin {
                return Err("fork origin mismatch".to_string());
            }
            Ok(())
        })
        .map_err(|e| e.to_string())?
    })
    .await?
    .map_err(anyhow::Error::msg)
}

pub fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
