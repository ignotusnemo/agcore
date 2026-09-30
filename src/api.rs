// SPDX-License-Identifier: Apache-2.0
use anyhow::{anyhow, ensure, Context, Result};
use noid_chain::storage::MdbxChainContext;
use noid_tx::experimental_object::{integer_program, policy, ObjectOpening};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{RwLock, Semaphore};

pub const MAX_LINE: usize = 2 * noid_block::contract_receipt::MAX_RECEIPT_BYTES + 4096;

pub struct Core {
    pub chain: Arc<RwLock<MdbxChainContext>>,
    pub runtime: Arc<noid_miner::HistoryProtocolRuntime>,
    pub verifier: Arc<Semaphore>,
    pub status: crate::network::Status,
    pub health: tokio::sync::watch::Receiver<noid_p2p::network::P2PHealthSnapshot>,
    pub min_peers: usize,
    pub max_tip_age: u64,
    pub receipt_cache: Mutex<ReceiptCache>,
}

#[derive(Default)]
pub struct ReceiptCache {
    entries: VecDeque<([u8; 32], Vec<u8>, noid_chain::BlockHeader, Value)>,
}
impl ReceiptCache {
    fn get(&mut self, bytes: &[u8], header: noid_chain::BlockHeader) -> Option<Value> {
        let key = *blake3::hash(bytes).as_bytes();
        // The digest is only an index. Exact bytes are compared as well.
        let index = self
            .entries
            .iter()
            .position(|(k, b, h, _)| *k == key && *h == header && b == bytes)?;
        let item = self.entries.remove(index)?;
        let result = item.3.clone();
        self.entries.push_back(item);
        Some(result)
    }
    fn insert(&mut self, bytes: Vec<u8>, header: noid_chain::BlockHeader, call: Value) {
        if self.entries.len() == 8 {
            self.entries.pop_front();
        }
        self.entries
            .push_back((*blake3::hash(&bytes).as_bytes(), bytes, header, call));
    }
    fn recheck(&mut self, id: &[u8]) -> Option<(noid_chain::BlockHeader, Value)> {
        let index = self.entries.iter().position(|(key, _, _, _)| key == id)?;
        let item = self.entries.remove(index)?;
        let result = (item.2, item.3.clone());
        self.entries.push_back(item);
        Some(result)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub id: String,
    pub method: String,
    #[serde(default = "empty_params")]
    pub params: Value,
}
fn empty_params() -> Value {
    json!({})
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RightParams {
    opening_hex: String,
    slot: u32,
    creation_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReceiptParams {
    receipt_hex: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RecheckParams {
    receipt_id: String,
}

impl Core {
    fn context(&self, chain: &MdbxChainContext) -> Value {
        let tip = chain.tip_header();
        let status = self.status.lock().unwrap();
        let health = self.health.borrow();
        let point = noid_p2p::object_protocol::ChainPoint::new(tip.height, chain.tip_hash());
        let domains: HashSet<_> = status
            .peers
            .values()
            .filter(|p| {
                p.selected
                    && p.tip.is_some_and(|(p, at)| {
                        p == point && at.elapsed() <= Duration::from_secs(60)
                    })
            })
            .map(|p| p.domain)
            .collect();
        let age = crate::runtime::now().saturating_sub(tip.timestamp);
        let ready = tip.height > 0
            && status.stage == "following"
            && domains.len() >= self.min_peers
            && health.dispatchable_peers >= self.min_peers
            && age <= self.max_tip_age
            && health.updated_at.elapsed() <= Duration::from_secs(30);
        json!({
            "height": tip.height.to_string(), "block_hash": hex::encode(chain.tip_hash()),
            "state_root": hex::encode(tip.state_root), "ready": ready,
            "tip_age_seconds": age, "confirming_peer_groups": domains.len(),
            "required_peer_groups": self.min_peers, "connected_peers": health.connected_peers,
            "stage": status.stage, "last_sync_error": status.error,
            "finalized_height": chain.finalized_checkpoint().height.to_string(),
        })
    }

    pub async fn handle(self: &Arc<Self>, req: Request) -> Value {
        let id = if req.id.len() <= 128 {
            Some(req.id.clone())
        } else {
            None
        };
        if id.is_none() {
            return failure(
                Value::Null,
                "INVALID_REQUEST",
                "id exceeds 128 bytes",
                false,
            );
        }
        let result = match req.method.as_str() {
            "status" => self.status_response(req.params).await,
            "right.inspect" => self.right(req.params).await,
            "receipt.verify" => self.receipt(req.params).await,
            "receipt.recheck" => self.recheck(req.params).await,
            "shutdown" => serde_json::from_value::<Empty>(req.params)
                .map(|_| json!({"stopping": true}))
                .map_err(Into::into),
            _ => return failure(json!(id), "UNKNOWN_METHOD", "unknown method", false),
        };
        match result {
            Ok(value) => json!({"id": id, "ok": true, "result": value}),
            Err(error) => {
                let message = error.to_string();
                let (code, retryable) = if message.starts_with("NOT_READY:") {
                    ("NOT_READY", true)
                } else if message.starts_with("CHAIN_CHANGED:") {
                    ("CHAIN_CHANGED", true)
                } else if message.starts_with("HEADER_UNAVAILABLE:") {
                    ("HEADER_UNAVAILABLE", true)
                } else if message.starts_with("CACHE_MISS:") {
                    ("CACHE_MISS", true)
                } else {
                    ("REJECTED", false)
                };
                failure(json!(id), code, &message, retryable)
            }
        }
    }

    async fn status_response(&self, params: Value) -> Result<Value> {
        serde_json::from_value::<Empty>(params)?;
        let chain = self.chain.read().await;
        Ok(json!({
            "product": "Agent Core", "version": env!("CARGO_PKG_VERSION"), "protocol": 1,
            "network": if cfg!(feature="isolated-v2-fork-testnet") { "isolated-v2-fork-testnet" } else { "mainnet" },
            "context": self.context(&chain),
            "methods": ["status", "right.inspect", "receipt.verify", "receipt.recheck", "shutdown"],
            "signing": false,
        }))
    }

    async fn right(&self, params: Value) -> Result<Value> {
        let params: RightParams = serde_json::from_value(params)?;
        let expected_id = decimal(&params.creation_id)?;
        let opening = ObjectOpening::from_bytes(&decode_hex(
            &params.opening_hex,
            noid_tx::experimental_object::OPENING_BYTES,
        )?)?;
        let chain = self.chain.read().await;
        let context = self.context(&chain);
        ensure!(
            context["ready"] == true,
            "NOT_READY: fresh independently verified State is not available"
        );
        ensure!(
            noid_chain::consensus::params::v2_active(chain.tip_height().saturating_add(1)),
            "contracts are not active at the next block"
        );
        let slot = read_slot(&chain, params.slot)?;
        let mut owner = [0u8; 32];
        owner[..16].copy_from_slice(&slot.owner_hi.0.to_le_bytes());
        owner[16..].copy_from_slice(&slot.owner_lo.0.to_le_bytes());
        let empty = slot == noid_chain::fri_state::SlotValue::EMPTY;
        let matches = !empty && owner == opening.root().0 && slot.creation_id() == expected_id;
        let next_height = chain
            .tip_height()
            .checked_add(1)
            .context("height overflow")?;
        Ok(json!({
            "context": context, "matches": matches,
            "slot": params.slot, "creation_id": slot.creation_id().to_string(), "amount_micronoid": slot.amount().to_string(),
            "terms": if matches { Some(describe(&opening)?) } else { None },
            "next_call_height": next_height.to_string(),
            "active_authority": if matches { Some(if next_height < opening.deadline { opening.claim_authority } else { opening.recovery_authority }.to_bech32()) } else { None },
            "action_authorized": false,
        }))
    }

    async fn receipt(self: &Arc<Self>, params: Value) -> Result<Value> {
        let params: ReceiptParams = serde_json::from_value(params)?;
        let bytes = decode_hex(
            &params.receipt_hex,
            noid_block::contract_receipt::MAX_RECEIPT_BYTES,
        )?;
        if bytes.starts_with(b"O1OBJRC") {
            self.contract_receipt(bytes).await
        } else {
            self.payment_receipt(bytes).await
        }
    }

    async fn payment_receipt(&self, bytes: Vec<u8>) -> Result<Value> {
        use bincode::Options;
        use noid_chain::consensus::receipt::{
            verify_against_header, verify_merkle_inclusion, ParanoidReceipt,
        };
        let limit = noid_chain::consensus::wire_limits::MAX_RPC_RECEIPT_BYTES;
        ensure!(bytes.len() <= limit, "payment receipt too large");
        let receipt: ParanoidReceipt = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(limit as u64)
            .reject_trailing_bytes()
            .deserialize(&bytes)?;
        ensure!(
            verify_merkle_inclusion(&receipt),
            "payment receipt inclusion failed"
        );
        let chain = self.chain.read().await;
        let header = chain
            .get_header_from_store(receipt.claimed_height)?
            .context("HEADER_UNAVAILABLE: receipt height not on the verified local chain")?;
        ensure!(
            verify_against_header(&receipt, &header),
            "payment receipt is not on the selected chain"
        );
        let summary = &receipt.summary;
        Ok(json!({
            "kind": "payment", "valid": true, "canonical": true, "context": self.context(&chain),
            "height": header.height.to_string(), "block_hash": hex::encode(noid_chain::hash_block_header(&header)),
            "finalized": header.height <= chain.finalized_checkpoint().height,
            "confirmations": chain.tip_height().saturating_sub(header.height).saturating_add(1).to_string(),
            "txid": hex::encode(summary.logical_txid), "fee_micronoid": summary.fee_micronoid.to_string(),
            "inputs": summary.inputs.iter().map(|(slot, owner)| json!({"slot": slot, "owner": owner.to_bech32()})).collect::<Vec<_>>(),
            "outputs": summary.outputs.iter().map(|(slot, amount, owner)| json!({"slot": slot, "amount_micronoid": amount.to_string(), "owner": owner.to_bech32()})).collect::<Vec<_>>(),
            "current_right_checked": false, "external_action_proven": false,
        }))
    }

    async fn contract_receipt(self: &Arc<Self>, bytes: Vec<u8>) -> Result<Value> {
        let receipt = noid_block::contract_receipt::ObjectTransitionReceipt::from_bytes(&bytes)
            .map_err(anyhow::Error::msg)?;
        let header = self
            .chain
            .read()
            .await
            .store
            .get_header(receipt.header.height)?
            .context("HEADER_UNAVAILABLE: receipt height not on the verified local chain")?;
        ensure!(
            header == receipt.header,
            "receipt belongs to a different selected chain"
        );
        let cached = self.receipt_cache.lock().unwrap().get(&bytes, header);
        let receipt_id = hex::encode(blake3::hash(&bytes).as_bytes());
        if let Some(call) = cached {
            let chain = self.chain.read().await;
            ensure!(
                chain.store.get_header(header.height)? == Some(header),
                "CHAIN_CHANGED: selected chain changed during verification"
            );
            return Ok(self.contract_result(&chain, header, call, receipt_id));
        }
        let permit = self.verifier.clone().acquire_owned().await?;
        let runtime = self.runtime.clone();
        let call = tokio::task::spawn_blocking(move || -> Result<Value> {
            let _permit = permit;
            noid_miner::install_inbound_verifier_cpu(|| -> Result<Value> {
                let origin = runtime.requested_origin(&receipt.terminal).map_err(anyhow::Error::msg)?.context("not a v2 contract receipt")?;
                let origin = runtime.verified_origin(&origin).map_err(anyhow::Error::msg)?;
                let checked = receipt.verify_v2(runtime.v2().map_err(anyhow::Error::msg)?, &origin, &header).map_err(anyhow::Error::msg)?;
                let input = &receipt.page.body.inputs[0];
                let payout = &receipt.page.body.outputs[usize::from(!checked.terminal)];
                let successor = if checked.terminal { None } else { Some(describe(&receipt.opening.successor(checked.next))?) };
                Ok(json!({
                    "txid": hex::encode(noid_tx::hash_paged_spend(std::slice::from_ref(&receipt.page))?.0),
                    "closed": checked.terminal, "authority": checked.authority.to_bech32(),
                    "input": {"slot": input.slot_index, "creation_id": input.creation_id.to_string(), "amount_micronoid": input.amount.to_string()},
                    "original": describe(&receipt.opening)?, "successor": successor,
                    "successor_slot": if checked.terminal { None } else { Some(receipt.page.body.outputs[0].slot_index) },
                    "retained_micronoid": if checked.terminal { 0 } else { receipt.page.body.outputs[0].amount }.to_string(),
                    "fee_micronoid": receipt.page.body.fee.to_string(),
                    "payout": {"address": payout.owner.to_bech32(), "amount_micronoid": payout.amount.to_string()},
                }))
            }).map_err(|e| anyhow!(e.to_string()))?
        }).await??;
        let chain = self.chain.read().await;
        ensure!(
            chain.store.get_header(header.height)? == Some(header),
            "CHAIN_CHANGED: selected chain changed during verification"
        );
        self.receipt_cache
            .lock()
            .unwrap()
            .insert(bytes, header, call.clone());
        Ok(self.contract_result(&chain, header, call, receipt_id))
    }

    async fn recheck(&self, params: Value) -> Result<Value> {
        let params: RecheckParams = serde_json::from_value(params)?;
        let id = decode_hex(&params.receipt_id, 32)?;
        ensure!(id.len() == 32, "receipt_id must be 32 bytes");
        let (header, call) = self
            .receipt_cache
            .lock()
            .unwrap()
            .recheck(&id)
            .context("CACHE_MISS: resubmit the complete receipt for verification")?;
        let chain = self.chain.read().await;
        ensure!(
            chain.store.get_header(header.height)? == Some(header),
            "receipt belongs to a different selected chain"
        );
        Ok(self.contract_result(&chain, header, call, hex::encode(id)))
    }

    fn contract_result(
        &self,
        chain: &MdbxChainContext,
        header: noid_chain::BlockHeader,
        call: Value,
        receipt_id: String,
    ) -> Value {
        json!({
            "kind": "contract", "valid": true, "canonical": true, "context": self.context(&chain),
            "height": header.height.to_string(), "block_hash": hex::encode(noid_chain::hash_block_header(&header)),
            "finalized": header.height <= chain.finalized_checkpoint().height,
            "confirmations": chain.tip_height().saturating_sub(header.height).saturating_add(1).to_string(),
            "call": call, "current_right_checked": false, "external_action_proven": false,
            "receipt_id": receipt_id,
        })
    }
}

pub fn describe(opening: &ObjectOpening) -> Result<Value> {
    opening.validate()?;
    let modes = opening.rules.modes;
    Ok(json!({
        "address": opening.root().to_bech32(), "opening_hex": hex::encode(opening.to_bytes()?),
        "code_id": hex::encode(opening.code_id()),
        "counters": integer_program::unpack_state(opening.state).map(|n| n.to_string()),
        "claim_authority": opening.claim_authority.to_bech32(), "recovery_authority": opening.recovery_authority.to_bech32(),
        "claim_recipient": opening.claim_recipient.to_bech32(), "recovery_recipient": opening.recovery_recipient.to_bech32(),
        "deadline_height": opening.deadline.to_string(), "max_fee_micronoid": opening.rules.max_fee.to_string(),
        "max_payout_micronoid": opening.rules.max_payout.to_string(), "min_retained_micronoid": opening.rules.min_retained.to_string(),
        "claim_can_continue": modes & policy::CLAIM_CONTINUE != 0, "claim_can_close": modes & policy::CLAIM_CLOSE != 0,
        "recovery_can_continue": modes & policy::RECOVERY_CONTINUE != 0, "recovery_can_close": modes & policy::RECOVERY_CLOSE != 0,
        "unrestricted_payout_recipient": modes & policy::ANY_PAYOUT_RECIPIENT != 0,
    }))
}

fn read_slot(chain: &MdbxChainContext, index: u32) -> Result<noid_chain::fri_state::SlotValue> {
    let state = &chain.state.state;
    ensure!(u64::from(index) < state.num_slots(), "slot out of range");
    let log = state.effective_log_segment_size();
    let id = (index >> log) as u16;
    if !state.is_evicted(id) {
        return Ok(state.slot(index));
    }
    let (stored_log, columns) = chain
        .store
        .get_segment(id)?
        .context("evicted State segment missing")?;
    ensure!(
        usize::from(stored_log) == log,
        "State segment depth mismatch"
    );
    let offset = (index & ((1u32 << log) - 1)) as usize;
    Ok(noid_chain::fri_state::SlotValue {
        value: *columns.values.get(offset).context("State value missing")?,
        owner_hi: *columns
            .owners_hi
            .get(offset)
            .context("State owner missing")?,
        owner_lo: *columns
            .owners_lo
            .get(offset)
            .context("State owner missing")?,
    })
}

fn decimal(s: &str) -> Result<u64> {
    ensure!(
        !s.is_empty() && s.len() <= 20 && s.bytes().all(|b| b.is_ascii_digit()),
        "expected unsigned decimal string"
    );
    ensure!(
        s == "0" || !s.starts_with('0'),
        "noncanonical decimal string"
    );
    Ok(s.parse()?)
}
fn decode_hex(s: &str, max: usize) -> Result<Vec<u8>> {
    ensure!(
        !s.is_empty() && s.len() % 2 == 0 && s.len() <= max * 2,
        "hex input length out of bounds"
    );
    hex::decode(s).map_err(Into::into)
}
pub fn failure(id: Value, code: &str, message: &str, retryable: bool) -> Value {
    json!({"id": id, "ok": false, "error": {"code": code, "message": message, "retryable": retryable}})
}

pub async fn serve(core: Arc<Core>) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    let mut input = tokio::io::BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    loop {
        let mut line = Vec::new();
        let size = (&mut input)
            .take((MAX_LINE + 1) as u64)
            .read_until(b'\n', &mut line)
            .await?;
        if size == 0 {
            return Ok(());
        }
        let oversized = size > MAX_LINE;
        let (response, stop) = if oversized {
            (
                failure(
                    Value::Null,
                    "REQUEST_TOO_LARGE",
                    "request exceeds frame limit",
                    false,
                ),
                true,
            )
        } else {
            match serde_json::from_slice::<Request>(&line) {
                Ok(request) => {
                    let stopping = request.method == "shutdown";
                    let response = core.handle(request).await;
                    let stop = stopping && response["ok"] == true;
                    (response, stop)
                }
                Err(_) => (
                    failure(
                        Value::Null,
                        "INVALID_REQUEST",
                        "expected a JSON request with id, method and params",
                        false,
                    ),
                    false,
                ),
            }
        };
        output
            .write_all(serde_json::to_string(&response)?.as_bytes())
            .await?;
        output.write_all(b"\n").await?;
        output.flush().await?;
        if stop {
            return Ok(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reject_lossy_or_ambiguous_identifiers() {
        assert_eq!(decimal("18446744073709551615").unwrap(), u64::MAX);
        for input in ["", "01", "-1", "+1", "1.0", "1e3", "18446744073709551616"] {
            assert!(decimal(input).is_err());
        }
        assert!(serde_json::from_value::<RightParams>(
            json!({"opening_hex":"00", "slot":1, "creation_id":9007199254740993u64})
        )
        .is_err());
    }
    #[test]
    fn reject_extra_authority_and_oversized_inputs() {
        assert!(serde_json::from_value::<Request>(
            json!({"id":"1", "method":"status", "override":true})
        )
        .is_err());
        assert!(serde_json::from_value::<RightParams>(
            json!({"opening_hex":"00", "slot":1, "creation_id":"1", "trust":true})
        )
        .is_err());
        assert!(decode_hex("0000", 1).is_err());
        assert!(decode_hex("0", 1).is_err());
    }
}
