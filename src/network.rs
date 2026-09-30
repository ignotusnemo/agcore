// SPDX-License-Identifier: Apache-2.0
use anyhow::{anyhow, bail, ensure, Result};
use libp2p::PeerId;
use noid_chain::BlockHeader;
use noid_p2p::header_protocol::HeaderInventoryRecord;
use noid_p2p::object_protocol::{ChainPoint, ObjectId};
use noid_p2p::protocol::{GetStateSegmentResponse, VerifiedStateManifest};
use noid_p2p::{
    NetworkCommand, NetworkCommandSender, NetworkEvent, NetworkEventReceiver, RequestFailureKind,
};
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{oneshot, Notify};

#[derive(Debug, thiserror::Error)]
#[error("provider busy for {0} ms")]
struct Busy(u16);

pub(crate) fn is_backpressure(error: &anyhow::Error) -> bool {
    error.downcast_ref::<Busy>().is_some()
}

fn request_failure(operation: &str, kind: RequestFailureKind) -> anyhow::Error {
    if kind == RequestFailureKind::LocalCapacity {
        // The native header path also maps a remote Busy reply to LocalCapacity.
        // It does not preserve retry_after_ms, so use a bounded local delay.
        Busy(500).into()
    } else {
        anyhow!("{operation}: {kind:?}")
    }
}

fn snapshot_header_reply(headers: Vec<BlockHeader>, count: u16) -> Result<Vec<BlockHeader>> {
    // Header protocol v4 encodes Busy as an empty successful response. This
    // exact range belongs to an advertised snapshot, so retry it in place.
    if headers.is_empty() && count > 0 {
        return Err(Busy(500).into());
    }
    ensure!(
        headers.len() == usize::from(count),
        "incomplete snapshot header range"
    );
    Ok(headers)
}

async fn retry_busy<T, F, Fut>(mut operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        match operation().await {
            Err(error) if error.downcast_ref::<Busy>().is_some() => {
                let delay = Duration::from_millis(u64::from(
                    error.downcast_ref::<Busy>().unwrap().0.clamp(100, 10_000),
                ));
                if tokio::time::Instant::now() + delay >= deadline {
                    return Err(error);
                }
                tokio::time::sleep(delay).await;
            }
            result => return result,
        }
    }
}

#[derive(Clone)]
pub struct PeerObservation {
    pub selected: bool,
    pub domain: u64,
    pub tip: Option<(ChainPoint, Instant)>,
}

pub struct SyncStatus {
    pub stage: &'static str,
    pub error: Option<String>,
    pub peers: BTreeMap<PeerId, PeerObservation>,
}
impl Default for SyncStatus {
    fn default() -> Self {
        Self {
            stage: "connecting",
            error: None,
            peers: BTreeMap::new(),
        }
    }
}
pub type Status = Arc<Mutex<SyncStatus>>;

// Each response has one owner. Unsolicited and cancelled responses are dropped
// with their byte-budget permits; no queue of orphaned payloads accumulates.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Key {
    Inventory(PeerId),
    Headers(PeerId, u64, u64),
    Manifest(PeerId, u64),
    Terminal(PeerId, u64),
    Object(PeerId, u64),
    Segment(PeerId, u16, ChainPoint, [u8; 32]),
}
impl Key {
    fn peer(&self) -> PeerId {
        match self {
            Self::Inventory(p)
            | Self::Headers(p, ..)
            | Self::Manifest(p, ..)
            | Self::Terminal(p, ..)
            | Self::Object(p, ..)
            | Self::Segment(p, ..) => *p,
        }
    }
    fn command(c: &NetworkCommand) -> Self {
        match c {
            NetworkCommand::FetchHeaders { peer, .. } => Self::Inventory(*peer),
            NetworkCommand::FetchSnapshotHeaders {
                peer,
                generation,
                token,
                ..
            } => Self::Headers(*peer, *generation, *token),
            NetworkCommand::RequestStateManifest {
                peer, generation, ..
            } => Self::Manifest(*peer, *generation),
            NetworkCommand::RequestHistoryStepTerminal { peer, token, .. } => {
                Self::Terminal(*peer, *token)
            }
            NetworkCommand::FetchObjects { peer, token, .. } => Self::Object(*peer, *token),
            NetworkCommand::RequestStateSegment {
                peer,
                segment_id,
                expected_tip_height,
                expected_tip_hash,
                manifest_digest,
            } => Self::Segment(
                *peer,
                *segment_id,
                ChainPoint::new(*expected_tip_height, *expected_tip_hash),
                *manifest_digest,
            ),
            _ => unreachable!("request command"),
        }
    }
    fn event(e: &NetworkEvent) -> Option<Self> {
        Some(match e {
            NetworkEvent::HeaderInventoryBatch { from, .. }
            | NetworkEvent::HeadersRequestFailed { from, .. } => Self::Inventory(*from),
            NetworkEvent::SnapshotHeadersBatch {
                from,
                generation,
                token,
                ..
            }
            | NetworkEvent::SnapshotHeadersRequestFailed {
                from,
                generation,
                token,
                ..
            } => Self::Headers(*from, *generation, *token),
            NetworkEvent::StateManifest {
                from, generation, ..
            }
            | NetworkEvent::StateManifestRequestFailed {
                from, generation, ..
            } => Self::Manifest(*from, *generation),
            NetworkEvent::HistoryStepTerminal { from, token, .. }
            | NetworkEvent::HistoryStepTerminalRequestFailed { from, token, .. }
            | NetworkEvent::HistoryStepTerminalRequestBusy { from, token, .. } => {
                Self::Terminal(*from, *token)
            }
            NetworkEvent::ObjectsResponse { from, token, .. }
            | NetworkEvent::ObjectsRequestFailed { from, token, .. }
            | NetworkEvent::ObjectsRequestBusy { from, token, .. } => Self::Object(*from, *token),
            NetworkEvent::StateSegment { from, response } => Self::Segment(
                *from,
                response.segment_id,
                ChainPoint::new(response.expected_tip_height, response.expected_tip_hash),
                response.manifest_digest,
            ),
            NetworkEvent::StateSegmentRequestFailed {
                from,
                segment_id,
                expected_tip_height,
                expected_tip_hash,
                manifest_digest,
                ..
            }
            | NetworkEvent::StateSegmentRequestBusy {
                from,
                segment_id,
                expected_tip_height,
                expected_tip_hash,
                manifest_digest,
                ..
            } => Self::Segment(
                *from,
                *segment_id,
                ChainPoint::new(*expected_tip_height, *expected_tip_hash),
                *manifest_digest,
            ),
            _ => return None,
        })
    }
}
type Pending = Arc<Mutex<HashMap<Key, (u64, oneshot::Sender<Result<NetworkEvent>>)>>>;
struct Lease {
    pending: Pending,
    key: Key,
    ticket: u64,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut pending = self.pending.lock().unwrap();
        if pending
            .get(&self.key)
            .is_some_and(|(ticket, _)| *ticket == self.ticket)
        {
            pending.remove(&self.key);
        }
    }
}

#[derive(Clone)]
pub struct Network {
    pub commands: NetworkCommandSender,
    pub status: Status,
    pending: Pending,
    wake: Arc<Notify>,
    token: Arc<AtomicU64>,
    generation: Arc<AtomicU64>,
}
pub struct Dispatcher {
    network: Network,
    events: NetworkEventReceiver,
}
impl Dispatcher {
    pub async fn run(mut self) -> Result<()> {
        loop {
            let event = match self.events.recv().await {
                Ok(event) => event,
                Err(noid_p2p::network::NetworkEventRecvError::Lagged(_)) => {
                    for peer in self.network.status.lock().unwrap().peers.values_mut() {
                        peer.tip = None;
                    }
                    self.network.pending.lock().unwrap().clear();
                    self.network.wake.notify_one();
                    continue;
                }
                Err(e) => bail!("P2P event receiver: {e:?}"),
            };
            match &event {
                NetworkEvent::PeerConnected {
                    peer,
                    locally_selected,
                    failure_domain,
                } => {
                    self.network.status.lock().unwrap().peers.insert(
                        *peer,
                        PeerObservation {
                            selected: *locally_selected,
                            domain: *failure_domain,
                            tip: None,
                        },
                    );
                    self.network.wake.notify_one();
                }
                NetworkEvent::PeerDisconnected(peer) => {
                    self.network.status.lock().unwrap().peers.remove(peer);
                    self.network
                        .pending
                        .lock()
                        .unwrap()
                        .retain(|key, _| key.peer() != *peer);
                    self.network.wake.notify_one();
                }
                NetworkEvent::NewTx { delivery, .. } => {
                    delivery.complete(libp2p::gossipsub::MessageAcceptance::Ignore);
                }
                NetworkEvent::HeaderAnnouncement { .. } => self.network.wake.notify_one(),
                _ => {}
            }
            if let Some(key) = Key::event(&event) {
                let sender = self.network.pending.lock().unwrap().remove(&key);
                if let Some((_, sender)) = sender {
                    let _ = sender.send(Ok(event));
                }
            }
        }
    }
}
impl Network {
    pub fn new(
        commands: NetworkCommandSender,
        events: NetworkEventReceiver,
        status: Status,
    ) -> (Self, Dispatcher) {
        let network = Self {
            commands,
            status,
            pending: Arc::default(),
            wake: Arc::default(),
            token: Arc::new(AtomicU64::new(0)),
            generation: Arc::new(AtomicU64::new(0)),
        };
        let dispatcher = Dispatcher {
            network: network.clone(),
            events,
        };
        (network, dispatcher)
    }
    fn token(&self) -> u64 {
        self.token.fetch_add(1, Ordering::Relaxed) + 1
    }
    pub async fn changed(&self, maximum: Duration) {
        let _ = tokio::time::timeout(maximum, self.wake.notified()).await;
    }
    async fn request<T>(
        &self,
        command: NetworkCommand,
        mut accept: impl FnMut(NetworkEvent) -> Option<Result<T>>,
    ) -> Result<T> {
        let key = Key::command(&command);
        let (send, receive) = oneshot::channel();
        let ticket = self.token();
        {
            let mut pending = self.pending.lock().unwrap();
            ensure!(pending.len() < 48, "request window exhausted");
            ensure!(!pending.contains_key(&key), "duplicate in-flight request");
            pending.insert(key.clone(), (ticket, send));
        }
        let _lease = Lease {
            pending: self.pending.clone(),
            key: key.clone(),
            ticket,
        };
        let timeout = match key {
            Key::Object(..) | Key::Terminal(..) | Key::Segment(..) => 70,
            _ => 36,
        };
        let event = tokio::time::timeout(Duration::from_secs(timeout), async {
            self.commands
                .send(command)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
            receive
                .await
                .map_err(|_| anyhow!("P2P request cancelled or peer disconnected"))?
        })
        .await
        .map_err(|_| anyhow!("P2P request timed out"))??;
        accept(event).ok_or_else(|| anyhow!("P2P response does not match its request"))?
    }
    pub async fn inventory(
        &self,
        peer: PeerId,
        start: u64,
        count: u16,
    ) -> Result<(Vec<HeaderInventoryRecord>, Option<ChainPoint>)> {
        retry_busy(|| self.inventory_once(peer, start, count)).await
    }
    async fn inventory_once(
        &self,
        peer: PeerId,
        start: u64,
        count: u16,
    ) -> Result<(Vec<HeaderInventoryRecord>, Option<ChainPoint>)> {
        self.request(
            NetworkCommand::FetchHeaders {
                peer,
                start_height: start,
                count,
            },
            move |event| match event {
                NetworkEvent::HeaderInventoryBatch {
                    from,
                    records,
                    snapshot_boundary,
                } if from == peer && records.first().is_none_or(|r| r.header.height == start) => {
                    Some(Ok((records, snapshot_boundary)))
                }
                NetworkEvent::HeadersRequestFailed {
                    from,
                    start_height,
                    count: received_count,
                    kind,
                } if from == peer && start_height == start && received_count == count => {
                    Some(Err(request_failure("headers", kind)))
                }
                _ => None,
            },
        )
        .await
    }
    pub async fn headers(&self, peer: PeerId, start: u64, count: u16) -> Result<Vec<BlockHeader>> {
        retry_busy(|| self.headers_once(peer, start, count)).await
    }
    async fn headers_once(&self, peer: PeerId, start: u64, count: u16) -> Result<Vec<BlockHeader>> {
        let token = self.token();
        let generation = self.generation.load(Ordering::Relaxed);
        self.request(
            NetworkCommand::FetchSnapshotHeaders {
                generation,
                token,
                peer,
                start_height: start,
                count,
            },
            move |event| match event {
                NetworkEvent::SnapshotHeadersBatch {
                    from,
                    generation: g,
                    token: t,
                    start_height,
                    requested_count,
                    headers,
                    ..
                } if from == peer
                    && g == generation
                    && t == token
                    && start_height == start
                    && requested_count == count =>
                {
                    Some(snapshot_header_reply(headers, count))
                }
                NetworkEvent::SnapshotHeadersRequestFailed {
                    from,
                    generation: g,
                    token: t,
                    kind,
                    ..
                } if from == peer && g == generation && t == token => {
                    Some(Err(request_failure("snapshot headers", kind)))
                }
                _ => None,
            },
        )
        .await
    }
    pub async fn manifest(&self, peer: PeerId, height: u64) -> Result<VerifiedStateManifest> {
        let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
        self.commands
            .send(NetworkCommand::AdvanceSnapshotGeneration { generation })
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
        self.request(
            NetworkCommand::RequestStateManifest {
                generation,
                peer,
                requester_height: height,
                requested_manifest_digest: [0; 32],
            },
            move |event| match event {
                NetworkEvent::StateManifest {
                    from,
                    generation: g,
                    requester_height,
                    manifest,
                } if from == peer && g == generation && requester_height == height => {
                    Some(Ok(manifest))
                }
                NetworkEvent::StateManifestRequestFailed {
                    from,
                    generation: g,
                    kind,
                    ..
                } if from == peer && g == generation => Some(Err(anyhow!("manifest: {kind:?}"))),
                _ => None,
            },
        )
        .await
    }
    pub async fn terminal(
        &self,
        peer: PeerId,
        point: ChainPoint,
    ) -> Result<(Vec<u8>, Option<Arc<tokio::sync::OwnedSemaphorePermit>>)> {
        retry_busy(|| self.terminal_once(peer, point)).await
    }
    async fn terminal_once(
        &self,
        peer: PeerId,
        point: ChainPoint,
    ) -> Result<(Vec<u8>, Option<Arc<tokio::sync::OwnedSemaphorePermit>>)> {
        let token = self.token();
        self.request(
            NetworkCommand::RequestHistoryStepTerminal {
                token,
                peer,
                height: point.height,
                block_hash: point.hash,
            },
            move |event| match event {
                NetworkEvent::HistoryStepTerminal {
                    from,
                    token: t,
                    height,
                    block_hash,
                    terminal_bytes,
                    inbound_memory_permit,
                } if from == peer
                    && t == token
                    && height == point.height
                    && block_hash == point.hash =>
                {
                    Some(if terminal_bytes.is_empty() {
                        Err(anyhow!("terminal unavailable"))
                    } else {
                        Ok((terminal_bytes, inbound_memory_permit))
                    })
                }
                NetworkEvent::HistoryStepTerminalRequestFailed {
                    from,
                    token: t,
                    kind,
                    ..
                } if from == peer && t == token => Some(Err(anyhow!("terminal: {kind:?}"))),
                NetworkEvent::HistoryStepTerminalRequestBusy {
                    from,
                    token: t,
                    retry_after_ms,
                    ..
                } if from == peer && t == token => Some(Err(Busy(retry_after_ms).into())),
                _ => None,
            },
        )
        .await
    }
    pub async fn object(
        &self,
        peer: PeerId,
        object: ObjectId,
    ) -> Result<(Vec<u8>, Option<Arc<tokio::sync::OwnedSemaphorePermit>>)> {
        retry_busy(|| self.object_once(peer, object)).await
    }
    async fn object_once(
        &self,
        peer: PeerId,
        object: ObjectId,
    ) -> Result<(Vec<u8>, Option<Arc<tokio::sync::OwnedSemaphorePermit>>)> {
        let token = self.token();
        let (bytes, transport) = self
            .request(
                NetworkCommand::FetchObjects {
                    token,
                    peer,
                    objects: vec![object],
                },
                move |event| match event {
                    NetworkEvent::ObjectsResponse {
                        from,
                        token: t,
                        mut objects,
                        inbound_memory_permit,
                    } if from == peer && t == token => Some((|| {
                        if objects.len() != 1 {
                            bail!("wrong object count");
                        }
                        let response = objects.pop().unwrap();
                        if response.object != object {
                            bail!("wrong object identity");
                        }
                        let bytes = response
                            .bytes
                            .ok_or_else(|| anyhow!("object unavailable"))?;
                        Ok((bytes, inbound_memory_permit))
                    })()),
                    NetworkEvent::ObjectsRequestFailed {
                        from,
                        token: t,
                        kind,
                        ..
                    } if from == peer && t == token => Some(Err(anyhow!("object: {kind:?}"))),
                    NetworkEvent::ObjectsRequestBusy {
                        from,
                        token: t,
                        retry_after_ms,
                        ..
                    } if from == peer && t == token => Some(Err(Busy(retry_after_ms).into())),
                    _ => None,
                },
            )
            .await?;
        tokio::task::spawn_blocking(move || -> Result<_> {
            let matches = noid_miner::install_inbound_verifier_cpu(|| object.matches_bytes(&bytes))
                .map_err(|e| anyhow!(e.to_string()))?;
            ensure!(matches, "object bytes do not match identity");
            Ok((bytes, transport))
        })
        .await?
    }
    pub async fn segment(
        &self,
        peer: PeerId,
        id: u16,
        point: ChainPoint,
        digest: [u8; 32],
    ) -> Result<GetStateSegmentResponse> {
        retry_busy(|| self.segment_once(peer, id, point, digest)).await
    }
    async fn segment_once(
        &self,
        peer: PeerId,
        id: u16,
        point: ChainPoint,
        digest: [u8; 32],
    ) -> Result<GetStateSegmentResponse> {
        self.request(
            NetworkCommand::RequestStateSegment {
                peer,
                segment_id: id,
                expected_tip_height: point.height,
                expected_tip_hash: point.hash,
                manifest_digest: digest,
            },
            move |event| match event {
                NetworkEvent::StateSegment { from, response }
                    if from == peer
                        && response.segment_id == id
                        && response.expected_tip_height == point.height
                        && response.expected_tip_hash == point.hash
                        && response.manifest_digest == digest =>
                {
                    Some(if let Some(delay) = response.status.busy_retry_after_ms() {
                        Err(Busy(delay).into())
                    } else {
                        Ok(response)
                    })
                }
                NetworkEvent::StateSegmentRequestFailed {
                    from,
                    segment_id,
                    expected_tip_height,
                    expected_tip_hash,
                    manifest_digest,
                    kind,
                } if from == peer
                    && segment_id == id
                    && expected_tip_height == point.height
                    && expected_tip_hash == point.hash
                    && manifest_digest == digest =>
                {
                    Some(Err(anyhow!("segment: {kind:?}")))
                }
                NetworkEvent::StateSegmentRequestBusy {
                    from,
                    segment_id,
                    expected_tip_height,
                    expected_tip_hash,
                    manifest_digest,
                    retry_after_ms,
                    ..
                } if from == peer
                    && segment_id == id
                    && expected_tip_height == point.height
                    && expected_tip_hash == point.hash
                    && manifest_digest == digest =>
                {
                    Some(Err(Busy(retry_after_ms).into()))
                }
                _ => None,
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn legacy_busy_preserves_an_exact_header_range() {
        let header = noid_chain::consensus::genesis_header();
        let mut attempts = 0;
        let headers = retry_busy(|| {
            attempts += 1;
            std::future::ready(snapshot_header_reply(
                if attempts == 1 {
                    Vec::new()
                } else {
                    vec![header]
                },
                1,
            ))
        })
        .await
        .unwrap();
        assert_eq!(headers, vec![header]);
        assert_eq!(attempts, 2);
        let truncated = snapshot_header_reply(vec![header], 2).unwrap_err();
        assert!(!is_backpressure(&truncated));
        assert!(snapshot_header_reply(vec![header; 2], 1).is_err());
    }

    #[tokio::test]
    async fn busy_wait_preserves_the_request_and_other_errors_are_final() {
        let mut attempts = 0;
        let started = Instant::now();
        let value = retry_busy(|| {
            attempts += 1;
            std::future::ready(match attempts {
                1 => Err(Busy(150).into()),
                2 => Err(request_failure(
                    "headers",
                    RequestFailureKind::LocalCapacity,
                )),
                _ => Ok(7),
            })
        })
        .await
        .unwrap();
        assert_eq!((value, attempts), (7, 3));
        assert!(started.elapsed() >= Duration::from_millis(650));
        for kind in [
            RequestFailureKind::InvalidResponse,
            RequestFailureKind::Unavailable,
            RequestFailureKind::ConnectionClosed,
        ] {
            let mut attempts = 0;
            let error = retry_busy(|| {
                attempts += 1;
                std::future::ready(Err::<(), _>(request_failure("headers", kind)))
            })
            .await
            .unwrap_err();
            assert!(!is_backpressure(&error));
            assert_eq!(attempts, 1);
        }
    }
    #[test]
    fn stale_generations_and_other_sources_have_no_response_owner() {
        let peer = PeerId::random();
        let wanted = Key::Headers(peer, 2, 17);
        let reply = |from, generation, token| NetworkEvent::SnapshotHeadersBatch {
            from,
            generation,
            token,
            start_height: 1,
            requested_count: 2,
            headers: Vec::new(),
            snapshot_boundary: None,
        };
        assert_eq!(Key::event(&reply(peer, 2, 17)), Some(wanted.clone()));
        assert_ne!(Key::event(&reply(peer, 1, 17)), Some(wanted.clone()));
        assert_ne!(Key::event(&reply(peer, 2, 16)), Some(wanted.clone()));
        assert_ne!(Key::event(&reply(PeerId::random(), 2, 17)), Some(wanted));
    }
    #[test]
    fn cancelled_request_cannot_remove_a_replacement() {
        let pending: Pending = Arc::default();
        let key = Key::Inventory(PeerId::random());
        let (sender, mut receiver) = oneshot::channel();
        pending.lock().unwrap().insert(key.clone(), (2, sender));
        drop(Lease {
            pending: pending.clone(),
            key: key.clone(),
            ticket: 1,
        });
        assert_eq!(pending.lock().unwrap().len(), 1);
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        drop(Lease {
            pending: pending.clone(),
            key,
            ticket: 2,
        });
        assert!(pending.lock().unwrap().is_empty());
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }
    #[test]
    fn segments_are_bound_to_the_entire_snapshot_identity() {
        let p = PeerId::random();
        let command = |height, hash, digest, id| NetworkCommand::RequestStateSegment {
            peer: p,
            segment_id: id,
            expected_tip_height: height,
            expected_tip_hash: hash,
            manifest_digest: digest,
        };
        let wanted = Key::command(&command(50, [1; 32], [2; 32], 4));
        for other in [
            command(51, [1; 32], [2; 32], 4),
            command(50, [0; 32], [2; 32], 4),
            command(50, [1; 32], [0; 32], 4),
            command(50, [1; 32], [2; 32], 5),
        ] {
            assert_ne!(Key::command(&other), wanted);
        }
    }
}
