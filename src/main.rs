// SPDX-License-Identifier: Apache-2.0
use anyhow::{anyhow, ensure, Result};
use clap::Parser;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{RwLock, Semaphore};

#[cfg(unix)]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser)]
#[command(
    version,
    about = "Agent Core: independent Parano1d verification over JSON lines"
)]
struct Args {
    /// Private State directory, separate from any other node.
    #[arg(long)]
    data_dir: PathBuf,
    /// P2P multiaddress. No HTTP or wallet server is started.
    #[arg(long, default_value = "/ip4/127.0.0.1/tcp/0")]
    listen: libp2p::Multiaddr,
    /// Bootstrap multiaddresses. Uses mainnet DNS seeds when omitted.
    #[arg(long)]
    peer: Vec<libp2p::Multiaddr>,
    /// Native verification worker threads, shared by proofs and headers.
    #[arg(long, default_value_t = 2)]
    threads: usize,
    /// Independent outbound network groups required for ready State.
    #[arg(long, default_value_t = 2)]
    min_peers: usize,
    #[arg(long, default_value_t = 300)]
    max_tip_age: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "agcore=info,noid_p2p=warn".into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    ensure!(
        args.threads > 0 && (1..=8).contains(&args.min_peers) && args.max_tip_age > 0,
        "invalid resource or freshness settings"
    );
    noid_core::cpu::ensure_production_hardware().map_err(|e| anyhow!(e.to_string()))?;
    noid_miner::configure_process_cpu_budget_with_threads(
        noid_miner::ProcessCpuBudgetMode::ProofOnly,
        Some(args.threads),
    )?;
    let _lock = lock_data(&args.data_dir)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(args));
    runtime.shutdown_timeout(Duration::from_secs(10));
    result
}

async fn run(args: Args) -> Result<()> {
    let chain =
        noid_chain::storage::MdbxChainContext::open_or_create(&args.data_dir.join("chain"))?;
    let runtime = agcore::runtime::load(&args.data_dir)?;
    runtime
        .attach_canonical_store(chain.store.clone())
        .map_err(anyhow::Error::msg)?;
    let bank = agcore::embedded::embedded_history_step_pack()
        .ok_or_else(|| anyhow!("authenticated proof pack is missing"))?
        .runtime_metadata_digest();
    let mut config = noid_mempool::MempoolConfig::default();
    if let Some(budgets) = runtime.v2_admission_budgets() {
        config = config.with_v2_block_budgets(budgets);
    }
    // The upstream transport requires this handle to serve empty mempool responses.
    // Agent Core never admits, verifies, signs or broadcasts pending transactions.
    let mempool =
        noid_mempool::AsyncMempool::new(noid_mempool::ChainView::from_mdbx(&chain), config);
    let chain = Arc::new(RwLock::new(chain));
    let network_config = noid_chain::consensus::NetworkConfig::mainnet();
    let mut peers = args.peer;
    if cfg!(feature = "isolated-v2-fork-testnet") {
        ensure!(
            !peers.is_empty() && loopback(&args.listen) && peers.iter().all(loopback),
            "isolated profile requires explicit loopback-only addresses"
        );
    } else if peers.is_empty() {
        peers = network_config
            .dns_seeds
            .iter()
            .map(|name| format!("/dns4/{name}/tcp/{}", network_config.default_p2p_port).parse())
            .collect::<Result<_, _>>()?;
    }
    let (p2p, mut network_task) = noid_p2p::P2PNetwork::start(
        args.listen,
        vec![],
        chain.clone(),
        mempool,
        noid_p2p::protocol::NetworkTopics::for_network_cfg(&network_config),
        bank,
        args.data_dir.clone(),
        noid_p2p::BackgroundCapacity::Full,
    )?;
    let status = Arc::new(Mutex::new(agcore::network::SyncStatus::default()));
    let core = Arc::new(agcore::api::Core {
        chain,
        runtime,
        verifier: Arc::new(Semaphore::new(1)),
        status: status.clone(),
        health: p2p.health_receiver(),
        min_peers: args.min_peers,
        max_tip_age: args.max_tip_age,
        receipt_cache: Mutex::new(agcore::api::ReceiptCache::default()),
    });
    let (network, dispatcher) =
        agcore::network::Network::new(p2p.cmd_tx.clone(), p2p.subscribe(), status);
    let mut dispatch_task = tokio::spawn(dispatcher.run());
    for addr in peers {
        p2p.cmd_tx
            .send(noid_p2p::NetworkCommand::Dial { addr })
            .await
            .map_err(|e| anyhow!(e.to_string()))?;
    }
    let mut sync_task = tokio::spawn(agcore::sync::run(core.clone(), network, args.data_dir));
    let result = tokio::select! {
        result = agcore::api::serve(core) => result,
        result = &mut sync_task => result.map_err(Into::into).and_then(|r|r),
        result = &mut dispatch_task => result.map_err(Into::into).and_then(|r|r),
        result = &mut network_task => result.map_err(Into::into).and_then(|r|r),
        _ = tokio::signal::ctrl_c() => Ok(()),
    };
    sync_task.abort();
    dispatch_task.abort();
    network_task.abort();
    result
}

fn loopback(addr: &libp2p::Multiaddr) -> bool {
    matches!(addr.iter().next(), Some(libp2p::multiaddr::Protocol::Ip4(ip)) if ip.is_loopback())
        || matches!(addr.iter().next(), Some(libp2p::multiaddr::Protocol::Ip6(ip)) if ip.is_loopback())
}

fn lock_data(path: &Path) -> Result<std::fs::File> {
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
        if !path.exists() {
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(path)?;
        }
        let meta = std::fs::symlink_metadata(path)?;
        ensure!(
            meta.is_dir()
                && !meta.file_type().is_symlink()
                && meta.mode() & 0o077 == 0
                && meta.uid() == unsafe { libc::geteuid() },
            "data directory must be owned by this user with mode 0700"
        );
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path.join("agcore.lock"))?;
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0,
            "data directory is already in use"
        );
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        anyhow::bail!("this preview currently requires Unix process and file isolation")
    }
}
