# Agent Core

Agent Core (`agcore`) gives an agent its own Parano1d verifier. It connects to native P2P peers, verifies recursive proofs, maintains authenticated live State and checks portable receipts. An agent supplies public contract terms and an exact State instance; Core checks them against its own selected chain.

[Canonical source](https://git.parano1d.org/ignotusnemo/agcore)

This first version is read-only. It provides verification of rights and receipts through private stdin/stdout pipes, with an asynchronous Python client. It does not start the full daemon, an HTTP server, a GUI, a wallet or a miner. It never holds a spending key or broadcasts a transaction.

## Interface

Each request and response occupies one JSON line. Diagnostics go to stderr. Request IDs are strings, unknown fields are rejected, and amounts, heights and instance identifiers are decimal strings to avoid loss of integer precision.

```json
{"id":"1","method":"status","params":{}}
```

| Method | Input | Result |
| --- | --- | --- |
| `status` | `{}` | Selected tip, State root, sync stage, peer confirmation and readiness |
| `right.inspect` | `opening_hex`, `slot`, `creation_id` | Whether those terms belong to that exact live instance, its balance, counters, authorities and policy limits |
| `receipt.verify` | `receipt_hex` | Native verification of a payment or contract receipt against the locally verified chain |
| `receipt.recheck` | `receipt_id` | Current canonicality and confirmation status of a contract receipt already verified in this process |
| `shutdown` | `{}` | Graceful shutdown |

`right.inspect` requires fresh State. It checks both the committed opening and the allocation counter; a matching contract address alone cannot identify an instance. When they do not match, `terms` and `active_authority` are null. Inspection does not authorize an action: the response explicitly returns `action_authorized: false`.

A receipt proves its recorded chain action. It does not prove that a service delivered work or that a historical right remains available now. Receipt responses expose these distinctions, the exact selected tip and whether the inclusion is finalized. Historical verification is available offline, with `context.ready: false`.

Core keeps at most eight verified contract receipts in memory. `receipt.recheck` avoids transmitting the same large receipt again, but checks its block against the current selected chain on every request. An unknown or evicted identifier returns `CACHE_MISS`; send the complete receipt again. This cache is discarded on restart.

## Python

Python 3.11 or newer. The client has no third-party runtime dependencies.

```sh
python3 -m pip install .
```

```python
import asyncio
from pathlib import Path
from agcore import AgentCore

async def main():
    async with AgentCore("./target/release/agcore", "./agent-state") as core:
        await core.wait_ready()
        receipt = await core.verify_receipt(Path("settlement.receipt"))
        print(receipt)

asyncio.run(main())
```

The SDK starts and owns the verifier process. It correlates every response with its request and discards the process after a timeout or malformed response, so a late reply cannot become the answer to a different request. Native rejections raise `agcore.Rejected`, with `code` and `retryable` fields.

Run the executable directly for another language:

```sh
agcore --data-dir ./agent-state --threads 2
```

The data directory must be private, owned by the current user and mode `0700`. A process lock prevents simultaneous use. The default listener is `/ip4/127.0.0.1/tcp/0`; outbound peers come from the native mainnet DNS seed set. Repeated `--peer /ip4/ADDRESS/tcp/PORT` arguments select explicit bootstrap peers.

## Verification and readiness

Consensus, proof verification, State storage and P2P come from the pinned Parano1d v2.0.1 source. The native release build authenticates the embedded proof packs and retirement keys. Core implements the smaller process lifecycle and request interface around those components.

Agent Core has its own synchronizer. A dedicated dispatcher correlates P2P replies while a bounded pipeline fetches up to six header batches and two State segments. Header checks share the configured CPU budget with proof verification. The snapshot proof and header chain are checked concurrently. State transfer starts at the same time, with a 32 MiB prefetch budget; those bytes enter verified staging only after the boundary proof succeeds. Recent block bodies use a separate four-request window. A busy peer delays only the requested object, preserving completed work. Fast peer responses are compared before verification to avoid checking several older tips in succession.

The selected snapshot has one exact height, block hash and manifest digest. Native validation checks PoW, targets, timestamp rules, expansion, cumulative work, the recursive terminal, fork origin and State root. State is installed in one transaction only when all checks agree. Temporary headers use an anonymous file and an integrity digest checked again during installation. Reorganizations use native State rollback and application.

Core retains the canonical header chain for PoW ordering and receipt inclusion. It does not replay old transaction bodies; their storage follows the native retention window. Live State is stored locally, and receipt files remain with their holders.

Current rights require agreement on the verified local tip from two selected outbound peer groups by default, recent peer observations and a tip no older than 300 seconds. `--min-peers` and `--max-tip-age` explicitly change that policy. Peer agreement is a connectivity check, not a substitute for proof verification or a guarantee against complete network isolation. The isolated tests use local peer identities as groups and a relaxed age limit for their saved chain.

The model receives verified facts. Any later signing or external action needs its own deterministic policy, explicit authority and handling of finality. Core deliberately exposes the selected tip so callers can bind decisions to the State they inspected.

## Build

The preview supports Linux x86-64 with the native Parano1d CPU requirements. The Rust implementation is shared with the protocol; Python is the integration interface.

```sh
git submodule update --init
python3 scripts/build.py --inputs /path/to/build-inputs.json
```

The public JSON file specifies `profile` (`mainnet` or `isolated-v2-fork-testnet`) and an `environment` object containing the seven native release inputs:

```json
{
  "profile": "mainnet",
  "environment": {
    "NOID_HISTORY_STEP_PACK_DIR": "/path/to/history-step-pack",
    "NOID_HISTORY_STEP_RUNTIME_METADATA_RELEASE_DIGEST": "published metadata digest",
    "NOID_V2_PACK_DIR": "/path/to/v2-pack",
    "NOID_V2_RELEASE_BANK": "published bank digest",
    "NOID_RETIREMENT_KEYS_DIR": "/path/to/retirement-keys",
    "NOID_RETIREMENT_KEY_0_PIN": "published key pin",
    "NOID_RETIREMENT_KEY_1_PIN": "published key pin"
  }
}
```

Use the matching public artifacts from the canonical Parano1d release. A release build without authenticated artifacts fails. The isolated profile activates forks at heights 5 and 10 and requires explicit loopback peers. Its executable is not a mainnet build. `--retired-history` omits the old matrix payload when building with the native authenticated retirement material.

Some proof runtime types are currently exported by the upstream `noid_miner` crate; importing them does not start mining. The transport also requires an empty mempool handle. Core does not admit pending transactions. These are source dependencies, not separate running services.

## Tests

```sh
cargo test --offline --lib --features isolated-v2-fork-testnet
python3 -m unittest discover -s tests -p 'test_*.py' -v
```

`scripts/benchmark.py` checks synchronization, current rights and portable receipts over actual P2P. `scripts/reorg.py` mines competing branches and checks that both a full receipt and its cached result are rejected after its block is orphaned. Both require a fresh Linux network namespace and copies of the isolated Live Agents fixture; neither contacts mainnet.

Licensed under Apache-2.0.
