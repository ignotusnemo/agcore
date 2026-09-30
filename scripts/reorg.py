#!/usr/bin/env python3
"""Mine competing v2 branches and invalidate a previously verified contract receipt."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import time

from benchmark import AgentCore, Reference, ready, rejects, rpc


async def mine(args, ref, count, label):
    before = await asyncio.to_thread(rpc, ref.port, "getChainInfo")
    print(f"[mine] {label} H{before['height']+1} +{count}", flush=True)
    with (args.output / f"{label}.log").open("wb") as log:
        proc = await asyncio.create_subprocess_exec(str(args.miner), "--rpc", f"http://127.0.0.1:{ref.port}",
                    "--key-file", str(args.output / "mining.key"), "--threads", "2", "--rpc-timeout", "600",
                    "--blocks", str(count), stdout=log, stderr=subprocess.STDOUT)
        try:
            await asyncio.wait_for(proc.wait(), 1200)
        except BaseException:
            proc.kill(); await proc.wait(); raise
        assert proc.returncode == 0, f"mining failed; see {label}.log"
    tip = await asyncio.to_thread(rpc, ref.port, "getChainInfo")
    assert tip["height"] == before["height"] + count
    return tip


def isolated():
    assert {line.split(":")[0].strip() for line in Path('/proc/net/dev').read_text().splitlines() if ":" in line} == {"lo"}
    subprocess.run(["ip", "link", "set", "lo", "up"], check=True)


async def branch(args):
    isolated()
    ref = Reference(args.reference, args.output / "branch-b", 27810, 27811)
    try:
        await ref.start(mining_key=args.output / "mining.key")
        tip = await mine(args, ref, 2, "branch-b-miner")
        (args.output / "branch-b-tip.json").write_text(json.dumps(tip))
    finally:
        await ref.stop()


async def main(args):
    isolated()
    args.output.mkdir(mode=0o700)
    key = args.output / "mining.key"
    key.write_text(secrets.token_hex(32) + "\n"); key.chmod(0o600)
    for name, source in (("branch-a", "issuer"), ("branch-b", "agent-a")):
        directory = args.output / name
        directory.mkdir(mode=0o700)
        shutil.copytree(args.fixture / source / "data", directory / "data")
    a = Reference(args.reference, args.output / "branch-a", 27800, 27801)
    b = Reference(args.reference, args.output / "branch-b", 27810, 27811)
    core, branch_process = None, None
    report = {"status": "running", "binary_sha256": hashlib.sha256(args.executable.read_bytes()).hexdigest(),
              "min_peer_groups": 1, "network": "two isolated loopback namespaces", "checks": {}}
    try:
        tip = await a.start(mining_key=key)
        with (args.output / "core.log").open("ab", buffering=0) as log:
            core = AgentCore(args.executable, args.output / "core", peers=["/ip4/127.0.0.1/tcp/27800", "/ip4/127.0.0.1/tcp/27810"], min_peers=1, max_tip_age=86400, stderr=log)
            await core.__aenter__()
            await ready(core, tip["best_hash"])
            raw = (args.evidence / "receipts" / "foreign-instance.receipt").read_bytes()
            checked = await core.verify_receipt(raw)
            opening = checked["call"]["successor"]["opening_hex"]
            instances = await asyncio.to_thread(rpc, a.port, "getObjectInstances", [opening, 0, 64])
            slot = instances["slots"][0]
            assert (await core.inspect_right(opening, slot["slot_index"], slot["creation_id"]))["matches"]
            call = {"opening_hex": opening, "slot_index": slot["slot_index"], "creation_id": slot["creation_id"], "terminal": True, "payout": None, "fee_micronoid": 0}
            preview = await asyncio.to_thread(rpc, a.port, "previewObjectCall", [call])
            call.update(expected_recovery=preview["recovery"], expected_txid=preview["txid"], expected_call_height=preview["call_height"])
            result = await asyncio.to_thread(rpc, a.port, "walletCallObject", [call])
            tip_a = await mine(args, a, 1, "branch-a-miner")
            begin = time.perf_counter()
            await ready(core, tip_a["best_hash"])
            report["follow_after_miner_exit_seconds"] = time.perf_counter() - begin
            encoded = await asyncio.to_thread(rpc, a.port, "exportObjectReceipt", [opening, result["transaction"]["txid"]])
            receipt = bytes.fromhex(encoded)
            accepted = await core.verify_receipt(receipt)
            assert accepted["valid"] and accepted["canonical"] and accepted["finalized"] is False and accepted["call"]["closed"]
            identifier = accepted["receipt_id"]
            assert (await core.recheck_receipt(identifier))["valid"]
            assert (await core.inspect_right(opening, slot["slot_index"], slot["creation_id"]))["matches"] is False
            report["checks"]["new_block_and_close"] = True
            report["branch_a"] = tip_a
            command = ["unshare", "--user", "--map-root-user", "--net", sys.executable, str(Path(__file__).resolve()), "--branch"]
            for name in ("reference", "miner", "fixture", "evidence", "executable", "output"):
                command += ["--" + name, str(getattr(args, name))]
            branch_process = await asyncio.create_subprocess_exec(*command)
            await asyncio.wait_for(branch_process.wait(), 1800)
            assert branch_process.returncode == 0
            tip_b = json.loads((args.output / "branch-b-tip.json").read_text())
            assert tip_b["height"] == 66
            report["branch_b"] = tip_b
            begin = time.perf_counter()
            await b.start(a.p2p)
            await ready(core, tip_b["best_hash"])
            report["reorg_seconds_including_peer_restart"] = time.perf_counter() - begin
            report["checks"]["orphaned_cached_receipt"] = await rejects(lambda: core.recheck_receipt(identifier), "REJECTED")
            report["checks"]["orphaned_full_receipt"] = await rejects(lambda: core.verify_receipt(receipt), "REJECTED")
            right = await core.inspect_right(opening, slot["slot_index"], slot["creation_id"])
            assert right["matches"] and int(right["context"]["height"]) == 66
            report["checks"]["right_restored_by_reorg"] = True
            assert (await core.verify_receipt(raw))["valid"]
            report["checks"]["common_prefix_receipt_still_valid"] = True
            report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"; report["error"] = repr(error)
        raise
    finally:
        if branch_process and branch_process.returncode is None:
            branch_process.terminate(); await branch_process.wait()
        if core:
            await core.close()
        await asyncio.gather(a.stop(), b.stop())
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print("[report]", args.output / "report.json", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("reference", "miner", "fixture", "evidence", "executable", "output"):
        parser.add_argument("--" + name, type=lambda p: Path(p).resolve(), required=True)
    parser.add_argument("--branch", action="store_true")
    args = parser.parse_args()
    asyncio.run(branch(args) if args.branch else main(args))
