#!/usr/bin/env python3
"""Real native P2P verification in a network namespace, using a copied test fixture."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from agcore import AgentCore, Rejected


def rpc(port, method, params=()):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}",
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": "paranoid_" + method,
                    "params": list(params)}).encode(),
        {"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=60) as response:
        value = json.load(response)
    if value.get("error") is not None:
        raise RuntimeError(value["error"])
    return value["result"]


class Reference:
    def __init__(self, binary, directory, p2p, port):
        self.binary, self.directory, self.p2p, self.port = binary, directory, p2p, port
        self.process = None
        self.log = None

    async def start(self, peer=None, *, mining_key=None):
        self.log = (self.directory / "node.log").open("ab", buffering=0)
        args = [str(self.binary), "--mode", "extminer" if mining_key else "node", "--config", str(self.directory / "config.toml"),
                "--data-dir", str(self.directory / "data"), "--disable-dns-seeds",
                "--p2p-listen", f"127.0.0.1:{self.p2p}", "--rpc-listen", f"127.0.0.1:{self.port}",
                "--log", "info"]
        if peer:
            args += ["--seed", f"127.0.0.1:{peer}"]
        if mining_key:
            args += ["--genesis", "--mining-key-file", str(mining_key)]
        self.process = subprocess.Popen(args, stdout=self.log, stderr=subprocess.STDOUT)
        async with asyncio.timeout(300):
            while True:
                if self.process.poll() is not None:
                    raise RuntimeError(f"reference exited; see {self.directory / 'node.log'}")
                try:
                    return await asyncio.to_thread(rpc, self.port, "getChainInfo")
                except (OSError, RuntimeError):
                    await asyncio.sleep(0.5)

    async def stop(self):
        if self.process is not None and self.process.poll() is None:
            try:
                await asyncio.to_thread(rpc, self.port, "stop")
            except (OSError, RuntimeError):
                self.process.terminate()
            try:
                await asyncio.to_thread(self.process.wait, 75)
            except subprocess.TimeoutExpired:
                self.process.kill()
                await asyncio.to_thread(self.process.wait)
        if self.log:
            self.log.close()


def resources(pid, directory):
    fields = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "VmHWM", "Threads"):
            fields[key] = int(value.split()[0])
    files = [p for p in directory.rglob("*") if p.is_file()]
    return {"rss_mib": fields["VmRSS"] / 1024, "peak_rss_mib": fields["VmHWM"] / 1024,
            "threads": fields["Threads"], "data_bytes": sum(p.stat().st_size for p in files),
            "allocated_data_bytes": sum(p.stat().st_blocks * 512 for p in files)}


def stats(samples):
    ordered = sorted(samples)
    return {"n": len(samples), "median_ms": statistics.median(samples),
            "p95_ms": ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))],
            "max_ms": max(samples), "min_ms": min(samples)}


async def timed(operation):
    begin = time.perf_counter()
    result = await operation()
    return result, (time.perf_counter() - begin) * 1000


async def repeated(operation, count):
    values = []
    for _ in range(count):
        _, elapsed = await timed(operation)
        values.append(elapsed)
    return stats(values)


async def rejects(operation, code=None):
    try:
        await operation()
    except Rejected as error:
        if code:
            assert error.code == code, (error.code, code)
        return {"code": error.code, "message": str(error)}
    raise AssertionError("invalid request was accepted")


async def ready(core, target, timeout=300):
    previous, stages = None, []
    begin = time.perf_counter()
    async with asyncio.timeout(timeout):
        while True:
            status = await core.status()
            ctx = status["context"]
            state = (ctx["height"], ctx["stage"], ctx["confirming_peer_groups"], ctx["last_sync_error"])
            if previous != state:
                print("[sync]", state, flush=True)
                stages.append({"seconds": time.perf_counter() - begin, "context": ctx})
                previous = state
            if ctx["ready"] and ctx["block_hash"] == target:
                return status, stages
            await asyncio.sleep(0.1)


async def main(args):
    interfaces = {line.split(":")[0].strip() for line in Path('/proc/net/dev').read_text().splitlines() if ":" in line}
    assert interfaces == {"lo"}, "run in a fresh network namespace"
    subprocess.run(["ip", "link", "set", "lo", "up"], check=True)
    args.output.mkdir(mode=0o700)
    fixture = json.loads((args.evidence / "report.json").read_text())
    refs = []
    core = None
    report = {"status": "running", "profile": "isolated-v2-fork-testnet", "network": "loopback only",
              "host": platform.platform(), "cpu": next(line.split(":", 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')),
              "proof_threads": 2, "os_page_cache_flushed": False,
              "max_tip_age_seconds": 86400, "min_peer_groups": 2,
              "fixture": {"source_head": fixture["source_head"], "final_tip": fixture["final_tip"]},
              "binary_sha256": hashlib.sha256(args.executable.read_bytes()).hexdigest(),
              "binary_bytes": args.executable.stat().st_size, "checks": {}}
    try:
        for i, source in enumerate(("issuer", "agent-a")):
            directory = args.output / f"reference-{i}"
            directory.mkdir(mode=0o700)
            shutil.copytree(args.fixture / source / "data", directory / "data")
            refs.append(Reference(args.reference, directory, 27700 + 10 * i, 27701 + 10 * i))
        tips = await asyncio.gather(refs[0].start(refs[1].p2p), refs[1].start(refs[0].p2p))
        assert tips[0]["height"] == tips[1]["height"] == 64, tips
        print("[references]", tips[0], flush=True)
        for permit in fixture["permits"]:
            assert await asyncio.to_thread(rpc, refs[0].port, "getBlock", [permit["height"]]) is None
            details = await asyncio.to_thread(rpc, refs[0].port, "getBlockDetails", [permit["height"]])
            assert details["retained"] is None
        report["checks"]["source_bodies_already_pruned"] = True
        target = tips[0]["best_hash"]
        peers = [f"/ip4/127.0.0.1/tcp/{ref.p2p}" for ref in refs]
        data = args.output / "core"
        with (args.output / "core.log").open("ab", buffering=0) as log:
            core = AgentCore(args.executable, data, peers=peers, max_tip_age=86400, stderr=log)
            begin = time.perf_counter()
            await core.__aenter__()
            first = await core.status()
            report["first_response_ms"] = (time.perf_counter() - begin) * 1000
            assert not first["context"]["ready"]
            status, report["cold_sync_stages"] = await ready(core, target)
            report["cold_ready_seconds"] = time.perf_counter() - begin
            report["verified_context"] = status["context"]
            report["after_sync"] = resources(core.process.pid, data)
            print("[cold ready seconds]", report["cold_ready_seconds"], flush=True)
            report["status"] = "checking"
            report["status_latency"] = await repeated(core.status, 100)
            checks = report["checks"]
            receipts = [(args.evidence / "receipts" / (p["txid"] + ".receipt")).read_bytes() for p in fixture["permits"]]
            times = []
            for raw, permit in zip(receipts, fixture["permits"]):
                checked, elapsed = await timed(lambda: core.verify_receipt(raw))
                assert checked["valid"] and checked["canonical"] and checked["finalized"]
                assert checked["call"]["txid"] == permit["txid"]
                assert checked["call"]["payout"] == {"address": permit["payout"]["address"], "amount_micronoid": str(permit["payout"]["amount_micronoid"])}
                times.append(elapsed)
                print("[receipt ms]", round(elapsed, 2), flush=True)
            report["contract_receipt_first_verification"] = stats(times)
            report["contract_receipt_cached"] = await repeated(lambda: core.verify_receipt(receipts[-1]), 30)
            receipt_id = checked["receipt_id"]
            report["contract_receipt_recheck"] = await repeated(lambda: core.recheck_receipt(receipt_id), 100)
            checks["unknown_receipt_id"] = await rejects(lambda: core.recheck_receipt("ff" * 32), "CACHE_MISS")
            checks["ten_pruned_contract_receipts"] = True
            bad = bytearray(receipts[-1]); bad[-20] ^= 1
            checks["modified_proof_after_cache"] = await rejects(lambda: core.verify_receipt(bytes(bad)))
            bad = bytearray(receipts[-1]); bad[40] ^= 1
            checks["modified_terms_after_cache"] = await rejects(lambda: core.verify_receipt(bytes(bad)))
            foreign = await core.verify_receipt((args.evidence / "receipts" / "foreign-instance.receipt"))
            opening = foreign["call"]["successor"]["opening_hex"]
            instances = await asyncio.to_thread(rpc, refs[0].port, "getObjectInstances", [opening, 0, 64])
            slot = instances["slots"][0]
            inspect = lambda: core.inspect_right(opening, slot["slot_index"], slot["creation_id"])
            right = await inspect()
            assert right["matches"] and right["terms"]["opening_hex"] == opening, right
            assert not right["action_authorized"]
            report["right_inspect"] = await repeated(inspect, 100)
            wrong = await core.inspect_right(opening, slot["slot_index"], slot["creation_id"] + 1)
            assert wrong["matches"] is False and wrong["terms"] is None and wrong["active_authority"] is None
            checks["wrong_incarnation_rejected"] = True
            enrolled = fixture["enrollment"]
            consumed = await core.inspect_right(enrolled["opening"]["opening_hex"], enrolled["slot"]["slot_index"], enrolled["slot"]["creation_id"])
            assert consumed["matches"] is False
            checks["consumed_instance_rejected"] = True
            encoded = await asyncio.to_thread(rpc, refs[0].port, "walletExportReceipt", [enrolled["funding_txid"]])
            payment = bytes.fromhex(encoded)
            checked = await core.verify_receipt(payment)
            assert checked["valid"] and checked["kind"] == "payment" and checked["txid"] == enrolled["funding_txid"]
            report["payment_receipt"] = await repeated(lambda: core.verify_receipt(payment), 100)
            checks["payment_receipt_trailing_bytes"] = await rejects(lambda: core.verify_receipt(payment + b"x"))
            checks["unknown_parameter"] = await rejects(lambda: core.request("status", trusted=True))
            report["after_receipts"] = resources(core.process.pid, data)
            await core.close()
            report["graceful_exit"] = core.process.returncode
            assert core.process.returncode == 0
            core = AgentCore(args.executable, data, peers=peers, max_tip_age=86400, stderr=log)
            begin = time.perf_counter()
            await core.__aenter__()
            await core.status()
            report["restart_first_response_ms"] = (time.perf_counter() - begin) * 1000
            _, report["restart_stages"] = await ready(core, target)
            report["restart_ready_seconds"] = time.perf_counter() - begin
            checks["receipt_cache_not_trusted_after_restart"] = await rejects(lambda: core.recheck_receipt(receipt_id), "CACHE_MISS")
            _, report["receipt_after_restart_ms"] = await timed(lambda: core.verify_receipt(receipts[-1]))
            assert (await inspect())["matches"]
            await asyncio.gather(*(ref.stop() for ref in refs))
            async with asyncio.timeout(90):
                while (await core.status())["context"]["ready"]:
                    await asyncio.sleep(0.2)
            checks["offline_right_fails_closed"] = await rejects(inspect, "NOT_READY")
            historical = await core.verify_receipt(receipts[-1])
            assert historical["valid"] and historical["context"]["ready"] is False
            checks["offline_historical_receipt_context"] = True
            report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = repr(error)
        raise
    finally:
        if core is not None:
            await core.close()
        await asyncio.gather(*(ref.stop() for ref in refs))
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print("[report]", args.output / "report.json", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    for name in ("executable", "reference", "fixture", "evidence", "output"):
        setattr(args, name, getattr(args, name).resolve())
    asyncio.run(main(args))
