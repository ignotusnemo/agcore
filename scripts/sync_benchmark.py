#!/usr/bin/env python3
"""Measure a fresh mainnet verifier through its native P2P connection."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import platform
import time

from benchmark import AgentCore, repeated, resources


async def run(args):
    args.output.mkdir(mode=0o700)
    data = args.data_dir or args.output / "core"
    report = {"status": "running", "profile": "mainnet", "host": platform.platform(),
              "proof_threads": 2, "os_page_cache_flushed": False,
              "existing_data": args.data_dir is not None,
              "binary_sha256": hashlib.sha256(args.executable.read_bytes()).hexdigest(),
              "binary_bytes": args.executable.stat().st_size, "stages": []}
    try:
        with (args.output / "core.log").open("wb", buffering=0) as log:
            core = AgentCore(args.executable, data, peers=args.peer, stderr=log)
            begin = time.perf_counter()
            async with core:
                status = await core.status()
                report["first_response_ms"] = (time.perf_counter() - begin) * 1000
                assert status["network"] == "mainnet", "use a mainnet executable"
                previous = None
                async with asyncio.timeout(args.timeout):
                    while True:
                        status = await core.status()
                        ctx = status["context"]
                        change = (ctx["height"], ctx["stage"], ctx["connected_peers"], ctx["confirming_peer_groups"], ctx["last_sync_error"])
                        if change != previous:
                            sample = {"seconds": time.perf_counter() - begin, "context": ctx}
                            print(json.dumps(sample), flush=True)
                            report["stages"].append(sample); previous = change
                        if ctx["ready"]:
                            break
                        await asyncio.sleep(0.2)
                report["ready_seconds"] = time.perf_counter() - begin
                report["verified_context"] = ctx
                report["after_sync"] = resources(core.process.pid, data)
                report["status_latency"] = await repeated(core.status, 100)
                report["subsequent_tips"] = []
                initial_height, last_hash = int(ctx["height"]), ctx["block_hash"]
                confirmed_height = initial_height
                async with asyncio.timeout(args.timeout):
                    while confirmed_height < initial_height + args.observe_blocks:
                        await asyncio.sleep(0.2)
                        ctx = (await core.status())["context"]
                        if ctx["ready"] and ctx["block_hash"] != last_hash:
                            report["subsequent_tips"].append(ctx)
                            last_hash = ctx["block_hash"]
                            confirmed_height = int(ctx["height"])
                            print("[new tip]", ctx["height"], last_hash, flush=True)
                report["after_following"] = resources(core.process.pid, data)
                report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"; report["error"] = repr(error)
        raise
    finally:
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print("[report]", args.output / "report.json", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executable", type=lambda p: Path(p).resolve(), required=True)
    parser.add_argument("--output", type=lambda p: Path(p).resolve(), required=True)
    parser.add_argument("--peer", action="append", default=[])
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--data-dir", type=lambda p: Path(p).resolve())
    parser.add_argument("--observe-blocks", type=int, default=0)
    asyncio.run(run(parser.parse_args()))
