#!/usr/bin/env python3
"""Check the video package against its hashes and raw agent tool transcripts."""
import argparse
import hashlib
import json
from pathlib import Path
from render_agent_log import render_agent_log


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def load_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def tool_results(path):
    events = load_jsonl(path)
    require(sum(event["type"] == "thread.started" for event in events) == 1,
            f"no unique Codex thread in {path}")
    require(sum(event["type"] == "turn.completed" for event in events) == 1,
            f"agent did not complete in {path}")
    results = []
    for event in events:
        item = event.get("item", {})
        if event["type"] != "item.completed" or item.get("type") != "command_execution":
            continue
        require(item.get("exit_code") == 0, f"agent command failed in {path}")
        response = json.loads(item["aggregated_output"])
        require(response["ok"], f"agent tool rejected in {path}")
        results.append(response["result"])
    require(results, f"agent did not call a tool in {path}")
    return results


def verify(directory):
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        expected, path = line.split("  ", 1)
        target = directory / path
        require(target.is_file(), f"missing {path}")
        actual = hashlib.sha256(target.read_bytes()).hexdigest()
        require(actual == expected, f"hash mismatch: {path}")
    metadata = json.loads((directory / "metadata.json").read_text())
    video = directory / "video.webm"
    require(video.is_file() and video.stat().st_size > 0, "missing original video")
    require(hashlib.sha256(video.read_bytes()).hexdigest() == metadata["video_sha256"],
            "metadata video hash mismatch")
    top = json.loads((directory / "topology.json").read_text())
    require(top["host_network_namespace"] != top["backend_network_namespace"],
            "network namespace was not isolated")
    peers = [p for p in top["processes"] if p["executable"].endswith("isolated_v2_node")]
    cores = [p for p in top["processes"] if p["executable"].endswith("/agcore")]
    require(len(peers) == 2 and len(cores) == 3, "expected two peers and three Agent Core processes")
    require(all(p["network_namespace"] == top["backend_network_namespace"] for p in peers + cores),
            "one process was outside the isolated network")
    data_dirs = [p["argv"][p["argv"].index("--data-dir") + 1] for p in cores]
    require(len(set(data_dirs)) == 3, "Agent Core data directories were shared")
    for index in (0, 1):
        log = directory / "nodes" / f"reference-{index}.log"
        require(log.is_file() and log.stat().st_size > 0, f"missing reference node log {index}")
    for name in ("requester", "provider", "auditor"):
        raw = directory / "agents" / f"{name}.jsonl"
        readable = directory / "agents" / f"{name}.txt"
        require(raw.is_file() and raw.stat().st_size > 0, f"missing raw agent log {name}")
        require(readable.read_text() == render_agent_log(raw),
                f"readable agent log differs from raw events for {name}")
    require(len((directory / "isolation.txt").read_text().splitlines()) == 1 and
            "lo:" in (directory / "isolation.txt").read_text(), "more than loopback was present")
    requester = tool_results(directory / "agents" / "requester.jsonl")
    provider = tool_results(directory / "agents" / "provider.jsonl")
    auditor = tool_results(directory / "agents" / "auditor.jsonl")
    require(any(r.get("ready") and r.get("height") == "64" for r in requester),
            "requester verifier was not ready at H64")
    require(any(r.get("matches") is False and r.get("action_authorized") is False for r in requester),
            "old current right was not rejected")
    require(any(r.get("accepted") and r.get("finalized") for r in provider),
            "provider did not verify the finalized receipt")
    require(any(r.get("matches") is True and "recorded_result" in r for r in provider),
            "provider did not recompute the recorded result")
    require(any(r.get("accepted") is False for r in auditor),
            "auditor did not reject the modified receipt")
    require(any(r.get("accepted") and r.get("finalized") for r in auditor),
            "auditor did not verify the original receipt")
    require(any(r.get("body_present") is False and r.get("retained_body_present") is False for r in auditor),
            "old body was not shown absent")
    require(any(r.get("matches") is True and "recorded_result" in r for r in auditor),
            "auditor did not recompute the recorded result")
    fixture = json.loads((directory / "fixture-report.json").read_text())
    receipt_hash = hashlib.sha256((directory / "receipt.bin").read_bytes()).hexdigest()
    require(receipt_hash == fixture["permits"][0]["receipt_sha256"],
            "receipt differs from the published experiment")
    events = load_jsonl(directory / "events.jsonl")
    require(any(e.get("event") == "verifier_start" and e.get("agent") == "auditor" for e in events),
            "late verifier did not start")
    require(any(e.get("event") == "receipt_rejected" and e.get("agent") == "auditor" for e in events),
            "network event log lacks the rejected proof")
    return {"agents": 3, "reference_nodes": 2, "height": 64, "receipt_sha256": receipt_hash,
            "network": "isolated-v2-fork-testnet", "result": "passed"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.directory), indent=2))


if __name__ == "__main__":
    main()
