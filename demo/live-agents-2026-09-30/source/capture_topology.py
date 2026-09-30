#!/usr/bin/env python3
"""Record process identities and network namespaces while the demo is running."""
import argparse
import json
import os
from pathlib import Path
import time


def process(pid):
    path = Path("/proc") / str(pid)
    try:
        status = path.joinpath("status").read_text()
        parent = int(next(line.split()[1] for line in status.splitlines() if line.startswith("PPid:")))
        argv = [arg.decode(errors="replace") for arg in path.joinpath("cmdline").read_bytes().split(b"\0") if arg]
        return {"pid": pid, "ppid": parent, "executable": os.readlink(path / "exe"),
                "network_namespace": os.readlink(path / "ns" / "net"),
                "argv": argv}
    except (OSError, ValueError, StopIteration):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    args = parser.parse_args()
    run = str(args.run.resolve())
    all_processes = [item for item in (process(int(path.name)) for path in Path("/proc").iterdir()
                                   if path.name.isdecimal()) if item]
    roots = [item for item in all_processes if "demo/video_backend.py" in item["argv"] and
             any(run in value for value in item["argv"])]
    if len(roots) != 1:
        raise SystemExit(f"expected one backend, found {len(roots)}")
    included = {roots[0]["pid"]}
    changed = True
    while changed:
        previous = len(included)
        included.update(item["pid"] for item in all_processes if item["ppid"] in included)
        changed = len(included) != previous
    snapshot = {"captured_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "host_network_namespace": os.readlink("/proc/self/ns/net"),
                "backend_network_namespace": roots[0]["network_namespace"],
                "processes": [item for item in all_processes if item["pid"] in included]}
    if snapshot["host_network_namespace"] == snapshot["backend_network_namespace"]:
        raise SystemExit("backend unexpectedly shares the host network namespace")
    output = args.run / "topology.json"
    output.write_text(json.dumps(snapshot, indent=2) + "\n")
    print(output)


if __name__ == "__main__":
    main()
