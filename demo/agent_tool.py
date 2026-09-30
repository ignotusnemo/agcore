#!/usr/bin/env python3
"""Small machine-readable tool for the isolated Live Agents video."""
import argparse
import json
import os
from pathlib import Path
import socket


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("agent", choices=("requester", "provider", "auditor"))
    parser.add_argument("operation", choices=("inbox", "send", "status", "right", "receipt", "pruning", "check_result"))
    parser.add_argument("--to", choices=("requester", "provider", "auditor"))
    parser.add_argument("--text")
    parser.add_argument("--which", choices=("valid", "tampered"), default="valid")
    args = parser.parse_args()
    path = os.getenv("AGCORE_DEMO_SOCKET")
    if not path or not Path(path).is_socket():
        parser.error("AGCORE_DEMO_SOCKET must point to the running private demo socket")
    request = {"agent": args.agent, "op": args.operation, "to": args.to,
               "text": args.text, "which": args.which}
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(180)
        client.connect(path)
        client.sendall(json.dumps(request).encode() + b"\n")
        data = bytearray()
        while not data.endswith(b"\n"):
            fragment = client.recv(8192)
            if not fragment:
                break
            data.extend(fragment)
    response = json.loads(data)
    print(json.dumps(response, indent=2))
    if not response["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
