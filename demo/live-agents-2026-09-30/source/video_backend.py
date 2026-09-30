#!/usr/bin/env python3
"""Private v2 chain and three independent Agent Core processes for a live demo."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from benchmark import AgentCore, Reference, ready, rpc


class Demo:
    def __init__(self, args):
        self.args = args
        self.output = args.output
        self.fixture = json.loads((args.evidence / "report.json").read_text())
        self.permit = self.fixture["permits"][0]
        self.receipt = (args.evidence / "receipts" / (self.permit["txid"] + ".receipt")).read_bytes()
        self.bad_receipt = bytearray(self.receipt)
        self.bad_receipt[20] ^= 1
        self.references = []
        self.cores = {}
        self.core_tasks = {}
        self.logs = {}
        self.messages = {name: [] for name in ("requester", "provider", "auditor")}
        self.recorded_job = next(job for job in self.fixture["jobs"] if job["txid"] == self.permit["txid"])
        self.foreign = None
        self.started = time.monotonic()

    def event(self, kind, **fields):
        record = {"seconds": round(time.monotonic() - self.started, 2), "event": kind, **fields}
        with (self.output / "events.jsonl").open("a") as file:
            file.write(json.dumps(record, separators=(",", ":")) + "\n")
        print(json.dumps(record, separators=(",", ":")), flush=True)

    async def start(self):
        interfaces = {line.split(":")[0].strip() for line in Path("/proc/net/dev").read_text().splitlines() if ":" in line}
        if interfaces != {"lo"}:
            raise RuntimeError("start inside a fresh loopback-only network namespace")
        os.system("ip link set lo up")
        self.output.mkdir(mode=0o700)
        for i, name in enumerate(("issuer", "agent-a")):
            directory = self.output / f"reference-{i}"
            directory.mkdir(mode=0o700)
            shutil.copytree(self.args.fixture / name / "data", directory / "data")
            self.references.append(Reference(self.args.reference, directory, 27900 + 10 * i, 27901 + 10 * i))
        await asyncio.gather(self.references[0].start(self.references[1].p2p),
                             self.references[1].start(self.references[0].p2p))
        tips = [await asyncio.to_thread(rpc, ref.port, "getChainInfo") for ref in self.references]
        target = self.fixture["final_tip"]
        if not all(tip["height"] == target["height"] and tip["best_hash"] == target["best_hash"] for tip in tips):
            raise RuntimeError("reference nodes do not match the recorded fixture")
        height = self.permit["height"]
        for ref in self.references:
            if await asyncio.to_thread(rpc, ref.port, "getBlock", [height]) is not None:
                raise RuntimeError("old transaction body unexpectedly present")
        self.event("references_ready", height=target["height"], block_hash=target["best_hash"][:16], old_body="pruned")
        await asyncio.gather(self.core("requester"), self.core("provider"))
        self.event("two_agents_ready", separate_verifiers=True)
        self.messages["requester"].append({"from": "broker", "text": "This old grant still has ten jobs left. Can your agent accept a new job?"})
        self.messages["provider"].append({"from": "broker", "text": "I have a saved receipt and result for a completed compute job. Can you verify them independently?"})
        self.messages["auditor"].append({"from": "broker", "text": "I sent the auditor a receipt. Check whether it proves payment. The copy I sent may be altered."})
        self.event("broker_offers_delivered", note="broker messages are untrusted")

    async def core(self, name):
        if name not in self.messages:
            raise ValueError("unknown agent")
        if name not in self.core_tasks:
            self.core_tasks[name] = asyncio.create_task(self.start_core(name))
        return await self.core_tasks[name]

    async def start_core(self, name):
        self.event("verifier_start", agent=name, data_dir=str(self.output / name))
        log = (self.output / f"{name}-core.log").open("ab", buffering=0)
        self.logs[name] = log
        peers = [f"/ip4/127.0.0.1/tcp/{ref.p2p}" for ref in self.references]
        core = AgentCore(self.args.executable, self.output / name, peers=peers,
                         max_tip_age=86400, stderr=log)
        await core.__aenter__()
        self.cores[name] = core
        status, _ = await ready(core, self.fixture["final_tip"]["best_hash"], timeout=180)
        self.event("verifier_ready", agent=name, height=status["context"]["height"],
                   state_root=status["context"]["state_root"][:16])
        return core

    async def dispatch(self, agent, op, request):
        if agent not in self.messages:
            raise ValueError("unknown agent")
        if op == "inbox":
            return {"messages": self.messages[agent]}
        if op == "send":
            to = request.get("to")
            message = request.get("text")
            if to not in self.messages or not isinstance(message, str) or not 1 <= len(message) <= 400:
                raise ValueError("expected a recipient and a message of 1-400 characters")
            self.messages[to].append({"from": agent, "text": message})
            self.event("agent_message", sender=agent, recipient=to, text=message)
            return {"delivered": True}
        core = await self.core(agent)
        if op == "status":
            status = await core.status()
            context = status["context"]
            return {"ready": context["ready"], "height": context["height"],
                    "state_root": context["state_root"], "peer_groups": context["confirming_peer_groups"],
                    "network": status["network"]}
        if op == "right":
            enrollment = self.fixture["enrollment"]
            opening, slot = enrollment["opening"], enrollment["slot"]
            result = await core.inspect_right(opening["opening_hex"], slot["slot_index"], slot["creation_id"])
            self.event("right_checked", agent=agent, matches=result["matches"], height=result["context"]["height"])
            return {"matches": result["matches"], "height": result["context"]["height"],
                    "amount_micronoid": result["amount_micronoid"],
                    "action_authorized": result["action_authorized"]}
        if op == "receipt":
            which = request.get("which", "valid")
            if which not in ("valid", "tampered"):
                raise ValueError("which must be valid or tampered")
            try:
                checked = await core.verify_receipt(self.receipt if which == "valid" else bytes(self.bad_receipt))
            except Exception as error:
                self.event("receipt_rejected", agent=agent, which=which, reason=str(error)[:90])
                return {"accepted": False, "reason": str(error)[:150]}
            call = checked["call"]
            self.event("receipt_verified", agent=agent, which=which, txid=call["txid"][:16],
                       finalized=checked["finalized"])
            return {"accepted": checked["valid"] and checked["canonical"],
                    "finalized": checked["finalized"], "height": checked["height"],
                    "txid": call["txid"], "payout": call["payout"]}
        if op == "pruning":
            height = self.permit["height"]
            body = await asyncio.to_thread(rpc, self.references[0].port, "getBlock", [height])
            details = await asyncio.to_thread(rpc, self.references[1].port, "getBlockDetails", [height])
            return {"height": height, "body_present": body is not None,
                    "retained_body_present": details["retained"] is not None}
        if op == "check_result":
            recorded = self.recorded_job
            seed = b"live-agents-v1"
            for _ in range(200000):
                seed = hashlib.sha256(seed).digest()
            matching = seed.hex() == recorded["result"]
            self.event("recorded_result_checked", agent=agent, matches=matching, result=seed.hex()[:16])
            return {"matches": matching, "recorded_result": recorded["result"],
                    "computed_result": seed.hex(), "job": recorded["job"],
                    "boundary": "This checks the recorded fixed computation, not delivery of an external service."}
        raise ValueError("unknown operation")

    async def handle(self, reader, writer):
        try:
            line = await asyncio.wait_for(reader.readline(), 30)
            if not line or len(line) > 4096:
                raise ValueError("invalid request frame")
            request = json.loads(line)
            result = await self.dispatch(request.get("agent"), request.get("op"), request)
            response = {"ok": True, "result": result}
        except Exception as error:
            response = {"ok": False, "error": str(error)[:300]}
        writer.write(json.dumps(response, separators=(",", ":")).encode() + b"\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    async def run(self):
        try:
            await self.start()
            socket = self.output / "demo.sock"
            server = await asyncio.start_unix_server(self.handle, path=socket)
            socket.chmod(0o600)
            self.event("demo_ready", socket=str(socket))
            async with server:
                await server.serve_forever()
        finally:
            for core in self.cores.values():
                await core.close()
            await asyncio.gather(*(ref.stop() for ref in self.references))
            for log in self.logs.values():
                log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("fixture", "evidence", "reference", "executable", "output"):
        parser.add_argument("--" + name, type=lambda path: Path(path).resolve(), required=True)
    args = parser.parse_args()
    asyncio.run(Demo(args).run())


if __name__ == "__main__":
    main()
