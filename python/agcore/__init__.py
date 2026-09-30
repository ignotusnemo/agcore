"""Own the verifier process; exchange bounded JSON messages over private pipes."""
import asyncio
import json
from pathlib import Path


class Rejected(Exception):
    def __init__(self, error):
        self.code = error["code"]
        self.retryable = error.get("retryable", False)
        super().__init__(error["message"])


class AgentCore:
    def __init__(self, executable, data_dir, *, peers=(), threads=2, min_peers=2,
                 max_tip_age=300, timeout=120, stderr=None):
        self.args = [str(executable), "--data-dir", str(data_dir), "--threads", str(threads),
                     "--min-peers", str(min_peers), "--max-tip-age", str(max_tip_age)]
        for peer in peers:
            self.args.extend(["--peer", peer])
        self.timeout = timeout
        self.stderr = stderr
        self.process = None
        self.lock = asyncio.Lock()
        self.sequence = 0
        self.poisoned = False

    async def __aenter__(self):
        if self.process is not None:
            raise RuntimeError("Agent Core is already started")
        self.process = await asyncio.create_subprocess_exec(
            *self.args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=self.stderr, limit=2_400_000)
        return self

    async def __aexit__(self, *_):
        await self.close()

    async def request(self, method, **params):
        async with self.lock:
            if self.process is None or self.poisoned or self.process.returncode is not None:
                raise RuntimeError("Agent Core is not running")
            self.sequence += 1
            identifier = str(self.sequence)
            frame = json.dumps({"id": identifier, "method": method, "params": params},
                               separators=(",", ":"), allow_nan=False).encode() + b"\n"
            if len(frame) > 2_225_344:
                raise ValueError("request exceeds the native frame limit")
            try:
                async with asyncio.timeout(self.timeout):
                    self.process.stdin.write(frame)
                    await self.process.stdin.drain()
                    line = await self.process.stdout.readline()
                    if not line:
                        raise RuntimeError("Agent Core closed its response pipe")
                    response = json.loads(line)
                    if response.get("id") != identifier or type(response.get("ok")) is not bool:
                        raise RuntimeError("invalid or uncorrelated Agent Core response")
                    if response["ok"] is False:
                        raise Rejected(response["error"])
                    return response["result"]
            except Rejected:
                raise
            except BaseException:
                # A timed-out reply must never be mistaken for the next request's result.
                self.poisoned = True
                if self.process.returncode is None:
                    self.process.kill()
                    await self.process.wait()
                raise

    async def status(self):
        return await self.request("status")

    async def wait_ready(self, timeout=600):
        async with asyncio.timeout(timeout):
            while True:
                result = await self.status()
                if result["context"]["ready"] is True:
                    return result
                await asyncio.sleep(0.5)

    async def inspect_right(self, opening_hex, slot, creation_id):
        if type(creation_id) is not int or not 0 <= creation_id < 2**64:
            raise ValueError("creation_id must be a uint64")
        if type(slot) is not int or not 0 <= slot < 2**32:
            raise ValueError("slot must be a uint32")
        return await self.request("right.inspect", opening_hex=opening_hex,
                                  slot=slot, creation_id=str(creation_id))

    async def verify_receipt(self, receipt):
        if isinstance(receipt, Path):
            with receipt.open("rb") as source:
                receipt = source.read(1_110_625)
        if not isinstance(receipt, bytes) or not 0 < len(receipt) <= 1_110_624:
            raise ValueError("expected receipt bytes within the native receipt size limit")
        return await self.request("receipt.verify", receipt_hex=receipt.hex())

    async def recheck_receipt(self, receipt_id):
        return await self.request("receipt.recheck", receipt_id=receipt_id)

    async def close(self):
        if self.process is None:
            return
        if self.process.returncode is None and not self.poisoned:
            try:
                await self.request("shutdown")
            except Exception:
                pass
        if self.process.returncode is None:
            try:
                await asyncio.wait_for(self.process.wait(), 15)
            except TimeoutError:
                self.process.kill()
                await self.process.wait()
