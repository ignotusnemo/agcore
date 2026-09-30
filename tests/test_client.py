import asyncio
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "python"))
from agcore import AgentCore, Rejected


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def fake(self, response, delay=0):
        client = AgentCore("unused", "unused", timeout=0.1 if delay else 2)
        code = ("import sys,json,time; request=json.loads(sys.stdin.readline());"
                f"time.sleep({delay}); print(json.dumps({response!r}),flush=True);")
        client.process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", code, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        return client

    async def test_wrong_request_id_poisoned(self):
        client = await self.fake({"id":"another", "ok":True, "result":{"valid":True}})
        with self.assertRaises(RuntimeError):
            await client.status()
        self.assertTrue(client.poisoned)
        with self.assertRaises(RuntimeError):
            await client.status()
        await client.process.wait()

    async def test_timeout_cannot_reuse_late_reply(self):
        client = await self.fake({"id":"1", "ok":True, "result":{}}, delay=1)
        with self.assertRaises(TimeoutError):
            await client.status()
        self.assertTrue(client.poisoned)
        self.assertIsNotNone(client.process.returncode)

    async def test_rejection_is_not_a_success_value(self):
        client = await self.fake({"id":"1", "ok":False,
                                  "error":{"code":"REJECTED", "message":"invalid proof", "retryable":False}})
        with self.assertRaises(Rejected) as failure:
            await client.status()
        self.assertEqual(failure.exception.code, "REJECTED")
        await client.process.wait()

    async def test_nonboolean_success_is_rejected(self):
        client = await self.fake({"id":"1", "ok":"false", "result":{"valid":True}})
        with self.assertRaises(RuntimeError):
            await client.status()
        await client.process.wait()

    async def test_lossy_identifiers_rejected_locally(self):
        client = AgentCore("unused", "unused")
        for creation in [True, -1, 2**64, 1.0, "1"]:
            with self.assertRaises(ValueError):
                await client.inspect_right("00", 0, creation)


if __name__ == "__main__":
    unittest.main()
