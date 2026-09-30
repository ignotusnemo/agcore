# Live Agents, 30 September 2026

[Watch the original WebM](video.webm). Runtime: 2:09. This is the continuous screen recording without audio.

Three Codex CLI agents used three separate Agent Core verifier processes. Two Parano1d nodes and all three verifiers ran in a Linux network namespace with only loopback access. This was an isolated v2 fork test network at height 64. The chain was prepared before recording. No new payment, contract call or paid job was created during the video.

The reference node prints a generic `mainnet` startup banner. Its first log line identifies the isolated fork test build, and the captured network namespace had no external interface.

The requester checked whether a broker's claimed ten-use grant could still fund a job and rejected it against current State. The provider verified a saved finalized receipt for a call at height 12 and recomputed the recorded fixed result. The auditor started a fresh verifier after the old transaction body had been pruned, rejected a modified receipt, verified the original, checked that the old body was absent and recomputed the result. The exact agent tasks are in [agent-tasks.json](agent-tasks.json).

The receipt proves a recorded chain call and payout. Recomputing the fixed result does not prove that a remote service delivered useful work. The video and logs document this run; their hashes do not independently attest when or how the screen was recorded.

The evidence is stored as files in this directory:

- `agents/*.jsonl` contains the raw Codex event streams, including commands and tool responses. `agents/*.txt` renders the same events for reading.
- `events.jsonl` records the broker messages, verifier startup and check outcomes. `nodes/reference-*.log` contains the two chain nodes' native logs. The three Agent Core processes produced no diagnostic stderr in this run, so there are no empty Core log files.
- `topology.json` records the running processes, commands, data directories and network namespaces during the recording. `isolation.txt` records the loopback-only interface inside that namespace.
- `receipt.bin`, `fixture-report.json` and `fixture-manifest.json` identify the saved test fixture. `source/` preserves the demo scripts and evidence checker.

Run `sha256sum -c SHA256SUMS` and `python3 source/verify_evidence.py .` from this directory. The checker compares the readable agent logs with the raw events, checks the receipt against the fixture report, checks the recorded process topology and verifies the reported agent outcomes. The isolated fixture comes from the Parano1d source commit listed in `metadata.json`; its full setup is documented in `research/live_agents/README.md` in that repository. Wallet files and chain databases are not included here.
