#!/usr/bin/env python3
"""Record a short, readable four-pane demonstration of real agent tool calls."""
import argparse
import asyncio
import curses
import dbus
import json
import os
from pathlib import Path
import re
import textwrap
import time


ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "demo" / "agent_tool.py"
TITLE = {
    "requester": "01  REQUESTER AGENT",
    "provider": "02  PROVIDER AGENT",
    "auditor": "03  AUDITOR AGENT",
    "events": "04  NETWORK AND BROKER",
}

PROMPT = {
    "requester": f"""You are the Requester agent in a short live Parano1d v2 demo. Your only task is to decide if an untrusted broker's claimed ten-use grant can fund a new job. Use only `python3 {TOOL} requester inbox`, `python3 {TOOL} requester status`, `python3 {TOOL} requester right`, and `python3 {TOOL} requester send --to provider --text '...'`. Check inbox and local State, then send the provider your factual decision in at most 120 characters. Do not read files, inspect source, use the web, or edit anything. After each check, state the reason for your next action in one sentence. End with one short conclusion. Do not claim a past receipt establishes a current right.""",
    "provider": f"""You are the Provider agent in a short live Parano1d v2 demo. An untrusted broker supplies a saved receipt and result of a job completed earlier. Use only `python3 {TOOL} provider inbox`, `python3 {TOOL} provider status`, `python3 {TOOL} provider receipt`, `python3 {TOOL} provider check_result`, and `python3 {TOOL} provider send --to auditor --text '...'`. Read messages. Independently verify the saved receipt with your local Core and recompute the recorded fixed result. Send the auditor the outcome and short digest in at most 120 characters. This is an audit of past work; do not start a new paid job or imply that a receipt proves external service delivery. Do not read files, inspect source, use the web, or edit anything. Explain your checks and decision in short, plain English, then end.""",
    "auditor": f"""You are the Auditor agent in a short live Parano1d v2 demo. You start with an empty verifier directory after the old transaction body was pruned. The broker may have altered one receipt. Use only `python3 {TOOL} auditor inbox`, `python3 {TOOL} auditor receipt --which tampered`, `python3 {TOOL} auditor receipt --which valid`, `python3 {TOOL} auditor pruning`, `python3 {TOOL} auditor check_result`, and `python3 {TOOL} auditor send --to requester --text '...'`. Read messages, reject the altered proof if it fails, verify the original receipt independently, check that the old transaction body is absent, and separately recompute the recorded fixed result. Send the requester your concise conclusion in at most 120 characters. Do not read files, inspect source, use the web, or edit anything. Explain your checks and decision in short, plain English, then end. The receipt proves a recorded call, not external service delivery.""",
}


def short_output(raw):
    try:
        response = json.loads(raw)
    except (ValueError, TypeError):
        return raw.strip()[:240]
    if not response.get("ok"):
        return "ERROR: " + str(response.get("error", "unknown"))[:160]
    result = response["result"]
    if "messages" in result:
        return " | ".join(f"{m['from']}: {m['text']}" for m in result["messages"][-3:]) or "Inbox empty"
    if "recorded_result" in result:
        return f"RECORDED RESULT: matches={result['matches']}, digest={result['computed_result'][:20]}...; external delivery not proven"
    if "matches" in result:
        return f"CURRENT RIGHT: {'PRESENT' if result['matches'] else 'ABSENT'} at H{result['height']}; amount {result['amount_micronoid']}"
    if "accepted" in result:
        if not result["accepted"]:
            return "RECEIPT REJECTED: " + result["reason"]
        payout = result["payout"]
        return f"RECEIPT VERIFIED: H{result['height']}, finalized={result['finalized']}, payout={payout['amount_micronoid']} micronoid, txid={result['txid'][:16]}..."
    if "ready" in result:
        return f"OWN VERIFIER: ready={result['ready']}, H{result['height']}, peer groups={result['peer_groups']}, State root={result['state_root'][:16]}..."
    if "body_present" in result:
        return f"OLD BODY AT H{result['height']}: present={result['body_present']}, retained={result['retained_body_present']}"
    if "delivered" in result:
        return "MESSAGE DELIVERED"
    return str(result)[:220]


def event_output(record):
    kind = record.get("event", "event")
    who = record.get("agent", record.get("sender", ""))
    if kind == "agent_message":
        return f"{record['sender']} > {record['recipient']}: {record['text']}"
    if kind == "verifier_start":
        return f"{who}: cold verifier starts"
    if kind == "verifier_ready":
        return f"{who}: verified H{record['height']}"
    if kind == "right_checked":
        return f"{who}: exact current right {'present' if record['matches'] else 'absent'}"
    if kind == "receipt_verified":
        return f"{who}: saved receipt verified; finalized={record['finalized']}"
    if kind == "receipt_rejected":
        return f"{who}: changed receipt rejected"
    if kind == "recorded_result_checked":
        return f"{who}: recorded result {'matched' if record['matches'] else 'failed'}"
    if kind == "references_ready":
        return f"isolated v2 H{record['height']}; old body pruned"
    if kind == "broker_offers_delivered":
        return "broker offers are untrusted"
    return kind.replace("_", " ")


class Screen:
    def __init__(self, stdscr, args):
        self.stdscr, self.args = stdscr, args
        self.lines = {name: [] for name in TITLE}
        self.focus = None
        self.duration = 0
        self.started = time.monotonic()
        self.event_offset = 0
        curses.curs_set(0)
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_GREEN, -1)
        curses.init_pair(2, curses.COLOR_CYAN, -1)
        curses.init_pair(3, curses.COLOR_YELLOW, -1)
        curses.init_pair(4, curses.COLOR_MAGENTA, -1)
        self.stdscr.nodelay(True)
        self.add("events", "QUESTION: Can a broker make agents trust a spent right or a forged receipt?")
        self.read_events()

    def add(self, name, line):
        for part in line.strip().splitlines():
            if part:
                self.lines[name].append(part.strip())
        self.lines[name] = self.lines[name][-100:]

    def read_events(self):
        path = self.args.run / "events.jsonl"
        if not path.exists():
            return
        with path.open() as file:
            file.seek(self.event_offset)
            for line in file:
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                self.add("events", f"{time.monotonic() - self.started:05.1f}s  {event_output(record)}")
            self.event_offset = file.tell()

    def box(self, name, x, y, width, height, active=False):
        if width < 18 or height < 5:
            return
        win = self.stdscr.derwin(height, width, y, x)
        win.box()
        color = curses.color_pair(1 if name == "events" else 2 if active else 3)
        win.addnstr(0, 2, f" {TITLE[name]} ", width - 4, color | curses.A_BOLD)
        inner = width - 4
        wrapped = []
        for line in self.lines[name]:
            wrapped.extend(textwrap.wrap(line, inner, break_long_words=True, break_on_hyphens=False) or [""])
            wrapped.append("")
        visible = wrapped[-(height - 3):]
        for row, line in enumerate(visible, 1):
            if row >= height - 1:
                break
            attr = curses.A_NORMAL
            if any(s in line for s in ("VERIFIED", "COMPLETE", "PRESENT", "finalized=True")):
                attr = curses.color_pair(1) | curses.A_BOLD
            elif any(s in line for s in ("REJECTED", "ABSENT", "REFUSED", "ERROR")):
                attr = curses.color_pair(4) | curses.A_BOLD
            win.addnstr(row, 2, line, inner, attr)
        win.noutrefresh()

    def draw(self):
        self.read_events()
        self.stdscr.erase()
        height, width = self.stdscr.getmaxyx()
        if height < 20 or width < 75:
            self.stdscr.addstr(0, 0, "Resize terminal to at least 75x20")
            self.stdscr.refresh()
            return
        header = " LIVE AGENTS   |   THREE AI AGENTS   |   THREE INDEPENDENT VERIFIERS   |   PARANO1D V2 TEST NETWORK "
        self.stdscr.addnstr(0, 1, header, width - 2, curses.color_pair(1) | curses.A_BOLD)
        footer = f" LIVE DEMO  {int(time.monotonic() - self.started):02d}s  |  Current State and saved receipts verified by Agent Core "
        self.stdscr.addnstr(height - 1, 1, footer, width - 2, curses.color_pair(2))
        self.stdscr.noutrefresh()
        usable = height - 2
        if self.focus is None:
            left = width // 2
            top = usable // 2
            self.box("requester", 0, 1, left, top)
            self.box("provider", left, 1, width - left, top)
            self.box("auditor", 0, top + 1, left, usable - top)
            self.box("events", left, top + 1, width - left, usable - top)
        else:
            wide = (width * 2) // 3
            self.box(self.focus, 0, 1, wide, usable, True)
            others = [name for name in TITLE if name != self.focus]
            part = usable // 3
            for index, name in enumerate(others):
                top = 1 + part * index
                span = part if index < 2 else usable - 2 * part
                self.box(name, wide, top, width - wide, span)
        curses.doupdate()


async def run_agent(screen, name):
    screen.focus = name
    screen.add(name, f"Starting {name}. Checking facts through its own verifier.")
    command = ["codex", "-s", "danger-full-access", "-a", "never", "exec", "--json",
               "--ephemeral", "--ignore-user-config", "-C", str(ROOT / "demo"), PROMPT[name]]
    raw = screen.args.run / f"{name}-agent.jsonl"
    errors = screen.args.run / f"{name}-agent.stderr"
    with raw.open("wb") as rawfile, errors.open("wb") as errfile:
        process = await asyncio.create_subprocess_exec(*command, stdout=asyncio.subprocess.PIPE,
                                                       stderr=errfile, env=os.environ.copy(), stdin=asyncio.subprocess.DEVNULL)
        try:
            while True:
                line = await asyncio.wait_for(process.stdout.readline(), timeout=80)
                if not line:
                    break
                rawfile.write(line)
                rawfile.flush()
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                item = event.get("item", {})
                if item.get("type") == "agent_message" and event.get("type") == "item.completed":
                    screen.add(name, item.get("text", ""))
                elif item.get("type") == "command_execution":
                    command_text = item.get("command", "")
                    match = re.search(r"agent_tool\.py\s+\w+\s+(\w+)", command_text)
                    if event.get("type") == "item.started":
                        screen.add(name, "> Agent Core: " + (match.group(1) if match else command_text[:80]))
                    elif event.get("type") == "item.completed":
                        screen.add(name, short_output(item.get("aggregated_output", "")))
            await process.wait()
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            screen.add(name, "Agent timed out.")
        if process.returncode:
            screen.add(name, f"Agent exited with code {process.returncode}. See local log.")
    return process.returncode


async def run_demo(screen):
    await asyncio.sleep(3)
    for name in ("requester", "provider", "auditor"):
        await run_agent(screen, name)
        await asyncio.sleep(2)
    screen.focus = None
    await asyncio.sleep(12)


def record(screen, path):
    bus = dbus.SessionBus()
    proxy = bus.get_object("org.gnome.Shell.Screencast", "/org/gnome/Shell/Screencast")
    screencast = dbus.Interface(proxy, "org.gnome.Shell.Screencast")
    success, actual = screencast.Screencast(str(path), dbus.Dictionary({}, signature="sv"))
    if not success:
        raise RuntimeError("GNOME refused to start screen recording")
    screen.add("events", "Screen recording started")
    return screencast, str(actual)


async def main_async(screen):
    for _ in range(40):
        screen.draw()
        await asyncio.sleep(0.05)
    screencast, actual = record(screen, screen.args.output)
    task = asyncio.create_task(run_demo(screen))
    try:
        while not task.done():
            screen.draw()
            await asyncio.sleep(0.07)
        await task
    finally:
        screen.draw()
        await asyncio.sleep(1)
        screencast.StopScreencast()
        (screen.args.run / "video-path.txt").write_text(actual + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=lambda p: Path(p).resolve(), required=True)
    parser.add_argument("--output", type=lambda p: Path(p).resolve(), required=True)
    args = parser.parse_args()
    if not (args.run / "demo.sock").is_socket():
        parser.error("demo backend socket is not ready")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    os.environ["AGCORE_DEMO_SOCKET"] = str(args.run / "demo.sock")
    curses.wrapper(lambda stdscr: asyncio.run(main_async(Screen(stdscr, args))))


if __name__ == "__main__":
    main()
