#!/usr/bin/env python3
"""Render a readable agent log directly from a raw Codex JSONL event stream."""
import json
from pathlib import Path


def render_agent_log(path):
    lines = []
    for raw in Path(path).read_text().splitlines():
        if not raw.strip():
            continue
        event = json.loads(raw)
        kind = event.get("type")
        item = event.get("item", {})
        if kind == "thread.started":
            lines.append(f"Codex thread: {event['thread_id']}")
        elif kind == "item.completed" and item.get("type") == "agent_message":
            lines.append("\nAGENT:")
            lines.append(item.get("text", "").strip())
        elif kind == "item.completed" and item.get("type") == "command_execution":
            lines.append("\nCOMMAND:")
            lines.append(item.get("command", ""))
            lines.append(f"EXIT: {item.get('exit_code')}")
            lines.append("OUTPUT:")
            output = item.get("aggregated_output", "").strip()
            try:
                lines.append(json.dumps(json.loads(output), indent=2, ensure_ascii=False))
            except (TypeError, ValueError):
                lines.append(output)
        elif kind == "turn.completed":
            lines.append("\nTURN COMPLETED")
    return "\n".join(lines) + "\n"
