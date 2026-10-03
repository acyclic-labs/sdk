"""Run the installed GraphCoder entrypoint through a Windows PTY.

The package and bridge are selected through the explicit environment consumed
by graphcoder-production-entrypoint.mjs. No mock fixture is selected here.
"""

from __future__ import annotations

import os
import sys
import threading
import time
from pathlib import Path


def main() -> int:
    try:
        from winpty import PtyProcess
    except ImportError:
        print("PTY qualification unavailable: Python winpty is not installed", file=sys.stderr)
        return 2

    sdk = Path(__file__).resolve().parents[1]
    entrypoint = sdk / "scripts" / "graphcoder-production-entrypoint.mjs"
    if not entrypoint.is_file():
        print(f"PTY qualification missing entrypoint: {entrypoint}", file=sys.stderr)
        return 1
    commands = tuple(sys.argv[1:])
    if not commands:
        print("PTY qualification requires at least one terminal command", file=sys.stderr)
        return 2
    node = os.environ.get("GRAPHCODER_NODE", "node")
    bridge_environment = {
        key: value
        for key, value in os.environ.items()
        if key == "PATH" or key.startswith("GRAPHCODER_")
    }
    process = PtyProcess.spawn([node, str(entrypoint)], cwd=str(sdk), env=bridge_environment)
    output: list[str] = []

    def drain() -> None:
        while True:
            try:
                output.append(process.read(4096))
            except EOFError:
                return

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()

    def prompt_count() -> int:
        return "".join(output).count("graphcoder>")

    def wait_for_prompt(previous: int) -> bool:
        deadline = time.monotonic() + 10
        while prompt_count() <= previous and time.monotonic() < deadline:
            time.sleep(0.05)
        return prompt_count() > previous

    if not wait_for_prompt(0):
        process.terminate()
        print("PTY qualification failed: CLI did not present its initial prompt", file=sys.stderr)
        return 1
    for command in commands:
        try:
            command = _expand_command(command, "".join(output))
        except RuntimeError as error:
            process.terminate()
            print(f"PTY qualification failed: {error}", file=sys.stderr)
            return 1
        previous = prompt_count()
        process.write(command + "\r")
        if not wait_for_prompt(previous):
            process.terminate()
            print(f"PTY qualification failed: no prompt after {command}", file=sys.stderr)
            return 1
    process.write("quit\r")
    deadline = time.monotonic() + 10
    while process.isalive() and time.monotonic() < deadline:
        time.sleep(0.05)
    if process.isalive():
        process.terminate()
        print("PTY qualification failed: CLI did not exit after quit", file=sys.stderr)
        return 1
    reader.join(timeout=2)
    transcript = "".join(output)
    required = ("graphcoder>", '"selectedSession"', '"activity"', '"messages"', '"approvals"', '"generation"', '"unifiedDiff"', '"mediaType"', '"applied":true', '"state":"cancelled"', '"exited":true')
    missing = [marker for marker in required if marker not in transcript]
    if missing:
        print("PTY qualification failed; missing markers: " + ", ".join(missing), file=sys.stderr)
        print(transcript, file=sys.stderr)
        return 1
    print(transcript)
    return 0


def _expand_command(command: str, transcript: str) -> str:
    import json
    import re

    context: dict[str, str] = {}
    ansi = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
    for line in ansi.sub("", transcript).splitlines():
        start = line.find("{")
        if start < 0:
            continue
        try:
            value = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        payload = value.get("value") if isinstance(value, dict) else None
        if not isinstance(payload, dict):
            continue
        selected = payload.get("selectedSession")
        if isinstance(selected, dict) and isinstance(selected.get("summary"), dict) and isinstance(selected["summary"].get("id"), str):
            context["session_id"] = selected["summary"]["id"]
        approvals = payload.get("approvals")
        if isinstance(approvals, list):
            pending = next((item for item in approvals if isinstance(item, dict) and item.get("state") == "pending"), None)
            if isinstance(pending, dict):
                if isinstance(pending.get("id"), str):
                    context["approval_id"] = pending["id"]
                if isinstance(pending.get("operationId"), str):
                    context["writeback_operation_id"] = pending["operationId"]
        for field in ("generation", "changesGeneration"):
            if isinstance(payload.get(field), (str, int)):
                context["workspace_generation"] = str(payload[field])
        writeback = payload.get("writeback")
        if isinstance(writeback, dict) and isinstance(writeback.get("operationId"), str):
            context["writeback_operation_id"] = writeback["operationId"]

    def substitute(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in context:
            raise RuntimeError(f"unresolved command substitution: {{{{{name}}}}}")
        return context[name]

    return re.sub(r"\{\{([a-z_]+)\}\}", substitute, command)


if __name__ == "__main__":
    raise SystemExit(main())
