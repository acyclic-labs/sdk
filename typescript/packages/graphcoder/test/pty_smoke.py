"""Drive the built GraphCoder CLI through a real Windows winpty session.

Run from the SDK root after building the package:
    python typescript/packages/graphcoder/test/pty_smoke.py
"""

from __future__ import annotations

import os
import subprocess
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

    sdk = Path(__file__).resolve().parents[4]
    command = [
        "bun",
        "typescript/packages/graphcoder/dist/cli.js",
        "--fixture=deterministic",
    ]
    commands = (
        "start inspect",
        "activity",
        "messages",
        "approvals",
        "approve approval-1 yes",
        "changes",
        "diff README.md",
        "file README.md",
        "writeback mock-writeback-1 1 yes",
        "cancel",
        "resume session-1",
        "cancel",
        "quit",
    )
    headless = subprocess.run(
        [*command, *commands],
        cwd=sdk,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    if headless.returncode != 0:
        print("headless qualification failed", file=sys.stderr)
        print(headless.stdout, file=sys.stderr)
        print(headless.stderr, file=sys.stderr)
        return 1
    process = PtyProcess.spawn(command, cwd=str(sdk), env=os.environ.copy())
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
        deadline = time.monotonic() + 5
        while prompt_count() <= previous and time.monotonic() < deadline:
            time.sleep(0.05)
        return prompt_count() > previous

    if not wait_for_prompt(0):
        process.terminate()
        print("PTY qualification failed: CLI did not present its initial prompt", file=sys.stderr)
        return 1

    interactive_commands = commands[:-1]
    for command_line in interactive_commands:
        previous = prompt_count()
        process.write(command_line + "\r")
        if not wait_for_prompt(previous):
            process.terminate()
            print(f"PTY qualification failed: no prompt after {command_line}", file=sys.stderr)
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
    required = (
        '"selectedSession"',
        '"activity"',
        '"messages"',
        '"approvals"',
        '"state":"approved"',
        '"generation"',
        '"unifiedDiff"',
        '"mediaType"',
        '"applied":true',
        '"state":"cancelled"',
        '"exited":true',
    )
    missing_headless = [marker for marker in required if marker not in headless.stdout]
    if missing_headless:
        print("headless qualification failed; missing markers:", ", ".join(missing_headless), file=sys.stderr)
        print(headless.stdout, file=sys.stderr)
        return 1
    missing = [marker for marker in ("graphcoder>", *required) if marker not in transcript]
    if missing:
        print("PTY qualification failed; missing markers:", ", ".join(missing), file=sys.stderr)
        print(transcript, file=sys.stderr)
        return 1
    print(transcript)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
