#!/usr/bin/env python3
"""Launch the production control binary, optionally injecting one abrupt restart."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


def is_post_tool(request: dict) -> bool:
    if request.get("method") != "tools/call":
        return False
    params = request.get("params") or {}
    return params.get("name") == "_hook_post_tool"


def main() -> int:
    if len(sys.argv) != 2:
        raise SystemExit("usage: launch_control.py CONTROL_BINARY")
    data = os.environ.get("ACYCLIC_EVAL_PLUGIN_DATA")
    if not data:
        raise SystemExit("ACYCLIC_EVAL_PLUGIN_DATA is required")
    env = os.environ.copy()
    env["PLUGIN_DATA"] = data
    crash_marker = Path(data) / "eval-control-crashed-once"
    inject = env.get("ACYCLIC_EVAL_CRASH_AFTER_POST_TOOL") == "1" and not crash_marker.exists()
    if not inject:
        os.execve(sys.argv[1], [sys.argv[1]], env)

    process = subprocess.Popen(
        [sys.argv[1]], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=sys.stderr,
        text=True, encoding="utf-8", bufsize=1, env=env,
    )
    assert process.stdin is not None and process.stdout is not None
    for line in sys.stdin:
        process.stdin.write(line)
        process.stdin.flush()
        try:
            request = json.loads(line)
        except json.JSONDecodeError:
            request = {}
        if "id" not in request:
            continue
        response = process.stdout.readline()
        if not response:
            return process.wait()
        sys.stdout.write(response)
        sys.stdout.flush()
        try:
            response_value = json.loads(response)
            succeeded = not response_value.get("result", {}).get("isError", False) and "error" not in response_value
        except json.JSONDecodeError:
            succeeded = False
        if is_post_tool(request) and succeeded:
            crash_marker.parent.mkdir(parents=True, exist_ok=True)
            crash_marker.write_text("injected after successful post-tool response\n", encoding="utf-8")
            process.kill()
            process.wait()
            return 86
    return process.wait()


if __name__ == "__main__":
    raise SystemExit(main())
