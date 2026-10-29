#!/usr/bin/env python3
"""No models: exercise the installed MCP entry point for every host adapter.

Run with a prebuilt binary: python3 mcp-framing.py ACYCLIC.
The never-newline peer keeps stdin open until the server has rejected it.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from native_fixture import fixture_environment
from native_mcp import McpClient, McpFailure

options = argparse.ArgumentParser(description=__doc__)
options.add_argument("binary")
options.add_argument("--host", choices=("codex", "claude-code", "pi"))
options.add_argument("--case", choices=("framed", "oversized-open", "oversized-delimited", "initialized", "closed-producer"))
args = options.parse_args()
binary = str(Path(args.binary).resolve())
started = time.monotonic()
results = []
with tempfile.TemporaryDirectory(prefix="plugins-mcp-framing-") as temporary:
    root = Path(temporary)
    env = fixture_environment(root)
    for host in ([args.host] if args.host else ("codex", "claude-code", "pi")):
        for case in ([args.case] if args.case else ("framed", "oversized-open", "oversized-delimited")):
            process = subprocess.Popen([binary, "__mcp", host], cwd=root, env=env,
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            client = None
            try:
                if case in ("initialized", "closed-producer"):
                    recovery = f"python3 plugin/tests/hosts/mcp-framing.py {binary} --host {host} --case {case}"
                    client = McpClient(process, recovery)
                    if case == "initialized":
                        client.send({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
                        tools = client.response(1)["tools"]
                        assert {tool["name"] for tool in tools} == {
                            "spawn", "send", "wait", "cancel", "archive", "resume"}, (
                                f"step=verify initialized MCP tools; recovery={recovery}")
                    process.stdin.close()
                    if case == "closed-producer":
                        try:
                            client.response(1)
                        except McpFailure as error:
                            assert "producer closed before response" in str(error), str(error)
                        else:
                            raise AssertionError(f"step=observe MCP producer closure; recovery={recovery}")
                elif case == "framed":
                    requests = [json.dumps({"jsonrpc": "2.0", "id": number,
                                            "method": "ping"}).encode()
                                for number in (1, 2)]
                    process.stdin.write(b"\r\n".join(requests) + b"\n")
                    process.stdin.flush()
                    for number in (1, 2):
                        assert json.loads(process.stdout.readline()) == {
                            "jsonrpc": "2.0", "id": number, "result": {}}
                    process.stdin.close()
                else:
                    frame = json.dumps({"jsonrpc": "2.0", "id": 3, "method": "ping",
                                        "padding": "x" * 65_537}).encode()
                    # A valid oversized ping is also the removal-control input:
                    # without the bound it is accepted, exposing the old path.
                    payload = frame[:65_537] if case == "oversized-open" else frame + b"\n"
                    try:
                        process.stdin.write(payload)
                        process.stdin.flush()
                    except BrokenPipeError:
                        pass  # The server can reject before the last write completes.
                    answer = json.loads(process.stdout.readline())
                    assert answer.get("error", {}).get("code") == -32700, answer
                    assert process.stdout.read(1) == b"", "failed frame kept the session open"
                    assert not process.stdin.closed, "peer must remain open during rejection"
                # Framing/decoding failures emit -32700 and terminate with an
                # error; the shipping CLI propagates that error as exit 1.
                expected_exit = 0 if case in ("framed", "initialized", "closed-producer") else 1
                actual_exit = process.wait(timeout=2)
                diagnostic = process.stderr.read().decode()
                assert actual_exit == expected_exit, (
                    f"step=verify MCP {host}/{case} termination: exit {actual_exit}, "
                    f"expected {expected_exit}: {diagnostic}; recovery=python3 "
                    f"plugin/tests/hosts/mcp-framing.py {binary} --host {host} --case {case}"
                )
                results.append({"host": host, "case": case, "passed": True})
            finally:
                if client is not None:
                    client.close()
                if process.poll() is None:
                    process.kill()
                    process.wait()
                for pipe in (process.stdin, process.stdout, process.stderr):
                    try:
                        pipe.close()
                    except BrokenPipeError:
                        pass
print(json.dumps({"cases": results, "wall_seconds": time.monotonic() - started}))
