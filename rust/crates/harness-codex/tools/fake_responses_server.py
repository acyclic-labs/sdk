#!/usr/bin/env python3
"""Fake OpenAI Responses API for probing `codex exec --json`.

Usage: fake_responses_server.py PORT LOGFILE
Mode is read per-request from env FAKE_MODE or file $FAKE_MODE_FILE (default
/tmp/codex-probe/mode):
  ok        -> assistant message "hi from fake"
  shell     -> first call: function_call (tool name auto-detected from request
               tools); once a function_call_output is present in input: final message
  patch     -> first call: apply_patch custom_tool_call adding hello.txt (needs a catalog model e.g. -m gpt-5.5)
  reasoning -> reasoning summary + message
  http402 / http500 / http429 / http401 -> that HTTP status with a JSON error body
  failed    -> 200 SSE with response.failed (generic, retryable)
  quota429  -> 429 with error.type=insufficient_quota (non-retryable QuotaExceeded)
  quotafailed -> 200 SSE response.failed with code insufficient_quota
Only POST */responses is served; everything else 404s. Every request is logged
(JSON lines: method, path, headers, body) to LOGFILE.
"""
import json
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1])
LOG = sys.argv[2]
MODE_FILE = os.environ.get("FAKE_MODE_FILE", "/tmp/codex-probe/mode")
COUNTER = {"n": 0}


def mode():
    try:
        with open(MODE_FILE) as f:
            return f.read().strip() or "ok"
    except FileNotFoundError:
        return os.environ.get("FAKE_MODE", "ok")


def sse(obj):
    return f"event: {obj['type']}\ndata: {json.dumps(obj)}\n\n".encode()


USAGE = {
    "input_tokens": 100,
    "input_tokens_details": {"cached_tokens": 40},
    "output_tokens": 7,
    "output_tokens_details": {"reasoning_tokens": 3},
    "total_tokens": 107,
}


def message_item(text, n):
    return {
        "type": "message",
        "id": f"msg_{n}",
        "role": "assistant",
        "status": "completed",
        "content": [{"type": "output_text", "text": text, "annotations": []}],
    }


def pick_shell_tool(body):
    names = []
    for t in body.get("tools", []) or []:
        names.append(t.get("name") or t.get("type"))
    for cand in ("exec_command", "shell_command", "shell", "local_shell"):
        if cand in names:
            return cand
    return names[0] if names else "shell"


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _log(self, body):
        COUNTER["n"] += 1
        rec = {
            "n": COUNTER["n"],
            "t": time.time(),
            "method": self.command,
            "path": self.path,
            "headers": dict(self.headers.items()),
            "mode": mode(),
        }
        try:
            rec["body"] = json.loads(body) if body else None
        except Exception:
            rec["body_raw"] = body.decode("utf-8", "replace")
        with open(LOG, "a") as f:
            f.write(json.dumps(rec) + "\n")
        return rec

    def _read(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b""
        if self.headers.get("Content-Encoding") == "zstd":
            try:
                import subprocess
                raw = subprocess.run(["zstd", "-dc"], input=raw, capture_output=True).stdout
            except Exception:
                pass
        return raw

    def _send(self, code, body, ctype="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._log(b"")
        self._send(404, b'{"error":"not found"}')

    def do_POST(self):
        raw = self._read()
        rec = self._log(raw)
        if not self.path.rstrip("/").endswith("/responses"):
            return self._send(404, b'{"error":"not found"}')
        m = mode()
        n = rec["n"]
        if m == "quota429":
            err = {"error": {"message": "budget exhausted", "type": "insufficient_quota", "code": "insufficient_quota"}}
            return self._send(429, json.dumps(err).encode())
        if m.startswith("http"):
            code = int(m[4:])
            err = {"error": {"message": f"fake {code}", "type": "fake_error", "code": f"fake_{code}"}}
            if code == 402:
                err = {"error": {"message": "Payment required: budget exhausted", "type": "billing", "code": "budget_exhausted"}}
            return self._send(code, json.dumps(err).encode())
        body = rec.get("body") or {}
        rid = f"resp_{n}"
        events = [{"type": "response.created", "response": {"id": rid}}]
        if m == "quotafailed":
            events.append({"type": "response.failed", "response": {"id": rid, "status": "failed",
                           "error": {"code": "insufficient_quota", "message": "budget exhausted (sse)"}}})
        elif m == "failed":
            events.append({"type": "response.failed", "response": {"id": rid, "status": "failed",
                           "error": {"code": "server_error", "message": "fake response.failed"}}})
        else:
            items = []
            if m == "reasoning":
                items.append({"type": "reasoning", "id": f"rs_{n}", "summary": [
                    {"type": "summary_text", "text": "**Thinking** about greeting"}]})
            has_output = any(
                isinstance(i, dict) and i.get("type") in ("function_call_output", "custom_tool_call_output")
                for i in body.get("input", []) or [])
            if m == "patch" and not has_output:
                items.append({"type": "custom_tool_call", "id": f"ctc_{n}", "call_id": f"call_{n}",
                              "name": "apply_patch", "status": "completed",
                              "input": "*** Begin Patch\n*** Add File: hello.txt\n+hello from patch\n*** End Patch\n"})
            elif m == "shell" and not has_output:
                tool = pick_shell_tool(body)
                args = {"cmd": "echo probe-output && ls", "command": ["bash", "-lc", "echo probe-output && ls"]}
                if tool == "shell_command":
                    args = {"command": "echo probe-output && ls"}
                elif tool == "exec_command":
                    args = {"cmd": "echo probe-output && ls"}
                items.append({"type": "function_call", "id": f"fc_{n}", "call_id": f"call_{n}",
                              "name": tool, "arguments": json.dumps(args), "status": "completed"})
            else:
                items.append(message_item("hi from fake", n))
            for it in items:
                events.append({"type": "response.output_item.added", "output_index": 0, "item": it})
                events.append({"type": "response.output_item.done", "output_index": 0, "item": it})
            events.append({"type": "response.completed", "response": {"id": rid, "status": "completed", "usage": USAGE}})
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        for e in events:
            self.wfile.write(sse(e))
            self.wfile.flush()
        self.close_connection = True


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
