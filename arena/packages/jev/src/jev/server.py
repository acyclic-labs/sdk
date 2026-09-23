"""Self-hosted System One over HTTP: `jev serve --backend local` on a GPU box,
`HttpBackend("http://host:8788")` everywhere else. No quota, no shared Space.

Stdlib only. One endpoint:
  POST /decide  {"state": str, "questions": [{question, options, type}]}
      -> {"decisions": [{"probs": [...]}], "timing": {...}, "temperature": float, "backend": str}
  GET  /health  -> {"ok": true, "backend": name}
"""
from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .types import Question
from .backends.base import Backend


MAX_BODY = int(os.environ.get("JEV_SERVE_MAX_BODY", str(1 << 20)))        # bytes per request
MAX_QUESTIONS = int(os.environ.get("JEV_SERVE_MAX_QUESTIONS", "64"))       # per request
MAX_OPTIONS = int(os.environ.get("JEV_SERVE_MAX_OPTIONS", "64"))           # per question
MAX_STATE = int(os.environ.get("JEV_SERVE_MAX_STATE", "200000"))           # characters


def make_server(backend: Backend, host: str = "127.0.0.1", port: int = 8788, token: str | None = None) -> ThreadingHTTPServer:
    """One locked backend behind a small HTTP API.

    `token` (or JEV_SERVE_TOKEN) makes every /decide require `Authorization: Bearer <token>`; without one the
    server refuses to bind anything but a loopback address. Bodies, question counts, option counts and state
    length are capped before anything is parsed or scheduled, so one client cannot exhaust the backend.
    """
    token = token or os.environ.get("JEV_SERVE_TOKEN")
    if not token and host not in ("127.0.0.1", "localhost", "::1"):
        raise SystemExit(f"jev serve: refusing to listen on {host} without JEV_SERVE_TOKEN (the API has no other authentication)")
    lock = threading.Lock()  # one GPU, one batch at a time

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):  # quiet
            pass

        def _send(self, code: int, obj: dict):
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path == "/health":
                return self._send(200, {"ok": True, "backend": backend.name, "temperature": backend.temperature})
            self._send(404, {"error": "not found"})

        def do_POST(self):
            if self.path != "/decide":
                return self._send(404, {"error": "not found"})
            if token and self.headers.get("Authorization", "") != f"Bearer {token}":
                return self._send(401, {"error": "unauthorized"})
            try:
                n = int(self.headers.get("Content-Length", "0"))
                if n > MAX_BODY:
                    return self._send(413, {"error": f"body exceeds {MAX_BODY} bytes"})
                req = json.loads(self.rfile.read(n) or b"{}")
                questions = req.get("questions", [])
                if not isinstance(questions, list) or len(questions) > MAX_QUESTIONS:
                    return self._send(400, {"error": f"questions must be a list of at most {MAX_QUESTIONS}"})
                if len(str(req.get("state", ""))) > MAX_STATE:
                    return self._send(413, {"error": f"state exceeds {MAX_STATE} characters"})
                if any(len(q.get("options", ())) > MAX_OPTIONS for q in questions):
                    return self._send(400, {"error": f"a question has more than {MAX_OPTIONS} options"})
                qs = [Question(q["question"], tuple(q.get("options", ())), q.get("type", "choice"), q.get("id"))
                      for q in questions]
                with lock:
                    res = backend.decide(str(req.get("state", "")), qs)
                self._send(200, {"backend": res.backend, "temperature": res.temperature,
                                 "timing": res.timing.__dict__, "plan": res.plan.to_dict(),
                                 "decisions": [{"probs": list(d.probs)} for d in res.decisions]})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": repr(e)})

    return ThreadingHTTPServer((host, port), Handler)


def serve(backend: Backend, host: str = "127.0.0.1", port: int = 8788, token: str | None = None) -> None:
    srv = make_server(backend, host, port, token)
    print(f"jev serve: {backend.name} on http://{host}:{srv.server_address[1]}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
