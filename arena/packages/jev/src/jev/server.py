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


def _optional_limit(name: str) -> int | None:
    value = os.environ.get(name)
    if value is None:
        return None
    limit = int(value)
    if limit < 0:
        raise ValueError(f"{name} must be non-negative")
    return limit


def make_server(backend: Backend, host: str = "127.0.0.1", port: int = 8788, token: str | None = None) -> ThreadingHTTPServer:
    """One locked backend behind a small HTTP API.

    `token` (or JEV_SERVE_TOKEN) makes every /decide require `Authorization: Bearer <token>`; without one the
    server refuses to bind anything but a loopback address. Request sizes and counts are unbounded
    unless the operator configures JEV_SERVE_MAX_BODY, _QUESTIONS, _OPTIONS or _STATE.
    """
    token = token or os.environ.get("JEV_SERVE_TOKEN")
    if not token and host not in ("127.0.0.1", "localhost", "::1"):
        raise SystemExit(f"jev serve: refusing to listen on {host} without JEV_SERVE_TOKEN (the API has no other authentication)")
    body_limit = _optional_limit("JEV_SERVE_MAX_BODY")
    questions_limit = _optional_limit("JEV_SERVE_MAX_QUESTIONS")
    options_limit = _optional_limit("JEV_SERVE_MAX_OPTIONS")
    state_limit = _optional_limit("JEV_SERVE_MAX_STATE")
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
                if n < 0:
                    return self._send(400, {"error": "invalid Content-Length"})
                if body_limit is not None and n > body_limit:
                    return self._send(413, {"error": f"body exceeds {body_limit} bytes"})
                req = json.loads(self.rfile.read(n) or b"{}")
                questions = req.get("questions", [])
                if not isinstance(questions, list):
                    return self._send(400, {"error": "questions must be a list"})
                if questions_limit is not None and len(questions) > questions_limit:
                    return self._send(400, {"error": f"questions exceed {questions_limit}"})
                state = str(req.get("state", ""))
                if state_limit is not None and len(state) > state_limit:
                    return self._send(413, {"error": f"state exceeds {state_limit} characters"})
                if options_limit is not None and any(len(q.get("options", ())) > options_limit for q in questions):
                    return self._send(400, {"error": f"a question has more than {options_limit} options"})
                qs = [Question(q["question"], tuple(q.get("options", ())), q.get("type", "choice"), q.get("id"))
                      for q in questions]
                with lock:
                    res = backend.decide(state, qs)
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
