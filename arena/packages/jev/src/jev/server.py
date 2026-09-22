"""Self-hosted System One over HTTP: `jev serve --backend local` on a GPU box,
`HttpBackend("http://host:8788")` everywhere else. No quota, no shared Space.

Stdlib only. One endpoint:
  POST /decide  {"state": str, "questions": [{question, options, type}]}
      -> {"decisions": [{"probs": [...]}], "timing": {...}, "temperature": float, "backend": str}
  GET  /health  -> {"ok": true, "backend": name}
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .types import Question
from .backends.base import Backend


def make_server(backend: Backend, host: str = "127.0.0.1", port: int = 8788) -> ThreadingHTTPServer:
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
            try:
                n = int(self.headers.get("Content-Length", "0"))
                req = json.loads(self.rfile.read(n) or b"{}")
                qs = [Question(q["question"], tuple(q.get("options", ())), q.get("type", "choice"), q.get("id"))
                      for q in req.get("questions", [])]
                with lock:
                    res = backend.decide(str(req.get("state", "")), qs)
                self._send(200, {"backend": res.backend, "temperature": res.temperature,
                                 "timing": res.timing.__dict__, "plan": res.plan.to_dict(),
                                 "decisions": [{"probs": list(d.probs)} for d in res.decisions]})
            except Exception as e:  # noqa: BLE001
                self._send(400, {"error": repr(e)})

    return ThreadingHTTPServer((host, port), Handler)


def serve(backend: Backend, host: str = "127.0.0.1", port: int = 8788) -> None:
    srv = make_server(backend, host, port)
    print(f"jev serve: {backend.name} on http://{host}:{srv.server_address[1]}", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        srv.server_close()
