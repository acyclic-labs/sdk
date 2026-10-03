import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


MAX_U64 = "18446744073709551615"


class Fixture(BaseHTTPRequestHandler):
    def log_message(self, *_):
        return

    def _reply(self, status, value):
        body = json.dumps(value).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.headers.get("Authorization") != "Bearer powershell-fixture-token":
            self._reply(401, {"code": "unauthenticated", "message": "token required"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length))
        if self.path == "/v1/actors/invoke":
            assert payload["actorId"] == "actor-1", payload
            assert payload["body"] == "AQID", payload
            self._reply(200, {"body": "AQID", "status": 200, "headers": []})
        elif self.path == "/v1/workers/deployments/prod/invoke":
            assert payload["body"] == "AQID", payload
            self._reply(200, {"body": "b2s=", "resolvedRevision": MAX_U64, "resolvedSha256": "AQID", "status": 200, "headers": []})
        elif self.path == "/v1/stream/read":
            assert payload["path"] == "root", payload
            if payload.get("limit") == 0:
                self._reply(503, {"code": "unavailable", "retryable": True})
            else:
                self._reply(200, {"record": {"commitId": "c1", "committedAtMicros": "1", "sequence": MAX_U64, "value": "AQID"}})
        elif self.path == "/v2/objects/objects/put":
            assert payload["body"] == "AQID", payload
            assert payload["complete"] is True, payload
            self._reply(200, {"etag": "etag-powershell", "lastModified": "2026-10-04T00:00:00Z", "metadata": {}, "size": "3"})
        elif self.path == "/v1/inference/runs/generate":
            assert payload["context"] == "AQID", payload
            assert payload["maximumOutput"] == MAX_U64, payload
            self._reply(200, {"run": {"cancellationRequested": False, "input": "AQID", "lastSequence": MAX_U64, "model": "fixture", "result": {}, "runId": "run-powershell"}})
        else:
            self._reply(404, {"code": "not_found"})


if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1])), Fixture).serve_forever()
