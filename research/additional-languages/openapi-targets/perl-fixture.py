import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


class Fixture(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write((fmt % args) + "\n")

    def _reply(self, status, payload):
        encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_POST(self):
        if self.headers.get("Authorization") != "Bearer perl-fixture-token":
            self._reply(401, {"code": "unauthenticated", "message": "bad token"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length))
        if self.path == "/v1/actors/invoke":
            assert payload["actorId"] == "actor-1", payload
            assert payload["body"] == "AQID", payload
            assert payload["method"] == "POST", payload
            self._reply(200, {"body": "AQID", "status": 200, "headers": []})
            return
        if self.path == "/v1/workers/deployments/prod/invoke":
            assert payload["body"] == "AQID", payload
            self._reply(200, {"body": "AQID", "resolvedRevision": "18446744073709551615", "status": 200, "resolvedSha256": "AQID", "headers": []})
            return
        if self.path == "/v1/stream/read":
            assert payload["path"] == "root", payload
            if payload.get("limit") == 0:
                self._reply(503, {"code": "unavailable", "retryable": True})
            else:
                self._reply(200, {"record": {"commitId": "c1", "committedAtMicros": "1", "sequence": "18446744073709551615", "value": "AQID"}})
            return
        if self.path == "/v2/objects/objects/put":
            assert payload["body"] == "AQID", payload
            assert payload["complete"] is True, payload
            self._reply(200, {"etag": "etag-fixture", "size": "3"})
            return
        if self.path == "/v1/inference/runs/generate":
            assert payload["context"] == "AQID", payload
            assert payload["maximumOutput"] == "18446744073709551615", payload
            self._reply(200, {"run": {"runId": "run-fixture", "input": "AQID", "lastSequence": "18446744073709551615", "model": "fixture", "cancellationRequested": False}})
            return
        self.send_error(404)


if __name__ == "__main__":
    server = HTTPServer(("127.0.0.1", int(sys.argv[1])), Fixture)
    print(server.server_address[1], flush=True)
    server.serve_forever()
