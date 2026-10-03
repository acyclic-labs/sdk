import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

class Fixture(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write((fmt % args) + "\n")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            self.send_error(400, "invalid JSON")
            return
        if self.headers.get("Authorization") != "Bearer bash-fixture-token":
            self._reply(401, {"code": "unauthenticated"})
            return
        if self.path == "/v1/actors/invoke":
            assert payload["actorId"] == "actor-1", payload
            assert payload["body"] == "AQID", payload
            assert payload["url"] == "https://example.test", payload
            self._reply(200, {"body": "AQID", "status": 200, "headers": []})
            return
        if self.path == "/v1/actors/checkpoint":
            assert payload["actorId"] == "actor-1", payload
            self._reply(200, {"actorId": "actor-1", "configurationRevision": "18446744073709551615"})
            return
        self.send_error(404)

    def _reply(self, status, response):
        encoded = json.dumps(response, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

if __name__ == "__main__":
    server = HTTPServer(("127.0.0.1", int(sys.argv[1])), Fixture)
    print(server.server_address[1], flush=True)
    server.serve_forever()
