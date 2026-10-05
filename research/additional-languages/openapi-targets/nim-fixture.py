import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

class Fixture(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write((fmt % args) + "\n")

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        payload = json.loads(self.rfile.read(length).decode("utf-8"))
        if self.headers.get("Authorization") != "Bearer bash-fixture-token":
            self.reply(401, {"code": "unauthenticated"})
            return
        if self.path == "/v1/actors/invoke":
            self.reply(200, {"family": "actors", "body": "AQID"})
            return
        if self.path == "/v1/stream/read":
            if payload.get("limit") == 0:
                self.reply(503, {"code": "unavailable", "retryable": True})
            else:
                self.reply(200, {"record": {"value": "AQID", "sequence": "1"}})
            return
        self.reply(404, {"code": "not_found"})

    def reply(self, status, body):
        encoded = json.dumps(body, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

if __name__ == "__main__":
    HTTPServer(("127.0.0.1", int(sys.argv[1])), Fixture).serve_forever()
