import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    authorized_requests = 0

    def do_POST(self):
        if self.headers.get("authorization") != "Bearer fixture-token":
            self.send_response(401)
            body = b'{"error":"AUTH_REQUIRED"}'
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        Handler.authorized_requests += 1
        if Handler.authorized_requests == 1:
            self.send_response(503)
            body = b'{"error":"RECOVERABLE"}'
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if any(part in self.path for part in ("/stream/", "/watch", "/follow")):
            body = b'{"cursor":1}\n{"cursor":2}\n'
        else:
            body = json.dumps({"operation": self.path, "status": "ok"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1])
    HTTPServer(("127.0.0.1", port), Handler).serve_forever()
