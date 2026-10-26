"""Capture real isolated Dex output. All configured accounts are public test inputs."""
import base64
import hashlib
import json
import pathlib
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    root = pathlib.Path(sys.argv[1])
    issuer = "http://127.0.0.1:15482/dex"
    opener = urllib.request.build_opener(NoRedirect)
    captures = {}
    values = {}
    for name, route in [("discovery", "/.well-known/openid-configuration"),
                        ("jwks", "/keys"), ("token", "/token"), ("auth", "/token")]:
        data = None
        headers = {}
        if name in ("token", "auth"):
            data = urllib.parse.urlencode({"grant_type": "password", "scope": "openid email",
                    "username": "admin@example.com", "password": "password",
                    "client_id": "conformance-turns",
                    "client_secret": "conformance-public-test-secret" if name == "token" else "invalid-public-test-secret"}).encode()
            headers["content-type"] = "application/x-www-form-urlencoded"
            (root / f"{name}.request.txt").write_bytes(data)
        request = urllib.request.Request(issuer + route, data=data, headers=headers)
        try:
            response = opener.open(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read(1_048_577)
            status = response.status
            (root / f"{name}.headers.txt").write_bytes(str(response.headers).encode())
        if len(body) > 1_048_576 or status != (401 if name == "auth" else 200):
            raise RuntimeError(f"{name} unexpected status/size: {status}/{len(body)}")
        (root / f"{name}.body").write_bytes(body)
        captures[name] = {"file": f"{name}.body", "status": status,
                          "url": issuer + route, "sha256": hashlib.sha256(body).hexdigest()}
        values[name] = json.loads(body)
        print(f"OIDC_CAPTURE {name} status={status} bytes={len(body)} sha256={captures[name]['sha256']}")
    encoded = values["token"]["id_token"].split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
    manifest = {"source": "bb2 isolated Dex v2.44.0; public disposable account; no production credentials",
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "issuer": issuer, "audience": "conformance-turns", "subject": claims["sub"],
                "jwks_uri": issuer + "/keys", "captures": captures}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"STEP_FAILED dex-capture: {error}; recovery: pwsh -NoProfile -File Q:/lanes/bs.ps1 bash scripts/be-edge-oidc.sh", file=sys.stderr)
        raise SystemExit(1)
