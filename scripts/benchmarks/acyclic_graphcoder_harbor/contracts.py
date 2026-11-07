"""Credential-free benchmark inputs, content identity and shell argument framing."""

import hashlib
import re
import shlex
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit


def digest(value: str, length: int) -> str:
    if not isinstance(value, str) or re.fullmatch(rf"[0-9a-f]{{{length}}}", value) is None:
        raise ValueError(f"expected a full lowercase {length}-digit digest")
    return value


@dataclass(frozen=True)
class Artifact:
    """The bytes that were verified are the bytes that will be uploaded."""

    contents: bytes
    sha256: str
    source_sha: str

    def __post_init__(self):
        digest(self.sha256, 64)
        digest(self.source_sha, 40)
        if type(self.contents) is not bytes:
            raise ValueError("artifact contents must be immutable bytes")
        if not self.contents or hashlib.sha256(self.contents).hexdigest() != self.sha256:
            raise ValueError("GraphCoder artifact hash mismatch")

    @classmethod
    def read(cls, path: str, sha256: str, source_sha: str):
        digest(sha256, 64)
        digest(source_sha, 40)
        binary = Path(path)
        if binary.name != "graphcoder" or binary.is_symlink():
            raise ValueError("select the regular Linux graphcoder build artifact")
        return cls(binary.read_bytes(), sha256, source_sha)


def catalog(model_name: str, base_url: str, context_tokens: int, output_tokens: int,
            *, allow_http_host: str | None = None, extra_env: dict | None = None):
    """One explicit endpoint; no key, auth-file, or host-login option exists."""
    if not isinstance(model_name, str) or "/" not in model_name:
        raise ValueError("model_name must be provider/model")
    provider, model = model_name.split("/", 1)
    if not provider or not model or any(c.isspace() for c in model_name):
        raise ValueError("invalid provider/model")
    if extra_env is None:
        extra_env = {}
    allowed = {"OPENAI_BASE_URL", "ACYCLIC_PROVIDER_AUTH_HEADER", "ACYCLIC_PROVIDER_AUTH_TOKEN"}
    if not isinstance(extra_env, dict) or any(name not in allowed or not isinstance(value, str) or any(c in value for c in "\r\n\0")
           for name, value in extra_env.items()):
        raise ValueError("catalog extra_env refused: raw provider keys and unknown variables are forbidden; recovery: pass --agent-kwarg 'extra_env={\"OPENAI_BASE_URL\":\"https://proxy.example/v1\"}' with only the paired ACYCLIC_PROVIDER_AUTH_HEADER/TOKEN proxy fields when required")
    base_url = extra_env.get("OPENAI_BASE_URL", base_url)
    header = extra_env.get("ACYCLIC_PROVIDER_AUTH_HEADER")
    token = extra_env.get("ACYCLIC_PROVIDER_AUTH_TOKEN")
    headers = {}
    if header is not None or token is not None:
        if (not header or not token or re.fullmatch(r"[A-Za-z0-9-]+", header) is None
                or header.lower() in {"authorization", "x-api-key", "x-goog-api-key", "cookie",
                                      "host", "content-length", "content-type"}):
            raise ValueError("catalog proxy authentication refused: header/token pair is missing or reserved; recovery: pass --agent-kwarg 'extra_env={\"ACYCLIC_PROVIDER_AUTH_HEADER\":\"x-proxy-token\",\"ACYCLIC_PROVIDER_AUTH_TOKEN\":\"PROXY_TOKEN\"}'")
        headers[header] = token
    url = urlsplit(base_url)
    _ = url.port  # Reject malformed ports before producing a job or catalogue.
    if (url.scheme not in ("http", "https") or not url.hostname
            or url.username is not None or url.password is not None
            or url.query or url.fragment):
        raise ValueError("endpoint must be an explicit public URL without credentials")
    if (type(context_tokens) is not int or type(output_tokens) is not int
            or not 0 < output_tokens <= context_tokens):
        raise ValueError("explicit positive model capacities required")
    result = {"benchmark": {
        "adapter": "responses", "authentication": "optional", "base_url": base_url,
        "models": {model: {"capacity": {"context_tokens": context_tokens,
                                      "output_tokens": output_tokens},
                           "features": {"tools": True, "images": False}}},
        "max_event_bytes": 1048576,
        "token_counter": {"path": "responses/input_tokens", "adapter": "responses",
                          "response_field": "input_tokens", "framing_tokens": 0},
    }}
    if allow_http_host is not None:
        result["benchmark"]["allow_http_host"] = allow_http_host
    if headers:
        result["benchmark"]["headers"] = headers
    return result


def run_command(binary: str, state: str, logs: str, model: str, instruction: str) -> str:
    """Keep prompt and every path as single shell arguments, including newlines."""
    home = str(PurePosixPath(state) / "home")
    arguments = [
        "HOME=" + home,
        "USERPROFILE=" + home, "CODEX_HOME=" + str(PurePosixPath(state) / "fixture-codex"),
        "XDG_CONFIG_HOME=" + home, "XDG_DATA_HOME=" + home,
        "XDG_STATE_HOME=" + home, "XDG_CACHE_HOME=" + home,
        "TMPDIR=" + str(PurePosixPath(state) / "tmp"),
        "ACYCLIC_SIGNING_CUSTODY=private-file",
        "ACYCLIC_SIGNING_CUSTODY_DIRECTORY=" + str(PurePosixPath(state) / "custody"),
        binary, "--catalog", str(PurePosixPath(state) / "catalog.json"),
        "--settings-root", home, "--model", "benchmark/" + model.split("/", 1)[1],
        "--prompt=" + instruction,
    ]
    # --workspace defaults to the task's native working directory. Redirection
    # preserves GraphCoder's exit status rather than that of a logging pipeline.
    return ('env -i PATH="$PATH" ' + shlex.join(arguments) + " > " + shlex.quote(str(PurePosixPath(logs) / "graphcoder.jsonl"))
            + " 2> " + shlex.quote(str(PurePosixPath(logs) / "graphcoder.stderr")))
