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
        contents = binary.read_bytes()
        if not contents or hashlib.sha256(contents).hexdigest() != sha256:
            raise ValueError("GraphCoder artifact hash mismatch")
        return cls(contents, sha256, source_sha)


def catalog(model_name: str, base_url: str, context_tokens: int, output_tokens: int):
    """One explicit endpoint; no key, auth-file, or host-login option exists."""
    if not isinstance(model_name, str) or "/" not in model_name:
        raise ValueError("model_name must be provider/model")
    provider, model = model_name.split("/", 1)
    if not provider or not model or any(c.isspace() for c in model_name):
        raise ValueError("invalid provider/model")
    url = urlsplit(base_url)
    _ = url.port  # Reject malformed ports before producing a job or catalogue.
    if (url.scheme not in ("http", "https") or not url.hostname
            or url.username is not None or url.password is not None
            or url.query or url.fragment):
        raise ValueError("endpoint must be an explicit public URL without credentials")
    if (type(context_tokens) is not int or type(output_tokens) is not int
            or not 0 < output_tokens <= context_tokens):
        raise ValueError("explicit positive model capacities required")
    return {"benchmark": {
        "adapter": "responses", "authentication": "optional", "base_url": base_url,
        "models": {model: {"capacity": {"context_tokens": context_tokens,
                                      "output_tokens": output_tokens},
                           "features": {"tools": True, "images": False}}},
        "max_event_bytes": 1048576,
        "token_counter": {"path": "responses/input_tokens", "adapter": "responses",
                          "response_field": "input_tokens", "framing_tokens": 0},
    }}


def run_command(binary: str, state: str, logs: str, model: str, instruction: str) -> str:
    """Keep prompt and every path as single shell arguments, including newlines."""
    home = str(PurePosixPath(state) / "home")
    arguments = [
        "HOME=" + home,
        "USERPROFILE=" + home, "CODEX_HOME=" + str(PurePosixPath(state) / "fixture-codex"),
        "XDG_CONFIG_HOME=" + home, "XDG_DATA_HOME=" + home,
        "XDG_STATE_HOME=" + home, "XDG_CACHE_HOME=" + home,
        "TMPDIR=" + str(PurePosixPath(state) / "tmp"),
        binary, "--catalog", str(PurePosixPath(state) / "catalog.json"),
        "--settings-root", home, "--model", "benchmark/" + model.split("/", 1)[1],
        "--prompt=" + instruction,
    ]
    # --workspace defaults to the task's native working directory. Redirection
    # preserves GraphCoder's exit status rather than that of a logging pipeline.
    return ('env -i PATH="$PATH" ' + shlex.join(arguments) + " > " + shlex.quote(str(PurePosixPath(logs) / "graphcoder.jsonl"))
            + " 2> " + shlex.quote(str(PurePosixPath(logs) / "graphcoder.stderr")))
