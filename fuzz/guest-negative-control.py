"""Require the independent metadata oracle to detect real admission mutations."""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent
source = root.parent / "rust/crates/actors/src/guest/runtime.rs"
original = source.read_text()
cases = [
    ("serde bypass", "Self::new(raw.version, raw.handle)",
     "Ok(Self { version: raw.version, handle: raw.handle })",
     "handle-zero-version.json", "metadata deserialization admission"),
    ("constructor byte bound", "if value.len() > MAX_GUEST_METADATA_BYTES {", "if false {",
     "handle-overlong.json", "metadata constructor admission"),
    ("expiry canonicalization", "if expiry == 0 || !canonical_expiry(&raw.expires_at_ms) {", "if false {",
     "caller-expiry-alias.json", "metadata deserialization admission"),
]
for name, check, mutation, seed, failure in cases:
    assert check in original, f"{name}: update the mutation"
    try:
        source.write_text(original.replace(check, mutation, 1))
        result = subprocess.run(
            ["cargo", "+nightly", "fuzz", "run", "--dev", "guest_json",
             f"corpus/guest_json/{seed}", "--", "-runs=1"],
            cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        assert result.returncode != 0, f"{name} was not detected"
        assert failure in result.stdout, result.stdout
        print(f"negative control: detected {name}", flush=True)
    finally:
        source.write_text(original)
