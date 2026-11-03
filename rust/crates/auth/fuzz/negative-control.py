"""Require the credential fuzz oracle to catch a removed production check."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parent
source = root.parent / "src" / "credential.rs"
oracle = root.parent / "tests" / "common" / "credential.rs"
check = b"        && !value.contains(['\\r', '\\n', '\\0'])"
step = "read original credential source and oracle"
try:
    original = source.read_bytes()
    oracle_hash = hashlib.sha256(oracle.read_bytes()).digest()
    step = "select production delimiter check"
    try:
        if original.count(check) != 1:
            raise RuntimeError("expected exactly one production delimiter check")
        source.write_bytes(original.replace(check, b""))
        command = ["cargo", "+nightly", "fuzz", "run", "--dev"]
        if target := os.environ.get("CARGO_TARGET_DIR"):
            command += ["--target-dir", target]
        command += ["credential", "corpus/credential/v2-opaque-bearer", "--", "-runs=1"]
        step = "detect omitted delimiter admission through the credential fuzzer"
        result = subprocess.run(
            command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        print(result.stdout, end="")
        if result.returncode == 0 or "credential transport delimiter must refuse" not in result.stdout:
            raise RuntimeError("the unchanged semantic oracle did not detect the production defect")
    finally:
        source.write_bytes(original)
    step = "verify unchanged credential oracle"
    if hashlib.sha256(oracle.read_bytes()).digest() != oracle_hash:
        raise RuntimeError("credential oracle changed during the negative control")
except (OSError, RuntimeError) as error:
    print(
        f"STEP {step} failed: {error}; inspect the original output and correct the source or command; "
        "recovery from the repository root: python3 rust/crates/auth/fuzz/negative-control.py",
        file=sys.stderr,
    )
    sys.exit(1)
print("credential negative control: original delimiter-refusal oracle detected the production defect")
