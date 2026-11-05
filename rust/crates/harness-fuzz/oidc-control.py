"""Detect discarded OIDC signature refusals with the unchanged public-API oracle."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tomllib

root = Path(__file__).resolve().parents[3]
source = root / "rust/crates/harness-core/src/oidc/claims.rs"
oracle = root / "rust/crates/harness-fuzz/fuzz_targets/oidc.rs"
original = source.read_bytes()
digest = hashlib.sha256(oracle.read_bytes()).digest()
command = ["cargo", "+nightly", "fuzz", "run", "--dev", "--fuzz-dir",
           "rust/crates/harness-fuzz", "--no-default-features", "--features", "oidc",
           "--target-dir", os.environ.get("CARGO_TARGET_DIR", str(root / "target")), "oidc", "--", "-runs=1000", "-seed=1", "-max_len=4096"]
step = "require the resolved graph to omit vulnerable RSA private-key support"
try:
    packages = tomllib.loads((root / "Cargo.lock").read_text())["package"]
    if any(package["name"] in {"rsa", "jsonwebtoken"} for package in packages):
        raise RuntimeError("the resolved graph retains the retired JWT provider")
    step = "qualify original captured signatures and the public OIDC decoders"
    subprocess.run(command, cwd=root, check=True)
    step = "detect a discarded production signature refusal"
    before = b"    key.verify("
    end = b"        &signature,\n    )?;"
    if original.count(before) != 1 or original.count(end) != 1:
        raise RuntimeError("expected one signature admission boundary")
    try:
        source.write_bytes(original.replace(before, b"    let _ = key.verify(").replace(end, b"        &signature,\n    );"))
        result = subprocess.run(command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(result.stdout, end="")
        if result.returncode == 0 or "OIDC signature binding lost" not in result.stdout:
            raise RuntimeError("the unchanged original oracle did not detect the planted defect")
    finally:
        source.write_bytes(original)
    step = "pass the unchanged oracle after restoring production source"
    if hashlib.sha256(oracle.read_bytes()).digest() != digest:
        raise RuntimeError("the signature oracle changed during the control")
    subprocess.run(command, cwd=root, check=True)
except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
    print(f"STEP {step} failed: {error}; recovery from repository root: "
          "python3 rust/crates/harness-fuzz/oidc-control.py", file=sys.stderr)
    sys.exit(1)
print("OIDC control PASS: vulnerable provider absent; real captured signature accepted; discarded refusal detected; source restored")
