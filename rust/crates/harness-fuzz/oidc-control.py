"""Challenge OIDC signature admission with the unchanged public-API oracle."""
import argparse
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tomllib

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("control", nargs="?", choices=("signature", "weak-issuer", "rsa-key"), default="signature")
control = parser.parse_args().control
root = Path(__file__).resolve().parents[3]
source = root / "rust/crates/harness-core/src/oidc" / ("claims.rs" if control == "signature" else "keys.rs")
oracle = root / "rust/crates/harness-fuzz/fuzz_targets/oidc.rs"
original = source.read_bytes()
digest = hashlib.sha256(oracle.read_bytes()).digest()
command = ["cargo", "+nightly", "fuzz", "run", "--dev", "--fuzz-dir",
           "rust/crates/harness-fuzz", "--no-default-features", "--features", "oidc",
           "--target-dir", os.environ.get("CARGO_TARGET_DIR", str(root / "target")), "oidc", "--"]
coverage = command + ["-runs=1000", "-seed=1", "-max_len=4096"]
probe = coverage if control == "signature" else command + ["-runs=1", "-seed=1"]
assertion = {
    "signature": "OIDC signature binding lost",
    "weak-issuer": "OIDC weak issuer forged an authenticated identity",
    "rsa-key": "OIDC invalid RSA modulus replaced last-good keys",
}[control]
step = "require the resolved graph to omit vulnerable RSA private-key support"
try:
    packages = tomllib.loads((root / "Cargo.lock").read_text())["package"]
    if any(package["name"] in {"rsa", "jsonwebtoken"} for package in packages):
        raise RuntimeError("the resolved graph retains the retired JWT provider")
    if control != "rsa-key":
        step = "qualify original captured signatures and the public OIDC decoders"
        subprocess.run(probe, cwd=root, check=True)
    step = f"detect the production {control} mutation"
    try:
        if control == "weak-issuer":
            subprocess.run(["git", "apply", "rust/crates/harness-fuzz/weak-issuer.patch"], cwd=root, check=True)
        elif control == "rsa-key":
            subprocess.run(["git", "apply", "rust/crates/harness-fuzz/rsa-key.patch"], cwd=root, check=True)
        else:
            before = b"    key.verify("
            end = b"        &signature,\n    )?;"
            if original.count(before) != 1 or original.count(end) != 1:
                raise RuntimeError("expected one signature admission boundary")
            source.write_bytes(original.replace(before, b"    let _ = key.verify(").replace(end, b"        &signature,\n    );"))
        result = subprocess.run(probe, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(result.stdout, end="")
        if result.returncode == 0 or assertion not in result.stdout:
            raise RuntimeError("the unchanged original oracle did not detect the planted defect")
    finally:
        source.write_bytes(original)
    step = "pass the unchanged oracle after restoring production source"
    if hashlib.sha256(oracle.read_bytes()).digest() != digest:
        raise RuntimeError("the signature oracle changed during the control")
    subprocess.run(coverage, cwd=root, check=True)
except (OSError, RuntimeError, subprocess.CalledProcessError) as error:
    print(f"STEP {step} failed: {error}; recovery from repository root: "
          f"python3 rust/crates/harness-fuzz/oidc-control.py {control}", file=sys.stderr)
    sys.exit(1)
print(f"OIDC {control} control PASS: vulnerable provider absent; captured signature accepted; production mutation detected; source restored")
