"""Require an unchanged Auth fuzz oracle to catch a production defect."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parent
choice = sys.argv[1] if len(sys.argv) == 2 else "delimiter"
if choice.startswith("issuance-"):
    target = "capability"
    source = root.parent / "src/trust" / ("issuance/wire.rs" if choice == "issuance-reply" else "issuance.rs")
    oracle = root / "fuzz_targets/capability.rs"
elif choice == "trust-target":
    target = "trust_record"
    source = root.parent / "src/trust/follower.rs"
    oracle = root / "fuzz_targets/trust_record.rs"
else:
    target = "credential"
    source = root.parent / "src/credential.rs"
    oracle = root.parent / "tests/common/credential.rs"
step = "read original Auth source and oracle"
try:
    original = source.read_bytes()
    oracle_hash = hashlib.sha256(oracle.read_bytes()).digest()
    newline = b"\r\n" if b"\r\n" in original else b"\n"
    mutations = {
        "delimiter": (
            b"        && !value.contains(['\\r', '\\n', '\\0'])", b"",
            "credential transport delimiter must refuse", "v2-opaque-bearer",
        ),
        "text-decoder": (
            b"base64::engine::general_purpose::URL_SAFE_NO_PAD" + newline + b"        .decode_vec",
            b"base64::engine::general_purpose::STANDARD_NO_PAD" + newline + b"        .decode_vec",
            "canonical capability text must decode original bytes", "v2-signed-capability",
        ),
        "json-field": (
            b"super::capability_from_text(&encoded).map_err(serde::de::Error::custom)",
            b"Ok(Vec::new())",
            "capability JSON must retain original bytes", "v2-signed-capability",
        ),
        "trust-target": (
            b"if !matches {", b"if false && !matches {",
            "trust record targets must be disjoint", "foreign-keyring",
        ),
        "issuance-request": (
            b"if request.wire() != bytes {", b"if false && request.wire() != bytes {",
            "duplicate issuance request version must refuse", "issuance-request",
        ),
        "issuance-reply": (
            b"if reply.encode_to_vec() != bytes {", b"if false && reply.encode_to_vec() != bytes {",
            "duplicate issuance reply version must refuse", "issuance-unknown",
        ),
    }
    step = "select production Auth mutation"
    if len(sys.argv) > 2 or choice not in mutations:
        raise RuntimeError("select delimiter, text-decoder, json-field, trust-target, issuance-request or issuance-reply")
    check, replacement, message, seed = mutations[choice]
    try:
        if original.count(check) != 1:
            raise RuntimeError("expected exactly one production mutation site")
        source.write_bytes(original.replace(check, replacement))
        command = ["cargo", "+nightly", "fuzz", "run", "--dev"]
        if cargo_target := os.environ.get("CARGO_TARGET_DIR"):
            command += ["--target-dir", cargo_target]
        command += [target, "corpus/" + target + "/" + seed, "--", "-runs=1"]
        step = "detect " + choice + " through the " + target + " fuzzer"
        result = subprocess.run(
            command, cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        print(result.stdout, end="")
        panic = result.stdout.partition("panicked at ")[2]
        if (result.returncode == 0 or message not in panic
                or "ERROR: libFuzzer: deadly signal" not in panic):
            raise RuntimeError("the unchanged semantic oracle did not detect the production defect")
    finally:
        source.write_bytes(original)
    step = "verify unchanged Auth oracle"
    if hashlib.sha256(oracle.read_bytes()).digest() != oracle_hash:
        raise RuntimeError("Auth oracle changed during the negative control")
except (OSError, RuntimeError) as error:
    print(
        f"STEP {step} failed: {error}; inspect the original output and correct the source or command; "
        f"recovery from the repository root: python3 rust/crates/auth/fuzz/negative-control.py {choice}",
        file=sys.stderr,
    )
    sys.exit(1)
print("Auth negative control: unchanged " + choice + " oracle detected the production defect")
