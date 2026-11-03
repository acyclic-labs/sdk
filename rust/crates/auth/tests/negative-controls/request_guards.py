"""Qualify both request guards against the unchanged public authorization property."""
import hashlib
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[5]
SOURCE = ROOT / "rust/crates/auth/src/authority.rs"
TEST = ROOT / "rust/crates/auth/tests/request_hardening.rs"
HELPER = TEST.parent / "common/request_hardening.rs"
COMMAND = ["cargo", "test", "--locked", "--all-features", "-p", "acyclic-auth",
           "--test", "main", "request_hardening::noncanonical_resources_and_audiences_never_match",
           "--", "--exact", "--nocapture"]


def refuse(step):
    raise SystemExit(f"STEP {step}; recovery: git diff -- rust/crates/auth/src/authority.rs; "
                     "python3 rust/crates/auth/tests/negative-controls/request_guards.py")


def run(label):
    result = subprocess.run(COMMAND, cwd=ROOT, capture_output=True, text=True)
    output = result.stdout + result.stderr
    print(label, "exit", result.returncode, output, flush=True)
    return result.returncode, output


original = {path: path.read_bytes() for path in (SOURCE, TEST, HELPER)}
for guard, denial in [("resource", "NonCanonicalResource"), ("audience", "NonCanonicalAudience")]:
    try:
        patch = Path(__file__).with_name(f"request_{guard}_guard.patch")
        for command in [["git", "apply", "--check", str(patch)], ["git", "apply", str(patch)]]:
            if subprocess.run(command, cwd=ROOT).returncode:
                refuse(f"{guard} mutation application failed")
        code, output = run(f"ACTUAL_{guard.upper()}_GUARD_REMOVAL")
        if not code or "0 passed; 1 failed;" not in output or "minimal failing input:" not in output or denial not in output:
            refuse(f"{guard} mutation missed the public property's typed-denial assertion")
        print("REQUEST_GUARD_EXPECTED_RED", guard, flush=True)
    finally:
        SOURCE.write_bytes(original[SOURCE])
        if any(path.read_bytes() != data for path, data in original.items()):
            refuse("byte-identical source/property restoration failed")
code, output = run("RESTORED_REQUEST_PRODUCTION")
if code or "1 passed; 0 failed;" not in output:
    refuse("restored public property failed")
print("REQUEST_GUARDS_RED_THEN_GREEN", {str(path.relative_to(ROOT)): hashlib.sha256(data).hexdigest()
                                      for path, data in original.items()}, flush=True)
