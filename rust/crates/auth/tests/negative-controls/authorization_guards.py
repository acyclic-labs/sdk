"""Qualify actual authorization guard mutations against existing public properties."""
import argparse
import hashlib
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[5]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("mode", choices=["request", "dns", "socket-work"])
MODE = parser.parse_args().mode
SOURCE = ROOT / f"rust/crates/auth/src/{'authority' if MODE == 'request' else 'socket'}.rs"
TEST = ROOT / "rust/crates/auth/tests/request_hardening.rs"
HELPER = TEST.parent / f"common/{'request' if MODE == 'request' else 'dns'}_hardening.rs"
PROPERTY = ("noncanonical_resources_and_audiences_never_match" if MODE == "request"
            else "numeric_dns_names_are_rejected_by_socket_and_egress_admission")
CONTROLS = ([("request_resource_guard", "NonCanonicalResource"),
             ("request_audience_guard", "NonCanonicalAudience")] if MODE == "request"
            else [("dns_numeric_guard", "!admit(name)")])
COMMAND = ["cargo", "test", "--locked", "--all-features", "-p", "acyclic-auth",
           "--test", "main", f"request_hardening::{PROPERTY}",
           "--", "--exact", "--nocapture"]
PATHS = [SOURCE, TEST, HELPER]
if MODE == "socket-work":
    TEST = SOURCE.parent / "socket/tests.rs"
    PATHS = [SOURCE, TEST]
    CONTROLS = [("socket_validation_work", "left: 32")]
    COMMAND = ["cargo", "test", "--locked", "--all-features", "-p", "acyclic-auth",
               "--lib", "socket::tests::destination_validation_is_once_per_grant_not_once_per_rule",
               "--", "--exact", "--nocapture"]


def refuse(step):
    raise SystemExit(f"STEP {step}; recovery: git diff -- {SOURCE.relative_to(ROOT)}; "
                     f"python3 rust/crates/auth/tests/negative-controls/authorization_guards.py {MODE}")


def run(label):
    result = subprocess.run(COMMAND, cwd=ROOT, capture_output=True, text=True)
    output = result.stdout + result.stderr
    print(label, "exit", result.returncode, output, flush=True)
    return result.returncode, output


original = {path: path.read_bytes() for path in PATHS}
for guard, denial in CONTROLS:
    try:
        patch = Path(__file__).with_name(f"{guard}.patch")
        for command in [["git", "apply", "--check", str(patch)], ["git", "apply", str(patch)]]:
            if subprocess.run(command, cwd=ROOT).returncode:
                refuse(f"{guard} mutation application failed")
        code, output = run(f"ACTUAL_{guard.upper()}_GUARD_REMOVAL")
        if not code or "0 passed; 1 failed;" not in output or denial not in output or (MODE != "socket-work" and "minimal failing input:" not in output):
            refuse(f"{guard} mutation missed the surviving assertion")
        print("AUTHORIZATION_GUARD_EXPECTED_RED", guard, flush=True)
    finally:
        SOURCE.write_bytes(original[SOURCE])
        if any(path.read_bytes() != data for path, data in original.items()):
            refuse("byte-identical source/property restoration failed")
code, output = run("RESTORED_AUTHORIZATION_PRODUCTION")
if code or "1 passed; 0 failed;" not in output:
    refuse("restored public property failed")
print("AUTHORIZATION_GUARDS_RED_THEN_GREEN", {str(path.relative_to(ROOT)): hashlib.sha256(data).hexdigest()
                                      for path, data in original.items()}, flush=True)
