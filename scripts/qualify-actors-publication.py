#!/usr/bin/env python3
"""Bind the existing publication models to actual production-source defects."""
import hashlib
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "rust/crates/actors/src/local.rs"
PREFIX = "local::model::composition::"
CONTROLS = (
    ("definite_refusals_settle_and_leave_the_input_available",
     "Err(source @ (StreamError::Unavailable | StreamError::InvalidResponse))",
     "Err(source)", "definite refusal must settle its original participant"),
    ("a_retry_refusal_cannot_settle_an_earlier_lost_ack",
     "if prepared.acknowledgement_unknown {", "if false {",
     "retry refusal incorrectly settled an uncertain publication"),
)


def run(names, diagnostic=None):
    expression = " | ".join("test(=" + PREFIX + name + ")" for name in names)
    command = ["cargo", "nextest", "run", "--locked", "-p", "acyclic-actors",
               "--lib", "--no-fail-fast", "-E", expression]
    print("STEP actual publication models: " + repr(command), flush=True)
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    output = result.stdout + result.stderr
    print(output, flush=True)
    valid = (result.returncode == 0 and re.search(r"2 tests run: 2 passed", output)
             if diagnostic is None else
             result.returncode != 0 and diagnostic in output
             and re.search(r"1 test run: 0 passed, 1 failed", output)
             and PREFIX + names[0] in output)
    if not valid:
        raise RuntimeError("publication models did not establish the expected source verdict; "
                           "recovery: inspect original diagnostics, correct source or command, "
                           "then python3 scripts/qualify-actors-publication.py")


def main():
    original = SOURCE.read_bytes()
    source = original.decode()
    if any(source.count(anchor) != 1 for _, anchor, _, _ in CONTROLS):
        raise RuntimeError("publication mutation anchor changed; recovery: inspect "
                           "Store::submit_retained before python3 scripts/qualify-actors-publication.py")
    print("Publication production sha256=" + hashlib.sha256(original).hexdigest(), flush=True)
    try:
        for name, anchor, replacement, diagnostic in CONTROLS:
            SOURCE.write_bytes(source.replace(anchor, replacement).encode())
            run([name], diagnostic)
        SOURCE.write_bytes(original)
        run([name for name, _, _, _ in CONTROLS])
    finally:
        SOURCE.write_bytes(original)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("STEP publication qualification failed: " + str(error), file=sys.stderr)
        sys.exit(1)
