#!/usr/bin/env python3
"""Qualify the socket commit model against isolated production-source defects."""
import hashlib
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "rust/crates/actors/src/local/sockets.rs"
MODEL = "local::sockets::loom_tests::concurrent_closes_and_messages_deliver_exactly_one_final_close"
RECOVERY = "inspect original diagnostics, correct source or command, then python3 scripts/qualify-actors-sockets.py"
CONTROLS = (
    ("Close socket guard",
     "    conditions.push(CommitCondition::Tail {\n        path: socket.clone(),\n        expected: 1,\n    });\n",
     "", "socket delivered other than exactly one Close"),
    ("message socket guard",
     "                vec![CommitCondition::Tail {\n                    path: layout.socket(connection)?,\n                    expected: 1,\n                }],",
     "                Vec::new(),", "an event followed the Close"),
)


def run(diagnostic=None):
    command = ["cargo", "test", "--locked", "-p", "acyclic-actors", "--features",
               "model-checking", "--lib", MODEL, "--", "--exact", "--nocapture"]
    environment = dict(os.environ, RUSTC_WORKSPACE_WRAPPER=str(
        ROOT / "scripts" / ("loom-rustc.cmd" if os.name == "nt" else "loom-rustc.sh")))
    print("STEP actual socket model: " + repr(command), flush=True)
    with subprocess.Popen(command, cwd=ROOT, env=environment, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True) as process:
        lines = []
        for line in process.stdout:
            print(line, end="", flush=True)
            lines.append(line)
        status = process.wait()
    output = "".join(lines)
    valid = (status == 0 and "1 passed; 0 failed" in output
             if diagnostic is None else
             status != 0 and "0 passed; 1 failed" in output
             and MODEL in output and diagnostic in output)
    if not valid:
        raise RuntimeError("socket model did not establish the expected source verdict; recovery: " + RECOVERY)


def main():
    original = SOURCE.read_bytes()
    source = original.decode().replace("\r\n", "\n")
    if any(source.count(anchor) != 1 for _, anchor, _, _ in CONTROLS):
        raise RuntimeError("socket production mutation anchor changed; recovery: " + RECOVERY)
    print("Socket production sha256=" + hashlib.sha256(original).hexdigest(), flush=True)
    try:
        for name, anchor, replacement, diagnostic in CONTROLS:
            print("STEP actual production-source plant: " + name, flush=True)
            SOURCE.write_bytes(source.replace(anchor, replacement).encode())
            run(diagnostic)
        SOURCE.write_bytes(original)
        run()
    finally:
        SOURCE.write_bytes(original)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("STEP socket qualification failed: " + str(error) + "; recovery: " + RECOVERY,
              file=sys.stderr)
        sys.exit(1)
