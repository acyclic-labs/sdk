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


def run(diagnostic=None, model=MODEL, instrumented=True, recovery=RECOVERY):
    command = ["cargo", "test", "--locked", "-p", "acyclic-actors"]
    environment = dict(os.environ)
    if instrumented:
        command.extend(["--features", "model-checking"])
        environment["RUSTC_WORKSPACE_WRAPPER"] = str(
            ROOT / "scripts" / ("loom-rustc.cmd" if os.name == "nt" else "loom-rustc.sh"))
    command.extend(["--lib", model, "--", "--exact", "--nocapture"])
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
             and model in output and diagnostic in output)
    if not valid:
        raise RuntimeError("socket check did not establish the expected source verdict; recovery: " + recovery)


def compaction_pages(before=None):
    """Restore eager encoding; the retained page-work and byte oracle must fail."""
    original = SOURCE.read_bytes()
    source = original.decode().replace("\r\n", "\n")
    recovery = "inspect original diagnostics, correct source or command, then python3 scripts/qualify-actors-sockets.py --compaction-pages"
    start = source.index("    async fn compact(&self)")
    end = source.index("    /// Commit the Open", start)
    current = source[start:end]
    if before is None:
        anchor = "        });\n        let segment = self.layout.segment(next)?;"
        if current.count(anchor) != 1:
            raise RuntimeError("compaction encoding anchor changed; recovery: " + recovery)
        eager = current.replace(anchor, "        }).collect::<Vec<_>>().into_iter();\n        let segment = self.layout.segment(next)?;")
    else:
        prior_start = before.index("    async fn compact(&self)")
        prior_end = before.index("    /// Commit the Open", prior_start)
        eager = before[prior_start:prior_end]
    model = "local::sockets::tests::compaction_keeps_only_open_sockets_for_recovery"
    print("Socket production sha256=" + hashlib.sha256(original).hexdigest(), flush=True)
    try:
        print("STEP actual production eager compaction encoding", flush=True)
        SOURCE.write_bytes((source[:start] + eager + source[end:]).encode())
        run("compaction encoded beyond its current page", model, False, recovery)
        SOURCE.write_bytes(original)
        run(model=model, instrumented=False, recovery=recovery)
    finally:
        SOURCE.write_bytes(original)


def main():
    if sys.argv[1:] == ["--compaction-pages"]:
        compaction_pages()
        return
    if sys.argv[1:]:
        raise RuntimeError("unknown socket qualification argument; recovery: python3 scripts/qualify-actors-sockets.py [--compaction-pages]")
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
