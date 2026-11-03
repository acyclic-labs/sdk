#!/usr/bin/env python3
"""Require the unchanged Actors fuzz oracle to detect production reject-all drift."""
import hashlib
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
PRODUCTION = ROOT.parent / "crates/actors/src/invocation.rs"
ORACLE = ROOT / "src/lib.rs"
COMMAND = ["cargo", "run", "--locked", "--example", "replay", "--", "actor_codecs"]


def run(step, expected):
    result = subprocess.run(COMMAND, cwd=ROOT, capture_output=True, text=True)
    output = result.stdout + result.stderr
    print(f"STEP {step}\n{output}", flush=True)
    if expected == "rejected":
        valid = (result.returncode != 0
                 and "invocation writer must be admitted" in output
                 and "decoder oracle actor_codecs/" in output)
    else:
        valid = result.returncode == 0 and "replayed " in output and " captured inputs" in output
    if not valid:
        raise RuntimeError(f"{step}: expected {expected}; recovery after correcting the "
                           "reported source or oracle: python3 actor-negative-control.py")


def main():
    original = {path: path.read_bytes() for path in (PRODUCTION, ORACLE)}
    needle = b"Ok(Self::decode_bound(record)?.map(|(_, invocation)| invocation))"
    source = original[PRODUCTION]
    if source.count(needle) != 1:
        raise RuntimeError("production mutation anchor changed; recovery: inspect "
                           "Invocation::decode and update actor-negative-control.py")
    mutant = source.replace(needle, b"Ok(None)")
    admission = (b"    static WRITERS: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {\n"
                 b'        actor_seeds().expect("Actors decoders must admit their production writers");\n'
                 b"    });\n    std::sync::LazyLock::force(&WRITERS);\n")
    oracle = original[ORACLE]
    if b"\r\n" in oracle:
        admission = admission.replace(b"\n", b"\r\n")
    if oracle.count(admission) != 1:
        raise RuntimeError("oracle admission anchor changed; recovery: inspect actor_codecs "
                           "and update actor-negative-control.py")
    try:
        for label, value in (("production", source), ("mutant", mutant), ("oracle", oracle)):
            print(f"{label} sha256={hashlib.sha256(value).hexdigest()}", flush=True)
        PRODUCTION.write_bytes(mutant)
        ORACLE.write_bytes(oracle.replace(admission, b""))
        run("conditional oracle misses production reject-all", "accepted")
        ORACLE.write_bytes(oracle)
        run("same oracle with writer admission detects same mutant", "rejected")
        PRODUCTION.write_bytes(source)
        run("original production writers and retained corpus", "accepted")
    finally:
        for path, value in original.items():
            path.write_bytes(value)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"STEP Actors negative control failed: {error}", file=sys.stderr)
        sys.exit(1)
