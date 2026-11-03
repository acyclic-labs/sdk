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


def run(step, expected, diagnostic="invocation writer must be admitted"):
    result = subprocess.run(COMMAND, cwd=ROOT, capture_output=True, text=True)
    output = result.stdout + result.stderr
    print(f"STEP {step}\n{output}", flush=True)
    if expected == "rejected":
        valid = (result.returncode != 0
                 and diagnostic in output
                 and "decoder oracle actor_codecs/" in output)
    else:
        valid = result.returncode == 0 and "replayed " in output and " captured inputs" in output
    if not valid:
        raise RuntimeError(f"{step}: expected {expected}; recovery after correcting the "
                           f"reported source or oracle: python3 actor-negative-control.py"
                           f" {' '.join(sys.argv[1:])}")


def main():
    if sys.argv[1:] == ["--capture"]:
        return capture()
    if sys.argv[1:] == ["--alarms"]:
        return alarms()
    if sys.argv[1:] == ["--persisted-alarms"]:
        return persisted_alarms()
    if sys.argv[1:]:
        raise RuntimeError("unknown control; recovery: python3 actor-negative-control.py [--alarms|--persisted-alarms|--capture]")
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


def alarms():
    production = ROOT.parent / "crates/actors/src/guest/wire.rs"
    original = {path: path.read_bytes() for path in (production, ORACLE)}
    source, oracle = original[production], original[ORACLE]
    plants = (
        (b"AlarmWire::Set(deadline_unix_ms) => AlarmChange::Set { deadline_unix_ms },",
         b"AlarmWire::Set(_) => AlarmChange::Keep,"),
        (b"AlarmWire::Clear(true) => AlarmChange::Clear,",
         b"AlarmWire::Clear(true) => AlarmChange::Keep,"),
    )
    for needle, _ in plants:
        if source.count(needle) != 1:
            raise RuntimeError("alarm production anchor changed; recovery: inspect "
                               "decode_transition and update actor-negative-control.py --alarms")
    start = oracle.index(b"    for (name, alarm) in [")
    end = oracle.index(b"] {", start) + len(b"] {")
    keep_only = (oracle[:start]
                 + b'    for (name, alarm) in [("transition", guest::AlarmChange::Keep)] {'
                 + oracle[end:])
    try:
        for label, value in (("alarm production", source), ("alarm oracle", oracle)):
            print(f"{label} sha256={hashlib.sha256(value).hexdigest()}", flush=True)
        production.write_bytes(source.replace(*plants[0]))
        ORACLE.write_bytes(keep_only)
        manifest = ROOT / "corpus/SOURCES.tsv"
        saved = manifest.read_bytes()
        try:
            manifest.write_bytes(b"".join(row for row in saved.splitlines(keepends=True)
                                         if not row.startswith(b"actor_codecs/transition-")))
            run("Keep-only corpus misses lost Set", "accepted")
        finally:
            manifest.write_bytes(saved)
        ORACLE.write_bytes(oracle)
        diagnostic = "transition writer must retain its alarm"
        run("same oracle detects production lost Set", "rejected", diagnostic)
        production.write_bytes(source.replace(*plants[1]))
        run("same oracle detects production lost Clear", "rejected", diagnostic)
        production.write_bytes(source)
        capture()
    finally:
        for path, value in original.items():
            path.write_bytes(value)


def persisted_alarms():
    production = ROOT.parent / "crates/actors/src/local/record.rs"
    manifest = ROOT / "corpus/SOURCES.tsv"
    original = {path: path.read_bytes() for path in (production, ORACLE, manifest)}
    source, oracle = original[production], original[ORACLE]
    plant = b"delivered_generation: record.alarm_delivered_generation,"
    admission = b"    seeds.extend(futures::executor::block_on(actor_alarm_seeds())?);"
    if b"\r\n" in oracle:
        admission += b"\r\n"
    else:
        admission += b"\n"
    if source.count(plant) != 1 or oracle.count(admission) != 1:
        raise RuntimeError("persisted alarm mutation anchor changed; recovery: inspect "
                           "State::decode and actor_alarm_seeds before running "
                           "python3 actor-negative-control.py --persisted-alarms")
    captured = False
    try:
        for label, value in (("persisted alarm production", source), ("persisted alarm oracle", oracle)):
            print(f"{label} sha256={hashlib.sha256(value).hexdigest()}", flush=True)
        production.write_bytes(source.replace(plant, b"delivered_generation: None,"))
        ORACLE.write_bytes(oracle.replace(admission, b""))
        manifest.write_bytes(b"".join(row for row in original[manifest].splitlines(keepends=True)
                                     if not row.startswith(b"actor_codecs/state-alarm-")))
        run("create-only State oracle misses lost delivery generation", "accepted")
        manifest.write_bytes(original[manifest])
        ORACLE.write_bytes(oracle)
        run("same oracle detects production lost delivery generation", "rejected",
            "persisted alarm writer must retain its committed tuple")
        production.write_bytes(source)
        capture()
        captured = True
    finally:
        for path, value in original.items():
            # Keep newly captured provenance on success; only a temporary
            # mutation-phase manifest view needs restoration on failure.
            if path != manifest or not captured:
                path.write_bytes(value)


def capture():
    command = ["cargo", "run", "--locked", "--example", "capture", "--",
               "--only-actors", "--extend-goldens"]
    result = subprocess.run(command, cwd=ROOT)
    if result.returncode:
        raise RuntimeError("original alarm writer capture failed; recovery after "
                           "correcting the reported failure: python3 actor-negative-control.py --capture")
    run("original production and captured alarm branches", "accepted")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"STEP Actors negative control failed: {error}; recovery after correcting "
              "the reported failure: python3 actor-negative-control.py"
              f" {' '.join(sys.argv[1:])}", file=sys.stderr)
        sys.exit(1)
