#!/usr/bin/env python3
"""Require the unchanged Actors fuzz oracle to detect production reject-all drift."""
import hashlib
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
PRODUCTION = ROOT.parent / "crates/actors/src/invocation.rs"
ORACLE = ROOT / "src/lib.rs"
COMMAND = ["cargo", "run", "--locked", "--example", "replay", "--", "actor_codecs"]


def run(step, expected, diagnostic="invocation writer must be admitted", entry=None):
    command = COMMAND + ([entry] if entry else [])
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
    output = result.stdout + result.stderr
    print(f"STEP {step}\n{output}", flush=True)
    if expected == "rejected":
        valid = (result.returncode != 0
                 and diagnostic in output
                 and ("decoder oracle actor_codecs/" in output if entry is None
                      else "corpus replay failed:" in output))
    else:
        valid = result.returncode == 0 and "replayed " in output and " captured inputs" in output
    if not valid:
        raise RuntimeError(f"{step}: expected {expected}; recovery after correcting the "
                           f"reported source or oracle: python3 actor-negative-control.py"
                           f" {' '.join(sys.argv[1:])}")
    return output


def writer_admission(oracle):
    admission = (b"    static WRITERS: std::sync::LazyLock<()> = std::sync::LazyLock::new(|| {\n"
                 b'        actor_seeds().expect("Actors decoders must admit their production writers");\n'
                 b"    });\n    std::sync::LazyLock::force(&WRITERS);\n")
    if b"\r\n" in oracle:
        admission = admission.replace(b"\n", b"\r\n")
    if oracle.count(admission) != 1:
        raise RuntimeError("writer admission anchor changed; recovery: inspect actor_codecs "
                           "before python3 actor-negative-control.py"
                           f" {' '.join(sys.argv[1:])}")
    return admission


def main():
    if sys.argv[1:] == ["--capture"]:
        return capture()
    if sys.argv[1:] == ["--alarms"]:
        return alarms()
    if sys.argv[1:] == ["--guest-wire"]:
        return guest_wire()
    if sys.argv[1:] == ["--reservation-book"]:
        return reservation_book()
    if sys.argv[1:] == ["--persisted-alarms"]:
        return persisted_alarms()
    if sys.argv[1:] == ["--call-records"]:
        return call_records()
    if sys.argv[1:] == ["--call-schedules"]:
        return call_schedules()
    if sys.argv[1:] == ["--replay-diagnostics"]:
        return replay_diagnostics()
    if sys.argv[1:] == ["--sockets"]:
        return sockets()
    if sys.argv[1:] == ["--socket-inputs"]:
        return socket_inputs()
    if sys.argv[1:] == ["--socket-metadata"]:
        return socket_metadata()
    if sys.argv[1:] == ["--socket-ledger"]:
        return socket_ledger()
    if sys.argv[1:] == ["--live-update"]:
        return live_update()
    if sys.argv[1:]:
        raise RuntimeError("unknown control; recovery: python3 actor-negative-control.py [--alarms|--guest-wire|--persisted-alarms|--reservation-book|--call-records|--call-schedules|--replay-diagnostics|--sockets|--socket-inputs|--socket-metadata|--socket-ledger|--live-update|--capture]")
    original = {path: path.read_bytes() for path in (PRODUCTION, ORACLE)}
    needle = b"Ok(Self::decode_bound(record)?.map(|(_, invocation)| invocation))"
    source = original[PRODUCTION]
    if source.count(needle) != 1:
        raise RuntimeError("production mutation anchor changed; recovery: inspect "
                           "Invocation::decode and update actor-negative-control.py")
    mutant = source.replace(needle, b"Ok(None)")
    oracle = original[ORACLE]
    admission = writer_admission(oracle)
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


def guest_wire():
    production = ROOT.parent / "crates/actors/src/guest/wire.rs"
    original = {path: path.read_bytes() for path in (production, ORACLE)}
    source, oracle = original[production], original[ORACLE]
    admission = writer_admission(oracle)
    plants = []
    for kind in ("manifest", "delivery"):
        anchor = (f'    let wire: {kind.title()}Wire = canonical(bytes, "{kind}", '
                  f'{kind.upper()}_DOMAIN)?;').encode()
        diagnostic = f"{kind} admission negative control"
        if source.count(anchor) != 1:
            raise RuntimeError(f"guest {kind} mutation anchor changed; recovery: inspect "
                               f"decode_{kind} before python3 actor-negative-control.py --guest-wire")
        replacement = anchor + f'\n    if !bytes.is_empty() {{ return Err(invalid("{diagnostic}")); }}'.encode()
        plants.append((anchor, replacement, diagnostic))
    try:
        for label, value in (("guest wire production", source), ("guest wire oracle", oracle)):
            print(f"{label} sha256={hashlib.sha256(value).hexdigest()}", flush=True)
        mutant = source
        for anchor, replacement, _ in plants:
            mutant = mutant.replace(anchor, replacement)
        production.write_bytes(mutant)
        ORACLE.write_bytes(oracle.replace(admission, b""))
        run("conditional guest wire oracle misses both production refusals", "accepted")
        ORACLE.write_bytes(oracle)
        for anchor, replacement, diagnostic in plants:
            production.write_bytes(source.replace(anchor, replacement))
            run("same writer oracle rejects production " + diagnostic, "rejected",
                'Actors decoders must admit their production writers: '
                f'Decode("non-canonical guest {diagnostic}")')
        production.write_bytes(source)
        run("restored guest wire writers and retained corpus", "accepted")
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
        run("restored production writers and retained corpus", "accepted")
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
        run("restored production writers and retained corpus", "accepted")
    finally:
        for path, value in original.items():
            path.write_bytes(value)


def reservation_book():
    production = ROOT.parent / "crates/actors/src/reservation.rs"
    writers = ROOT.parent / "crates/actors/src/reservation/codec_fuzz.rs"
    original = {path: path.read_bytes() for path in (production, writers)}
    source, oracle = original[production], original[writers]
    anchor = b"    pub fn validate(&self) -> Result<(), GuestError> {"
    admission = b"    seeds.extend(futures::executor::block_on(publication_seeds())?);"
    if source.count(anchor) != 1 or oracle.count(admission) != 1:
        raise RuntimeError("reservation mutation anchor changed; recovery: inspect "
                           "Book::validate and publication_seeds before running "
                           "python3 actor-negative-control.py --reservation-book")
    mutant = source.replace(anchor, anchor + b"\n        if self.used != 0 { return Err(invalid()); }")
    try:
        for label, value in (("reservation production", source), ("reservation oracle", oracle)):
            print(f"{label} sha256={hashlib.sha256(value).hexdigest()}", flush=True)
        production.write_bytes(mutant)
        writers.write_bytes(oracle.replace(admission, b""))
        run("empty Book oracle misses refusal of valid populated state", "accepted")
        writers.write_bytes(oracle)
        run("same oracle detects production populated Book refusal", "rejected",
            "reservation writer must admit its populated checkpoint")
        production.write_bytes(source)
        run("restored production writers and retained corpus", "accepted")
    finally:
        for path, value in original.items():
            path.write_bytes(value)


def call_records():
    production = ROOT.parent / "crates/actors/src/call.rs"
    original = {path: path.read_bytes() for path in (production, ORACLE)}
    source, oracle = original[production], original[ORACLE]
    cancellation = b"Outcome::Cancelled(reason) if self.accepted.is_some() => {"
    producer = oracle.index(b"acyclic_actors::call::model::codec_seeds(clock)")
    start = oracle.rindex(b"    for (name, bytes)", 0, producer)
    end = oracle.index(b"    for (name, bytes) in acyclic_actors::reservation::codec_fuzz::seeds()?", start)
    if source.count(cancellation) != 1:
        raise RuntimeError("call mutation anchor changed; recovery: inspect "
                           "Journal::apply and actor_codecs before running "
                           "python3 actor-negative-control.py --call-records")
    mutant = source.replace(cancellation, b"Outcome::Cancelled(reason) if false => {")
    try:
        for label, value in (("call production", source), ("call oracle", oracle)):
            print(f"{label} sha256={hashlib.sha256(value).hexdigest()}", flush=True)
        production.write_bytes(mutant)
        ORACLE.write_bytes(oracle[:start] + oracle[end:])
        run("unbound call oracle misses lost accepted cancellation context", "accepted")
        ORACLE.write_bytes(oracle)
        run("same oracle detects lost accepted cancellation context", "rejected",
            "call reader must retain its acceptance context")
        production.write_bytes(source)
        run("restored production writers and retained corpus", "accepted")
    finally:
        for path, value in original.items():
            path.write_bytes(value)


def call_schedules():
    production = ROOT.parent / "crates/actors/src/call.rs"
    model = production.with_name("call") / "model.rs"
    original = {path: path.read_bytes() for path in (production, model, ORACLE)}
    source, driver, oracle = original[production], original[model], original[ORACLE]
    entry = b"pub async fn check(bytes: &[u8]) -> Result<(), String> {"
    guard = b"path: self.settled.clone(),\n        })\n        .chain"
    replacement = b"path: self.settled.clone(),\n        }).filter(|_| false)\n        .chain"
    previous = (b"    futures::executor::block_on(acyclic_actors::call::model::check(bytes))\n"
                b'        .expect("original call protocol must conserve its committed receipts");\n')
    boundary = b"    use acyclic_actors::{guest::wire, local::State};"
    if b"\r\n" in source:
        guard, replacement = (value.replace(b"\n", b"\r\n") for value in (guard, replacement))
    if b"\r\n" in oracle:
        previous = previous.replace(b"\n", b"\r\n")
    if source.count(guard) != 1 or driver.count(entry) != 1 or oracle.count(boundary) != 1:
        raise RuntimeError("call schedule anchor changed; recovery: inspect CallPath::open, "
                           "model::check and actor_codecs before python3 actor-negative-control.py --call-schedules")
    try:
        for label, value in (("call production", source), ("call model", driver), ("codec oracle", oracle)):
            print(f"{label} sha256={hashlib.sha256(value).hexdigest()}", flush=True)
        model.write_bytes(driver.replace(entry, entry +
            b' return Err("decoder input drove a call schedule".into());'))
        ORACLE.write_bytes(oracle.replace(boundary, previous + boundary))
        run("previous decoder target drives unrelated schedule", "rejected",
            "decoder input drove a call schedule")
        ORACLE.write_bytes(oracle)
        run("decoder target performs no arbitrary schedule", "accepted")
        model.write_bytes(driver)
        production.write_bytes(source.replace(guard, replacement))
        run("same retained schedule oracle rejects missing settlement guard", "rejected",
            "call record after its terminal", "call_record")
        production.write_bytes(source)
        run("original schedules retain their committed receipts", "accepted", entry="call_record")
    finally:
        for path, value in original.items():
            path.write_bytes(value)


def replay_diagnostics():
    model = ROOT.parent / "crates/actors/src/call/model.rs"
    original = model.read_bytes()
    anchor = b"pub async fn check(bytes: &[u8]) -> Result<(), String> {"
    if original.count(anchor) != 1:
        raise RuntimeError("schedule entry changed; recovery: inspect model::check before "
                           "python3 actor-negative-control.py --replay-diagnostics")
    print("call model sha256=" + hashlib.sha256(original).hexdigest(), flush=True)
    try:
        for failure in (b'return Err("replay recovery control".into());',
                        b'panic!("replay recovery control");'):
            model.write_bytes(original.replace(anchor, anchor + failure))
            output = run("actual schedule failure retains its reproduction", "rejected",
                         "replay recovery control", "call_record")
            diagnostic = re.search(
                r"decoder oracle (actor_codecs/[^\s]+) \(entry call_record\) failed:", output)
            if (diagnostic is None or
                "recovery: cargo +nightly fuzz run --fuzz-dir . call_record corpus/" +
                    diagnostic[1] not in output or
                "recovery after correcting the reported mismatch: "
                    "cargo run --locked --example replay -- actor_codecs call_record" not in output):
                raise RuntimeError("replay recovery lost the actual entry or captured input; "
                                   "recovery: correct examples/replay.rs then python3 "
                                   "actor-negative-control.py --replay-diagnostics")
        model.write_bytes(original)
        run("restored production replays original decoder inputs", "accepted")
    finally:
        model.write_bytes(original)


def sockets():
    production = ROOT.parent / "crates/actors/src/sockets.rs"
    original = {path: path.read_bytes() for path in (production, ORACLE)}
    source, oracle = original[production], original[ORACLE]
    plants = (
        (b"if wire.version != 1 || wire.subscription.is_empty() {",
         b"if true || wire.subscription.is_empty() {",
         "Actors decoders must admit their production writers"),
        (b"u16::try_from(code).map_err(|_| invalid())", b"Ok(0)",
         "socket frame writer must retain its fields"),
        (b"&& !open.subprotocols.contains(selected)", b"&& false",
         "socket verdict must refuse unoffered subprotocols"),
    )
    writers = (
        b"    seeds.extend(actor_socket_seeds()?);",
        b"    seeds.extend(actor_socket_input_seeds()?);",
    )
    start = oracle.index(b"    use acyclic_actors::sockets::{SocketFrame, SocketOpen};",
                         oracle.index(b"pub fn actor_codecs("))
    end = oracle.index(b"    use acyclic_actors::{guest::wire, local::State};", start)
    if (any(oracle.count(writer) != 1 for writer in writers)
            or any(source.count(anchor) != 1 for anchor, _, _ in plants)):
        raise RuntimeError("socket source anchor changed; recovery: inspect the original "
                           "socket decoders and shared target before python3 actor-negative-control.py --sockets")
    previous = oracle[:start] + oracle[end:]
    for writer in writers:
        previous = previous.replace(writer, b"")
    try:
        for label, value in (("socket production", source), ("socket oracle", oracle)):
            print(f"{label} sha256={hashlib.sha256(value).hexdigest()}", flush=True)
        production.write_bytes(source.replace(*plants[0][:2]))
        ORACLE.write_bytes(previous)
        run("previous shared target misses socket reject-all", "accepted")
        ORACLE.write_bytes(oracle)
        for anchor, replacement, diagnostic in plants:
            production.write_bytes(source.replace(anchor, replacement))
            run("same socket oracle rejects production defect", "rejected", diagnostic)
        production.write_bytes(source)
        run("restored production writers and retained corpus", "accepted")
    finally:
        for path, value in original.items():
            path.write_bytes(value)


def socket_inputs():
    production = ROOT.parent / "crates/actors/src/sockets.rs"
    original = {path: path.read_bytes() for path in (production, ORACLE)}
    source, oracle = original[production], original[ORACLE]
    anchor = b"pub fn socket(&self) -> Result<Option<SocketDelivery>, GuestError> {"
    admission = b"    seeds.extend(actor_socket_input_seeds()?);"
    if source.count(anchor) != 1 or oracle.count(admission) != 1:
        raise RuntimeError("socket input anchor changed; recovery: inspect Delivery::socket "
                           "and actor_socket_input_seeds before python3 actor-negative-control.py --socket-inputs")
    try:
        print("socket input production sha256=" + hashlib.sha256(source).hexdigest(), flush=True)
        production.write_bytes(source.replace(anchor, anchor + b" return Ok(None);"))
        ORACLE.write_bytes(oracle.replace(admission, b""))
        run("conditional input oracle misses production reject-all", "accepted")
        ORACLE.write_bytes(oracle)
        run("same socket input oracle rejects production reject-all", "rejected",
            "socket input writer must be admitted")
        production.write_bytes(source)
        run("restored production writers and retained corpus", "accepted")
    finally:
        for path, value in original.items():
            path.write_bytes(value)


def socket_metadata():
    production = ROOT.parent / "crates/actors/src/sockets.rs"
    original = {path: path.read_bytes() for path in (production, ORACLE)}
    source, oracle = original[production], original[ORACLE]
    admission = b"        actor_socket_input_refusals(&delivery)?;"
    plants = (
        (b"!matches!(&self.trigger, Trigger::Record { sequence, .. } if *sequence == record.sequence)",
         b"false", "socket input rejects wrong sequence"),
        (b"attributes.get(index).ok_or_else(invalid)?.value.as_ref() != VERSION",
         b"false", "socket input rejects unknown version"),
    )
    if oracle.count(admission) != 1 or any(source.count(a) != 1 for a, _, _ in plants):
        raise RuntimeError("socket metadata anchor changed; recovery: inspect Delivery::socket "
                           "and actor_socket_input_refusals before python3 actor-negative-control.py --socket-metadata")
    try:
        print("socket metadata production sha256=" + hashlib.sha256(source).hexdigest(), flush=True)
        production.write_bytes(source.replace(*plants[0][:2]))
        ORACLE.write_bytes(oracle.replace(admission, b""))
        run("positive-only oracle misses production sequence bypass", "accepted")
        ORACLE.write_bytes(oracle)
        for anchor, replacement, diagnostic in plants:
            production.write_bytes(source.replace(anchor, replacement))
            run("same derived-record oracle rejects actual metadata bypass", "rejected", diagnostic)
        production.write_bytes(source)
        run("restored production admits original envelopes and rejects derived violations", "accepted")
    finally:
        for path, value in original.items():
            path.write_bytes(value)


def socket_ledger():
    production = ROOT.parent / "crates/actors/src/local/sockets.rs"
    original = {path: path.read_bytes() for path in (production, ORACLE)}
    source, oracle = original[production], original[ORACLE]
    admission = b"    seeds.extend(actor_socket_ledger_seeds()?);"
    check = b"    acyclic_actors::local::socket_ledger_codec::check(bytes);"
    plants = (
        (b"    let wire = M::decode(bytes).map_err(|_| invalid(\"invalid socket ledger record\"))?;",
         b'    let wire = M::decode(bytes).map_err(|_| invalid("invalid socket ledger record"))?;\n'
         b'    if !bytes.is_empty() { return Err(invalid("invalid socket ledger record")); }',
         "Actors decoders must admit their production writers"),
        (b"if wire.encode_to_vec().as_slice() != bytes {", b"if false {",
         "socket epoch rejects unknown fields"),
        (b'value\n        .checked_add(count)\n        .ok_or_else(|| invalid("socket ledger counter exhausted"))',
         b"Ok(value.wrapping_add(count))",
         "socket epoch checks format capacity and frontier"),
    )
    if (oracle.count(admission) != 1 or oracle.count(check) != 1
            or any(source.count(anchor) != 1 for anchor, _, _ in plants)):
        raise RuntimeError("socket ledger anchor changed; recovery: inspect production persisted "
                           "decoders and actor_socket_ledger_seeds before python3 actor-negative-control.py --socket-ledger")
    try:
        print("socket ledger production sha256=" + hashlib.sha256(source).hexdigest(), flush=True)
        production.write_bytes(source.replace(*plants[0][:2]))
        ORACLE.write_bytes(oracle.replace(admission, b"").replace(check, b""))
        run("previous decoder target misses persisted ledger refusal", "accepted")
        ORACLE.write_bytes(oracle)
        for anchor, replacement, diagnostic in plants:
            production.write_bytes(source.replace(anchor, replacement))
            run("same persisted oracle rejects actual production defect", "rejected", diagnostic)
        production.write_bytes(source)
        run("restored persisted writers and retained corpus", "accepted")
    finally:
        for path, value in original.items():
            path.write_bytes(value)


def live_update():
    production = ROOT.parent / "crates/actors/src/live_update.rs"
    original = {path: path.read_bytes() for path in (production, ORACLE)}
    source, oracle = original[production], original[ORACLE]
    anchor = b"document.v != 1 || encode(&document.value)? != bytes"
    dispatch = b"    live_update::check(bytes);"
    if source.count(anchor) != 1 or oracle.count(dispatch) != 1:
        raise RuntimeError("live-update mutation anchor changed; recovery: inspect "
                           "live_update::decode and actor_codecs before running "
                           "python3 actor-negative-control.py --live-update")
    try:
        for label, value in (("live-update production", source), ("live-update oracle", oracle)):
            print(f"{label} sha256={hashlib.sha256(value).hexdigest()}", flush=True)
        production.write_bytes(source.replace(anchor, b"document.v != 1"))
        ORACLE.write_bytes(oracle.replace(dispatch, b""))
        run("previous Actors oracle misses noncanonical deployment admission", "accepted")
        ORACLE.write_bytes(oracle)
        run("unchanged deployment oracle detects noncanonical admission", "rejected",
            "live-update document admission differs from canonical writer")
        production.write_bytes(source)
        run("restored deployment codec and retained Actors corpus", "accepted")
    finally:
        for path, value in original.items():
            path.write_bytes(value)


def capture():
    command = ["cargo", "run", "--locked", "--example", "capture", "--",
               "--only-actors", "--extend-goldens"]
    result = subprocess.run(command, cwd=ROOT)
    if result.returncode:
        raise RuntimeError("original Actors writer capture failed; recovery after "
                           "correcting the reported failure: python3 actor-negative-control.py --capture")
    run("original production and captured Actors branches", "accepted")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"STEP Actors negative control failed: {error}; recovery after correcting "
              "the reported failure: python3 actor-negative-control.py"
              f" {' '.join(sys.argv[1:])}", file=sys.stderr)
        sys.exit(1)
