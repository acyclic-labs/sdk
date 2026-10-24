#!/usr/bin/env python3
"""Reject new host deadline/park sources, including aliases and function values."""
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1] / "rust/crates"
CRATES = ("harness", "filesystem", "stream", "objects", "native-runtime", "actors")
# Count capabilities as well as calls: imports, aliases and function pointers
# cannot hide a later call. Whitespace is irrelevant to these token patterns.
PATTERNS = {
    "instant": r"\bInstant\s*(?:::|>\s*::)\s*now\b|\bInstant\s+as\b|=\s*(?:[A-Za-z_]\w*\s*::\s*)*Instant\b",
    "sleep": r"\bsleep(?:_until)?\b",
    "timed-park": r"\b(?:wait_timeout(?:_while)?|park_timeout)\b",
    "elapsed": r"\.\s*elapsed\b|::\s*elapsed\b",
}
MANIFEST = Path(__file__).with_name("dst-wait-allowances.json")


def test_only(path):
    return any(part == "tests" or part.endswith("_tests") for part in path.parts) or (
        path.name == "tests.rs" or path.name.endswith("_tests.rs")
        or path.name.startswith("tests_") or path.name == "models.rs")


def counts(text):
    text = re.sub(r"\b(?:crate|acyclic_native_runtime)\s*::\s*deadline\s*::\s*sleep\b", "virtual_park", text)
    text = re.sub(r"\b(?:(?:crate|acyclic_native_runtime)\s*::\s*)?deadline\s*::\s*elapsed\b", "virtual_elapsed", text)
    return {kind: len(re.findall(pattern, text))
            for kind, pattern in PATTERNS.items() if re.search(pattern, text)}


def sources():
    return {str(path.relative_to(ROOT)).replace("\\", "/"): counts(path.read_text(encoding="utf-8"))
            for crate in CRATES for path in (ROOT / crate / "src").rglob("*.rs")
            if not test_only(path)}


def violations(files, allowances):
    expected = {}
    for row in allowances:
        assert row["reason"].strip(), "every allowance needs an exact reason"
        key = (row["file"], row["source"])
        assert key not in expected, f"duplicate allowance {key}"
        expected[key] = row["count"]
    actual = {(path, kind): count for path, kinds in files.items()
              for kind, count in kinds.items()}
    return [f"{path}: {kind} expected {expected.get((path, kind), 0)}, "
            f"found {actual.get((path, kind), 0)}"
            for path, kind in sorted(actual.keys() | expected.keys())
            if actual.get((path, kind), 0) != expected.get((path, kind), 0)]


def controls(files, allowed):
    # Each spelling is planted into a NEW production helper, outside the seam.
    plants = {
        "instant": ["Instant::now()", "cached.unwrap_or_else(Instant::now)",
                    "std::time::Instant :: now()", "tokio::time::Instant::now()",
                    "use std::time::Instant as Tick; Tick::now()",
                    "type Tick = std::time::Instant;", "type Tick = tokio::time::Instant;",
                    "type Tick = host::time::Instant;", "use std::time::{Instant as Tick};"],
        "sleep": ["std::thread::sleep(delay)", "std::thread :: sleep (delay)",
                  "let park = std::thread::sleep;", "use std::thread::sleep as park;",
                  "use std::thread::{sleep as park};", "tokio::time::sleep(delay)",
                  "tokio::time::sleep_until(deadline)"],
        "timed-park": ["cv.wait_timeout(guard, duration)",
                       "Condvar::wait_timeout(&cv, guard, duration)",
                       "let wait = Condvar::wait_timeout;",
                       "cv.wait_timeout_while(guard, duration, predicate)",
                       "std::thread::park_timeout(delay)"],
        "elapsed": ["started.elapsed()", "started . elapsed ()",
                    "Instant::elapsed(&started)", "let read = Instant::elapsed;",
                    "std::time::Instant::elapsed(&started)",
                    "<std::time::Instant>::elapsed(&started)",
                    "let read = Tick::elapsed;"],
    }
    assert set(plants) == set(PATTERNS)
    for kind, forms in plants.items():
        for form in forms:
            planted = dict(files)
            planted["harness/src/planted_wait.rs"] = counts(form)
            found = violations(planted, allowed)
            assert any("planted_wait.rs: " + kind in item for item in found), (form, found)
    # An extra source beside an allowance and removing an allowance both fail.
    for row in allowed:
        changed = {path: dict(kinds) for path, kinds in files.items()}
        changed[row["file"]][row["source"]] += 1
        assert violations(changed, allowed), row
        assert violations(files, [other for other in allowed if other != row]), row


def main():
    files = sources()
    assert len(files) > 1000, "source gate lost its six crate roots"
    allowed = json.loads(MANIFEST.read_text(encoding="utf-8"))
    found = violations(files, allowed)
    if found:
        sys.exit("\n".join(found))
    controls(files, allowed)
    print(f"DST wait source gate: {len(files)} files; all planted controls rejected")


if __name__ == "__main__":
    main()
