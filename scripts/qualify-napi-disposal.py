#!/usr/bin/env python3
"""Run the real Worker controls, then the original dependency omission control."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
RECOVERY = "python scripts/qualify-napi-disposal.py"


def run(arguments, **options):
    result = subprocess.run(arguments, cwd=ROOT, **options)
    if result.returncode:
        raise ValueError(f"step=command command={arguments!r} rc={result.returncode}; recovery: {RECOVERY}")
    return result


def main():
    if sys.platform != "linux":
        raise ValueError(f"step=platform: this qualification selects the Linux shared library; recovery: run {RECOVERY} on Linux")
    vendor = ROOT / "rust/vendor/napi-3.15.0"
    path = vendor / "src/threadsafe_function.rs"
    fixed = path.read_bytes()
    origin = json.loads((vendor / "ORIGIN.json").read_text())
    entry = next(item for item in origin["files"] if item["path"] == "src/threadsafe_function.rs")
    if hashlib.sha256(fixed).hexdigest() != entry["sha256"]:
        raise ValueError(f"step=patched-source: dependency differs from its source binding; recovery: git restore -- {path.relative_to(ROOT)}")
    text = fixed.decode()
    guard = """  // Node may request disposal without a live JavaScript environment. Reclaim
  // the queued item first: its completion sender must close on this path.
  if data.is_null() {
    return;
  }
  let val = unsafe {"""
    old = """  // env and/or callback can be null when shutting down
  if raw_env.is_null() || js_callback.is_null() {
    return;
  }

  let callback: &mut R = unsafe { Box::leak(Box::from_raw(context.cast())) };
  let val = unsafe {"""
    moved = """  if raw_env.is_null() || js_callback.is_null() {
    return;
  }
  let callback: &mut R = unsafe { Box::leak(Box::from_raw(context.cast())) };

  let mut recv = ptr::null_mut();"""
    if text.count(guard) != 1 or text.count(moved) != 1:
        raise ValueError(f"step=omission-source: expected exactly one ownership correction; recovery: {RECOVERY}")
    original = text.replace(guard, old).replace(moved, "  let mut recv = ptr::null_mut();").encode()
    if hashlib.sha256(original).hexdigest() != entry["upstream_sha256"]:
        raise ValueError(f"step=original-source: omission is not the original registry source; recovery: {RECOVERY}")
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    with tempfile.TemporaryDirectory(prefix="native-callback-control-") as directory:
        def build(name):
            run(["cargo", "build", "--locked", "-p", "acyclic-napi-progress-test"])
            addon = Path(directory) / (name + ".node")
            shutil.copyfile(target / "debug/libacyclic_napi_progress_test.so", addon)
            return addon

        positive = build("fixed")
        run(["node", "scripts/qualify-napi-progress.mjs", str(positive)])
        try:
            path.write_bytes(original)
            negative = build("original")
            result = subprocess.run(["node", "scripts/qualify-napi-progress.mjs", str(negative)],
                                    cwd=ROOT, text=True, capture_output=True)
            print(result.stdout, end="")
            print(result.stderr, end="", file=sys.stderr)
            if result.returncode == 0 or "step=callback-progress method=queueDirect abort=true;" not in result.stderr:
                raise ValueError(f"step=omission-oracle: original source did not fail its missing-completion assertion; recovery: {RECOVERY}")
            print("PASS original-source omission: unchanged Worker control detects missing completion")
        finally:
            path.write_bytes(fixed)
        print(json.dumps({"fixed_source_sha256": hashlib.sha256(fixed).hexdigest(),
                          "original_source_sha256": hashlib.sha256(original).hexdigest(),
                          "positive_controls": 4, "original_omission_detected": True,
                          "scope": "native callback disposal; no credential or platform-wide qualification"}))


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError) as error:
        print(f"NATIVE_CALLBACK_QUALIFICATION_FAILED {error}; recovery: {RECOVERY}", file=sys.stderr)
        sys.exit(1)
