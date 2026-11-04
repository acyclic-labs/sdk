"""Run from the repository root: python3 rust/crates/harness-core/tests/continuation-negative-control.py."""
import hashlib
import json
import os
import pathlib
import shlex
import subprocess
import tempfile
import time

root = pathlib.Path.cwd()
source = root / "rust/crates/harness-core/src/executor/record_index.rs"
observer = root / "rust/crates/harness-core/src/executor/tests/request_continuation.rs"
original = source.read_bytes()
observer_hash = hashlib.sha256(observer.read_bytes()).hexdigest()
target = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", "target"))
target.mkdir(parents=True, exist_ok=True)
evidence = pathlib.Path(tempfile.mkdtemp(prefix="continuation-index-", dir=target))
command = ["cargo", "test", "--locked", "-p", "acyclic-harness-core", "--lib", "--all-features", "executor::tests::request_continuation::one_stock_turn_preserves_admitted_continuations_across_step_limits", "--", "--exact", "--nocapture"]
receipt = {"source": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(), "evidence": str(evidence), "phases": [], "observer_sha256": observer_hash, "scope": "Actual ordinary StockExecutor and authenticated FilesystemExecutionJournal: six original live step-limit cases and five restored executions must request no public replay. Existing same-journal one-record replay must emit one replay request and one decoded record. Source control disables the existing indexed selection in place; no alternate journal, provider, constructor, input or oracle."}

def require(ok, step):
    if not ok:
        raise RuntimeError("STEP_FAILED " + step + "; recovery: inspect the original phase log and source before " + shlex.join(command))

def run(label, expected):
    require(hashlib.sha256(observer.read_bytes()).hexdigest() == observer_hash, label + "-unchanged-observer")
    output_path = evidence / (label + ".log")
    print("PHASE " + label + " " + shlex.join(command), flush=True)
    start = time.monotonic()
    with output_path.open("w") as output:
        child = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in child.stdout:
            output.write(line)
            print(line, end="", flush=True)
        code = child.wait()
    text = output_path.read_text()
    receipt["phases"].append({"step": label, "exit": code, "seconds": time.monotonic()-start, "output": str(output_path), "output_sha256": hashlib.sha256(output_path.read_bytes()).hexdigest(), "product_sha256": hashlib.sha256(source.read_bytes()).hexdigest(), "observer_sha256": hashlib.sha256(observer.read_bytes()).hexdigest()})
    require(code == expected, label + "-exit")
    marker = "test result: ok. 1 passed; 0 failed; 0 ignored;" if expected == 0 else "test result: FAILED. 0 passed; 1 failed; 0 ignored;"
    require(marker in text, label + "-runtime-verdict")
    if expected:
        require("ordinary continuation requested journal replay at 1 steps (None)" in text, label + "-original-replay-assertion")

try:
    needle = b"        if let Some(future) = journal.indexed_records(query, self.io)?"
    require(original.count(needle) == 1, "unique-original-indexed-selection")
    source.write_bytes(original.replace(needle, b"        if false && let Some(future) = journal.indexed_records(query, self.io)?", 1))
    run("actual-indexed-selection-omitted", 101)
    source.write_bytes(original)
    run("restored-original-indexed-selection", 0)
    receipt["verdict"] = "QUALIFIED actual indexed-selection omission CATCH; restored original PASS"
finally:
    source.write_bytes(original)
    receipt["restored_sha256"] = hashlib.sha256(source.read_bytes()).hexdigest()
    receipt["observer_unchanged"] = hashlib.sha256(observer.read_bytes()).hexdigest() == observer_hash
    saved = evidence / "receipt.json"
    saved.write_text(json.dumps(receipt, indent=2) + "\n")
    print("CONTINUATION_INDEX_RECEIPT " + json.dumps(receipt, sort_keys=True), flush=True)
