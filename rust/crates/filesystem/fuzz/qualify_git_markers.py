"""Replay captured Git bytes, measure coverage, and kill a production mutation."""
from pathlib import Path
import hashlib
import json
import shutil
import subprocess
import tempfile

root = Path(__file__).resolve().parent
source = root.parent / "src/git_compat/merge_markers.rs"
recovery = "python3 rust/crates/filesystem/fuzz/qualify_git_markers.py"


def fail(step, detail):
    raise SystemExit(f"{step}: {detail}\nRecovery after correcting the cause: {recovery}")


def run(step, corpus, artifacts, runs):
    command = ["cargo", "+nightly", "fuzz", "run", "--dev", "--sanitizer", "none",
               "--features", "fuzz-targets", "git_markers", str(corpus), "--",
               f"-runs={runs}", "-seed=1", "-print_final_stats=1",
               f"-artifact_prefix={artifacts}/"]
    print(f"{step}: {' '.join(command)}", flush=True)
    try:
        with subprocess.Popen(command, cwd=root, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True) as child:
            lines = []
            for line in child.stdout:
                print(line, end="", flush=True)
                lines.append(line)
            result = subprocess.CompletedProcess(command, child.wait(), "".join(lines))
    except OSError as error:
        fail(step, f"cannot execute {' '.join(command)}: {error}")
    return result


try:
    manifest = json.loads((root / "git-markers-capture.json").read_text())
except (OSError, ValueError) as error:
    fail("corpus provenance", str(error))
for capture in manifest["captures"]:
    body = (root / "corpus/git_markers" / capture["path"]).read_bytes()
    if len(body) != capture["bytes"] or hashlib.sha256(body).hexdigest() != capture["sha256"]:
        fail("corpus provenance", f"captured bytes changed: {capture['path']}")

with tempfile.TemporaryDirectory(prefix="git-markers-") as temporary:
    folder = Path(temporary)
    corpus = folder / "corpus"
    shutil.copytree(root / "corpus/git_markers", corpus)
    artifacts = folder / "artifacts"
    artifacts.mkdir()
    original = source.read_bytes()
    check = b' | b"|||||||"'
    if original.count(check) != 1:
        fail("base-marker mutation", "production check changed; update the kept mutation")
    try:
        source.write_bytes(original.replace(check, b""))
        negative = run("base-marker mutation", corpus / "git-base-eof", artifacts, 1)
        if negative.returncode == 0 or "marker grammar: final recognition" not in negative.stdout:
            fail("base-marker mutation", "fuzzer did not identify the removed production check")
    finally:
        source.write_bytes(original)
    positive = run("production recognizer", corpus, artifacts, 2000)
    if positive.returncode != 0:
        fail("production recognizer", f"fuzzer exited {positive.returncode}")
    if "cov:" not in positive.stdout or "DONE" not in positive.stdout:
        fail("production coverage", "completed instrumented campaign counters are absent")
    print("PASS: captured corpus admitted; base-marker mutation killed; production campaign completed.")
