"""Plant one production admission defect; require the unchanged fuzz oracle to fail."""
import argparse
import hashlib
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
FUZZ = ROOT / "rust/fuzz"
CASES = {
    "decision-version": (
        "experiments/probes.rs", "decision.version != 1", "false", "decision-version",
    ),
    "scorer-version": (
        "experiments/probes.rs", "scorer.version != 1 || ", "", "scorer-version",
    ),
    "observation-version": (
        "experiments/mod.rs", "row.version != 2", "false", "observation-version",
    ),
    "scorer-distinct": (
        "experiments/probes.rs", " || scorer.expected == scorer.injected", "", "scorer-equal",
    ),
    "trailing-input": (
        "executor/json/object.rs", "    decoder.end()?;", "", "decision",
    ),
}


def fail(step, detail):
    raise SystemExit(
        f"{step}: {detail}; recovery: correct the reported failure, then run "
        f"python3 rust/fuzz/experiment-negative-control.py {args.case}"
    )


def run(inputs):
    return subprocess.run(
        ["cargo", "run", "--locked", "-p", "acyclic-sdk-fuzz", "--bin", "experiment_files",
         "--no-default-features", "--features", "experiments", "--", *map(str, inputs)],
        cwd=ROOT / "rust", stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("case", choices=CASES)
args = parser.parse_args()
path, needle, replacement, seed = CASES[args.case]
source = ROOT / "rust/crates/harness-core/src" / path
original = source.read_bytes()
old = needle.encode()
if original.count(old) != 1:
    fail("locate production mutation", "source check no longer has exactly one match")
oracle = FUZZ / "fuzz_targets/experiment_files.rs"
oracle_hash = hashlib.sha256(oracle.read_bytes()).hexdigest()
corpus = FUZZ / "corpus/experiment_files"
try:
    # The original production binary must accept the entire retained corpus.
    positive = run(sorted(corpus.iterdir()))
    print(positive.stdout, flush=True)
    if positive.returncode != 0:
        fail("original corpus replay", f"cargo returned {positive.returncode}; no mutation applied")
    mutated = original.replace(old, replacement.encode())
    source.write_bytes(mutated)
    print(f"production mutant SHA256 {hashlib.sha256(mutated).hexdigest()}", flush=True)
    negative = run([corpus / seed])
    print(negative.stdout, flush=True)
    if negative.returncode == 0 or "independent admission invariant" not in negative.stdout:
        fail("production mutant replay", "expected original admission oracle failure was not observed")
    if hashlib.sha256(oracle.read_bytes()).hexdigest() != oracle_hash:
        fail("oracle identity", "fuzz oracle changed during control")
    print(f"PASS {args.case}: unchanged oracle {oracle_hash} rejected production mutant", flush=True)
except OSError as error:
    fail("execute production mutation control", str(error))
finally:
    source.write_bytes(original)
