"""Qualify native skill writer bytes and the production admission checks."""
from pathlib import Path
import hashlib
import json
import os
import subprocess
import sys
import tempfile

root = Path(__file__).resolve().parent
repository = root.parents[2]
core = root.parent / "harness-core/src"
corpus = root / "corpus/retained_skill_records"
recovery = "python3 rust/crates/harness-fuzz/qualify_retained_skill_records.py"
capture_and_campaign = sys.argv[1:] == ["--capture-and-campaign"]
refusal_contract = sys.argv[1:] == ["--refusal-contract"]
sibling_validation = sys.argv[1:] == ["--sibling-validation"]
if capture_and_campaign:
    recovery += " --capture-and-campaign"
elif refusal_contract:
    recovery += " --refusal-contract"
elif sibling_validation:
    recovery += " --sibling-validation"
test = "context::native_skills::tests::retained_records_share_one_sync_on_the_native_io_service"
native = ["cargo", "test", "--locked", "-p", "acyclic-harness-core", "--lib",
          "--features", "filesystem-local,native-execution,test-support", test, "--", "--exact"]


def fail(step, detail):
    raise SystemExit(f"{step}: {detail}\nRecovery after correcting the cause: {recovery}")


def run(step, command, cwd, env=None):
    print(f"{step}: {' '.join(command)}", flush=True)
    result = subprocess.run(command, cwd=cwd, env=env, text=True,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    print(result.stdout, end="", flush=True)
    return result


def main():
    if sys.argv[1:] and not (capture_and_campaign or refusal_contract or sibling_validation):
        fail("qualification arguments", "expected no arguments, --capture-and-campaign, --refusal-contract or --sibling-validation")
    mutations = [
        (core / "context/native_skills/retained.rs",
         b"    for (key, record) in &batch {\n        validate_record(record, key)?;\n    }\n",
         b"", "changed sibling must reject the whole physical bundle"),
        (core / "resources/typed.rs",
         b'                if resource.kind() != ResourceKind::$kind {\n'
         b'                    return Err(serde::de::Error::custom("typed resource kind mismatch"));\n'
         b'                }\n',
         b"", "foreign skill receipt kind admitted"),
    ]
    refusal_mutation = (
        core / "context/native_skills/retained.rs",
        b'Error::Conflict("retained skill source changed".into())',
        b'Error::Conflict("skill source refused".into())',
        "changed skill sibling admitted or refusal changed",
    )
    mutations.append(refusal_mutation)
    # A retained source-mutation result need not be compiled a second time when
    # only capture/campaign admission changes.
    selected = mutations[:1] if sibling_validation else mutations[-1:] if refusal_contract else [] if capture_and_campaign else mutations
    for source, needle, replacement, assertion in selected:
        original = source.read_bytes()
        if b"\r\n" in original:
            needle = needle.replace(b"\n", b"\r\n")
        if original.count(needle) != 1:
            fail(assertion, "production check changed; update the kept mutation")
        try:
            source.write_bytes(original.replace(needle, replacement))
            result = run(assertion, native, repository)
            if result.returncode == 0 or "panicked at" not in result.stdout or assertion not in result.stdout:
                fail(assertion, "unchanged contract did not reject the actual source defect")
        finally:
            source.write_bytes(original)

    env = {**os.environ}
    if not (refusal_contract or sibling_validation):
        env["ACYCLIC_RETAINED_SKILL_CORPUS"] = str(corpus)
    result = run("native writer and admission", native, repository, env)
    if result.returncode != 0 or "test result: ok. 1 passed; 0 failed; 0 ignored;" not in result.stdout:
        fail("native writer and admission", f"exit {result.returncode}; exact native control required")
    if refusal_contract or sibling_validation:
        print("PASS: actual selected source mutation rejected; restored native contract passed.")
        return
    captures = []
    for name in ["native-batch.json", "materialized.json"]:
        body = (corpus / name).read_bytes()
        captures.append(dict(path=name, bytes=len(body), sha256=hashlib.sha256(body).hexdigest()))
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    (root / "retained-skill-records-capture.json").write_text(json.dumps(dict(
        version=1, producer=native, source=revision,
        configuration="native skill roots relative to the contributor working directory",
        captures=captures), indent=2) + "\n")
    with tempfile.TemporaryDirectory(prefix="retained-skill-records-") as artifacts:
        command = ["cargo", "+nightly", "fuzz", "run", "--fuzz-dir", str(root),
                   "--dev", "--sanitizer", "none",
                   "--no-default-features", "--features", "native-skill-records",
                   "--target-dir", os.environ.get("CARGO_TARGET_DIR", str(repository / "target")),
                   "retained_skill_records", str(corpus), "--", "-runs=2000", "-seed=1",
                   "-print_final_stats=1", f"-artifact_prefix={artifacts}/"]
        result = run("retained skill finite campaign", command, root)
        if result.returncode != 0 or "DONE" not in result.stdout or "cov:" not in result.stdout:
            fail("retained skill finite campaign", f"exit {result.returncode}; instrumented completion required")
    print("PASS: actual native writer corpus and finite campaign completed.")


try:
    main()
except (OSError, ValueError, subprocess.SubprocessError) as error:
    fail("native skill qualification execution", str(error))
