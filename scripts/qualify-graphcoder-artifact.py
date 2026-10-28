#!/usr/bin/env python3
"""Run selected suites from Cargo's exact test artifact without rebuilding."""
import json
import hashlib
import re
import subprocess
import sys


def fail(step, detail):
    recovery = ("bash scripts/qualify-dst.sh" if sys.argv[1:2] == ["--runner"]
                else "bash scripts/qualify-graphcoder-lane.sh")
    sys.exit(f"STEP_FAILED {step}: {detail}; recovery: {recovery}")


executable = None
for number, line in enumerate(sys.stdin, 1):
    try:
        message = json.loads(line)
    except json.JSONDecodeError:
        fail("cargo-artifact-json", f"invalid Cargo JSON at line {number}")
    if not isinstance(message, dict):
        fail("cargo-artifact-json", f"Cargo JSON object required at line {number}")
    if message.get("reason") == "compiler-message":
        rendered = message["message"].get("rendered")
        if rendered:
            print(rendered, end="", file=sys.stderr)
    if (message.get("reason") == "compiler-artifact"
            and message.get("profile", {}).get("test")
            and message.get("executable")):
        if executable is not None and executable != message["executable"]:
            fail("cargo-artifact-target", "qualification requires one focused test target")
        executable = message["executable"]
if executable is None:
    fail("cargo-artifact-target", "Cargo did not produce a test executable")

if sys.argv[1:2] == ["--runner"]:
    if len(sys.argv) != 3:
        fail("artifact-runner", "qualification requires exactly one artifact runner")
    try:
        subprocess.run([sys.executable, sys.argv[2], executable], check=True)
    except (OSError, subprocess.CalledProcessError) as error:
        fail("artifact-runner", str(error))
    sys.exit(0)

try:
    listed = subprocess.check_output([executable, "--list"], text=True)
except (OSError, subprocess.CalledProcessError) as error:
    fail("artifact-list", str(error))
tests = [line.removesuffix(": test") for line in listed.splitlines()
         if line.endswith(": test")]
whole_suite = "--suite" in sys.argv[1:]
inventory_only = "--inventory" in sys.argv[1:]
prefixes = [argument for argument in sys.argv[1:]
            if argument not in ("--suite", "--inventory")]
if not prefixes:
    fail("artifact-selector", "qualification requires explicit test filters")
for prefix in prefixes:
    if not any(name.startswith(prefix) for name in tests):
        fail("artifact-selector", f"no tests matched qualification filter {prefix}")
selected = [name for name in tests if name.startswith(tuple(prefixes))]
if inventory_only:
    names = sorted(tests)
    print(json.dumps({"executable": executable, "discovered": names,
                      "selected": sorted(selected),
                      "execution": "NOT_RUN", "ignored_status": "NOT_OBSERVED",
                      "filters": {prefix: sum(name.startswith(prefix) for name in tests)
                                  for prefix in prefixes},
                      "case_ids_sha256": hashlib.sha256(
                          "\n".join(names).encode()).hexdigest()}))
    sys.exit(0)
print(f"Test executable: {executable}", flush=True)
groups = [selected] if whole_suite else [[name] for name in selected]
for names in groups:
    # Nested native process tests own their stdio pipes; do not pass the remote
    # build transport's descriptors into their process/runtime fixture.
    command = [executable, *names, "--exact"]
    if whole_suite:
        command.extend(["--test-threads=1", "--nocapture"])
    try:
        result = subprocess.run(command, capture_output=True,
                                text=True, timeout=9)
    except subprocess.TimeoutExpired as error:
        for output, stream in [(error.stdout, sys.stdout), (error.stderr, sys.stderr)]:
            if output:
                stream.write(output.decode(errors="replace"))
        fail("artifact-timeout", f"test exceeded its original nine-second bound: {names}")
    except OSError as error:
        fail("artifact-execute", str(error))
    print(result.stdout, end="", flush=True)
    print(result.stderr, end="", file=sys.stderr, flush=True)
    if result.returncode:
        fail("artifact-execute", f"test returned {result.returncode}: {names}")
    summaries = re.findall(
        r"^test result: ok\. ([0-9]+) passed; ([0-9]+) failed; "
        r"([0-9]+) ignored; ([0-9]+) measured; [0-9]+ filtered out;",
        result.stdout, re.MULTILINE,
    )
    if not summaries or tuple(map(int, summaries[-1])) != (len(names), 0, 0, 0):
        fail("artifact-completion", f"expected {len(names)} passed, no ignored cases: {names}")
