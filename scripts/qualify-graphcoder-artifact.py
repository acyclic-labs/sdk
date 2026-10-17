#!/usr/bin/env python3
"""Run selected suites from Cargo's exact test artifact without rebuilding."""
import json
import subprocess
import sys

executable = None
for line in sys.stdin:
    message = json.loads(line)
    if message.get("reason") == "compiler-message":
        rendered = message["message"].get("rendered")
        if rendered:
            print(rendered, end="", file=sys.stderr)
    if (message.get("reason") == "compiler-artifact"
            and message.get("profile", {}).get("test")
            and message.get("executable")):
        if executable is not None and executable != message["executable"]:
            sys.exit("Qualification requires one focused test target")
        executable = message["executable"]
if executable is None:
    sys.exit("Cargo did not produce a test executable")

listed = subprocess.check_output([executable, "--list"], text=True)
tests = [line.removesuffix(": test") for line in listed.splitlines()
         if line.endswith(": test")]
prefixes = sys.argv[1:]
if not prefixes:
    sys.exit("Qualification requires explicit test filters")
for prefix in prefixes:
    if not any(name.startswith(prefix) for name in tests):
        sys.exit(f"No tests matched qualification filter {prefix}")
selected = [name for name in tests if name.startswith(tuple(prefixes))]
print(f"Test executable: {executable}", flush=True)
for name in selected:
    # Nested native process tests own their stdio pipes; do not pass the remote
    # build transport's descriptors into their process/runtime fixture.
    try:
        result = subprocess.run([executable, name, "--exact"], capture_output=True,
                                text=True, timeout=9)
    except subprocess.TimeoutExpired as error:
        for output, stream in [(error.stdout, sys.stdout), (error.stderr, sys.stderr)]:
            if output:
                stream.write(output.decode(errors="replace"))
        raise
    print(result.stdout, end="", flush=True)
    print(result.stderr, end="", file=sys.stderr, flush=True)
    result.check_returncode()
