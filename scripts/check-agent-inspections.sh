#!/usr/bin/env bash
# One feature closure; run only the fs/inspection proof targets and real CLI.
set -euo pipefail
features=acyclic-harness/filesystem-local,acyclic-harness/native-execution,acyclic-harness/terminal,acyclic-harness/tui,acyclic-harness/test-support,acyclic-harness/grpc,acyclic-harness/search-http
artifact=$(mktemp)
trap 'rm -f "$artifact"' EXIT
# Resolve all three test targets together so GraphCoder's transitive features
# cannot force a second build of the shared Harness library.
cargo test --locked -p acyclic-harness -p graphcoder --features "$features" --lib --test e2e --test cli --no-run --message-format=json-render-diagnostics > "$artifact"
python3 - "$artifact" <<'PY'
import json
import os
import subprocess
import sys
import time

messages = [json.loads(line) for line in open(sys.argv[1])]

def run(target, *filters, **environment):
    artifacts = [message for message in messages if message.get("target", {}).get("name") == target]
    started = time.monotonic()
    subprocess.run(["python3", "scripts/qualify-graphcoder-artifact.py", *filters],
                   input="".join(json.dumps(message) + "\n" for message in artifacts),
                   text=True, env=os.environ | environment, check=True)
    print(f"{target} {filters}: {time.monotonic() - started:.3f}s", flush=True)

run("acyclic_harness", "tools::agent")
run("e2e", "local_tests::fs_family")
run("e2e", "scenarios", E2E_FILTER="default-tools/fs/")
run("e2e", "scenarios", E2E_FILTER="agents/inspection-rejected-input.json")
run("e2e", "scenarios", E2E_FILTER="agents/spawn-wait.json")
run("cli", "fs_inspection_refusals_reach_http")
PY
# Type-check the fuzz entry point as well as replaying its corpus above. Other
# fuzz targets have separate gates; this proof must not silently skip command.
cargo check --locked -p acyclic-harness-fuzz --bin command
