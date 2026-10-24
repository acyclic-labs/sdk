#!/bin/bash
# Compiler artifacts, not executable basenames, identify libFuzzer drivers.
set -euo pipefail
if [ "${1:-}" = --build ]; then
  metadata=$2; manifest=$3; shift 3
  [ "$1" = -- ] || exit 2
  shift
  # Complete artifact discovery before executing anything. Cargo's JSON stream
  # otherwise races its runner, which may start before a consumer sees the event.
  "$@" --no-run --message-format=json-render-diagnostics > "$manifest.artifacts" || exit $?
  exec bash "$0" --collect "$metadata" "$manifest.artifacts" "$manifest"
fi
if [ "${1:-}" = --collect ]; then
  shift
  exec python3 - "$@" <<'PY'
import json, os, sys
from pathlib import Path
try:
    metadata, artifacts, output = map(Path, sys.argv[1:])
    m = json.loads(metadata.read_text())
    fuzz = {(p['id'], t['name']) for p in m['packages'] if (p.get('metadata') or {}).get('cargo-fuzz')
            for t in p['targets'] if t['kind'] == ['bin']}
    paths = set()
    for line in artifacts.read_text().splitlines():
        item = json.loads(line)
        if item.get('reason') != 'compiler-artifact':
            continue
        if (item['package_id'], item['target']['name']) in fuzz and item['target']['kind'] == ['bin'] and item.get('executable'):
            executable = Path(item['executable'])
            if not executable.is_absolute() or not executable.is_file():
                raise RuntimeError('invalid fuzz executable artifact: ' + str(executable))
            paths.add(str(executable.resolve()))
    tmp = Path(str(output) + '.tmp')
    tmp.write_text(''.join(p + '\n' for p in sorted(paths)))
    os.replace(tmp, output)
except Exception as error:
    print('error: GATE_RUNNER_INFRA compiler-artifact discovery: ' + str(error), file=sys.stderr)
    sys.exit(2)
PY
fi
program=$1; shift
if [ -z "${GATE_FUZZ_EXECUTABLES:-}" ] || [ ! -f "$GATE_FUZZ_EXECUTABLES" ]; then
  echo 'error: GATE_RUNNER_INFRA missing completed compiler-artifact manifest' >&2
  exit 2
fi
program=$(realpath -e -- "$program")
match=0
grep -Fxq -- "$program" "$GATE_FUZZ_EXECUTABLES" || match=$?
if [ "$match" -gt 1 ]; then
  echo 'error: GATE_RUNNER_INFRA cannot read compiler-artifact manifest' >&2
  exit 2
fi
if [ "$match" = 0 ]; then
  echo "GATE_FUZZ_SMOKE executable=$program runs=1 seed=1"
  set -- -runs=1 -seed=1
fi
if [ -n "${GATE_BASE_TEST_RUNNER:-}" ]; then
  exec "$GATE_BASE_TEST_RUNNER" "$program" "$@"
fi
exec "$program" "$@"
