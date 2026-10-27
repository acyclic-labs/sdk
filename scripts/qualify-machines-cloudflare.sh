#!/usr/bin/env bash
# Run through Q:/lanes/bs.ps1 on bb2. No Cloudflare account is used.
set -euo pipefail
trap 'printf "Cloudflare qualification step at line %s failed; recovery: bash scripts/qualify-machines-cloudflare.sh\n" "$LINENO" >&2' ERR
fail() { printf '%s failed; recovery: %s\n' "$1" "$2" >&2; exit 1; }
cargo test --locked -q -p acyclic-machines -p acyclic-machines-daytona -p acyclic-conformance --features cloudflare,e2b,modal,machines-shell --lib || fail 'Machines regression tests' 'cargo test --locked -p acyclic-machines -p acyclic-machines-daytona -p acyclic-conformance --features cloudflare,e2b,modal,machines-shell --lib'
cargo build --locked -q -p acyclic-conformance --features cloudflare,e2b,modal,machines-shell --example cloudflare-conformance || fail 'Cloudflare conformance build' 'cargo build --locked -p acyclic-conformance --features cloudflare,e2b,modal,machines-shell --example cloudflare-conformance'
cargo build --locked -q -p acyclic-native-runtime --bin acyclic-process-host || fail 'Shared process custodian build' 'cargo build --locked -p acyclic-native-runtime --bin acyclic-process-host'
binary=$(realpath "${CARGO_TARGET_DIR:-target}/debug/examples/cloudflare-conformance")
owner=$(realpath "${CARGO_TARGET_DIR:-target}/debug/acyclic-process-host")
node=$(command -v node)
export WRANGLER_SEND_METRICS=false
cd deploy/machines-cloudflare
npm ci --ignore-scripts --no-audit --no-fund || fail 'Worker dependency installation' 'cd deploy/machines-cloudflare && npm ci --ignore-scripts --no-audit --no-fund'
npm run check || fail 'Worker type check' 'cd deploy/machines-cloudflare && npm run check'
scratch=$(mktemp -d "${TMPDIR:-/tmp}/acyclic-machines.XXXXXX")
coproc WORKER { exec "$owner" --completion "$scratch/retired.json" "$node" "$PWD/node_modules/wrangler/bin/wrangler.js" dev --local --ip 127.0.0.1 --port 15440 --inspector-port 15441 >"$scratch/workerd.log" 2>&1; }
devpid=$WORKER_PID
control=${WORKER[1]}
cleanup() {
  local status=$? retired
  trap - EXIT
  set +e
  exec {control}>&-
  wait "$devpid"
  retired=$?
  if ! python3 - "$scratch/retired.json" "$retired" <<'PY'
import json, sys
try:
    with open(sys.argv[1], encoding="utf-8") as source:
        receipt = json.load(source)
    valid = (receipt.get("schema") == "acyclic.process-custody-completion.v2"
             and receipt.get("process_tree_retired") is True
             and receipt.get("exit_code") == int(sys.argv[2])
             and int(sys.argv[2]) in (0, 124))
    if not valid:
        raise ValueError("process-tree retirement receipt missing or inconsistent")
except (OSError, ValueError) as error:
    print(f"step=workerd-retirement: {error}; recovery: inspect {sys.argv[1]} then bash scripts/qualify-machines-cloudflare.sh", file=sys.stderr)
    sys.exit(1)
PY
  then
    status=1
  else
    printf 'Local workerd process tree retired (custody receipt, exit %s)\n' "$retired"
  fi
  if (( status != 0 )); then
    cat "$scratch/workerd.log"
    printf 'step=Cloudflare qualification; recovery: bash scripts/qualify-machines-cloudflare.sh; retained artifacts: %s\n' "$scratch" >&2
  else
    rm -r -- "$scratch" || { printf 'step=workerd-artifact-retirement; recovery: rm -r -- %q\n' "$scratch" >&2; status=1; }
  fi
  exit "$status"
}
trap cleanup EXIT
ready=false
for attempt in $(seq 1 30); do
  if curl --silent --max-time 1 http://127.0.0.1:15440/ >/dev/null; then ready=true; break; fi
  sleep 1
done
if ! "$ready"; then fail 'Local workerd startup' 'bash scripts/qualify-machines-cloudflare.sh'; fi
warm=http://127.0.0.1:15440/sandboxes/acyclic-00000000-0000-0000-0000-000000000099-00000000-0000-0000-0000-000000000099-0
curl --fail --silent --show-error --max-time 10 -X DELETE "$warm" || fail 'Previous warmup Container cleanup' 'bash scripts/qualify-machines-cloudflare.sh'
curl --fail --silent --show-error --max-time 25 -X POST "$warm" -H 'content-type: application/json' --data '{"image":"workspace","env":{},"internet":false}' || fail 'Native Container cold start' 'bash scripts/qualify-machines-cloudflare.sh'
curl --fail --silent --show-error --max-time 5 "$warm" || fail 'Native Container state read' 'bash scripts/qualify-machines-cloudflare.sh'
curl --fail --silent --show-error --max-time 10 -X DELETE "$warm" || fail 'Warmup Container cleanup' 'bash scripts/qualify-machines-cloudflare.sh'
cd ../..
/usr/bin/time -f 'Canonical Machines suite: %e seconds' timeout 55s "$binary" || fail 'Cloudflare canonical Machines conformance' 'bash scripts/qualify-machines-cloudflare.sh'
