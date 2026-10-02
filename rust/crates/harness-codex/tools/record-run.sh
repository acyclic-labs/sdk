#!/bin/bash
# usage: run.sh NAME MODE [extra codex args...]
name=$1; mode=$2; shift 2
cd ${CODEX_PROBE_DIR:-/tmp/codex-probe}
echo "$mode" > mode
: > requests.log
start=$(date +%s)
env -u OPENAI_API_KEY -u CODEX_API_KEY CODEX_HOME=${CODEX_PROBE_DIR:-/tmp/codex-probe}/home FAKE_PROVIDER_KEY=dummy-key-123 \
  ./node_modules/.bin/codex exec --json --skip-git-repo-check -C ${CODEX_PROBE_DIR:-/tmp/codex-probe}/work "$@" \
  > fixtures/run-$name.stdout.jsonl 2> fixtures/run-$name.stderr.txt < /dev/null
code=$?
end=$(date +%s)
echo "exit=$code elapsed=$((end-start))s" | tee fixtures/run-$name.exit.txt
cp requests.log fixtures/run-$name.requests.jsonl
cat fixtures/run-$name.stdout.jsonl
echo "--- stderr"; cat fixtures/run-$name.stderr.txt | head -40
echo "--- requests"; node -e '
for (const line of require("fs").readFileSync("requests.log", "utf8").split("\n").filter(Boolean)) {
  const r = JSON.parse(line); const b = r.body ?? {};
  console.log(r.n, (r.t % 1000).toFixed(2), r.method, r.path, (b.input ?? []).map(i => i.type).slice(-4));
}'
