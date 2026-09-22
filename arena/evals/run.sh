#!/usr/bin/env bash
# Run the eval three ways on fresh copies of the seed repo and write compare.md.
#   ./run.sh                # routed, frontier, raced
#   ARMS="routed raced" ./run.sh
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
ARENA=${ARENA:-node "$HERE/../packages/arena/dist/cli.js"}
OUT=${OUT:-$HERE/results/$(date +%Y%m%d-%H%M%S)}
FRONTIER=${FRONTIER:-anthropic/claude-sonnet-5}
CHEAP=${CHEAP:-deepseek/deepseek-v4-flash}
ARMS=${ARMS:-"routed frontier raced"}
mkdir -p "$OUT"
for arm in $ARMS; do
  W=$(mktemp -d "${TMPDIR:-/tmp}/arena-eval-XXXXXX")
  cp -R "$HERE/seed/." "$W/" && (cd "$W" && git init -q -b main && git add -A && git -c user.email=eval@arena -c user.name=arena commit -q -m seed)
  case $arm in
    routed)   extra="" ;;
    frontier) extra="--models $FRONTIER" ;;
    raced)    extra="--models $CHEAP,$FRONTIER" ;;
    cheap)    extra="--models $CHEAP" ;;
  esac
  echo "== $arm  ($W)"
  (cd "$W" && $ARENA run "$HERE/tasks.json" --promote --forks git --timeout 300 --log "$OUT/$arm.jsonl" $extra 2>&1 | tee "$OUT/$arm.log" | grep -E "route:|judge|done in|race\(s\)")
  rm -rf "$W"
done
node "$HERE/compare.mjs" "$OUT" | tee "$OUT/compare.md"
