#!/bin/bash
# Installed as gate-checks-verdict.sh; this is the executable, with no alias wrapper.
# gate-checks-verdict.sh <log> <baseline main-gate log>: what gate-checks.sh found in <log> that main's run did not.
# Prints "clippy: +<n> <file>: <lint>" for every (file, lint) whose count rose, "clippy-error: ..." for new compiler errors,
# and "features: <cargo set>" for each newly failing feature set. Exit 1 if any; 0 if none. A baseline without gate-checks
# lines (older main-gate run) or a run whose clippy/feature step did not report is "no baseline" (exit 0, line printed),
# so the ratchet never blocks on missing data. Once main has no findings the ratchet is plain -D warnings.
log=$1; base=${3:-$2}; feature_base=${4:-$2}; new=0
phase_reason=$(bash "$(dirname "$0")/gate-checks.sh" --required-phases "$log"); phase_status=$?
if [ "$phase_status" != 0 ]; then
  printf '%s\n' "$phase_reason"
  exit "$phase_status"
fi
# Numbers in the lint text are masked ("too many lines (115/100)" -> "(N/N)"), and counts summed per masked key, so a
# shrinking-but-still-long function is not a "new" finding (2026-10-22: it forced exact-line-count layout games on #1258).
findings(){ grep -a '^GATE_CLIPPY_FINDING ' "$1" 2>/dev/null | cut -d' ' -f2- | awk '{ n=$1; $1=""; i=index($0, ": "); k=substr($0,1,i+1); m=substr($0,i+2); gsub(/[0-9]+/,"N",m); c[k m]+=n } END { for (x in c) print c[x] x }'; }
if grep -aq '^GATE_CLIPPY rc=' "$base" 2>/dev/null && grep -aq '^GATE_CLIPPY rc=' "$log"; then
  out=$(awk 'FILENAME==ARGV[1] { n=$1; $1=""; b[$0]=n; next } { n=$1; $1=""; if (n > b[$0] + 0) print "clippy: +" (n - b[$0]) $0 }' \
    <(findings "$base") <(findings "$log"))
  [ -n "$out" ] && { printf '%s\n' "$out"; new=1; }
  out=$(comm -13 <(grep -a '^GATE_CLIPPY_ERROR ' "$base" | sort -u) <(grep -a '^GATE_CLIPPY_ERROR ' "$log" | sort -u) | sed 's/^GATE_CLIPPY_ERROR /clippy-error: /')
  [ -n "$out" ] && { printf '%s\n' "$out"; new=1; }
elif grep -aq '^GATE_CLIPPY rc=' "$base" 2>/dev/null; then echo "clippy: the run did not report (main's baseline has a clippy step)"; new=1
else echo "clippy: no baseline"; fi
if grep -aq '^GATE_FEATURES ' "$log"; then
  out=$(comm -13 <(grep -a '^GATE_FEATURES FAILED ' "$feature_base" 2>/dev/null | sort -u) <(grep -a '^GATE_FEATURES FAILED ' "$log" | sort -u) | sed 's/^GATE_FEATURES FAILED /features: /')
  [ -n "$out" ] && { printf '%s\n' "$out"; new=1; }
fi
exit $new
