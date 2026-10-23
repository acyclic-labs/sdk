#!/bin/bash
# PR merge gate: run the harness, providers, TUI and GraphCoder suites on bb2 against PRs #$@ merged together onto current origin/main (one run per batch).
# The caller (gate-drain.sh) holds the gate lock.
# Ratchet while main is red: fail on any failing test not already failing in the newest main-gate log.
# Once main is green (newest main-gate log has 0 failures) this is equivalent to "all tests pass".
# Exit 0 = pass. Prints "GATE PASS|FAIL #n: ..." and appends it to the fleet alert log.
gate_started=$SECONDS
prs="$*"; n=$(echo $prs | tr ' ' '+'); W=Q:/wt/pr-gate; L=Q:/lanes/pr-gate; mkdir -p $L
# Local preparation failures have a separate held state, never a bb2 retry.
python Q:/lanes/gate-prepare.py prepare $prs; preparation_rc=$?
[ "$preparation_rc" = 0 ] || exit "$preparation_rc"
# Standing rule: rebase, never merge main. A PR whose own commits include a merge is refused (exit 3 like a conflict, so the
# rest of the batch still gates); the author moves it to a rebased superseding PR.
for p in $prs; do if [ -n "$(git -C Q:/sdk log --merges --format=%h origin/main..refs/pr-gate/$p)" ]; then echo "GATE FAIL #$n: #$p contains a merge commit"; echo "$(date -u +%FT%T.000Z) pr-gate GATE_MERGECOMMIT #$p contains a merge commit; needs a rebased superseding PR" >> ~/.codex-fleet/alerts.log; echo $p > $L/.conflict; exit 3; fi; done
# GitHub applies closing keywords on merge (2026-10-21 "fix #1237" in #1231 closed the open #1237): a PR naming any issue or
# PR but its own superseded ones with close/fix/resolve is refused like a merge commit until its text is corrected.
for p in $prs; do k=$(bash Q:/lanes/closing-keywords.sh $p); kr=$?
  if [ $kr = 2 ]; then echo "GATE INFRA #$n: could not read #$p"; echo "$(date -u +%FT%T.000Z) pr-gate GATE_INFRA #$n: could not read #$p for closing keywords" >> ~/.codex-fleet/alerts.log; exit 4; fi
  if [ $kr != 0 ]; then echo "GATE FAIL #$n: #$p uses closing keywords: $k"; echo "$(date -u +%FT%T.000Z) pr-gate GATE_CLOSINGKEYWORD #$p: $(printf '%s; ' $k | cut -c1-300) edit the PR body/commits (only superseded PRs may be closed)" >> ~/.codex-fleet/alerts.log; echo $p > $L/.conflict; exit 3; fi; done
base=$(cat "$L/.prepared-base")  # pinned main, before any batch merge
log=$L/$n-$(git -C $W rev-parse --short HEAD).log
# Known intermittent hangs (owned, fix in flight) are skipped: one hang stalls the run past the 30-min job limit and
# fails every PR in the batch as "incomplete". hangs.txt lists them with their owner; remove an entry when its fix merges.
# A lane may require bb2 even when the shared wrapper offers other boxes.
preparation_seconds=$((SECONDS-gate_started))
box_env=()
for p in $prs; do
  if grep -qx "$p" "$L/bb2-only.txt" 2>/dev/null; then
    box_env=(--env BUILD_SLOT_BUILDBOX_HOST=bb2)
    break
  fi
done
skips=$(grep -v '^#' Q:/lanes/hangs.txt | sed 's/^/--skip /' | paste -sd' ')
# Compute the merged batch's complete delta against the pinned origin/main base,
# before resetting the checkout. Selection runs locally; no build-box job is used.
if ! python Q:/lanes/gate-affected.py "$W" "$base" --output "$W/.gate-selection.json" > "$log" 2>&1; then
  v="FAIL #$n: affected-crate planning failed log=$log"
  echo "GATE $v"; echo "$(date -u +%FT%T.000Z) pr-gate GATE_FAIL ${v#* }" >> ~/.codex-fleet/alerts.log
  exit 1
fi
cp "$W/.gate-selection.json" "$log.scope.json"
cp Q:/lanes/pr-gate-checks.sh "$W/.gate-checks.sh"
if [ "$(tr -d '\r' < "$W/.gate-scope-mode")" = full ] || grep -qx 'acyclic-native-runtime' "$W/.gate-clippy-packages" "$W/.gate-tests-packages"; then
  echo 'GATE_WINDOWS_RUNTIME_REQUIRED target=x86_64-pc-windows-gnu package=acyclic-native-runtime features=all targets=all' >> "$log"
fi
if [ ! -s "$W/.gate-tests-packages" ]; then
  printf '%s
' 'GATE_EMPTY no changed Cargo inputs' 'GATE_DONE rc=0' >> "$log"; rc=0
else
  (cd "$W" && BUILD_SLOT_TIER=merge WSLENV=BUILD_SLOT_TIER pwsh -NoProfile -File Q:/lanes/bs.ps1 "${box_env[@]}" --env BUILD_SLOT_TIER=merge --env BUILD_SLOT_PR="$n" --env BUILD_SLOT_PHASE=pr-gate --env BUILD_SLOT_GATE_PREPARATION_SECONDS="$preparation_seconds" --env BUILD_SLOT_LABEL=pr-gate --env BUILD_SLOT_GATE_LOG="$log" bash .gate-checks.sh $skips) >> "$log" 2>&1; rc=$?
fi
echo "GATE_PR_WALL seconds=$((SECONDS-gate_started)) remote_rc=$rc" >> "$log"
if grep -aq '^BB_DISK_LOW ' "$log"; then
  v="INFRA #$n: BB_DISK_LOW: $(grep -am1 '^BB_DISK_FREE ' "$log") log=$log"
  echo "GATE $v"; echo "$(date -u +%FT%T.000Z) pr-gate BB_DISK_LOW ${v#* }" >> ~/.codex-fleet/alerts.log
  exit 4
fi
# Executor isolation failures must not inherit PASS from a green Cargo footer.
if grep -aq '^error: GATE_HOME_LEAK ' "$log"; then
  v="INFRA #$n: $(grep -am1 '^error: GATE_HOME_LEAK ' "$log") log=$log"
  echo "GATE $v"; echo "$(date -u +%FT%T.000Z) pr-gate GATE_INFRA ${v#* }" >> ~/.codex-fleet/alerts.log
  exit 4
fi
# A successful payload cannot hide a failed executor/export/cleanup boundary.
if [ "$rc" != 0 ] && grep -aq '^GATE_DONE rc=0$' "$log"; then
  v="INFRA #$n: executor exited rc=$rc after successful payload; inspect wrapper stderr log=$log"
  echo "GATE $v"; echo "$(date -u +%FT%T.000Z) pr-gate GATE_INFRA ${v#* }" >> ~/.codex-fleet/alerts.log
  exit 4
fi

# A new strict target requirement cannot inherit a PASS from main's old gap.
windows_reason=$(bash Q:/lanes/gate-windows-runtime-verdict.sh "$log"); windows_status=$?
if [ "$windows_status" != 0 ]; then
  if [ "$windows_status" = 4 ] || [ "$rc" = 224 ]; then kind=INFRA; code=4; else kind=FAIL; code=1; fi
  v="$kind #$n: $(printf '%s' "$windows_reason" | tr '\n' ';' | cut -c1-900) log=$log"
  echo "GATE $v"
  echo "$(date -u +%FT%T.000Z) pr-gate GATE_${v%% *} ${v#* }" >> ~/.codex-fleet/alerts.log
  exit "$code"
fi
# One selector owns the test and lint baseline. A failed wide discovery run
# cannot displace the last green narrow run or claim unexecuted packages.
if ! base_log=$(python Q:/lanes/gate-baseline.py "$W" "$base" 2> "$log.baseline-error"); then
  cat "$log.baseline-error" >> "$log"
  echo "GATE BASELINE #$n: no complete main baseline; verdict deferred log=$log"
  echo "$(date -u +%FT%T.000Z) pr-gate GATE_BASELINE #$n: no complete main baseline; verdict deferred log=$log" >> ~/.codex-fleet/alerts.log
  exit 5
fi
echo "GATE_BASELINE log=$base_log" >> "$log"
clippy_base=$(python Q:/lanes/gate-baseline.py "$W" "$base" --phase clippy 2>> "$log.baseline-error")
features_base=$(python Q:/lanes/gate-baseline.py "$W" "$base" --phase features 2>> "$log.baseline-error")
if [ -z "$clippy_base" ] || [ -z "$features_base" ]; then
  echo "GATE BASELINE #$n: complete main check baseline unavailable; verdict deferred log=$log"
  echo "$(date -u +%FT%T.000Z) pr-gate GATE_BASELINE #$n: complete main check baseline unavailable; verdict deferred log=$log" >> ~/.codex-fleet/alerts.log
  exit 5
fi
echo "GATE_CHECKS_BASELINE clippy=$clippy_base features=$features_base" >> "$log"
test_base_log=""
if { [ "$rc" != 0 ] || grep -aqE '^GATE_CLIPPY rc=[1-9]' "$log"; } && [ "$rc" != 224 ] && grep -aq '^GATE_DONE rc=' "$log" && ! grep -aq 'No space left on device' "$log"; then
  if ! test_base_log=$(GATE_BASELINE_PRS="$n" python Q:/lanes/gate-test-baseline.py "$W" "$base" "$log" --scope "$W/.gate-selection.json" --baseline-log "$base_log" 2> "$log.baseline-error"); then
    cat "$log.baseline-error" >> "$log"
    v="BASELINE #$n: focused main baseline unavailable; verdict deferred; inspect $log.baseline-error log=$log"
    echo "GATE $v"; echo "$(date -u +%FT%T.000Z) pr-gate GATE_BASELINE ${v#* }" >> ~/.codex-fleet/alerts.log
    exit 5
  fi
  [ -z "$test_base_log" ] || echo "GATE_TEST_BASELINE log=$test_base_log" >> "$log"
fi
git -C $W checkout -q --detach -f origin/main
pr_fails=$(bash Q:/lanes/failed-tests.sh $log)
base_fails=$( { bash Q:/lanes/failed-tests.sh "$base_log" 2>/dev/null; [ -z "$test_base_log" ] || bash Q:/lanes/failed-tests.sh "$test_base_log"; cat Q:/lanes/flakes.txt; } | sort -u)  # known flakes (owned, being fixed) never block a PR
new=$(comm -23 <(printf '%s\n' "$pr_fails" | grep .) <(printf '%s\n' "$base_fails" | grep .))
compile=$(grep -cE '^error(\[E[0-9]+\])?:' $log)
results=$(grep -cE '^test result:' $log)
# Include actual Cargo diagnostics in every failure summary sent to the owner.
cargo_errors=$(grep -aE '^error(\[E[0-9]+\])?:|^GATE_CLIPPY_RAW (error(\[E[0-9]+\])?:|.*\.rs:[0-9]+:[0-9]+: error)' "$log" | head -3 | cut -c1-220 | tr '\n' ';')
terminated=$(grep -aE 'signal: (9|15), SIG(KILL|TERM)' "$log" | head -1 | sed -E 's/.*(signal: (9|15), SIG(KILL|TERM)[^)]*).*/\1/')
# A full build disk fails arbitrary tests with ENOSPC (2026-10-21: the build box's /mnt/bb at 100% failed #1260 in 9 unrelated tests).
if [ $rc != 0 ] && grep -aq 'No space left on device' $log; then v="INFRA #$n: build box disk full (ENOSPC), not the PRs log=$log"
elif [ -n "$terminated" ]; then v="INFRA #$n: build process terminated ($terminated); $cargo_errors log=$log"
# A known failing test elsewhere in the same run cannot hide a new compile
# failure. The focused main check above has already qualified compiler roots.
elif grep -aq '^error: could not compile ' "$log"; then v="FAIL #$n: build errors ($compile): $cargo_errors log=$log"
elif [ "$compile" -gt 0 ] && [ -z "$pr_fails" ]; then v="FAIL #$n: build errors ($compile): $cargo_errors log=$log"
elif [ -n "$new" ]; then v="FAIL #$n: new failures vs main ($(basename $base_log .log)): $(printf '%s ' $new | cut -c1-500) log=$log"
# A run cut off mid-stream (ssh drop, box stop) can still exit 0 with partial "test result" lines: without gate-checks.sh's
# GATE_DONE marker the run proves nothing (2026-10-22: #1334 passed on a log truncated mid-test, with no clippy step).
elif ! grep -aq '^GATE_DONE rc=' $log; then v="INFRA #$n: run cut off before gate-checks finished (rc=$rc) log=$log"
elif [ $rc = 224 ] && grep -aq '^\[build-slot\] remote failed:' $log; then v="INFRA #$n: build box failed, not the PRs ($(grep -a -m1 '^\[build-slot\] remote failed:' $log | cut -c30-150)) log=$log"
elif [ $rc != 0 ] && [ "$results" -eq 0 ]; then v="FAIL #$n: no test results (rc=$rc): ${cargo_errors:-no Cargo error diagnostics captured} log=$log"
elif grep -aq '^GATE_EMPTY no changed Cargo inputs$' "$log"; then v="PASS #$n: no changed Cargo inputs log=$log"
# Clippy and feature-set ratchet: no (file, lint) count above main's, no new compiler error, no newly failing feature set.
elif ! checks=$(bash Q:/lanes/gate-checks-verdict.sh "$log" "$base_log" "$clippy_base" "$features_base"); then v="FAIL #$n: new clippy/feature findings vs qualified main checks: $(printf '%s' "$checks" | tr '\n' ';' | cut -c1-500) log=$log"
else v="PASS #$n: no new failures ($(printf '%s\n' "$pr_fails" | grep -c .) pre-existing on main; $(printf '%s' "$checks" | tr '\n' ' ')) log=$log"; fi
printf '%s\n' "$checks" > "$log.checks"
echo "GATE $v"; echo "$(date -u +%FT%T.000Z) pr-gate GATE_${v%% *} ${v#* }" >> ~/.codex-fleet/alerts.log
# Exit 4 = the build box failed (spot stop, ssh loss): the drain re-queues the batch instead of bisecting it.
case $v in PASS*) exit 0;; INFRA*) exit 4;; *) exit 1;; esac
