#!/bin/bash
# Main discovery covers every workspace target, doctests and browser consumers.
# MAIN_WIDE reports discovery independently. Only a green complete run is
# promoted to MAIN_GATE; until then PRs retain the last qualified narrow log.
W=Q:/wt/main-gate; L=Q:/lanes/main-gate; mkdir -p "$L/wide" "$L/infra"
last=$(ls -t "$L"/wide/[0-9a-f]*.log "$L"/[0-9a-f]*.log 2>/dev/null | head -1 | xargs -r basename | sed 's/.log$//')
if [ -s "$L/.last-input" ]; then last=$(cat "$L/.last-input");
elif [ -n "$last" ]; then last=$(git -C Q:/sdk rev-parse --verify "$last^{commit}" 2>/dev/null || printf '%s' "$last"); fi
remember_input(){ printf '%s\n' "$tip" > "$L/.last-input.$$" && mv -f "$L/.last-input.$$" "$L/.last-input"; }
# Single runner: a second copy (e.g. a restart whose old shell survived) would run the same tip twice and post two results.
mkdir $L/.runner 2>/dev/null || { echo "main-gate already running (remove $L/.runner if not)"; exit 0; }; trap "rmdir $L/.runner" EXIT
while :; do
  if ! git -C Q:/sdk fetch -q origin main > "$L/.fetch.log" 2>&1; then
    tip=$(git -C Q:/sdk rev-parse --verify 'origin/main^{commit}' 2>/dev/null || echo unavailable)
    remember_input || exit 6; last=$tip
    if ! cmp -s "$L/.fetch.log" "$L/.fetch-held.log"; then
      cp "$L/.fetch.log" "$L/.fetch-held.log"
      echo "$(date -u +%FT%T.000Z) main-gate MAIN_WIDE preparation-INFRA $tip: fetch failed; held until changed input or .rerun; diagnostics=$L/.fetch-held.log" >> ~/.codex-fleet/alerts.log
    fi
    sleep 300; continue
  fi
  tip=$(git -C Q:/sdk rev-parse --verify 'origin/main^{commit}') || { sleep 300; continue; }
  # .rerun: gate the current tip again (e.g. after gate-checks.sh changed); the old log stays the PR baseline until replaced.
  if [ "$tip" != "$last" ] || [ -e $L/.rerun ]; then rm -f $L/.rerun
    remember_input || { echo 'main input hold could not be persisted'; exit 6; }; last=$tip
    log=$L/.running.log  # renamed to <sha>.log only when complete, so the PR gate never baselines on a partial run
    # Tests, plus workspace clippy and every published crate's feature sets (each feature alone too): the PR gate's baseline.
    if ! python Q:/lanes/main-prepare.py Q:/sdk "$W" "$L" "$tip" Q:/lanes/gate-checks.sh > "$log" 2>&1; then
      infra_log="$L/infra/$tip-preparation-$(date -u +%Y%m%dT%H%M%SZ)-$$.log"
      mv -f "$log" "$infra_log"
      echo "$(date -u +%FT%T.000Z) main-gate MAIN_WIDE preparation-INFRA $tip: local preparation failed; input held; baseline retained log=$infra_log" >> ~/.codex-fleet/alerts.log
      sleep 300; continue
    fi
    # Keep Windows native streams on a pipe; one writer owns the log file.
    (cd $W && BUILD_SLOT_TIER=merge WSLENV=BUILD_SLOT_TIER pwsh -NoProfile -File Q:/lanes/bs.ps1 --env BUILD_SLOT_TIER=merge --env BUILD_SLOT_LABEL=main-gate --env BUILD_SLOT_PHASE=main-wide --env BUILD_SLOT_GATE_LOG="$log" bash .gate-checks.sh --each-feature --locked $(bash Q:/lanes/gate-pkgs.sh .)                                                      --all-features --no-fail-fast -- $(grep -v '^#' Q:/lanes/hangs.txt | sed 's/^/--skip /' | paste -sd' ')) 2>&1 | tee -a "$log" >/dev/null
    capture_status=("${PIPESTATUS[@]}"); rc=${capture_status[0]}
    if [ "${capture_status[1]}" != 0 ]; then
      echo 'error: GATE_RUNNER_INFRA coordinator log writer failed' >> "$log"
      rc=224
    fi
    echo "GATE_EXECUTOR rc=$rc" >> "$log"
    if ! python Q:/lanes/main-prepare.py Q:/sdk "$W" "$L" "$tip" Q:/lanes/gate-checks.sh --verify >> "$log" 2>&1; then rc=224; fi
    # A client cut-off or executor/payload disagreement never qualifies main,
    # even if a partial stream contains successful tests or a payload footer.
    completed=$(grep -acE '^GATE_DONE rc=(0|1|101)$' "$log")
    payload_rc=$(sed -nE 's/^GATE_DONE rc=(-?[0-9]+)$/\1/p' "$log")
    if [ "$completed" != 1 ] || [ "$payload_rc" != "$rc" ] || grep -aqE '^error: GATE_RUNNER_INFRA |^\[buildbox\] (HOME guard|cleanup|export) INFRA:' "$log"; then
      infra_log="$L/infra/$tip-$(date -u +%Y%m%dT%H%M%SZ)-$$.log"
      if grep -aq '^BB_DISK_LOW ' "$log"; then
        echo "$(date -u +%FT%T.000Z) main-gate BB_DISK_LOW INFRA $tip: $(grep -am1 '^BB_DISK_FREE ' "$log") log=$infra_log" >> ~/.codex-fleet/alerts.log
      fi
      mv -f "$log" "$infra_log"
      echo "$(date -u +%FT%T.000Z) main-gate MAIN_WIDE INFRA $tip: executor rc=$rc payload=${payload_rc:-absent}; baseline retained log=$infra_log" >> ~/.codex-fleet/alerts.log
      # Hold this input until a changed main or an explicit recovery rerun.
      last=$tip; sleep 300; continue
    fi
    mv -f "$log" "$L/wide/$tip.log"; log="$L/wide/$tip.log"
    fails=$( { bash Q:/lanes/failed-tests.sh $log; grep -E '^error(\[E[0-9]+\])?:' $log; } | sort -u)
    res=$(grep -E '^test result:' $log | awk '{p+=$4; f+=$6} END {print p" passed "f" failed"}')
    n=$(printf '%s\n' "$fails" | grep -c .)
    clippy=$(grep -a '^GATE_CLIPPY_FINDING ' $log | awk '{n+=$2} END {print n+0}'); feat=$(grep -a '^GATE_FEATURES feature matrix:' $log | cut -d' ' -f4-)
    if [ $rc = 0 ]; then st=GREEN; else st=RED; fi
    echo "$(date -u +%FT%T.000Z) main-gate MAIN_WIDE $st $tip: $res; clippy $clippy findings; features ${feat:-absent}; $n failing: $(printf '%s ' $fails | cut -c1-600) log=$log" >> ~/.codex-fleet/alerts.log
    if [ "$rc" = 0 ] && python -c 'import importlib.util,sys,pathlib; s=importlib.util.spec_from_file_location("baseline","Q:/lanes/gate-baseline.py"); m=importlib.util.module_from_spec(s); s.loader.exec_module(m); sys.exit(0 if m.eligible(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8",errors="replace")) else 1)' "$log"; then
      cp "$log" "$L/$tip.log"
      echo "$(date -u +%FT%T.000Z) main-gate MAIN_GATE GREEN $tip: complete workspace baseline; $res log=$L/$tip.log" >> ~/.codex-fleet/alerts.log
      # Qualification failures do not repeatedly rebuild or route PRs. A new
      # complete baseline makes these parked scopes eligible again.
      if [ -s Q:/lanes/pr-gate/baseline-pending.txt ]; then
        (
          trap - EXIT
          # The PR drain writes this queue under its gate lock. Transfer under
          # the same lock, so a concurrent append cannot be lost to removal.
          until mkdir Q:/lanes/pr-gate/.lock 2>/dev/null; do sleep 1; done
          trap 'rmdir Q:/lanes/pr-gate/.lock' EXIT
          if [ -s Q:/lanes/pr-gate/baseline-pending.txt ]; then
            python Q:/lanes/gate-queue.py Q:/lanes/pr-gate move baseline-pending pending || exit 6
          fi
          rmdir Q:/lanes/pr-gate/.lock; trap - EXIT
          exec bash Q:/lanes/gate-drain.sh
        ) >> Q:/lanes/pr-gate/baseline-resume.out 2>&1 < /dev/null &
      fi
    else
      baseline=$(python Q:/lanes/gate-baseline.py "$W" "$tip" 2>/dev/null || true)
      echo "MAIN_WIDE $st $tip; PR baseline retained: ${baseline:-unavailable}"
    fi
    last=$tip
  fi
  sleep 300
done
