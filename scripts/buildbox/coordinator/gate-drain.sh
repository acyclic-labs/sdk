#!/bin/bash
# All direct and bisect paths share the same preparation/conflict handling.
T=${GATE_LANES:-Q:/lanes}; L=${GATE_STATE:-$T/pr-gate}; P=$L/pending.txt
mkdir -p "$L"
queue(){ python "$T/gate-queue.py" "$L" "$@"; }
enqueue(){ queue add pending "$@" || { preparation_infra=1; return 6; }; }
prep(){ python "$T/gate-prepare.py" "$@" --repo "${GATE_REPO:-Q:/sdk}" --worktree "${GATE_WORKTREE:-Q:/wt/pr-gate}" --state "$L" --alerts "${GATE_ALERTS:-$HOME/.codex-fleet/alerts.log}"; }
route(){ nohup bash "$T/route.sh" "$@" >> "$L/route.out" 2>&1 < /dev/null & }
run_gate(){
  local reason rc p
  reason=$(mktemp "$L/route-verdict.XXXXXXXX.out") || return 6
  bash "$T/pr-gate.sh" "$@" > "$reason" 2>&1; rc=$?
  cat "$reason"
  if [ "$rc" = 3 ]; then
    p=$(cat "$L/.conflict")
    if grep -aq "^GATE FAIL .*: #$p conflicts (rebase needed)$" "$reason"; then
      nohup bash "$T/auto-rebase.sh" "$p" --reason-file "$reason" >> "$L/auto-rebase.out" 2>&1 < /dev/null &
    else
      # Closing-keyword policy can be fixed by editing only the PR body. Do
      # not require a commit change before merge-bg can submit that fix.
      if grep -aq "#$p contains a merge commit" "$reason"; then
        prep park "$p" --reason "$(tail -1 "$reason")" || return 6
      fi
      route GATE_CONFLICT "$p" "$reason"
    fi
  elif [ "$rc" = 1 ] && [ "$#" = 1 ]; then
    route GATE_FAIL "$1" "$reason"
  fi
  return "$rc"
}
merge(){
  local diagnostic
  if ! diagnostic=$(python "$T/gate-merge.py" merge "$@" --repo "${GATE_REPO:-Q:/sdk}" --worktree "${GATE_WORKTREE:-Q:/wt/pr-gate}" --state "$L" --alerts "${GATE_ALERTS:-$HOME/.codex-fleet/alerts.log}" 2>&1); then
    printf '%s\n' "$diagnostic"
    prep hold "$@" --reason "$diagnostic"
    enqueue "$@"; preparation_infra=1
  fi
}
# Read readiness at dispatch, including recursive bisects and deferred solos.
# A draft transition while peers gate must not start another expensive run.
ready_group(){
  ready=()
  local p state api_rc
  for p in "$@"; do
    state=$(gh pr view "$p" -R acyclic-labs/sdk --json state,isDraft --jq 'if .state != "OPEN" then .state elif .isDraft then "DRAFT" else "READY" end'); api_rc=$?
    if [ "$api_rc" != 0 ]; then enqueue "$p"; api_infra=1
    else case "$state" in
      READY) ready+=("$p");;
      DRAFT) echo "GATE DEFER #$p: draft at batch formation"; queue add held-pending "$p" || preparation_infra=1;;
      MERGED|CLOSED) :;;
      *) enqueue "$p"; api_infra=1;;
    esac; fi
  done
}
# A recursion encountering local preparation INFRA parks every unvisited peer,
# propagates the hold, and never calls boxwait. Batch-only collisions defer just
# that member and continue gating its peers in both direct and bisect paths.
gate_group(){
  local group=("$@") rc c rest=() p deferred=() ready=()
  while [ ${#group[@]} -gt 0 ]; do
    if [ "$preparation_infra" = 1 ] || [ "$api_infra" = 1 ]; then enqueue "${group[@]}"; break; fi
    ready_group "${group[@]}"; group=("${ready[@]}")
    if [ "$preparation_infra" = 1 ] || [ "$api_infra" = 1 ]; then enqueue "${group[@]}"; break; fi
    [ ${#group[@]} -gt 0 ] || break
    run_gate "${group[@]}"; rc=$?
    case "$rc" in
      3|7)
        c=$(cat "$L/.conflict"); rest=()
        for p in "${group[@]}"; do [ "$p" = "$c" ] || rest+=("$p"); done
        if [ ${#rest[@]} -eq ${#group[@]} ]; then
          echo 'preparation-INFRA: missing or invalid excluded PR'; preparation_infra=1
          enqueue "${group[@]}"; break
        fi
        if [ "$rc" = 7 ]; then
          if [ ${#group[@]} -eq 1 ]; then
            echo 'preparation-INFRA: batch-only collision reported for a solo PR'; preparation_infra=1
            enqueue "$c"; break
          fi
          deferred+=("$c")
        else queue add held-pending "$c" || preparation_infra=1; fi
        group=("${rest[@]}");;
      0) merge "${group[@]}"; break;;
      4) enqueue "${group[@]}"; infra=1; break;;
      5) queue add baseline-pending "${group[@]}" || preparation_infra=1; break;;
      6) enqueue "${group[@]}"; preparation_infra=1; break;;
      *)
        if [ ${#group[@]} -gt 1 ]; then
          local h=$(( (${#group[@]}+1)/2 ))
          gate_group "${group[@]:0:$h}"
          gate_group "${group[@]:$h}"
        fi
        break;;
    esac
  done
  # Collision deferral is scheduling, not a failed gate retry. Every excluded
  # member gets an independent main-based tree in this drain, before new work.
  for p in "${deferred[@]}"; do
    if [ "$preparation_infra" = 1 ] || [ "$api_infra" = 1 ] || [ "$infra" = 1 ]; then enqueue "$p"
    else gate_group "$p"; fi
  done
}
boxwait(){ until (cd "${GATE_WORKTREE:-Q:/wt/pr-gate}" && timeout 600 pwsh -NoProfile -File "$T/bs.ps1" true >/dev/null 2>&1); do sleep 120; done; }
while :; do
  mkdir "$L/.lock" 2>/dev/null || exit 0
  trap 'rmdir "$L/.lock"' EXIT
  infra=0; preparation_infra=0; api_infra=0
  # All producers use the same crash-released queue transaction. Membership
  # is a set, and admissions concurrent with this snapshot remain pending.
  active=$(mktemp "$L/active.XXXXXXXX") || exit 1
  queue take pending held-pending > "$active" || exit 6
  candidates=()
  for p in $(tr -d '\r' < "$active" | sort -un); do
    grep -qx "$p" "$T/merge-hold.txt" 2>/dev/null && continue
    candidates+=("$p")
  done
  rm -f "$active"
  ready_group "${candidates[@]}"; candidates=("${ready[@]}")
  if [ ${#candidates[@]} -gt 0 ]; then
    if ! prep refresh "${candidates[@]}"; then
      enqueue "${candidates[@]}"; preparation_infra=1
    else
      batch=(); pri=()
      for p in "${candidates[@]}"; do
        if prep eligible "$p"; then
          batch+=("$p")
          grep -qx "$p" "$L/first.txt" 2>/dev/null && pri+=("$p")
        else queue add held-pending "$p" || preparation_infra=1; fi
      done
      if [ ${#pri[@]} -gt 0 ]; then
        for p in "${batch[@]}"; do grep -qx "$p" "$L/first.txt" || enqueue "$p"; done
        for p in "${pri[@]}"; do gate_group "$p"; done
      else
        gate_group "${batch[@]}"
      fi
    fi
  fi
  rmdir "$L/.lock"; trap - EXIT
  # Local metadata failures are held for explicit coordinator recovery. They
  # must never submit a build-box availability probe or loop on pending.txt.
  [ "$preparation_infra" = 0 ] || exit 0
  [ "$api_infra" = 0 ] || exit 0
  queue has pending; queue_rc=$?
  [ "$queue_rc" = 0 ] || exit "$((queue_rc == 1 ? 0 : queue_rc))"
  [ "$infra" = 0 ] || boxwait
done
