#!/bin/bash
# All direct and bisect paths share the same preparation/conflict handling.
T=${GATE_LANES:-Q:/lanes}; L=${GATE_STATE:-$T/pr-gate}; P=$L/pending.txt
mkdir -p "$L"
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
    printf '%s\n' "$@" >> "$P"; preparation_infra=1
  fi
}
is_open(){ [ "$(gh pr view "$1" -R acyclic-labs/sdk --json state --jq .state)" = OPEN ]; }
# A recursion encountering local preparation INFRA parks every unvisited peer,
# propagates the hold, and never calls boxwait. Batch-only collisions defer just
# that member and continue gating its peers in both direct and bisect paths.
gate_group(){
  local group=("$@") rc c rest=() p
  while [ ${#group[@]} -gt 0 ]; do
    if [ "$preparation_infra" = 1 ]; then printf '%s\n' "${group[@]}" >> "$P"; return; fi
    run_gate "${group[@]}"; rc=$?
    case "$rc" in
      3|7)
        c=$(cat "$L/.conflict"); rest=()
        for p in "${group[@]}"; do [ "$p" = "$c" ] || rest+=("$p"); done
        if [ ${#rest[@]} -eq ${#group[@]} ]; then
          echo 'preparation-INFRA: missing or invalid excluded PR'; preparation_infra=1
          printf '%s\n' "${group[@]}" >> "$P"; return
        fi
        if [ "$rc" = 7 ]; then echo "$c" >> "$P"; else echo "$c" >> "$L/held-pending.txt"; fi
        group=("${rest[@]}");;
      0) merge "${group[@]}"; return;;
      4) printf '%s\n' "${group[@]}" >> "$P"; infra=1; return;;
      5) printf '%s\n' "${group[@]}" >> "$L/baseline-pending.txt"; return;;
      6) printf '%s\n' "${group[@]}" >> "$P"; preparation_infra=1; return;;
      *)
        if [ ${#group[@]} -gt 1 ]; then
          local h=$(( (${#group[@]}+1)/2 ))
          gate_group "${group[@]:0:$h}"
          gate_group "${group[@]:$h}"
        fi
        return;;
    esac
  done
}
boxwait(){ until (cd "${GATE_WORKTREE:-Q:/wt/pr-gate}" && timeout 600 pwsh -NoProfile -File "$T/bs.ps1" true >/dev/null 2>&1); do sleep 120; done; }
while :; do
  mkdir "$L/.lock" 2>/dev/null || exit 0
  trap 'rmdir "$L/.lock"' EXIT
  infra=0; preparation_infra=0; api_infra=0
  # A producer appends pending then kicks a drain. Rename its current queue so
  # concurrent appends land in a new file rather than being truncated/lost.
  active=$(mktemp "$L/active.XXXXXXXX") || exit 1
  [ ! -e "$P" ] || mv "$P" "$active"
  [ ! -e "$L/held-pending.txt" ] || { cat "$L/held-pending.txt" >> "$active"; : > "$L/held-pending.txt"; }
  candidates=()
  for p in $(tr -d '\r' < "$active" | sort -un); do
    grep -qx "$p" "$T/merge-hold.txt" 2>/dev/null && continue
    state=$(gh pr view "$p" -R acyclic-labs/sdk --json state --jq .state); api_rc=$?
    if [ "$api_rc" != 0 ]; then echo "$p" >> "$P"; api_infra=1
    else case "$state" in OPEN) candidates+=("$p");; MERGED|CLOSED) :;; *) echo "$p" >> "$P"; api_infra=1;; esac; fi
  done
  rm -f "$active"
  if [ ${#candidates[@]} -gt 0 ]; then
    if ! prep refresh "${candidates[@]}"; then
      printf '%s\n' "${candidates[@]}" >> "$P"; preparation_infra=1
    else
      batch=(); pri=()
      for p in "${candidates[@]}"; do
        if prep eligible "$p"; then
          batch+=("$p")
          grep -qx "$p" "$L/first.txt" 2>/dev/null && pri+=("$p")
        else echo "$p" >> "$L/held-pending.txt"; fi
      done
      if [ ${#pri[@]} -gt 0 ]; then
        for p in "${batch[@]}"; do grep -qx "$p" "$L/first.txt" || echo "$p" >> "$P"; done
        batch=("${pri[@]}")
      fi
      gate_group "${batch[@]}"
    fi
  fi
  rmdir "$L/.lock"; trap - EXIT
  # Local metadata failures are held for explicit coordinator recovery. They
  # must never submit a build-box availability probe or loop on pending.txt.
  [ "$preparation_infra" = 0 ] || exit 0
  [ "$api_infra" = 0 ] || exit 0
  [ -s "$P" ] || exit 0
  [ "$infra" = 0 ] || boxwait
done
