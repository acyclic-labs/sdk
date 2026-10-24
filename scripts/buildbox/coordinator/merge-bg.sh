#!/bin/bash
# Background: CI/threads checks; a clean PR joins the gate queue and gate-drain.sh gates queued PRs in batches.
n=$1; out=Q:/lanes/pr-gate/merge-$n.out; mkdir -p Q:/lanes/pr-gate
grep -qx "$n" Q:/lanes/merge-hold.txt 2>/dev/null && { echo "$(date -u +%FT%T.000Z) pr-gate MERGE1 #$n HELD (in merge-hold.txt)" >> ~/.codex-fleet/alerts.log; exit 0; }
(cd Q:/sdk && bash Q:/lanes/merge1.sh $n) > $out 2>&1
if grep -q "^#$n READY" $out; then python Q:/lanes/gate-queue.py Q:/lanes/pr-gate add pending "$n" || exit 6; echo "$(date -u +%FT%T.000Z) pr-gate MERGE1 #$n queued for gate" >> ~/.codex-fleet/alerts.log; exec bash Q:/lanes/gate-drain.sh
else
  # A conflict verdict takes the rebase path before lane routing. merge1.sh
  # normally reports CI/thread blocks; accept an explicit conflict if supplied.
  if grep -aqE "^GATE_CONFLICT #$n( |$)|^GATE FAIL .*: #$n conflicts \(rebase needed\)$" "$out"; then
    conflict_reason=$(mktemp "Q:/lanes/pr-gate/rebase-conflict-$n.XXXXXXXX.out")
    if [ -n "$conflict_reason" ] && cp "$out" "$conflict_reason"; then
      echo "$(date -u +%FT%T.000Z) pr-gate GATE_CONFLICT #$n: $(tr '\n' ' ' < "$conflict_reason")" >> ~/.codex-fleet/alerts.log
      nohup bash Q:/lanes/auto-rebase.sh "$n" --reason-file "$conflict_reason" >> Q:/lanes/pr-gate/auto-rebase.out 2>&1 < /dev/null &
    fi
    exit 0
  fi
  # The reason is every line that isn't a clean check, gh/network errors included (2026-10-22: a TLS handshake timeout
  # left the alert with no reason at all).
  echo "$(date -u +%FT%T.000Z) pr-gate MERGE1 #$n BLOCKED $(grep -avE "^#$n (fails: \[\]|threads=0|closing: \[\])$" $out | tr '\n' ' ')" >> ~/.codex-fleet/alerts.log;
  blocked_reason=$(mktemp "Q:/lanes/pr-gate/route-blocked-$n.XXXXXXXX.out")
  if [ -n "$blocked_reason" ] && cp "$out" "$blocked_reason"; then
    nohup bash Q:/lanes/route.sh BLOCKED "$n" "$blocked_reason" >> Q:/lanes/pr-gate/route.out 2>&1 < /dev/null &
  fi
fi
