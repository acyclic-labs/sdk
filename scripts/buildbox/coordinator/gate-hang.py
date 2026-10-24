#!/usr/bin/env python3
"""Recognize an unfinished libtest case at a confirmed idle-watchdog cutoff.

A slow warning alone proves nothing. A transport loss, admission ceiling or
spot stop remains INFRA. Terminal compute receipts retain the watchdog cause;
this reader supplies a separate source verdict, never a fabricated completion.
"""
import argparse
from pathlib import Path
import re
import sys

RUNNING = re.compile(r'^\s+Running (.+) \((.+)\)$')
SLOW = re.compile(r'^test (.+) has been running for over [0-9]+ seconds$')
FINISHED = re.compile(r'^test (.+) \.\.\. (?:ok|FAILED|ignored)(?:\b|$)')
WATCHDOG = re.compile(r'^\[buildbox\] hang reaper: .*presumed hung .*killed \(rc 124\), admission slot freed\.')


def unfinished(text, executor_rc):
    active = None
    pending = {}
    watchdog = False
    for line in text.splitlines():
        running = RUNNING.match(line)
        if running:
            active = running.group(1)
            pending.clear()
        slow = SLOW.match(line)
        if slow and active:
            # Reinserting preserves the order of the latest observed starts.
            pending.pop(slow.group(1), None)
            pending[slow.group(1)] = None
        finished = FINISHED.match(line)
        if finished:
            pending.pop(finished.group(1), None)
        if WATCHDOG.match(line):
            watchdog = True
    # The wrapper's stderr and the child's stdout can be forwarded in either
    # order. A completion arriving after the watchdog still retires the case.
    if watchdog and executor_rc == 124 and active and pending:
        names = list(reversed(pending))
        return f'hung test {names[0]} in {active}; unfinished cases: {", ".join(names)}; confirmed idle-watchdog cutoff rc=124'
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('log', type=Path)
    parser.add_argument('--executor-rc', type=int, required=True)
    args = parser.parse_args()
    try:
        evidence = unfinished(args.log.read_text(encoding='utf-8', errors='replace'), args.executor_rc)
    except OSError as error:
        print(f'GATE hang evidence unavailable: {error}', file=sys.stderr)
        return 6
    if not evidence:
        return 1
    print(evidence)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
