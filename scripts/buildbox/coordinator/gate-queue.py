#!/usr/bin/env python3
"""The coordinator's sole queue writer: ordered sets, atomic files, one lock.

SQLite supplies the crash-released process lock on both Windows and POSIX.
Queue files remain inspectable text; no caller does read/modify/write itself.
Transfers publish the destination before removing the source. Claims stay in
the active queue until the drain acknowledges them, including partial clears,
failed output delivery and coordinator death. Admission removes duplicates.
"""
import argparse
from contextlib import contextmanager
import os
from pathlib import Path
import sqlite3
import sys
import tempfile

QUEUES = {'pending', 'held-pending', 'baseline-pending', 'active'}


def members(path):
    try:
        text = path.read_text(encoding='utf-8')
    except FileNotFoundError:
        return []
    values = text.split()
    if any(not p.isascii() or not p.isdecimal() or int(p) <= 0 for p in values):
        raise ValueError(f'invalid PR queue: {path}')
    return sorted(set(map(int, values)))


def publish(path, prs):
    fd, name = tempfile.mkstemp(prefix='.queue-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as stream:
            stream.writelines(f'{p}\n' for p in sorted(set(prs)))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


@contextmanager
def locked(state):
    state.mkdir(parents=True, exist_ok=True)
    # Native database locking avoids PID files, stale-lock recovery and a
    # second per-OS implementation. No command is retried on failure.
    with sqlite3.connect(state/'.queue-lock.sqlite3') as lock:
        lock.execute('BEGIN IMMEDIATE')
        yield


def queue_path(state, name):
    if name not in QUEUES:
        raise ValueError(f'unknown queue: {name}')
    return state/(name+'.txt')


def add(state, name, prs):
    path = queue_path(state, name)
    if any(not isinstance(p, int) or p <= 0 for p in prs):
        raise ValueError('PR numbers must be positive integers')
    with locked(state):
        publish(path, [*members(path), *prs])


def take(state, names):
    """Called under the queue lock; persist custody before clearing sources."""
    paths = [queue_path(state, name) for name in names]
    claim = queue_path(state, 'active')
    prs = sorted({p for path in [claim, *paths] for p in members(path)})
    publish(claim, prs)
    for path in paths:
        if path != claim:
            publish(path, [])
    return prs


def acknowledge(state, prs):
    if any(not isinstance(p, int) or p <= 0 for p in prs):
        raise ValueError('PR numbers must be positive integers')
    claim = queue_path(state, 'active')
    publish(claim, set(members(claim)) - set(prs))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('state', type=Path)
    parser.add_argument('operation', choices=['add', 'take', 'move', 'has', 'ack'])
    parser.add_argument('queue', choices=sorted(QUEUES))
    parser.add_argument('values', nargs='*')
    args = parser.parse_args()
    try:
        if args.operation == 'add':
            add(args.state, args.queue, list(map(int, args.values)))
            return 0
        source = queue_path(args.state, args.queue)
        with locked(args.state):
            if args.operation == 'ack':
                if args.queue != 'active':
                    raise ValueError('only active claims can be acknowledged')
                acknowledge(args.state, list(map(int, args.values)))
                return 0
            if args.operation == 'has':
                if args.values:
                    raise ValueError('has accepts no additional arguments')
                return 0 if members(source) else 1
            if args.operation == 'move':
                if len(args.values) != 1:
                    raise ValueError('move requires one destination queue')
                dest = queue_path(args.state, args.values[0])
                if dest == source:
                    raise ValueError('move requires different queues')
                publish(dest, [*members(dest), *members(source)])
                publish(source, [])
            else:
                prs = take(args.state, [args.queue, *args.values])
                sys.stdout.writelines(f'{p}\n' for p in prs)
                sys.stdout.flush()
        return 0
    except (OSError, ValueError, sqlite3.Error) as error:
        print(f'GATE preparation-INFRA: queue: {error}', file=sys.stderr)
        return 6


if __name__ == '__main__':
    raise SystemExit(main())
