#!/usr/bin/env python3
"""Codex: share completed compilations, execute every requested test.

Installed behind a PATH cargo proxy. bb-job also routes explicit cargo paths
through it. Unknown commands/targets bypass the store and execute normally.
"""
import fcntl
from contextlib import contextmanager, ExitStack
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import bb_target_store as store

CARGO = '/home/var/.cargo/bin/cargo'


def compilation(argv):
    # Never interpret libtest arguments as Cargo flags or subcommands.
    cargo_args = argv[:argv.index('--')] if '--' in argv else list(argv)
    verb = next((a for a in cargo_args if not a.startswith(('-', '+'))), None)
    if verb in ['build', 'check', 'clippy']:
        return list(argv), False
    if verb in ['test', 'bench']:
        if '--doc' in cargo_args:
            return None, False  # rustdoc has no equivalent no-run artifact.
        if '--no-run' in cargo_args:
            return list(argv), False
        return cargo_args + ['--no-run'], True
    return None, False


def target_path(argv, env):
    target = env.get('CARGO_TARGET_DIR', '')
    for i, arg in enumerate(argv):
        if arg == '--':
            break
        if arg == '--target-dir' and i + 1 < len(argv):
            target = argv[i + 1]
        elif arg.startswith('--target-dir='):
            target = arg.split('=', 1)[1]
    p = Path(target).absolute()
    if p.is_symlink() or not p.resolve().is_relative_to(Path('/work/target')):
        return None
    return p


def record(file, receipt):
    # One write under a file lock; parent reads only after all payload exits.
    with open(file + '.target-store.jsonl', 'a') as out:
        fcntl.flock(out, fcntl.LOCK_EX)
        out.write(json.dumps(receipt) + '\n'); out.flush()


def local_target_lock(job, target):
    # /work is shared even though /work/target is a private bind mount. The
    # parent of the virtual target must never own a supposedly job-local lease.
    # Keep leases outside snapshots and under the private job's lifetime.
    directory = job / 'cargo-target-leases'
    directory.mkdir(parents=True, exist_ok=True)
    return directory / store.digest(str(target.resolve()))


@contextmanager
def local_target_leases(job, target, root=Path('/work/target')):
    # A snapshot of a parent also reads every descendant target. Shared leases
    # on ancestors let siblings compile concurrently while a parent snapshot
    # excludes all child writers. All acquisitions follow root-to-leaf order.
    root, target = root.resolve(), target.resolve()
    relative = target.relative_to(root)
    paths = [root]
    for part in relative.parts:
        paths.append(paths[-1] / part)
    with ExitStack() as stack:
        leases = []
        for path in paths:
            lease = stack.enter_context(local_target_lock(job, path).open('a'))
            fcntl.flock(lease, fcntl.LOCK_EX if path == target else fcntl.LOCK_SH)
            leases.append(lease)
        yield leases


def run(argv):
    env = dict(os.environ)
    build, run_tests = compilation(argv)
    file = env.get('BUILD_SLOT_RECEIPT_META')
    if not file or build is None or store.disabled(env):
        os.execv(CARGO, [CARGO] + argv)
    meta = json.loads(Path(file).read_text())
    target = target_path(argv, env)
    if not target or not store.private_target(meta):
        os.execv(CARGO, [CARGO] + argv)
    # Only the actual compile command defines the request; arbitrary driver
    # text and runtime test arguments cannot stand in for compiler provenance.
    meta['argv'] = ['cargo'] + build
    env['CARGO_TARGET_DIR'] = str(target)
    job = Path(env['BB_JOB_HOME']).parent
    identity = {}
    key = store.request_key(meta, env, job, components=identity)
    if not key:
        os.execv(CARGO, [CARGO] + argv)
    target.mkdir(parents=True, exist_ok=True)
    # Exclude another Cargo invocation in this same job while snapshotting its
    # target. This is separate from the cross-job single-flight request lease.
    local_started = time.monotonic()
    with local_target_leases(job, target) as local:
        session = store.Session(store.ROOT, key, target)
        session.receipt['request_identity'] = identity
        session.receipt['local_wait_seconds'] = time.monotonic() - local_started
        try:
            # Cargo is authoritative for existing private files. Do not merge a
            # stored snapshot into a target already used by this job.
            occupied = bool(store.children(target))
            if occupied or not store.source_unchanged(meta, env):
                # Still single-flight compilation, but keep the existing tree.
                (store.ROOT / 'locks').mkdir(parents=True, exist_ok=True)
                session.lock = (store.ROOT / 'locks' / key).open('a')
                start = time.monotonic(); fcntl.flock(session.lock, fcntl.LOCK_EX)
                session.receipt.update(status='private-target' if occupied else 'changed-input', wait_seconds=time.monotonic()-start)
            else:
                try:
                    session.open()
                except Exception as error:
                    session.close()
                    # Discard a partial clone under this job's local lease.
                    # The root mount itself and launcher scaffold stay intact.
                    for item in store.children(target):
                        if item.is_dir() and not item.is_symlink():
                            store.remove_tree(item)
                        else:
                            item.unlink()
                    session.receipt.update(status='fresh-fallback', error=type(error).__name__)
            start = time.monotonic()
            # Cargo retains both kernel leases if this proxy is killed. An
            # orphaned live builder must not race a successor publication.
            leases = tuple(lease.fileno() for lease in local) + ((session.lock.fileno(),) if session.lock else ())
            result = subprocess.run([CARGO] + build, env=env, pass_fds=leases)
            session.receipt['compile_seconds'] = time.monotonic() - start
            session.receipt['compile_rc'] = result.returncode
            if result.returncode == 0 and not occupied and session.lock:
                if not store.source_unchanged(meta, env):
                    session.receipt['publish'] = 'source-mutated'
                else:
                    try:
                        session.publish()
                    except Exception as error:
                        session.receipt['publish_error'] = type(error).__name__
            record(file, session.receipt)
        finally:
            session.close()
        if result.returncode:
            return result.returncode if result.returncode > 0 else 128-result.returncode
        if run_tests:
            # The store contains build output from before this execution.
            rc = subprocess.call([CARGO] + argv, env=env, pass_fds=tuple(lease.fileno() for lease in local))
            return rc if rc >= 0 else 128-rc
        return 0


if __name__ == '__main__':
    raise SystemExit(run(sys.argv[1:]))
