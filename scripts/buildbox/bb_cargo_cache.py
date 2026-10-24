#!/usr/bin/env python3
"""Codex: share completed compilations, execute every requested test.

Installed behind a PATH cargo proxy. bb-job also routes explicit cargo paths
through it. Unknown commands/targets bypass the store after storage admission.
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
from admission_policy import QUERIES

CARGO = '/home/var/.cargo/bin/cargo'


def storage_request(argv, env):
    """Project path/config selectors; Cargo, not this proxy, resolves config.

    metadata has no --target-dir option. Its CARGO_TARGET_DIR has the same
    meaning, so project that one CLI override into the resolver environment.
    Runtime arguments after -- never participate in Cargo configuration.
    """
    args = argv[:argv.index('--')] if '--' in argv else list(argv)
    selectors, global_selectors, toolchain = [], [], []
    effective = dict(env)
    verb = None
    install_path = None
    index = 0
    while index < len(args):
        arg = args[index]
        if index == 0 and arg.startswith('+'):
            toolchain.append(arg)
        elif arg in ('--config', '--manifest-path', '--lockfile-path', '-Z', '-C', '--target-dir', '--color', '--path'):
            if index + 1 == len(args):
                raise RuntimeError('Cargo storage selector is missing its value: ' + arg)
            index += 1
            value = args[index]
            if arg == '--target-dir':
                effective['CARGO_TARGET_DIR'] = value
            elif arg == '--path':
                install_path = value
            elif arg != '--color':
                if arg in ('--config', '-Z', '-C'):
                    global_selectors.extend([arg, value])
                else:
                    selectors.extend([arg, value])
        elif arg.startswith('--target-dir='):
            effective['CARGO_TARGET_DIR'] = arg.split('=', 1)[1]
        elif arg.startswith('--path='):
            install_path = arg.split('=', 1)[1]
        elif arg.startswith(('--config=', '--manifest-path=', '--lockfile-path=')) or arg.startswith(('-Z', '-C')):
            if arg.startswith(('--config=', '-Z', '-C')):
                global_selectors.append(arg)
            else:
                selectors.append(arg)
        elif verb is None and not arg.startswith('-'):
            verb = arg
        index += 1
    return verb, toolchain, selectors, global_selectors, effective, install_path


def admit_storage(argv, env):
    verb, toolchain, selectors, globals_, effective, install_path = storage_request(argv, env)
    if verb is None or verb in QUERIES or verb in ('help', 'new', 'init'):
        return
    cargo_args = argv[:argv.index('--')] if '--' in argv else argv
    if verb == 'install' and install_path is None:
        if any(arg in ('--list', '--help', '-h') for arg in cargo_args):
            return
        # Registry/git installs discover configuration outside the caller's
        # workspace. Admit the downloaded/local producer, never substitute an
        # unrelated workspace's metadata as proof of its output directories.
        raise RuntimeError('Cargo install storage admission requires a local --path producer')
    # Cargo identifies aliases, including recursive/config aliases. They can
    # hide path overrides from this boundary. Require the expanded command
    # instead of maintaining a second alias/config parser.
    directory = None
    if verb == 'install' and install_path is not None:
        # Unlike other Cargo commands, install discovers local configuration
        # at --path. Keep CLI selectors relative to the caller while asking
        # Cargo to discover the installed producer's configuration there.
        directory = Path(install_path).absolute()
        if any(arg.startswith('-C') for arg in globals_):
            raise RuntimeError('Cargo install storage admission requires running from the producer directory instead of -C')
        for selected in (selectors, globals_):
            for index, arg in enumerate(selected):
                if index and selected[index - 1] in ('--config', '--manifest-path', '--lockfile-path') and '=' not in arg:
                    selected[index] = str(Path(arg).absolute())
                elif arg.startswith('--config=') and '=' not in arg.split('=', 1)[1]:
                    selected[index] = '--config=' + str(Path(arg.split('=', 1)[1]).absolute())
        for key in ('CARGO_TARGET_DIR', 'CARGO_BUILD_TARGET_DIR'):
            if effective.get(key):
                effective[key] = str(Path(effective[key]).absolute())
    catalog = subprocess.run([CARGO, *toolchain, *globals_, '--list'], env=env, cwd=directory,
                             text=True, capture_output=True, check=True)
    for line in catalog.stdout.splitlines():
        fields = line.split(None, 1)
        if len(fields) == 2 and fields[0] == verb and fields[1].startswith('alias:'):
            raise RuntimeError('Cargo storage admission requires the expanded alias command: ' + verb)
    resolved = subprocess.run([CARGO, *toolchain, *globals_, 'metadata', '--locked', '--offline',
                               '--no-deps', '--format-version=1', *selectors],
                              env=effective, cwd=directory, text=True, capture_output=True, check=True)
    metadata = json.loads(resolved.stdout)
    if any(not isinstance(metadata.get(key), str) for key in ('target_directory', 'build_directory')):
        raise RuntimeError('Cargo metadata must identify both target and build directories')
    for key in ('target_directory', 'build_directory'):
        store.require_job_target(metadata[key], env)
    return Path(metadata['target_directory'])


def compilation(argv):
    # Never interpret libtest arguments as Cargo flags or subcommands.
    cargo_args = argv[:argv.index('--')] if '--' in argv else list(argv)
    verb = storage_request(argv, {})[0]
    if verb in ['build', 'check', 'clippy']:
        return list(argv), False
    if verb in ['test', 'bench']:
        if '--doc' in cargo_args:
            return None, False  # rustdoc has no equivalent no-run artifact.
        if '--no-run' in cargo_args:
            return list(argv), False
        return cargo_args + ['--no-run'], True
    return None, False


def target_path(target):
    # Cargo's already admitted result is the only target selector. The cache
    # merely determines eligibility; it never interprets path overrides again.
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
    admitted_target = admit_storage(argv, env)
    build, run_tests = compilation(argv)
    file = env.get('BUILD_SLOT_RECEIPT_META')
    if not file or build is None or store.disabled(env):
        os.execv(CARGO, [CARGO] + argv)
    meta = json.loads(Path(file).read_text())
    target = target_path(admitted_target)
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
    try:
        raise SystemExit(run(sys.argv[1:]))
    except (RuntimeError, OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print('[buildbox] Cargo storage admission INFRA: ' + str(error), file=sys.stderr)
        raise SystemExit(224)
