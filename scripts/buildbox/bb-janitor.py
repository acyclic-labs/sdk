#!/usr/bin/env python3
"""Size-bounded bb2 janitor: runs detached before each job (bb-job.py).

Oldest-first eviction to fixed budgets, never anything a live process uses:
  sccache  /mnt/bb/sccache   files by last use (mtime) down to SCCACHE_BUDGET
  jobs     /mnt/bb/jobs      entries idle > JOB_AGE, then oldest idle > JOB_MIN_AGE
                             while over JOBS_BUDGET; never one a live process names
  wt-sdk   /mnt/bb/wt-sdk    prune-wt-sdk.py (lease-aware) with WT_AGE / WT_COUNT,
                             plus orphaned .prep markers
  keys     @u, _persistent   test-leaked user keys (KEY_PREFIXES) invalidated once
                             first seen KEY_AGE ago and every job live at that
                             sighting has finished (a job's HOME is disposable);
                             they live in the shared per-uid quota (200 keys)
One run at a time (flock), at most once per INTERVAL (stamp). Never touches
/mnt/bb/cx, docker or anything outside the three roots. Log: /mnt/bb/log/janitor.log
"""
import fcntl
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

GiB = 1 << 30
SCCACHE, SCCACHE_BUDGET = Path('/mnt/bb/sccache'), 50 * GiB
JOBS, JOBS_BUDGET, JOB_AGE, JOB_MIN_AGE = Path('/mnt/bb/jobs'), 40 * GiB, 6 * 3600, 3600
WT_AGE, WT_COUNT = 6 * 3600, 32  # ~1.7 GiB per copy: ~55 GiB
INTERVAL = 600
KEY_PREFIXES, KEY_AGE = ('acyclic.authority.signing-seed.v1:', 'keyring-rs:'), 20 * 60
STATE = Path('/mnt/bb/locks/janitor')
LOG = Path('/mnt/bb/log/janitor.log')


def log(event, **fields):
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with LOG.open('a') as out:
        out.write(json.dumps({'at': time.strftime('%FT%TZ', time.gmtime()), 'event': event, **fields}) + '\n')


def live_references():
    """Every path a live process holds as cwd or names in its argv/environ."""
    refs = set()
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            refs.add(os.readlink(proc / 'cwd'))
        except OSError:
            pass
        for name in ('cmdline', 'environ'):
            try:
                refs.update(os.fsdecode(part) for part in (proc / name).read_bytes().replace(b'=', b'\0').split(b'\0')
                            if b'/mnt/bb/jobs/' in part)
            except OSError:
                pass
    return refs


def tree_size(path):
    total = 0
    try:
        if path.is_symlink() or not path.is_dir():
            return path.lstat().st_size
    except OSError:
        return 0
    for base, dirs, files in os.walk(path, onerror=lambda e: None):
        for name in files:
            try:
                total += os.lstat(os.path.join(base, name)).st_size
            except OSError:
                pass
    return total


def job_group(name):
    """A job's files share one id prefix: <id>.receipt-meta.v2.json, <id>.receipt-meta.v2.json.exec, ..."""
    return name.split('.', 1)[0]


def live_jobs(refs=None):
    """Job groups that a live process references (cwd, argv or environment)."""
    groups = set()
    for ref in live_references() if refs is None else refs:
        at = ref.find(str(JOBS) + '/')
        if at >= 0:
            top = ref[at + len(str(JOBS)) + 1:].split('/', 1)[0]
            if top:
                groups.add(job_group(top))
    return groups


def remove(path):
    """Remove an entry; returns the bytes actually freed (failures are logged)."""
    before = tree_size(path)
    if path.is_dir() and not path.is_symlink():
        try:
            path.chmod(path.stat().st_mode | 0o700)
        except OSError:
            pass
        for base, dirs, _ in os.walk(path):
            for name in dirs:
                child = Path(base) / name
                try:
                    if not child.is_symlink():
                        child.chmod(child.stat().st_mode | 0o700)
                except OSError:
                    pass
        shutil.rmtree(path, ignore_errors=True)
    else:
        try:
            path.unlink()
        except OSError:
            pass
    left = tree_size(path) if (path.exists() or path.is_symlink()) else 0
    if left:
        log('remove-failed', path=str(path), left_bytes=left)
    return before - left


def evict_sccache():
    files, total = [], 0
    for base, _, names in os.walk(SCCACHE):
        for name in names:
            path = os.path.join(base, name)
            try:
                stat = os.lstat(path)
            except OSError:
                continue
            files.append((stat.st_mtime, stat.st_size, path))
            total += stat.st_size
    before, removed = total, 0
    if total > SCCACHE_BUDGET:
        files.sort()
        for _, size, path in files:
            if total <= SCCACHE_BUDGET:
                break
            try:
                os.unlink(path)  # A server that later misses this entry recompiles.
                total -= size
                removed += 1
            except OSError:
                pass
    log('sccache', before_gib=round(before / GiB, 1), after_gib=round(total / GiB, 1), removed=removed)


def clean_jobs():
    groups, now = live_jobs(), time.time()
    def live(path):  # The whole file group of a live job is protected, not just the named file.
        return job_group(path.name) in groups
    rows = []
    for entry in JOBS.iterdir():
        try:
            rows.append((entry.lstat().st_mtime, entry))
        except OSError:
            pass
    rows.sort()
    removed = held = 0
    for mtime, entry in rows:  # Idle beyond JOB_AGE: always reclaimed.
        if now - mtime <= JOB_AGE:
            break
        if live(entry):
            held += 1
            continue
        remove(entry)
        removed += 1
    remaining = [(m, e) for m, e in rows if e.exists() or e.is_symlink()]
    total = sum(tree_size(e) for _, e in remaining)
    for mtime, entry in remaining:  # Over budget: oldest idle > JOB_MIN_AGE.
        if total <= JOBS_BUDGET or now - mtime <= JOB_MIN_AGE:
            break
        if live(entry):
            continue
        freed = remove(entry)
        total -= freed
        removed += 1 if freed else 0
    log('jobs', removed=removed, held_live=held, remaining_gib=round(total / GiB, 1))


def clean_wt_sdk():
    spec = importlib.util.spec_from_file_location('prune_wt_sdk', '/home/var/bin/prune-wt-sdk.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module.prune(Path('/mnt/bb/wt-sdk'), Path('/home/var/mirror/sdk.git'), Path('/mnt/bb/locks/wt-sdk'),
                          max_age=WT_AGE, max_count=WT_COUNT)
    # Orphaned .prep markers whose copy is gone (idle > 1 h).
    markers = 0
    for marker in Path('/mnt/bb/wt-sdk').glob('*.prep'):
        try:
            if marker.is_file() and not marker.with_suffix('').exists() and time.time() - marker.stat().st_mtime > JOB_MIN_AGE:
                marker.unlink()
                markers += 1
        except OSError:
            pass
    log('wt-sdk', markers=markers, removed=len(result['removed']), remaining=result['remaining'], held=len(result['held']),
        errors=len(result['errors']))


def keyctl(*args):
    return subprocess.run(['keyctl', *args], capture_output=True, text=True, timeout=30).stdout.strip()


def leaked_keys():
    """Serials of test-prefixed user keys reachable from @u and the persistent keyring."""
    rings = ['@u', keyctl('get_persistent', '@s')]  # The service links @u (KeyringMode=shared).
    found = set()
    for ring in filter(None, rings):
        for serial in keyctl('rlist', ring).split():
            kind, _, _, _, description = (keyctl('rdescribe', serial).split(';', 4) + [''] * 5)[:5]
            if kind == 'user' and description.startswith(KEY_PREFIXES):
                found.add(serial)
    return found


def reclaim_keys():
    seen_file, now = STATE / 'keys.json', time.time()
    try:
        seen = json.loads(seen_file.read_text())
    except (OSError, ValueError):
        seen = {}
    current, jobs = leaked_keys(), live_jobs()
    # A key's creator is a job that was live when the key was first seen, or one
    # that had already finished. Record that set; reclaim only once every job in
    # it has finished, so a long-running job keeps its own signing seed.
    seen = {serial: seen[serial] if isinstance(seen.get(serial), dict) else {'first': now, 'jobs': sorted(jobs)}
            for serial in current}
    reclaimed = 0
    for serial, record in list(seen.items()):
        if now - record['first'] > KEY_AGE and not jobs.intersection(record['jobs']):
            keyctl('invalidate', serial)  # Invalidated keys leave the quota at the next key GC.
            del seen[serial]
            reclaimed += 1
    seen_file.write_text(json.dumps(seen))
    log('keys', tracked=len(seen), reclaimed=reclaimed)


def main(force=False):
    STATE.mkdir(parents=True, exist_ok=True)
    with (STATE / 'run.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return  # Another janitor is running.
        stamp = STATE / 'last'
        if not force and stamp.exists() and time.time() - stamp.stat().st_mtime < INTERVAL:
            return
        stamp.touch()
        free = shutil.disk_usage('/mnt/bb')
        log('start', free_gib=round(free.free / GiB, 1))
        for step in (reclaim_keys, evict_sccache, clean_jobs, clean_wt_sdk):
            try:
                step()
            except Exception as error:  # One root's failure never blocks the others.
                log('error', step=step.__name__, error=repr(error))
        free = shutil.disk_usage('/mnt/bb')
        log('done', free_gib=round(free.free / GiB, 1))


if __name__ == '__main__':
    try:
        os.nice(19)
    except OSError:
        pass
    main(force='--force' in sys.argv)
