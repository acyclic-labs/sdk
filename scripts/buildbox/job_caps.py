#!/usr/bin/python3 -B
"""Per-job kernel caps: the one sizing and reporting rule for admitted bb2 jobs.

Every admitted job (SDK and Cloud) runs in a root-owned cgroup with the
approved interim ceilings (2026-10-26): 16384 tasks and 160 GiB of memory per
job, whatever its CPU share. A job cannot raise its own ceiling; the payload
only owns delegated children below it. A job that hits a ceiling fails alone
with a typed refusal; it is never retried or re-sized.
"""
import os
import shlex
from pathlib import Path

TASKS_MAX = 16384
MEMORY_MAX = 160 << 30
RECOVERY = ('fix the unbounded process/thread or memory growth in this command; '
            'the per-job caps in scripts/buildbox/job_caps.py are never raised to admit it')


def limits(cpus):
    if type(cpus) is not int or cpus < 1: raise ValueError('admitted CPU count required: '+repr(cpus))
    return {'pids.max': TASKS_MAX, 'memory.max': MEMORY_MAX}


def write(directory, values):
    """Apply and read back ceilings on a cgroup the caller (root) owns."""
    directory = Path(directory)
    for name, value in values.items(): (directory/name).write_text(str(value))
    actual = {name: int((directory/name).read_text()) for name in values}
    if actual != values:
        raise RuntimeError('kind=JOB_CAP step=effective-ceiling expected='+repr(values)+' actual='+repr(actual))
    (directory/'memory.oom.group').write_text('1')


def events(directory):
    def table(name):
        try: return {key: int(value) for key, value in (line.split() for line in (Path(directory)/name).read_text().splitlines())}
        except FileNotFoundError: return {}
    return {'pids.max': table('pids.events').get('max', 0), 'memory.max': table('memory.events').get('oom_kill', 0)}


def exceeded(directory, before):
    """Typed refusals for every ceiling hit since `before` (an events() value)."""
    after = events(directory); values = {}
    for name in after:
        try: values[name] = (Path(directory)/name).read_text().strip()
        except FileNotFoundError: values[name] = 'unavailable'
    kinds = {'pids.max': 'TASKS_MAX', 'memory.max': 'MEMORY_MAX'}
    return ['BB_CAP_EXCEEDED kind='+kinds[name]+' cap='+name+'='+values[name]+' events='+str(after[name]-before.get(name, 0))+
            ' owner=gate-speed recovery='+shlex.quote(RECOVERY)
            for name in after if after[name] > before.get(name, 0)]
