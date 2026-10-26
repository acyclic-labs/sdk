#!/usr/bin/python3 -B
"""Per-job kernel caps: the one sizing and reporting rule for admitted bb2 jobs.

Every admitted job (SDK and Cloud) runs in a root-owned cgroup whose task and
memory ceilings follow its admitted CPU share: 256 tasks per CPU and an equal
part of 85% of RAM across the 128-CPU lease pool. A job cannot raise its own
ceiling; the payload only owns delegated children below it. A job that hits a
ceiling fails alone with a typed refusal; it is never retried or re-sized.
"""
import os
import shlex
from pathlib import Path

POOL_CPUS = 128
TASKS_PER_CPU = 256
MEMORY_FRACTION = 0.85
RECOVERY = ('fix the unbounded process/thread or memory growth in this command; '
            'the per-CPU caps in scripts/buildbox/job_caps.py are never raised to admit it')


def physical_memory():
    return os.sysconf('SC_PAGE_SIZE')*os.sysconf('SC_PHYS_PAGES')


def limits(cpus, memory=None):
    if type(cpus) is not int or cpus < 1: raise ValueError('admitted CPU count required: '+repr(cpus))
    # memory.max is kept in whole pages; page-align so the read-back is exact.
    page = os.sysconf('SC_PAGE_SIZE')
    share = int((physical_memory() if memory is None else memory)*MEMORY_FRACTION)//POOL_CPUS//page*page
    return {'pids.max': TASKS_PER_CPU*cpus, 'memory.max': share*cpus}


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
