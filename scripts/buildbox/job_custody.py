"""Join the existing delegated job cgroup before deleting its files.

Process groups and inherited descriptors cannot represent daemon descendants:
sccache closes inherited descriptors and changes its session. Cgroup membership
survives both operations. This uses the build box's existing delegation; it does
not install another isolation boundary.
"""
import os
from pathlib import Path
import re
import select
import uuid


ROOT = Path('/sys/fs/cgroup')


def boot_id():
    return Path('/proc/sys/kernel/random/boot_id').read_text().strip()


def current_group():
    lines = Path('/proc/self/cgroup').read_text().splitlines()
    return ROOT / next(line[3:].lstrip('/') for line in lines if line.startswith('0::'))


def _owned_path(value):
    path = Path(value)
    relative = path.relative_to(ROOT)
    parts = relative.parts
    if (len(parts) < 4 or parts[0] != 'bbjobs.slice'
            or not re.fullmatch(r'run-[^/]+\.scope', parts[1])
            or parts[2] != 'job'
            or not re.fullmatch(r'owned-[0-9a-f]{32}', parts[-1])):
        raise ValueError('custody must name an owned child of the delegated job')
    if '..' in parts or path.resolve() != path:
        raise ValueError('custody path is not canonical')
    return path


def prepare():
    """Publish this identity in the runtime receipt before calling enter."""
    parent = current_group()
    path = _owned_path(str(parent / ('owned-' + uuid.uuid4().hex)))
    path.mkdir()
    identity = path.stat()
    return {'boot_id': boot_id(), 'path': str(path),
            'device': identity.st_dev, 'inode': identity.st_ino}


def _open(record):
    path = _owned_path(record['path'])
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    identity = os.fstat(fd)
    if (identity.st_dev, identity.st_ino) != (record['device'], record['inode']):
        os.close(fd)
        raise ValueError('custody cgroup generation changed')
    return path, fd


def enter(record):
    if record['boot_id'] != boot_id():
        raise ValueError('cannot enter a custody group from another boot')
    _, fd = _open(record)
    try:
        member = os.open('cgroup.procs', os.O_WRONLY, dir_fd=fd)
        try:
            os.write(member, str(os.getpid()).encode())
        finally:
            os.close(member)
    finally:
        os.close(fd)


class QuiescentJob:
    """Proof returned only after the kernel excludes every descendant."""
    def __init__(self, record, reason, key):
        if key is not _KEY:
            raise ValueError('quiescence requires a kernel join')
        self.record = record
        self.reason = reason

    def require(self, record):
        if self.record != record:
            raise ValueError('quiescence belongs to another job generation')


_KEY = object()


def join(record):
    """Kill and join all descendants, including detached cache compilers.

    There is no deadline that can turn unfinished cleanup into permission to
    delete. A removed cgroup also proves emptiness: cgroup v2 refuses removal of
    a populated group. A changed boot proves that the old processes cannot live.
    """
    path = _owned_path(record['path'])
    if record['boot_id'] != boot_id():
        return QuiescentJob(record, 'boot-changed', _KEY)
    if current_group().is_relative_to(path):
        raise ValueError('cleanup must run outside the owned job')
    try:
        _, fd = _open(record)
    except FileNotFoundError:
        return QuiescentJob(record, 'kernel-removed-empty-group', _KEY)
    try:
        events = os.open('cgroup.events', os.O_RDONLY, dir_fd=fd)
        try:
            poller = select.poll()
            poller.register(events, select.POLLPRI | select.POLLERR)
            kill = os.open('cgroup.kill', os.O_WRONLY, dir_fd=fd)
            try:
                os.write(kill, b'1')
            finally:
                os.close(kill)
            while True:
                os.lseek(events, 0, os.SEEK_SET)
                state = dict(line.split() for line in os.read(events, 4096).decode().splitlines())
                if state.get('populated') == '0':
                    return QuiescentJob(record, 'kernel-populated-zero', _KEY)
                poller.poll()
        finally:
            os.close(events)
    finally:
        os.close(fd)
