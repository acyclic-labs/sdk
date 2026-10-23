"""Fixture-scoped Linux mount custody, retained beyond acceptance process exit.

Process exhaustion is the publication barrier for this lease: no acceptance
process can register another mount after retirement begins. Normal unmount and
a mount-ID recheck are mandatory; detach, ignored failures and retries are not
part of this protocol.
"""
import ctypes
import errno
import os
import re
import shutil
import subprocess


def mount_records():
    with open("/proc/self/mountinfo", encoding="utf-8", errors="surrogateescape") as source:
        for row in source:
            fields = row.split()
            separator = fields.index("-")
            path = re.sub(r"\\([0-7]{3})", lambda match: chr(int(match[1], 8)), fields[4])
            yield int(fields[0]), path, fields[separator + 1]


class MountLease:
    def __init__(self, root):
        self.root = os.path.realpath(root, strict=True)
        self.fd = os.open(self.root, os.O_PATH | os.O_DIRECTORY | os.O_CLOEXEC)
        try:
            if self.mounts():
                raise RuntimeError("acceptance mount root must be fresh and unmounted")
        except BaseException:
            self.close()
            raise

    def mounts(self):
        return [(identity, path, kind) for identity, path, kind in mount_records()
                if path == self.root or path.startswith(self.root + os.sep)]

    def retire(self):
        pinned = os.fstat(self.fd)
        current = os.stat(self.root, follow_symlinks=False)
        if (pinned.st_dev, pinned.st_ino) != (current.st_dev, current.st_ino):
            raise RuntimeError("acceptance mount root changed identity")
        owned = sorted(self.mounts(), key=lambda record: record[1].count(os.sep), reverse=True)
        for identity, path, kind in owned:
            if kind != "fuse" and not kind.startswith("fuse."):
                raise RuntimeError(f"unexpected non-FUSE fixture mount: {identity} {kind}")
            relative = os.path.relpath(path, self.root)
            anchored = os.fsencode(f"/proc/self/fd/{self.fd}/{relative}")
            library = ctypes.CDLL(None, use_errno=True)
            # UMOUNT_NOFOLLOW: retain the root descriptor and refuse a leaf link.
            if library.umount2(anchored, 8) != 0:
                error = ctypes.get_errno()
                if error != errno.EPERM:
                    raise OSError(error, os.strerror(error), path)
                helper = shutil.which("fusermount3") or shutil.which("fusermount")
                if helper is None:
                    raise RuntimeError("normal fixture unmount requires fusermount")
                # fusermount rejects a final procfd magic link. The retained,
                # identity-checked root and exhausted process tree fence this path.
                subprocess.run([helper, "-u", "--", path], check=True)
            if any(record[0] == identity for record in mount_records()):
                raise RuntimeError(f"fixture mount {identity} remains registered")
        if self.mounts():
            raise RuntimeError("fixture registered a mount after process exhaustion")

    def close(self):
        os.close(self.fd)
