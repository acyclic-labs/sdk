"""Create a real FUSE registration, without touching it through the VFS.

The readiness message means fusermount has returned the actual device. Closing
that descriptor models a killed provider and leaves a disconnected registration.
The caller owns normal unmount, including in the negative control.
"""
import array
import json
import os
import shutil
import socket
import subprocess
import sys


def main():
    path = sys.argv[1]
    os.mkdir(path)
    parent, child = socket.socketpair()
    helper = shutil.which("fusermount3") or shutil.which("fusermount")
    if helper is None:
        raise RuntimeError("real-mount proof requires fusermount")
    process = subprocess.Popen([helper, "-o", "fsname=e2e-owned-fixture", "--", path],
        env={**os.environ, "_FUSE_COMMFD": str(child.fileno())},
        pass_fds=[child.fileno()], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    child.close()
    _, ancillary, _, _ = parent.recvmsg(1, socket.CMSG_SPACE(array.array("i").itemsize))
    parent.close()
    error = process.communicate()[1]
    if process.returncode:
        raise RuntimeError(error.decode())
    devices = array.array("i")
    for level, kind, data in ancillary:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            devices.frombytes(data)
    if len(devices) != 1:
        raise RuntimeError("fusermount did not return exactly one device")
    print(json.dumps({"mounted": path, "pid": os.getpid()}), flush=True)
    if len(sys.argv) > 2 and sys.argv[2] == "wait":
        # A socket event barrier, not a duration or readiness sleep.
        with socket.socket() as stop:
            stop.bind(("127.0.0.1", 0))
            stop.listen()
            stop.accept()
    os.close(devices[0])


if __name__ == "__main__":
    main()
