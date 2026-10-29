"""Own an external acceptance command until its services have stopped.

Control stdin belongs only to this host: EOF cancels the command. Linux adopts
orphaned descendants as a subreaper. macOS owns a new session, including service
launchers' separate process groups. Neither boundary wraps the shipping orphan
oracle. All waits use kernel events, with no polling delays.
"""
import ctypes
from contextlib import closing
import os
import select
import signal
import subprocess
import sys


def kill(pid):
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def drain_linux(root):
    status = None
    while True:
        # Adoption retains ownership even when a service changes its group or
        # session, or a double-forking launcher exits before cleanup starts.
        with open(f"/proc/self/task/{os.getpid()}/children", encoding="ascii") as children:
            for pid in children.read().split():
                kill(int(pid))
        try:
            pid, result = os.waitpid(-1, 0)
        except ChildProcessError:
            return status
        if pid == root:
            status = result


def session_members(session):
    rows = subprocess.run(["ps", "-axo", "pid=,stat="], check=True,
                          capture_output=True, text=True).stdout.splitlines()
    for row in rows:
        pid, state = row.split()
        pid = int(pid)
        if pid == session or state.startswith("Z"):
            continue
        try:
            if os.getsid(pid) == session:
                yield pid
        except ProcessLookupError:
            pass


def drain_macos(root):
    # A process-group-only signal misses spawn_service_process(process_group=0).
    # The live session leader pins this session's identity throughout the drain.
    with closing(select.kqueue()) as queue:
        while True:
            pending = set()
            for pid in session_members(os.getpid()):
                try:
                    queue.control([select.kevent(pid, filter=select.KQ_FILTER_PROC,
                        flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                        fflags=select.KQ_NOTE_EXIT)], 0)
                    if os.getsid(pid) == os.getpid():
                        kill(pid)
                        pending.add(pid)
                except ProcessLookupError:
                    pass
            if not pending:
                break
            while pending:
                for event in queue.control(None, len(pending)):
                    if event.flags & select.KQ_EV_ERROR:
                        raise OSError(event.data, "process exit notification failed")
                    pending.discard(event.ident)
            # A child forked just before its parent died still belongs to the
            # session. Enumerate again after exit, rather than returning early.
    return os.waitpid(root, 0)[1]


def wait_linux(root):
    handle = os.pidfd_open(root)
    try:
        poll = select.poll()
        poll.register(handle, select.POLLIN)
        poll.register(0, select.POLLIN | select.POLLHUP)
        ready = dict(poll.poll())
        return handle not in ready
    finally:
        os.close(handle)


def wait_macos(root):
    with closing(select.kqueue()) as queue:
        changes = [select.kevent(0, filter=select.KQ_FILTER_READ, flags=select.KQ_EV_ADD),
                   select.kevent(root, filter=select.KQ_FILTER_PROC,
                       flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                       fflags=select.KQ_NOTE_EXIT)]
        try:
            events = queue.control(changes, 2)
        except ProcessLookupError:
            return False
        for event in events:
            if event.flags & select.KQ_EV_ERROR:
                raise OSError(event.data, "acceptance owner notification failed")
        return not any(event.filter == select.KQ_FILTER_PROC for event in events)


def main():
    linux = sys.platform == "linux"
    if not linux and sys.platform != "darwin":
        raise RuntimeError("unsupported POSIX acceptance host")
    command = sys.argv[1:]
    lease = None
    if command[0] == "--mount-root":
        if not linux or len(command) < 4 or command[2] != "--":
            raise RuntimeError("Linux mount lease requires --mount-root ROOT -- COMMAND")
        from owned_mounts import MountLease
        lease = MountLease(command[1])
        command = command[3:]
    if linux:
        library = ctypes.CDLL(None, use_errno=True)
        # PR_SET_CHILD_SUBREAPER, before any acceptance code can fork.
        if library.prctl(36, 1, 0, 0, 0) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
    os.setsid()
    root = os.fork()
    if root == 0:
        try:
            with open(os.devnull, "rb") as empty:
                os.dup2(empty.fileno(), 0)
            os.execvpe(command[0], command, os.environ)
        except BaseException as error:
            print(f"acceptance command could not start: {error}", file=sys.stderr, flush=True)
            os._exit(127)
    # A report request leaves the original tree owner and its physical drain
    # intact. EOF on control stdin remains the force-retirement operation.
    def report_request(_signal, _frame):
        try:
            os.kill(root, signal.SIGTERM)
        except ProcessLookupError:
            pass

    signal.signal(signal.SIGTERM, report_request)
    cancelled = True
    try:
        try:
            cancelled = wait_linux(root) if linux else wait_macos(root)
        finally:
            status = drain_linux(root) if linux else drain_macos(root)
        if lease is not None:
            lease.retire()
    finally:
        if lease is not None:
            lease.close()
    if cancelled:
        return 124
    result = os.waitstatus_to_exitcode(status)
    return result if result >= 0 else 128 - result


if __name__ == "__main__":
    sys.exit(main())
