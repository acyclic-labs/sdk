"""Own an external acceptance command until its services have stopped.

Control stdin belongs only to this host: EOF cancels the command. With --pty
the command runs on a sized terminal: control bytes become its input and
its output is copied to stdout until every terminal holder has exited. Linux adopts
orphaned descendants as a subreaper. macOS owns a new session, including service
launchers' separate process groups. Neither boundary wraps the shipping orphan
oracle. All waits use kernel events, with no polling delays.
"""
import ctypes
from contextlib import closing
import errno
import fcntl
import os
import select
import signal
import struct
import subprocess
import sys
import termios


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


def session_members(sessions):
    rows = subprocess.run(["ps", "-axo", "pid=,stat="], check=True,
                          capture_output=True, text=True).stdout.splitlines()
    for row in rows:
        pid, state = row.split()
        pid = int(pid)
        if pid == os.getpid() or state.startswith("Z"):
            continue
        try:
            if os.getsid(pid) in sessions:
                yield pid
        except ProcessLookupError:
            pass


def drain_macos(root):
    # A process-group-only signal misses spawn_service_process(process_group=0).
    # This live owner and the unreaped root pin both session identities (the
    # root leads its own session on a PTY) throughout the drain.
    sessions = {os.getpid(), root}
    with closing(select.kqueue()) as queue:
        while True:
            pending = set()
            for pid in session_members(sessions):
                try:
                    queue.control([select.kevent(pid, filter=select.KQ_FILTER_PROC,
                        flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                        fflags=select.KQ_NOTE_EXIT)], 0)
                    if os.getsid(pid) in sessions:
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


class Readiness:
    """Kernel readiness for descriptors and the root's exit, with no timeout.

    Linux polls a pidfd with the descriptors; macOS registers all of them in
    one kqueue. A root of None waits on the descriptors alone.
    """
    def __init__(self, root, sources):
        self.linux = sys.platform == "linux"
        self.exit = None
        if self.linux:
            self.poll = select.poll()
            for fd in sources:
                self.poll.register(fd, select.POLLIN)
            if root is not None:
                self.exit = os.pidfd_open(root)
                self.poll.register(self.exit, select.POLLIN)
            return
        self.queue, self.exited, self.count = select.kqueue(), False, len(sources)
        changes = [select.kevent(fd, filter=select.KQ_FILTER_READ, flags=select.KQ_EV_ADD)
                   for fd in sources]
        if root is not None:
            self.count += 1
            changes.append(select.kevent(root, filter=select.KQ_FILTER_PROC,
                flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT, fflags=select.KQ_NOTE_EXIT))
        try:
            self.queue.control(changes, 0)
        except ProcessLookupError:
            self.exited = True

    def wait(self):
        """Block until a source is readable or the root exits: (exited, readable)."""
        if self.linux:
            ready = {fd for fd, _ in self.poll.poll()}
            return self.exit in ready, ready - {self.exit}
        if self.exited:
            return True, set()
        events = self.queue.control(None, self.count)
        for event in events:
            if event.flags & select.KQ_EV_ERROR:
                raise OSError(event.data, "acceptance owner notification failed")
        self.exited = any(event.filter == select.KQ_FILTER_PROC for event in events)
        return self.exited, {event.ident for event in events if event.filter == select.KQ_FILTER_READ}

    def remove(self, fd):
        if self.linux:
            self.poll.unregister(fd)
        else:
            self.queue.control([select.kevent(fd, filter=select.KQ_FILTER_READ,
                flags=select.KQ_EV_DELETE)], 0)
            self.count -= 1

    def close(self):
        if not self.linux:
            self.queue.close()
        elif self.exit is not None:
            os.close(self.exit)


def read_terminal(fd):
    try:
        return os.read(fd, 65536)
    except OSError as error:
        # Linux reports EIO once every terminal holder has closed.
        if error.errno != errno.EIO:
            raise
        return b""


def write_all(fd, data):
    while data:
        data = data[os.write(fd, data):]


def relay(root, terminal):
    """Wait for the root, relaying terminal I/O; True when control cancels.

    Without a terminal any control readiness cancels. With one, control bytes
    are terminal input and only EOF cancels. A root of None ends at terminal EOF.
    """
    ready = Readiness(root, [0] if terminal is None else [0, terminal])
    output = terminal is not None
    try:
        while True:
            exited, readable = ready.wait()
            if output and terminal in readable:
                data = read_terminal(terminal)
                if data:
                    write_all(1, data)
                elif root is None:
                    return False
                else:
                    ready.remove(terminal)
                    output = False
            if exited:
                return False
            if 0 in readable:
                data = os.read(0, 65536)
                if terminal is None or not data:
                    return True
                write_all(terminal, data)
    finally:
        ready.close()


def spawn(command, terminal):
    """Fork the root. On a terminal it leads a new session with that PTY as
    its controlling terminal, so interrupts reach the root and not this owner."""
    root = os.fork()
    if root == 0:
        try:
            if terminal is None:
                with open(os.devnull, "rb") as empty:
                    os.dup2(empty.fileno(), 0)
            else:
                master, slave = terminal
                os.setsid()
                fcntl.ioctl(slave, termios.TIOCSCTTY, 0)
                for fd in (0, 1, 2):
                    os.dup2(slave, fd)
                os.close(slave)
                os.close(master)
            os.execvpe(command[0], command, os.environ)
        except BaseException as error:
            print(f"acceptance command could not start: {error}", file=sys.stderr, flush=True)
            os._exit(127)
    if terminal is not None:
        os.close(terminal[1])
    return root


def main():
    linux = sys.platform == "linux"
    if not linux and sys.platform != "darwin":
        raise RuntimeError("unsupported POSIX acceptance host")
    command = sys.argv[1:]
    terminal = None
    if command[0] == "--pty":
        rows, columns = int(command[1]), int(command[2])
        if not (0 < rows <= 32767 and 0 < columns <= 32767):
            raise ValueError("step=terminal-size; recovery=use rows and columns in 1..32767")
        # Opened before setsid, so this owner never acquires it as a terminal.
        terminal = os.openpty()
        fcntl.ioctl(terminal[1], termios.TIOCSWINSZ, struct.pack("HHHH", rows, columns, 0, 0))
        command = command[3:]
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
    root = spawn(command, terminal)
    master = None if terminal is None else terminal[0]

    # SIGTERM requests a report; SIGUSR1 kills the executable, not this owner.
    # Both leave the tree owner and its physical drain intact. EOF on control
    # stdin remains the force-retirement operation.
    def forward(number):
        def handler(_signal, _frame):
            try:
                os.kill(root, number)
            except ProcessLookupError:
                pass
        return handler

    signal.signal(signal.SIGTERM, forward(signal.SIGTERM))
    signal.signal(signal.SIGUSR1, forward(signal.SIGKILL))
    cancelled = True
    try:
        try:
            cancelled = relay(root, master)
        finally:
            # The drain reaps the root, after which its pid may be reused.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            signal.signal(signal.SIGUSR1, signal.SIG_IGN)
            status = drain_linux(root) if linux else drain_macos(root)
        # Copy the terminal's retained output now that its holders are dead.
        # Control EOF still cancels if a holder escaped the drain.
        if master is not None and not cancelled:
            cancelled = relay(None, master)
        if lease is not None:
            lease.retire()
    finally:
        if master is not None:
            os.close(master)
        if lease is not None:
            lease.close()
    if cancelled:
        return 124
    result = os.waitstatus_to_exitcode(status)
    return result if result >= 0 else 128 - result


if __name__ == "__main__":
    sys.exit(main())
