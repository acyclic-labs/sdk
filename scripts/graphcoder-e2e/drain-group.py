"""Wait for signalled POSIX group members through kernel exit notifications."""
import os
import select
import subprocess
import sys


def main():
    group = int(sys.argv[1])
    rows = subprocess.run(
        ["ps", "-axo", "pid=,pgid=,stat="], check=True, capture_output=True, text=True
    ).stdout.splitlines()
    members = [int(pid) for pid, pgid, state in (row.split() for row in rows)
               if int(pgid) == group and not state.startswith("Z")]
    if sys.platform == "linux":
        poll = select.poll()
        handles = []
        try:
            for pid in members:
                try:
                    handle = os.pidfd_open(pid)
                except ProcessLookupError:
                    continue
                handles.append(handle)
                poll.register(handle, select.POLLIN)
            remaining = set(handles)
            while remaining:
                for handle, _ in poll.poll():
                    remaining.discard(handle)
                    poll.unregister(handle)
        finally:
            for handle in handles:
                os.close(handle)
    elif sys.platform == "darwin":
        with select.kqueue() as queue:
            remaining = set()
            for pid in members:
                try:
                    queue.control([select.kevent(pid, filter=select.KQ_FILTER_PROC,
                        flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                        fflags=select.KQ_NOTE_EXIT)], 0)
                    remaining.add(pid)
                except ProcessLookupError:
                    continue
            while remaining:
                for event in queue.control(None, len(remaining)):
                    if event.flags & select.KQ_EV_ERROR:
                        raise OSError(event.data, "process exit notification failed")
                    remaining.discard(event.ident)
    else:
        raise RuntimeError("unsupported POSIX process exit watcher")


if __name__ == "__main__":
    main()
