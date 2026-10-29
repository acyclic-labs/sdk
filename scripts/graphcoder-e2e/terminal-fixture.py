"""Terminal roots for shared PTY custody tests; every wait is a signal or input."""
import json
import os
import signal
import subprocess
import sys


def main():
    mode = sys.argv[1]
    if mode == "probe":
        signal.signal(signal.SIGINT, lambda *_: print("interrupted", flush=True))
        print(json.dumps({"tty": os.isatty(0) and os.isatty(1), "size": list(os.get_terminal_size(0)),
                          "leader": os.getsid(0) == os.getpid(), "parent": os.getppid(),
                          "foreground": os.tcgetpgrp(0) == os.getpgrp()}), flush=True)
        print("got " + sys.stdin.readline().strip(), flush=True)
        # Exit at once, with this output possibly still buffered in the terminal.
        tail = memoryview(b"x" * 100000 + b"\nend\n")
        while tail:
            tail = tail[os.write(1, tail):]
        os._exit(3)
    # A descendant outside the root's process group (or session) keeps the
    # inherited output open after the executable is killed. Descriptor 3 is
    # the test's death witness: it ends only when every holder has exited.
    escape = {"group": {"preexec_fn": os.setpgrp}, "session": {"start_new_session": True}}[mode]
    holder = subprocess.Popen([sys.executable, "-c",
        "import os,signal; signal.signal(signal.SIGHUP, signal.SIG_IGN); print(os.getpid(), flush=True); signal.pause()"],
        stdout=subprocess.PIPE, pass_fds=(3,), **escape)
    pid = int(holder.stdout.readline())
    holder.stdout.close()
    print(json.dumps({"pids": [os.getpid(), pid]}), flush=True)
    signal.pause()


if __name__ == "__main__":
    main()
