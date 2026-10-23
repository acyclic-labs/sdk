"""Real service descendants with a readiness event and independent lifetimes."""
import json
import os
import socket
import subprocess
import sys


def main():
    mode = sys.argv[1]
    separate_group = "--separate-group" in sys.argv[2:]
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen()
        pids = []
        if mode != "leaf":
            group = ({"preexec_fn": os.setpgrp} if os.name == "posix" else
                     {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}) if separate_group else {}
            child = subprocess.Popen(
                [sys.executable, __file__, "leaf" if mode == "branch" else "branch",
                 *(["--separate-group"] if separate_group else [])],
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                **group,
            )
            pids = json.loads(child.stdout.readline())["pids"]
        pids.append(os.getpid())
        print(json.dumps({"pids": pids}), flush=True)
        if mode != "exit":
            server.accept()


if __name__ == "__main__":
    main()
