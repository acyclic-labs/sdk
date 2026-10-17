#!/usr/bin/env python3
"""A real PTY for restart qualification; SIGUSR1 kills the executable itself."""
import errno
import fcntl
import os
import pty
import selectors
import signal
import struct
import subprocess
import sys
import termios

master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
child = subprocess.Popen(sys.argv[1:], stdin=slave, stdout=slave, stderr=slave,
                         start_new_session=True, preexec_fn=lambda: fcntl.ioctl(0, termios.TIOCSCTTY, 0))
os.close(slave)
def kill(_signal, _frame):
    child.kill()
signal.signal(signal.SIGUSR1, kill)
try:
    with selectors.DefaultSelector() as selector:
        selector.register(master, selectors.EVENT_READ)
        selector.register(sys.stdin.buffer, selectors.EVENT_READ)
        while child.poll() is None:
            for key, _ in selector.select(0.05):
                if key.fileobj == master:
                    try:
                        data = os.read(master, 65536)
                    except OSError as error:
                        if error.errno != errno.EIO:
                            raise
                        data = b""
                    if data:
                        sys.stdout.buffer.write(data)
                        sys.stdout.buffer.flush()
                else:
                    data = os.read(sys.stdin.fileno(), 65536)
                    if data:
                        os.write(master, data)
                    else:
                        selector.unregister(sys.stdin.buffer)
    code = child.wait()
finally:
    if child.poll() is None:
        child.kill()
        child.wait()
    os.close(master)
sys.exit(code if code >= 0 else 128 - code)
