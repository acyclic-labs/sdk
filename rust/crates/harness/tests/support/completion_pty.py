import errno
import fcntl
import os
import pty
import select
import signal
import subprocess
import sys
import struct
import termios
import time

master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
def terminal():
    os.setsid()
    fcntl.ioctl(0, termios.TIOCSCTTY, 0)
child = subprocess.Popen([sys.argv[1], "--exact", "tui_completion::pty_child", "--nocapture"],
    stdin=slave, stdout=slave, stderr=slave, preexec_fn=terminal,
    env={**os.environ, "GC_COMPLETION_PTY": "1", "TERM": "xterm-256color"})
os.close(slave)
transcript = bytearray()
deadline = time.monotonic() + 5

def until(marker):
    while marker not in transcript:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([master], [], [], remaining)[0]:
            raise AssertionError(f"timeout waiting for {marker!r}: {transcript!r}")
        try:
            chunk = os.read(master, 65536)
        except OSError as error:
            if error.errno != errno.EIO:
                raise
            chunk = b""
        if not chunk:
            raise AssertionError(f"child exited before {marker!r}: {transcript!r}")
        transcript.extend(chunk)

try:
    until(b"READY")
    os.write(master, b"/th\r")
    until(b"ACCEPT")
    os.write(master, b"\r")
    until(b"SUBMIT")
    until(b"new.rs")
    until(b"LIVE")
    os.write(master, b"\x1b")
    until(b"ESCAPED")
    assert child.wait(timeout=max(0.01, deadline-time.monotonic())) == 0, transcript
finally:
    if child.poll() is None:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait()
    os.close(master)
