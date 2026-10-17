import errno
import fcntl
import json
import os
import pty
import select
import signal
import struct
import subprocess
import sys
import termios
import time

binary, configuration, scenario, trigger = sys.argv[1:]
with open(scenario, encoding="utf-8") as source:
    writes = json.load(source)["writes"]
master, slave = pty.openpty()
fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))

def controlling_terminal():
    os.setsid()
    fcntl.ioctl(0, termios.TIOCSCTTY, 0)

child = subprocess.Popen([binary, "--exact", "native::tests::running_plan_panel_follows_authenticated_private_writes_over_pty", "--nocapture"], stdin=slave, stdout=slave, stderr=slave,
    preexec_fn=controlling_terminal, env={**os.environ, "TERM": "xterm-256color", "GC_PLAN_PTY_CONFIG": configuration})
os.close(slave)
deadline = time.monotonic() + 8
pending = bytearray()

def until(marker):
    while marker not in pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([master], [], [], remaining)[0]:
            raise AssertionError(f"timeout waiting for {marker!r}: {pending!r}")
        try:
            data = os.read(master, 65536)
        except OSError as error:
            if error.errno != errno.EIO:
                raise
            data = b""
        if not data:
            raise AssertionError(f"binary ended before {marker!r}: {pending!r}")
        pending.extend(data)
    end = pending.index(marker) + len(marker)
    result = bytes(pending[:end])
    del pending[:end]
    return result

def mutate(index):
    with open(trigger, "w", encoding="utf-8") as output:
        output.write(str(index))
    while True:
        with open(trigger, encoding="utf-8") as source:
            if source.read() == f"{index}-done":
                return
        assert time.monotonic() < deadline, "writer did not commit the requested revision"
        time.sleep(0.002)

try:
    until(b"graphcoder> ")
    # The first revision is committed before selection: it must arrive in snapshot.
    for index, revision in enumerate(writes):
        if index > 0:
            with open(trigger, "w", encoding="utf-8") as output:
                output.write(str(index))
        if index == 0:
            os.write(master, b"/plan\n")
        frame = until(("\u2514" + "\u2500" * 78 + "\u2518").encode("utf-8")).decode("utf-8")
        rows = [line.strip("\r") for line in frame.split("\n") if line.startswith("\u2502")]
        actual = [line[1:-1].rstrip() for line in rows]
        while actual and not actual[-1]:
            actual.pop()
        assert actual == [line.rstrip() for line in revision["panel"]], (actual, revision["panel"], frame)
    os.write(master, b"/reopen\n")
    until(b"native providers reopened; no admission or effect resent")
    os.write(master, b"/plan\n")
    frame = until(("\u2514" + "\u2500" * 78 + "\u2518").encode("utf-8")).decode("utf-8")
    rows = [line.strip("\r")[1:-1].rstrip() for line in frame.split("\n") if line.startswith("\u2502")]
    while rows and not rows[-1]:
        rows.pop()
    assert rows == [line.rstrip() for line in writes[-1]["panel"]], (rows, frame)
    # Both follow-time and snapshot-time errors stay inside the selected view.
    mutate(len(writes))
    until(b"plans:")
    os.write(master, b"/plan\n")
    until(b"plans:")
    until(b"graphcoder> ")
    mutate(len(writes) + 1)
    os.write(master, b"/plan\n")
    frame = until(("\u2514" + "\u2500" * 78 + "\u2518").encode("utf-8")).decode("utf-8")
    rows = [line.strip("\r")[1:-1].rstrip() for line in frame.split("\n") if line.startswith("\u2502")]
    while rows and not rows[-1]:
        rows.pop()
    assert rows == [line.rstrip() for line in writes[-1]["panel"]], (rows, frame)
    os.write(master, b"/quit\n")
    assert child.wait(timeout=max(0.01, deadline-time.monotonic())) == 0
finally:
    if child.poll() is None:
        os.killpg(child.pid, signal.SIGKILL)
        child.wait()
    os.close(master)
