#!/usr/bin/env python3
"""Event-driven real PTY; SIGUSR1 kills the executable, not its wrapper."""
import asyncio
import errno
import fcntl
import os
import pty
import signal
import struct
import sys
import termios


async def main():
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 80, 0, 0))
    child = await asyncio.create_subprocess_exec(
        *sys.argv[1:], stdin=slave, stdout=slave, stderr=slave,
        start_new_session=True,
        preexec_fn=lambda: fcntl.ioctl(0, termios.TIOCSCTTY, 0))
    os.close(slave)
    loop = asyncio.get_running_loop()
    drained = loop.create_future()

    def output():
        try:
            data = os.read(master, 65536)
        except OSError as error:
            if error.errno != errno.EIO:
                drained.set_exception(error)
                loop.remove_reader(master)
                return
            data = b""
        if data:
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
        else:
            loop.remove_reader(master)
            drained.set_result(None)

    def input_ready():
        data = os.read(sys.stdin.fileno(), 65536)
        if not data:
            loop.remove_reader(sys.stdin.fileno())
            return
        while data:
            count = os.write(master, data)
            data = data[count:]

    loop.add_reader(master, output)
    loop.add_reader(sys.stdin.fileno(), input_ready)
    loop.add_signal_handler(signal.SIGUSR1, child.kill)
    try:
        code = await child.wait()
        await drained
        return code if code >= 0 else 128 - code
    finally:
        loop.remove_reader(master)
        loop.remove_reader(sys.stdin.fileno())
        loop.remove_signal_handler(signal.SIGUSR1)
        if child.returncode is None:
            child.kill()
            await child.wait()
        os.close(master)


sys.exit(asyncio.run(main()))
