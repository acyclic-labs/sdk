"""Real pipe byte-progress controls; run with python3 on a POSIX host."""
import errno
from contextlib import contextmanager
import os
from pathlib import Path
import runpy
import unittest
from unittest.mock import patch


write_all = runpy.run_path(Path(__file__).with_name("owned-process.py"))["write_all"]
native_write = os.write


@contextmanager
def pipe():
    reader, writer = os.pipe()
    with os.fdopen(reader, "rb", buffering=0) as source, \
            os.fdopen(writer, "wb", buffering=0) as destination:
        yield source, destination


class ByteProgress(unittest.TestCase):
    def test_short_writes_preserve_the_original_pipe_bytes(self):
        calls = []

        def short_write(fd, data):
            calls.append(len(data))
            return native_write(fd, data[:3])

        with pipe() as (source, destination):
            with patch.object(os, "write", short_write):
                write_all(destination.fileno(), b"abcdefghi")
            destination.close()
            self.assertEqual(source.read(4096), b"abcdefghi")
            self.assertEqual(source.read(4096), b"")
            self.assertEqual(calls, [9, 6, 3])

    def test_zero_after_a_real_prefix_ends_before_another_write(self):
        calls = []

        def zero_after_prefix(fd, data):
            calls.append(len(data))
            # Inject only the second syscall result. Restoring the old loop
            # makes a third real write and fails instead of hanging this test.
            if len(calls) == 2:
                return 0
            return native_write(fd, data[:3])

        with pipe() as (source, destination):
            with patch.object(os, "write", zero_after_prefix):
                with self.assertRaises(OSError) as failure:
                    write_all(destination.fileno(), b"abcdefghi")
                self.assertEqual(failure.exception.errno, errno.EIO)
                self.assertIn("acceptance write", str(failure.exception))
                self.assertIn("recovery:", str(failure.exception))
            destination.close()
            self.assertEqual(source.read(4096), b"abc")
            self.assertEqual(source.read(4096), b"")
            self.assertEqual(calls, [9, 6])

    def test_actual_reader_loss_preserves_the_original_error(self):
        calls, failures = [], []

        def lose_reader(fd, data):
            calls.append(len(data))
            if len(calls) == 1:
                return native_write(fd, data[:3])
            self.assertEqual(source.read(3), b"abc")
            source.close()
            try:
                return native_write(fd, data)
            except BrokenPipeError as error:
                failures.append(error)
                raise

        with pipe() as (source, destination):
            with patch.object(os, "write", lose_reader):
                with self.assertRaises(BrokenPipeError) as failure:
                    write_all(destination.fileno(), b"abcdefghi")
            self.assertIs(failure.exception, failures[0])
            self.assertEqual(calls, [9, 6])


if __name__ == "__main__":
    unittest.main()
