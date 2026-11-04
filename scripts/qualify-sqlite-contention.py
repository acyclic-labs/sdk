"""Check the real qualification children, their lock refusals and sleep syscalls."""

import hashlib
import json
from contextlib import closing
from pathlib import Path
import shlex
import shutil
import sqlite3
import subprocess
import sys
import tempfile


def main():
    step = "arguments"
    recovery = shlex.join([sys.executable, *sys.argv])
    try:
        if len(sys.argv) != 2:
            raise ValueError("expected the built qualify executable path")
        binary = Path(sys.argv[1]).resolve(strict=True)
        step = "native-trace"
        tracer = shutil.which("strace")
        if sys.platform != "linux" or tracer is None:
            raise RuntimeError("Linux strace is required for the syscall oracle")
        records = []
        with tempfile.TemporaryDirectory(prefix="sqlite-contention-") as temporary:
            root = Path(temporary)
            database = root / "wal.db"

            def invoke(mode, expected_exit, required_error=None):
                trace = root / f"trace-{len(records)}"
                command = [str(binary), mode, str(database)]
                result = subprocess.run(
                    [tracer, "-e", "trace=nanosleep,clock_nanosleep", "-o", str(trace), *command],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    check=False,
                )
                observed = trace.read_text()
                if result.returncode != expected_exit:
                    raise RuntimeError(f"{mode}: exit {result.returncode}: {result.stderr}")
                if required_error is not None and required_error not in result.stderr:
                    raise RuntimeError(f"{mode}: original lock refusal absent: {result.stderr}")
                sleeps = sum(
                    "nanosleep(" in line or "clock_nanosleep(" in line
                    for line in observed.splitlines()
                )
                records.append({"command": command, "exit": result.returncode, "sleepSyscalls": sleeps})

            step = "original-wal-writer-and-reader"
            invoke("--sqlite-wal-child", 0)
            with closing(sqlite3.connect(database, timeout=0)) as peer:
                if peer.execute("SELECT value FROM entries ORDER BY id").fetchall() != [
                    ("first",), ("second",)
                ]:
                    raise RuntimeError("original committed records differ")
                peer.execute("BEGIN IMMEDIATE")
                step = "original-recovery-with-held-writer"
                invoke("--sqlite-verify-child", 1, "SQLite recovery checkpoint remained busy")
                peer.rollback()
                step = "original-recovery-after-release"
                invoke("--sqlite-verify-child", 0)
            step = "no-retry-syscalls"
            with binary.open("rb") as executable:
                digest = hashlib.file_digest(executable, "sha256").hexdigest()
            print(json.dumps({"binarySha256": digest, "records": records}), flush=True)
            if any(record["sleepSyscalls"] for record in records):
                raise RuntimeError("SQLite contention invoked timed readiness retries")
    except Exception as error:
        print(f"step={step} failed: {error}; recovery command: {recovery}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
