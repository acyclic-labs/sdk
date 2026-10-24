#!/usr/bin/env python3
"""Run a cross-check with job-owned SDK snapshots, one per matrix worker."""
import argparse
import fcntl
import os
from pathlib import Path
import shutil
import subprocess
import tempfile


def snapshot(seed, destination):
    seed.mkdir(parents=True, exist_ok=True)
    destination.mkdir()
    sdk = destination / "xwin"
    sdk.mkdir()
    # Same preparation/snapshot custody as the qualified Windows test helper.
    with (seed / ".win-test.lock").open("a") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX)
        subprocess.run(["cargo", "xwin", "cache", "xwin"],
                       env=dict(os.environ, XWIN_CACHE_DIR=str(seed), XWIN_ACCEPT_LICENSE="1"), check=True)
        subprocess.run(["cp", "-a", "--reflink=auto", "--", str(seed / "xwin" / "crt"),
                        str(seed / "xwin" / "sdk"), str(seed / "xwin" / "DONE"), str(sdk)], check=True)
        for entry in sdk.rglob("*"):
            if entry.is_symlink() and not entry.resolve(strict=True).is_relative_to(sdk):
                raise RuntimeError("SDK alias escapes private snapshot: " + str(entry))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if args.workers < 1 or not command:
        parser.error("positive worker count and command required")
    with tempfile.TemporaryDirectory(prefix="gate-xwin-") as temporary:
        root = Path(temporary)
        for index in range(args.workers):
            snapshot(Path("/mnt/bb/toolchains/win-test-xwin-v1"), root / f"worker-{index}")
        compilers = root / "bin"
        compilers.mkdir()
        driver = compilers / "clang-cl"
        shutil.copyfile(Path(__file__).parent / "buildbox" / "msvc-compiler.py", driver)
        driver.chmod(0o755)
        env = dict(os.environ, GATE_XWIN_CACHE_ROOT=str(root),
                   XWIN_CACHE_DIR=str(root / "worker-0"), XWIN_ACCEPT_LICENSE="1",
                   PATH=str(compilers) + os.pathsep + os.environ["PATH"])
        return subprocess.run(command, env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())
