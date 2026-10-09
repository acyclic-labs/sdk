"""Installed headless presentation checks; no durable-runtime qualification."""

import hashlib
import os
from pathlib import Path
import subprocess
import sys


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: test-graphcoder.py ABSOLUTE_INSTALLED_BINARY")
    binary = Path(sys.argv[1])
    if not binary.is_absolute() or not binary.is_file():
        raise SystemExit("an existing absolute installed binary is required")
    print(f"binary={binary}")
    print(f"binary_sha256={hashlib.sha256(binary.read_bytes()).hexdigest()}")
    print(f"suite_sha256={hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}")
    # The installed binary must work without Node or a tool search path. No
    # environment credentials are supplied to this fixture-only invocation.
    environment = {
        key: os.environ[key]
        for key in ("SystemRoot", "WINDIR", "TEMP", "TMP")
        if key in os.environ
    }
    environment["PATH"] = ""

    def run(name, arguments, commands, success):
        result = subprocess.run(
            [str(binary), *arguments],
            input=commands,
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=environment,
            timeout=5,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        print(f"case={name} argv={arguments!r} exit={result.returncode}")
        print(f"stdout={result.stdout!r}\nstderr={result.stderr!r}")
        if (result.returncode == 0) != success:
            raise AssertionError(f"{name}: unexpected exit")
        return result

    default = run("default-unavailable", [], "", False)
    if "durable terminal host is unavailable" not in default.stderr:
        raise AssertionError("default did not report the missing host")
    arguments = ["--fixture", "wire", "--headless"]
    playback = run("explicit-playback", arguments, "/next\n/next\n/reset\n/next\n/quit\n", True)
    if (
        "UI playback only, no durable effects" not in playback.stdout
        or playback.stdout.count("activity ") != 2
        or playback.stdout.count("end of fixture activity") != 1
        or "fixture display reset; no session restored" not in playback.stdout
        or "fixture> " in playback.stdout
    ):
        raise AssertionError("fixture paging/reset/headless contract changed")
    unavailable = run("input-not-queued", arguments, "hello\n/next\n", False)
    if "fixture input is not queued" not in unavailable.stderr or "activity " in unavailable.stdout:
        raise AssertionError("unavailable input was hidden or subsequent input executed")
    oversized = run("input-bound", arguments, "x" * 65_537 + "\n/next\n", False)
    if "terminal input exceeds bounds" not in oversized.stderr or "activity " in oversized.stdout:
        raise AssertionError("oversized line was continued as commands")


if __name__ == "__main__":
    main()
