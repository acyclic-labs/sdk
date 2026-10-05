"""Run Swift with a single case-insensitive PATH entry.

Some Windows process environments contain both PATH and Path. Swift's
environment loader treats those as duplicate keys and aborts before the
compiler starts, so the launcher normalizes them in the child environment.
"""

from __future__ import annotations

import os
import subprocess
import sys


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: run-swift-sanitized.py SWIFT [ARGUMENT ...]", file=sys.stderr)
        return 2

    environment = dict(os.environ)
    path_value = next(
        (value for key, value in os.environ.items() if key.lower() == "path" and value),
        "",
    )
    for key in list(environment):
        if key.lower() == "path":
            del environment[key]
    environment["PATH"] = path_value
    completed = subprocess.run(sys.argv[1:], env=environment, check=False)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
