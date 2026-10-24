#!/usr/bin/env python3
"""Own clang-cl's assembly intermediates for one compiler invocation."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def compile_owned(arguments):
    # clang-cl otherwise preprocesses .S beside its current working directory,
    # even when /Fo names an owned object path. Cargo registry sources are
    # shared and may be read-only. Each compiler invocation owns its scratch,
    # including when two crates compile the same assembly basename at once.
    with tempfile.TemporaryDirectory(prefix="msvc-assembly-") as temporary:
        flags = list(arguments)
        index = flags.index("--") if "--" in flags else len(flags)
        flags.insert(index, "/Fa" + str(Path(temporary)) + os.sep)
        return subprocess.run(["clang", "--driver-mode=cl", *flags]).returncode


if __name__ == "__main__":
    raise SystemExit(compile_owned(sys.argv[1:]))
