#!/usr/bin/env python3
"""Apply cfg(loom) to opted-in workspace crates, preserving upstream runtimes."""
import subprocess
import sys


def compiler_args(args):
    if 'feature="model-checking"' in args and "--cfg" in args:
        return [*args, "--cfg", "loom"]
    return args


if __name__ == "__main__":
    try:
        status = subprocess.call(compiler_args(sys.argv[1:]))
        if status:
            print("STEP_FAILED loom-rustc: compiler failed; recovery: correct the "
                  "compiler diagnostics, then rerun node scripts/qualify-mcp-loom.mjs --gate",
                  file=sys.stderr)
        sys.exit(status)
    except OSError as error:
        sys.exit(f"STEP_FAILED loom-rustc: {error}; recovery: restore the Rust toolchain "
                 "and rerun node scripts/qualify-mcp-loom.mjs --gate")
