#!/usr/bin/env python3
"""Apply cfg(loom) to opted-in workspace crates, preserving upstream runtimes."""
import os
import sys


def compiler_args(args):
    if 'feature="model-checking"' in args and "--cfg" in args:
        return [*args, "--cfg", "loom"]
    return args


if __name__ == "__main__":
    try:
        args = compiler_args(sys.argv[1:])
        os.execvp(args[0], args)
    except OSError as error:
        sys.exit(f"STEP_FAILED loom-rustc: {error}; recovery: restore the Rust toolchain "
                 "and rerun node scripts/qualify-mcp-loom.mjs --gate")
