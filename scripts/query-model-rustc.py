#!/usr/bin/env python3
"""Keep Loom on the real Tokio/provider code, outside unused compio adapters."""
import os
import sys

compiler, *args = sys.argv[1:]
if "--crate-name" in args and args[args.index("--crate-name") + 1] == "synchrony":
    args = [arg for arg in args if arg != "--cfg=loom"]
    for index in range(len(args) - 1):
        if args[index:index + 2] == ["--cfg", "loom"]:
            del args[index:index + 2]
            break
os.execvp(compiler, [compiler, *args])
