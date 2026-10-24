#!/usr/bin/env python3
"""Time the real-binary suite after compilation, retaining its strict result."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import runpy

binaries = runpy.run_path(str(Path(__file__).with_name("graphcoder-binaries.py")))["binaries"]


def main(manifest):
    selected = binaries(manifest)
    start = time.perf_counter()
    result = subprocess.run([
        "node", "scripts/graphcoder-e2e/run.mjs",
        selected["graphcoder"], selected["harness-mock-model"],
    ])
    wall = time.perf_counter() - start
    output = Path(os.environ["E2E_OUTPUT"])
    (output / "wall.json").write_text(json.dumps({
        "seconds": wall, "exit": result.returncode,
    }), encoding="utf-8")
    print("E2E_WALL", wall, flush=True)
    return result.returncode


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
