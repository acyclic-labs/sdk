#!/usr/bin/env python3
"""Select normal shipping executables from Cargo's compiler-artifact stream."""
import json
from pathlib import Path
import shlex
import sys


def binaries(manifest, names=("acyclic", "harness-mock-model")):
    found = {name: set() for name in names}
    for line in Path(manifest).read_text(encoding="utf-8").splitlines():
        row = json.loads(line)
        target = row.get("target", {})
        name = target.get("name")
        if (row.get("reason") == "compiler-artifact" and name in found
                and "bin" in target.get("kind", [])
                and not row.get("profile", {}).get("test")
                and row.get("executable")):
            found[name].add(row["executable"])
    result = {}
    for name, paths in found.items():
        if len(paths) != 1:
            raise ValueError(f"expected one normal {name} executable, got {sorted(paths)}")
        path = Path(next(iter(paths)))
        if not path.is_absolute() or not path.is_file():
            raise ValueError(f"Cargo executable is absent or relative: {path}")
        result[name] = str(path)
    return result


if __name__ == "__main__":
    try:
        variables = {"acyclic": "SESSIONS_ACYCLIC_BINARY", "graphcoder": "SESSIONS_GRAPHCODER_BINARY",
                     "harness-mock-model": "SESSIONS_MOCK_BINARY"}
        for name, path in binaries(sys.argv[1], sys.argv[2:] or ("acyclic", "harness-mock-model")).items():
            print(f"export {variables[name]}={shlex.quote(path)}")
    except (IndexError, KeyError, OSError, ValueError) as error:
        print(f"step=select Cargo executables: {error}; recovery command: "
              "python3 scripts/graphcoder-binaries.py ORIGINAL_CARGO_JSON acyclic graphcoder", file=sys.stderr)
        raise SystemExit(1) from error
