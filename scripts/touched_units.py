#!/usr/bin/env python3
"""Select the units a change touches, for any kind of unit.

    touched_units.py UNITS METADATA CHANGED [--home DIR]...

UNITS is a JSON list of {"unit": name, "files": [path, ...]}, paths relative to
the repository root. METADATA is `cargo metadata --format-version 1 --no-deps`
output. CHANGED lists changed paths, one per line, relative to the root.

A unit's homes are the package directories holding its files: the deepest
Cargo package directory, or `--home` directory, that contains each file. A
unit is touched when a changed path lies in one of its homes. A changed path
outside every home (the lockfile, the toolchain, a shared script) touches every
unit, so a change the selector cannot place never skips a unit.

Prints the selected unit names, one per line, in UNITS order, and one
TOUCHED_PLAN summary line on stderr.
"""

import json
import sys
from pathlib import PurePosixPath


def main(argv):
    if len(argv) < 3 or (len(argv) - 3) % 2 or any(flag != "--home" for flag in argv[3::2]):
        raise SystemExit(__doc__)
    units = json.load(open(argv[0], encoding="utf-8"))
    root = PurePosixPath(json.load(open(argv[1], encoding="utf-8"))["workspace_root"].replace("\\", "/"))
    homes = [
        PurePosixPath(package["manifest_path"].replace("\\", "/")).parent.relative_to(root)
        for package in json.load(open(argv[1], encoding="utf-8"))["packages"]
    ] + [PurePosixPath(home) for home in argv[4::2]]
    changed = [PurePosixPath(line) for line in open(argv[2], encoding="utf-8").read().splitlines() if line]

    def home(path):
        owners = [directory for directory in homes if path.is_relative_to(directory)]
        return max(owners, key=lambda directory: len(directory.parts)) if owners else None

    unplaced = [path for path in changed if home(path) is None]
    touched = {home(path) for path in changed}
    names = [unit["unit"] for unit in units]
    if len(set(names)) != len(names):
        raise SystemExit("touched_units: duplicate unit names")
    selected = [
        unit["unit"]
        for unit in units
        if unplaced or {home(PurePosixPath(path)) for path in unit["files"]} & touched
    ]
    for name in selected:
        print(name)
    print(
        f"TOUCHED_PLAN units={len(units)} selected={len(selected)} changed={len(changed)} "
        f"unplaced={len(unplaced)}" + (f" first_unplaced={unplaced[0]}" if unplaced else ""),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
