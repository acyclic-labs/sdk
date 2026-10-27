#!/usr/bin/env python3
"""Read Cargo editions with the gate planner's standard-library TOML parser."""
import json
import sys
import tomllib


def edition(manifest):
    workspace = manifest.get("workspace")
    package = manifest.get("package")
    value = package.get("edition", "2015") if package is not None else None
    result = {
        "workspace": workspace.get("package", {}).get("edition", False)
        if workspace is not None else None,
        "edition": "inherit" if isinstance(value, dict) and value.get("workspace") is True else value,
    }
    if package is not None and "workspace" in package:
        result["workspacePath"] = package["workspace"]
    return result


if __name__ == "__main__":
    result = {}
    for path, text in json.load(sys.stdin).items():
        try:
            result[path] = edition(tomllib.loads(text))
        except (tomllib.TOMLDecodeError, TypeError, AttributeError) as error:
            sys.exit(f"STEP_FAILED rustfmt-manifest {path}: {error}; "
                     "recovery: correct that Cargo.toml, then run bun scripts/qualify-rustfmt.mjs")
    json.dump(result, sys.stdout)
