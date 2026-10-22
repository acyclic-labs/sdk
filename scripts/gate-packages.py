#!/usr/bin/env python3
"""Select gate roots and their checkout-local Cargo dependencies."""
import json
import pathlib
import shlex
import subprocess
import sys
import tomllib


def packages(checkout):
    root = pathlib.Path(checkout).resolve()
    paths = subprocess.check_output(
        ["git", "-C", str(root), "ls-files", "-z", "*Cargo.toml"]
    ).decode().split("\0")
    manifests = {}
    for path in filter(None, paths):
        manifest = root / path
        with manifest.open("rb") as source:
            manifests[manifest.resolve()] = tomllib.load(source)
    by_name = {data["package"]["name"]: path for path, data in manifests.items()
               if "package" in data}

    def workspace(path):
        for parent in (path.parent, *path.parent.parents):
            candidate = parent / "Cargo.toml"
            if "workspace" in manifests.get(candidate, {}):
                return candidate, manifests[candidate]["workspace"].get("dependencies", {})
        return None, {}

    def dependencies(data):
        for key in ("dependencies", "dev-dependencies", "build-dependencies"):
            yield from data.get(key, {}).items()
        for target in data.get("target", {}).values():
            yield from dependencies(target)

    selected = set()
    pending = [name for name in by_name if
               (name.startswith("acyclic-harness") or name == "graphcoder")
               and not name.endswith(("-fuzz", "-conformance"))]
    while pending:
        name = pending.pop()
        if name in selected:
            continue
        selected.add(name)
        path = by_name[name]
        owner, inherited = workspace(path)
        for alias, dependency in dependencies(manifests[path]):
            if not isinstance(dependency, dict):
                continue
            base = path.parent
            if dependency.get("workspace"):
                dependency = inherited[alias]
                base = owner.parent
            if not isinstance(dependency, dict) or "path" not in dependency:
                continue
            target = (base / dependency["path"] / "Cargo.toml").resolve()
            if target not in manifests or "package" not in manifests[target]:
                raise ValueError(f"untracked local dependency: {path}: {alias}: {target}")
            pending.append(manifests[target]["package"]["name"])
    # Path dependencies outside the workspace can be traversed, but cargo test
    # cannot select them as roots when they have dev-dependencies.
    metadata = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1", "--offline"],
        cwd=root,
    ))
    member_ids = set(metadata["workspace_members"])
    members = {package["name"] for package in metadata["packages"]
               if package["id"] in member_ids}
    return sorted(selected & members)


if __name__ == "__main__":
    print(" ".join(f"-p {shlex.quote(name)}" for name in packages(sys.argv[1])))
