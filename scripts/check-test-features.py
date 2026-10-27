#!/usr/bin/env python3
"""Reject Cargo test roots whose feature cfg can silently select zero tests."""
from pathlib import Path
import re
import subprocess
import sys
import tomllib


def gate_requirements(gate):
    """Return alternative positive requirements; platform cfgs are independent."""
    tokens = re.findall(r'"[^"\\]*(?:\\.[^"\\]*)*"|[A-Za-z_][A-Za-z_0-9]*|[(),=]', gate)
    position = 0

    def parse():
        nonlocal position
        name = tokens[position]
        position += 1
        if position < len(tokens) and tokens[position] == "=":
            value = tokens[position + 1].strip('"')
            position += 2
            return (name, value)
        if position < len(tokens) and tokens[position] == "(":
            position += 1
            children = []
            while tokens[position] != ")":
                children.append(parse())
                if tokens[position] == ",":
                    position += 1
            position += 1
            return (name, children)
        return (name, None)

    def has_feature(node):
        return node[0] == "feature" or (isinstance(node[1], list)
                and any(has_feature(child) for child in node[1]))

    def requirements(node):
        name, value = node
        if name == "feature":
            return [{value}]
        if name in ("loom", "kani"):
            return [{"model-checking"}]
        if name == "not":
            if has_feature(node):
                raise ValueError("negated feature gate cannot be represented by required-features; "
                                 "move this gate inside the test target")
            return [set()]
        if name == "any":
            return [option for child in value for option in requirements(child)]
        if name == "all":
            options = [set()]
            for child in value:
                options = [left | right for left in options for right in requirements(child)]
            return options
        return [set()]

    tree = parse()
    if position != len(tokens):
        raise ValueError(f"unsupported cfg gate: {gate}")
    return requirements(tree)


def violations(manifest):
    data = tomllib.loads(manifest.read_text(encoding="utf-8"))
    package = data.get("package")
    if package is None:
        return []
    targets = {
        target.get("path", f"tests/{target['name']}.rs"): target
        for target in data.get("test", [])
    }
    if package.get("autotests", True):
        for source in (manifest.parent / "tests").glob("*.rs"):
            targets.setdefault(source.relative_to(manifest.parent).as_posix(), {})
        for source in (manifest.parent / "tests").glob("*/main.rs"):
            targets.setdefault(source.relative_to(manifest.parent).as_posix(), {})
    errors = []
    for path, target in targets.items():
        source = manifest.parent / path
        text = source.read_text(encoding="utf-8")
        gates = re.findall(r"#!\[cfg\((.*?)\)\]", text, re.S)
        enabled = set(target.get("required-features", []))
        pending = list(enabled)
        while pending:
            for feature in data.get("features", {}).get(pending.pop(), []):
                if feature not in enabled:
                    enabled.add(feature)
                    pending.append(feature)
        for gate in gates:
            try:
                options = gate_requirements(gate)
            except (ValueError, IndexError) as error:
                errors.append(f"{source}: {error}")
                continue
            if not any(option <= enabled for option in options):
                choices = " OR ".join(", ".join(sorted(option - enabled)) for option in options)
                errors.append(f"{source}: add required-features satisfying ({choices}) "
                              f"to its [[test]] in {manifest}")
    return errors


def main():
    root = Path(__file__).resolve().parent.parent
    paths = subprocess.check_output(
        ["git", "ls-files", "-z", "--", "*Cargo.toml",
         ":(exclude)scripts/tests/fixtures/**"], cwd=root
    ).decode().split("\0")
    errors = []
    for path in paths:
        manifest = root / path
        if not path or not manifest.is_file() or path.startswith("rust/vendor/"):
            continue
        try:
            errors.extend(violations(manifest))
        except (OSError, ValueError) as error:
            errors.append(f"{manifest}: {error}")
    if errors:
        sys.exit("STEP_FAILED test-feature-hygiene\n" + "\n".join(errors)
                 + "\nrecovery: correct the listed test manifests, then run "
                 "python scripts/check-test-features.py")
    print("Test feature hygiene: every feature/model-gated Cargo target opts in explicitly.")


if __name__ == "__main__":
    main()
