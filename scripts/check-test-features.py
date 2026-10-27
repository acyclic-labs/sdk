#!/usr/bin/env python3
"""Reject Cargo test roots whose feature cfg can silently select zero tests."""
from pathlib import Path
import re
import subprocess
import sys
import tomllib


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
        needed = {feature for gate in gates
                  for feature in re.findall(r'feature\s*=\s*"([^"]+)"', gate)}
        # Model binaries also need an explicit opt-in in normal Cargo runs.
        if any(re.search(r"\b(?:loom|kani)\b", gate)
               and not re.search(r"not\(loom\)", gate) for gate in gates):
            needed.add("model-checking")
        enabled = set(target.get("required-features", []))
        pending = list(enabled)
        while pending:
            for feature in data.get("features", {}).get(pending.pop(), []):
                if feature not in enabled:
                    enabled.add(feature)
                    pending.append(feature)
        if missing := needed - enabled:
            errors.append(f"{source}: add required-features for {', '.join(sorted(missing))} "
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
