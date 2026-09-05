#!/usr/bin/env python3
"""Prove the public cache-digest exception cannot suppress other credential matches."""

import pathlib
import re
import subprocess
import sys
import tempfile


def main() -> None:
    root = pathlib.Path(__file__).resolve().parent.parent
    scanner = pathlib.Path(sys.argv[1]).resolve(strict=True)
    lines = [line for line in (root / "azure-pipelines.yml").read_text().splitlines()
             if "key:" in line and "bun-v4-" in line]
    if len(lines) != 4:
        raise SystemExit("expected four native cache identities")
    with tempfile.TemporaryDirectory(prefix="sdk-secret-policy-") as temporary:
        scratch = pathlib.Path(temporary)
        for index, line in enumerate(lines):
            digest = re.search(r"bun-v4-([0-9a-f]{64})", line).group(1)
            changed = digest[:-1] + ("0" if digest[-1] != "0" else "1")
            cases = [
                ("azure-pipelines.yml", line, 0),
                ("azure-pipelines.yml", line.replace(digest, changed), 1),
                ("azure-pipelines.yml", line + " # changed context", 1),
                ("unrelated.yml", line, 1),
            ]
            for case, (name, content, expected) in enumerate(cases):
                directory = scratch / f"{index}-{case}"
                directory.mkdir()
                (directory / name).write_text(content + "\n")
                result = subprocess.run(
                    [str(scanner), "dir", ".", "--config", str(root / ".gitleaks.toml"),
                     "--no-banner", "--redact"], cwd=directory, capture_output=True, timeout=5,
                )
                if result.returncode != expected:
                    raise SystemExit(f"secret-policy case {index}-{case}: unexpected scanner outcome")
    print("secret policy: all 16 boundary cases passed")


if __name__ == "__main__":
    main()
