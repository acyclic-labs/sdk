#!/usr/bin/env python3
"""Create and verify the exact TypeScript package qualification receipt."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path
import re
import sys
import time


MAX_RECEIPT_BYTES = 1_048_576
PACKAGES = (
    ("objects", "objects"),
    ("stream", "stream"),
    ("inference", "inference"),
    ("machines", "machines"),
    ("fs", "filesystem"),
    ("sdk", "sdk"),
)


def digest(path: Path) -> tuple[str, int]:
    checksum = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while block := source.read(1_048_576):
            checksum.update(block)
            size += len(block)
    return checksum.hexdigest(), size


def expected_assets(root: Path) -> list[dict[str, object]]:
    assets: list[dict[str, object]] = []
    for asset_slug, directory in PACKAGES:
        manifest = json.loads(
            (root / "typescript" / "packages" / directory / "package.json").read_text(
                encoding="utf-8"
            )
        )
        version = manifest["version"]
        assets.append(
            {
                "asset": f"acyclic-labs-{asset_slug}-{version}.tgz",
                "name": manifest["name"],
                "version": version,
            }
        )
    return assets


def create(output: Path, source_sha: str, root: Path) -> None:
    if not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", source_sha):
        raise RuntimeError("source commit must be a full lowercase Git object ID")
    packages = []
    expected_names = set()
    for package in expected_assets(root):
        archive = output / str(package["asset"])
        checksum, size = digest(archive)
        package.update({"sha256": checksum, "size": size})
        packages.append(package)
        expected_names.add(archive.name)
    observed_names = {path.name for path in output.glob("*.tgz")}
    if observed_names != expected_names:
        raise RuntimeError("qualification output does not contain exactly the six core archives")
    receipt = {"revision": 1, "source_commit": source_sha, "packages": packages}
    target = output / "QUALIFICATION.json"
    if target.exists():
        raise RuntimeError("qualification receipt already exists")
    target.write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def read_receipt(path: Path) -> dict[str, object]:
    with path.open("rb") as source:
        payload = source.read(MAX_RECEIPT_BYTES + 1)
    if len(payload) > MAX_RECEIPT_BYTES:
        raise RuntimeError("qualification receipt exceeds its size bound")
    receipt = json.loads(payload)
    if not isinstance(receipt, dict) or set(receipt) != {
        "revision",
        "source_commit",
        "packages",
    }:
        raise RuntimeError("qualification receipt schema is invalid")
    packages = receipt["packages"]
    if type(receipt["revision"]) is not int or receipt["revision"] != 1 or not isinstance(packages, list) or len(packages) != 6:
        raise RuntimeError("qualification receipt schema is invalid")
    expected_keys = {"asset", "name", "version", "sha256", "size"}
    names = set()
    for package in packages:
        if not isinstance(package, dict) or set(package) != expected_keys:
            raise RuntimeError("qualification receipt package is invalid")
        asset = package["asset"]
        if (
            not isinstance(asset, str)
            or not re.fullmatch(r"acyclic-labs-[a-z]+-[0-9A-Za-z.+-]+\.tgz", asset)
            or not isinstance(package["name"], str)
            or not isinstance(package["version"], str)
            or not isinstance(package["sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", package["sha256"])
            or type(package["size"]) is not int
            or package["size"] <= 0
            or asset in names
        ):
            raise RuntimeError("qualification receipt package is invalid")
        names.add(asset)
    return receipt


def verify(
    receipt_path: Path, source_sha: str, asset_name: str, archive: Path, root: Path
) -> None:
    receipt = read_receipt(receipt_path)
    if receipt["source_commit"] != source_sha:
        raise RuntimeError("qualification receipt belongs to another source commit")
    identities = {
        (item["asset"], item["name"], item["version"])
        for item in receipt["packages"]
    }
    expected = {
        (item["asset"], item["name"], item["version"])
        for item in expected_assets(root)
    }
    if identities != expected:
        raise RuntimeError("qualification receipt does not identify the six core packages")
    matches = [item for item in receipt["packages"] if item["asset"] == asset_name]
    if len(matches) != 1:
        raise RuntimeError("archive is absent from the qualification receipt")
    checksum, size = digest(archive)
    if matches[0]["sha256"] != checksum or matches[0]["size"] != size:
        raise RuntimeError("archive bytes differ from the qualified artifact")


def consumer(root: Path, bun: str) -> dict[str, object]:
    """Run one small consumer through package exports, Node, and the WASM core."""
    started = time.monotonic()
    launcher = (
        ["cmd", "/d", "/c", bun]
        if os.name == "nt" and Path(bun).suffix.lower() in {".cmd", ".bat"}
        else [bun]
    )
    commands = [
        [*launcher, "x", "tsc", "-b", "--force", "typescript/packages/sdk/tsconfig.json",
         "--pretty", "false"],
        [*launcher, "x", "tsc", "-p", "typescript/packages/sdk/consumer-tsconfig.json",
         "--pretty", "false"],
        [*launcher, "test", "typescript/packages/sdk/test/public-consumer.test.ts"],
    ]
    for command in commands:
        try:
            result = subprocess.run(
                command, cwd=root, capture_output=True, text=True, check=False, timeout=90,
            )
        except (OSError, subprocess.TimeoutExpired) as failure:
            return {
                "schema": 1,
                "passed": False,
                "elapsed_ms": int((time.monotonic() - started) * 1000),
                "error": str(failure)[-2000:],
            }
        if result.returncode != 0:
            return {
                "schema": 1,
                "passed": False,
                "elapsed_ms": int((time.monotonic() - started) * 1000),
                "error": (result.stderr + result.stdout)[-2000:],
            }
    return {
        "schema": 1,
        "passed": True,
        "elapsed_ms": int((time.monotonic() - started) * 1000),
        "consumer": "typescript/packages/sdk/test/public-consumer.test.ts",
        "surfaces": ["stream", "objects", "filesystem", "wasm"],
    }


def main() -> None:
    if len(sys.argv) == 3 and sys.argv[1] == "consumer":
        result = consumer(Path(__file__).resolve().parent.parent, sys.argv[2])
        print(json.dumps(result, separators=(",", ":")))
        raise SystemExit(0 if result["passed"] else 1)
    if len(sys.argv) == 4 and sys.argv[1] == "create":
        create(Path(sys.argv[2]), sys.argv[3], Path(__file__).resolve().parent.parent)
    elif len(sys.argv) == 6 and sys.argv[1] == "verify":
        verify(
            Path(sys.argv[2]),
            sys.argv[3],
            sys.argv[4],
            Path(sys.argv[5]),
            Path(__file__).resolve().parent.parent,
        )
    else:
        raise SystemExit(
            "usage: typescript-qualification.py create OUTPUT SOURCE_SHA | "
            "verify RECEIPT SOURCE_SHA ASSET ARCHIVE | consumer BUN"
        )
if __name__ == "__main__":
    main()
