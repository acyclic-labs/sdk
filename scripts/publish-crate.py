#!/usr/bin/env python3
"""Upload one already-verified .crate archive through Cargo's registry API."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import struct
import subprocess
import sys
import tarfile
import tomllib
import urllib.error
import urllib.request


CRATES_IO_PUBLISH_URL = "https://crates.io/api/v1/crates/new"


def workspace_package(package_name: str) -> dict[str, object]:
    """Return one crates.io-publishable workspace package."""
    completed = subprocess.run(
        ["cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    packages = [
        package
        for package in json.loads(completed.stdout)["packages"]
        if package["name"] == package_name
    ]
    if len(packages) != 1:
        raise RuntimeError("package does not identify exactly one workspace member")
    package = packages[0]
    allowed = package.get("publish")
    if allowed == [] or (allowed is not None and "crates-io" not in allowed):
        raise RuntimeError("package is not publishable to crates.io")
    return package


def package_metadata(
    package: dict[str, object],
    normalized_manifest: dict[str, object],
    archived_files: dict[str, bytes],
) -> dict[str, object]:

    normalized_package = normalized_manifest.get("package", {})
    if not isinstance(normalized_package, dict):
        raise RuntimeError("normalized manifest has no package table")
    if (
        normalized_package.get("name") != package["name"]
        or normalized_package.get("version") != package["version"]
    ):
        raise RuntimeError("archive identity differs from workspace metadata")

    def archived_path(field: str) -> str | None:
        value = normalized_package.get(field)
        return value if isinstance(value, str) else None

    readme_file = archived_path("readme")
    readme = None
    if readme_file is not None:
        try:
            readme = archived_files[readme_file].decode("utf-8")
        except KeyError as error:
            raise RuntimeError("normalized readme is absent from archive") from error

    license_file = archived_path("license-file")
    if license_file is not None and license_file not in archived_files:
        raise RuntimeError("normalized license file is absent from archive")

    dependencies = []
    for dependency in package["dependencies"]:
        dependencies.append(
            {
                "name": dependency["name"],
                "version_req": dependency["req"],
                "features": dependency["features"],
                "optional": dependency["optional"],
                "default_features": dependency["uses_default_features"],
                "target": dependency["target"],
                "kind": dependency["kind"] or "normal",
                "registry": dependency["registry"],
                "explicit_name_in_toml": dependency["rename"],
            }
        )

    return {
        "name": package["name"],
        "vers": package["version"],
        "deps": dependencies,
        "features": package["features"],
        "authors": package["authors"],
        "description": package["description"],
        "documentation": package["documentation"],
        "homepage": package["homepage"],
        "readme": readme,
        "readme_file": readme_file,
        "keywords": package["keywords"],
        "categories": package["categories"],
        "license": package["license"],
        "license_file": license_file,
        "repository": package["repository"],
        "badges": {},
        "links": package["links"],
        "rust_version": package["rust_version"],
    }


def validate_archive(
    archive_path: Path, package_name: str, version: str, expected_sha256: str
) -> tuple[bytes, dict[str, object], dict[str, bytes]]:
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise RuntimeError("expected checksum must be lowercase SHA-256")
    archive = archive_path.read_bytes()
    observed_sha256 = hashlib.sha256(archive).hexdigest()
    if observed_sha256 != expected_sha256:
        raise RuntimeError("verified crate checksum changed before upload")

    expected_prefix = f"{package_name}-{version}/"
    archived_files: dict[str, bytes] = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as crate:
        members = crate.getmembers()
        if not members:
            raise RuntimeError("crate archive is empty")
        for member in members:
            if not member.name.startswith(expected_prefix):
                raise RuntimeError("crate archive has an unexpected package root")
            relative_name = member.name.removeprefix(expected_prefix)
            relative_path = PurePosixPath(relative_name)
            if relative_path.is_absolute() or ".." in relative_path.parts:
                raise RuntimeError("crate archive contains an unsafe path")
            if member.issym() or member.islnk():
                raise RuntimeError("crate archive contains a link")
            if not member.isfile() and not member.isdir():
                raise RuntimeError("crate archive contains a special file")
            if member.isfile():
                source = crate.extractfile(member)
                if source is None:
                    raise RuntimeError("crate archive member cannot be read")
                archived_files[relative_name] = source.read()
    try:
        normalized_manifest = tomllib.loads(archived_files["Cargo.toml"].decode("utf-8"))
    except KeyError as error:
        raise RuntimeError("crate archive has no normalized Cargo.toml") from error
    return archive, normalized_manifest, archived_files


def publish(metadata: dict[str, object], archive: bytes, token: str) -> None:
    metadata_bytes = json.dumps(metadata, separators=(",", ":")).encode("utf-8")
    if len(metadata_bytes) >= 2**32 or len(archive) >= 2**32:
        raise RuntimeError("publish payload exceeds Cargo registry protocol limits")
    body = b"".join(
        (
            struct.pack("<I", len(metadata_bytes)),
            metadata_bytes,
            struct.pack("<I", len(archive)),
            archive,
        )
    )
    request = urllib.request.Request(
        CRATES_IO_PUBLISH_URL,
        data=body,
        method="PUT",
        headers={
            "Accept": "application/json",
            "Authorization": token,
            "Content-Type": "application/octet-stream",
            "User-Agent": "cargo/1.94.0 (trusted-publishing exact-archive uploader)",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read(16_384).decode("utf-8", errors="replace")
        raise RuntimeError(f"crates.io rejected the verified archive: {detail}") from error
    errors = result.get("errors")
    if errors:
        raise RuntimeError(f"crates.io rejected the verified archive: {errors}")
    warnings = result.get("warnings") or {}
    if any(warnings.values()):
        print(json.dumps(warnings, sort_keys=True), file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("package")
    parser.add_argument("archive", type=Path)
    parser.add_argument("sha256")
    arguments = parser.parse_args()

    package = workspace_package(arguments.package)
    version = str(package["version"])
    archive, normalized_manifest, archived_files = validate_archive(
        arguments.archive, arguments.package, version, arguments.sha256
    )
    metadata = package_metadata(package, normalized_manifest, archived_files)
    if arguments.check:
        return

    token = os.environ.get("CARGO_REGISTRY_TOKEN")
    if not token:
        raise RuntimeError("CARGO_REGISTRY_TOKEN is required")
    publish(metadata, archive, token)
    print(f"Uploaded {arguments.package} {version} from verified archive")


if __name__ == "__main__":
    main()
