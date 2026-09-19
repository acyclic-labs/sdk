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
import tempfile
import tomllib
import urllib.error
import urllib.request
import zlib


CRATES_IO_PUBLISH_URL = "https://crates.io/api/v1/crates/new"
MAX_CRATE_BYTES = 104_857_600
MAX_TAR_BYTES = 536_870_912
PACKAGE_PATHS = {
    "acyclic-native-runtime": "rust/crates/native-runtime",
    "acyclic-objects": "rust/crates/objects",
    "acyclic-stream": "rust/crates/stream",
    "acyclic-inference": "rust/crates/inference",
    "acyclic-machines": "rust/crates/machines",
    "acyclic-fs": "rust/crates/filesystem",
}


def qualified_package_path(package: str) -> str:
    try:
        return PACKAGE_PATHS[package]
    except KeyError as error:
        raise RuntimeError("package has no qualified release path") from error


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
        raise RuntimeError("archive identity differs from Cargo metadata")

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
    archive_path: Path,
    package_name: str,
    version: str,
    expected_sha256: str,
    source_sha: str,
    path_in_vcs: str,
) -> tuple[bytes, dict[str, object], dict[str, bytes]]:
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise RuntimeError("expected checksum must be lowercase SHA-256")
    archive_size = archive_path.stat().st_size
    if not 0 < archive_size <= MAX_CRATE_BYTES:
        raise RuntimeError("crate archive exceeds its size bound")
    archive = archive_path.read_bytes()
    if len(archive) != archive_size:
        raise RuntimeError("crate archive changed while it was being read")
    observed_sha256 = hashlib.sha256(archive).hexdigest()
    if observed_sha256 != expected_sha256:
        raise RuntimeError("verified crate checksum changed before upload")

    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", source_sha):
        raise RuntimeError("source commit must be a full Git object ID")
    decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
    tar_payload = decompressor.decompress(archive, MAX_TAR_BYTES + 1)
    if decompressor.unconsumed_tail or len(tar_payload) > MAX_TAR_BYTES:
        raise RuntimeError("crate archive expands beyond its size bound")
    tar_payload += decompressor.flush()
    if (
        not decompressor.eof
        or decompressor.unused_data
        or len(tar_payload) > MAX_TAR_BYTES
    ):
        raise RuntimeError("crate archive is not one complete gzip stream")

    expected_prefix = f"{package_name}-{version}/"
    archived_files: dict[str, bytes] = {}
    archived_names: set[str] = set()
    extracted_bytes = 0
    with tarfile.open(fileobj=io.BytesIO(tar_payload), mode="r:") as crate:
        members = crate.getmembers()
        if not members:
            raise RuntimeError("crate archive is empty")
        for member in members:
            if not member.name.startswith(expected_prefix):
                raise RuntimeError("crate archive has an unexpected package root")
            relative_name = member.name.removeprefix(expected_prefix)
            relative_path = PurePosixPath(relative_name)
            if (
                relative_name in archived_names
                or relative_path.is_absolute()
                or ".." in relative_path.parts
                or (relative_name and relative_path.as_posix() != relative_name)
                or (not relative_name and not member.isdir())
            ):
                raise RuntimeError("crate archive contains an unsafe path")
            archived_names.add(relative_name)
            if member.issym() or member.islnk():
                raise RuntimeError("crate archive contains a link")
            if not member.isfile() and not member.isdir():
                raise RuntimeError("crate archive contains a special file")
            if member.isfile():
                if (
                    member.sparse is not None
                    or member.size < 0
                    or member.size > MAX_TAR_BYTES - extracted_bytes
                ):
                    raise RuntimeError("crate archive members exceed their size bound")
                extracted_bytes += member.size
                source = crate.extractfile(member)
                if source is None:
                    raise RuntimeError("crate archive member cannot be read")
                contents = source.read(member.size + 1)
                if len(contents) != member.size:
                    raise RuntimeError("crate archive member size is inconsistent")
                archived_files[relative_name] = contents
    try:
        normalized_manifest = tomllib.loads(archived_files["Cargo.toml"].decode("utf-8"))
    except KeyError as error:
        raise RuntimeError("crate archive has no normalized Cargo.toml") from error
    try:
        vcs_info = json.loads(archived_files[".cargo_vcs_info.json"])
    except KeyError as error:
        raise RuntimeError("crate archive has no Cargo VCS metadata") from error
    if vcs_info != {"git": {"sha1": source_sha}, "path_in_vcs": path_in_vcs}:
        raise RuntimeError("crate archive is not bound to the selected source commit")
    return archive, normalized_manifest, archived_files


def archived_package(
    package_name: str, version: str, archived_files: dict[str, bytes]
) -> dict[str, object]:
    """Read Cargo publication metadata from the already-validated archive."""
    with tempfile.TemporaryDirectory(prefix="crate-metadata-") as temporary:
        package_root = Path(temporary) / f"{package_name}-{version}"
        for relative_name, contents in archived_files.items():
            destination = package_root.joinpath(*PurePosixPath(relative_name).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(contents)
        completed = subprocess.run(
            [
                "cargo",
                "metadata",
                "--no-deps",
                "--format-version",
                "1",
                "--manifest-path",
                str(package_root / "Cargo.toml"),
            ],
            check=True,
            stdout=subprocess.PIPE,
            text=True,
        )
    packages = [
        package
        for package in json.loads(completed.stdout)["packages"]
        if package["name"] == package_name and package["version"] == version
    ]
    if len(packages) != 1:
        raise RuntimeError("archive does not identify exactly one selected crate")
    package = packages[0]
    allowed = package.get("publish")
    if allowed == [] or (allowed is not None and "crates-io" not in allowed):
        raise RuntimeError("archive is not publishable to crates.io")
    return package


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
            payload = response.read(1_048_577)
            if len(payload) > 1_048_576:
                raise RuntimeError("crates.io response exceeds its size bound")
            result = json.loads(payload)
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
    parser.add_argument("version")
    parser.add_argument("archive", type=Path)
    parser.add_argument("sha256")
    parser.add_argument("source_sha")
    arguments = parser.parse_args()

    path_in_vcs = qualified_package_path(arguments.package)
    archive, normalized_manifest, archived_files = validate_archive(
        arguments.archive,
        arguments.package,
        arguments.version,
        arguments.sha256,
        arguments.source_sha,
        path_in_vcs,
    )
    package = archived_package(arguments.package, arguments.version, archived_files)
    metadata = package_metadata(package, normalized_manifest, archived_files)
    if arguments.check:
        return

    token = os.environ.get("CARGO_REGISTRY_TOKEN")
    if not token or len(token) > 8192:
        raise RuntimeError("CARGO_REGISTRY_TOKEN is required")
    publish(metadata, archive, token)
    print(f"Uploaded {arguments.package} {arguments.version} from verified archive")


if __name__ == "__main__":
    main()
