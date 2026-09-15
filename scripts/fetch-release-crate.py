#!/usr/bin/env python3
"""Download one exact GitHub release asset after verifying its metadata."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import urllib.parse
import urllib.request


MAX_METADATA_BYTES = 1_048_576
MAX_ASSET_BYTES = 104_857_600


class GitHubRedirects(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, file_pointer, code, message, headers, url):
        parsed = urllib.parse.urlsplit(url)
        hostname = parsed.hostname or ""
        if (
            parsed.scheme != "https"
            or parsed.username
            or parsed.password
            or parsed.port not in (None, 443)
            or (hostname != "github.com" and not hostname.endswith(".githubusercontent.com"))
        ):
            raise RuntimeError("release asset redirected outside GitHub")
        return super().redirect_request(
            request, file_pointer, code, message, headers, url
        )


def read_bounded(response, limit: int) -> bytes:
    payload = response.read(limit + 1)
    if len(payload) > limit:
        raise RuntimeError("response exceeds its size bound")
    return payload


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit("usage: fetch-release-crate.py RELEASE_TAG ASSET OUTPUT")
    release_tag, asset_name, output_name = sys.argv[1:]
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    token = os.environ.get("GITHUB_TOKEN", "")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise RuntimeError("invalid GitHub repository")
    if not re.fullmatch(r"[A-Za-z0-9_.+-]+", release_tag):
        raise RuntimeError("invalid release tag")
    if not (
        asset_name == "QUALIFICATION.json"
        or re.fullmatch(r"[A-Za-z0-9_.+-]+\.(?:crate|tgz)", asset_name)
    ):
        raise RuntimeError("invalid release asset name")
    if not token or len(token) > 8192:
        raise RuntimeError("GitHub token is required")

    encoded_tag = urllib.parse.quote(release_tag, safe="")
    metadata_url = f"https://api.github.com/repos/{repository}/releases/tags/{encoded_tag}"
    metadata_request = urllib.request.Request(
        metadata_url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "acyclic-sdk-exact-asset-publisher",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(metadata_request, timeout=30) as response:
        release = json.loads(read_bounded(response, MAX_METADATA_BYTES))
    if (
        release.get("tag_name") != release_tag
        or release.get("draft") is not False
        or release.get("immutable") is not True
    ):
        raise RuntimeError("release metadata does not match the selected tag")

    assets = [asset for asset in release.get("assets", []) if asset.get("name") == asset_name]
    if len(assets) != 1:
        raise RuntimeError("release must contain exactly one selected asset")
    asset = assets[0]
    expected_size = asset.get("size")
    expected_digest = asset.get("digest")
    expected_url = (
        f"https://github.com/{repository}/releases/download/"
        f"{urllib.parse.quote(release_tag, safe='')}/{urllib.parse.quote(asset_name, safe='')}"
    )
    if (
        asset.get("state") != "uploaded"
        or type(expected_size) is not int
        or not 0 < expected_size <= MAX_ASSET_BYTES
        or not isinstance(expected_digest, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", expected_digest)
        or asset.get("browser_download_url") != expected_url
    ):
        raise RuntimeError("release asset metadata is invalid")

    output = Path(output_name)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise RuntimeError("release asset output already exists")
    opener = urllib.request.build_opener(GitHubRedirects)
    request = urllib.request.Request(
        expected_url, headers={"User-Agent": "acyclic-sdk-exact-asset-publisher"}
    )
    temporary = tempfile.NamedTemporaryFile(
        mode="w+b",
        prefix=f".{output.name}.",
        suffix=".tmp",
        dir=output.parent,
        delete=False,
    )
    temporary_path = Path(temporary.name)
    try:
        with temporary as target, opener.open(request, timeout=120) as response:
            digest = hashlib.sha256()
            size = 0
            while block := response.read(min(1_048_576, expected_size - size + 1)):
                size += len(block)
                if size > expected_size:
                    raise RuntimeError("release asset exceeds its declared size")
                digest.update(block)
                target.write(block)
        observed_digest = digest.hexdigest()
        if size != expected_size or f"sha256:{observed_digest}" != expected_digest:
            raise RuntimeError("release asset bytes differ from GitHub metadata")
        os.link(temporary_path, output)
    finally:
        temporary_path.unlink(missing_ok=True)
    print(observed_digest)


if __name__ == "__main__":
    main()
