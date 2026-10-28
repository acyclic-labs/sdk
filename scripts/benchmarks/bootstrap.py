"""Install the pinned runner in an explicit disposable job directory on a Linux host."""

import argparse
import hashlib
import io
import json
import os
import subprocess
import urllib.request
import zipfile
from pathlib import Path


UV_VERSION = "0.13.5"
UV_WHEEL = "uv-0.13.5-py3-none-manylinux_2_17_x86_64.manylinux2014_x86_64.whl"
UV_SHA256 = "fa8d26e08e34c29c0c4e820cf3181757086d547240b4f4d31ff1d8bd810ca946"


def verified_uv(payload: bytes) -> bytes:
    if hashlib.sha256(payload).hexdigest() != UV_SHA256:
        raise ValueError("pinned uv wheel hash mismatch")
    with zipfile.ZipFile(io.BytesIO(payload)) as wheel:
        return wheel.read("uv-0.13.5.data/scripts/uv")


def setup(directory: Path):
    if not directory.is_absolute() or directory == Path(directory.anchor):
        raise ValueError("supply an absolute job-owned tools directory")
    directory.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(f"https://pypi.org/pypi/uv/{UV_VERSION}/json") as response:
        metadata = json.load(response)
    wheel = next(item for item in metadata["urls"] if item["filename"] == UV_WHEEL)
    with urllib.request.urlopen(wheel["url"]) as response:
        executable = verified_uv(response.read())
    uv = directory / "uv"
    uv.write_bytes(executable)
    uv.chmod(0o755)
    environment = {**os.environ, "UV_CACHE_DIR": str(directory / "cache")}
    venv = directory / "environment"
    subprocess.run([str(uv), "venv", "--python", "python3", str(venv)],
                   env=environment, check=True)
    python = venv / "bin/python"
    subprocess.run([str(uv), "pip", "sync", "--require-hashes", "--python", str(python),
                    str(Path(__file__).with_name("requirements.lock"))],
                   env=environment, check=True)
    print(python)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, required=True)
    setup(parser.parse_args().directory)
