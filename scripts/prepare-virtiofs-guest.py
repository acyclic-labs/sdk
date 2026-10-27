#!/usr/bin/env python3
"""Prepare a tiny real Linux guest for virtio-fs qualification on bb2."""

import argparse
import gzip
import hashlib
import json
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import sys
import tempfile
import urllib.request


def run(step, command, **kwargs):
    result = subprocess.run(command, capture_output=True, **kwargs)
    if result.returncode:
        raise RuntimeError(
            f"step={step}: {result.stderr.decode(errors='replace')}; "
            f"recovery: {shlex.join(map(str, command))}"
        )
    return result.stdout


def fetch(url, limit):
    for attempt in range(2):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                contents = response.read(limit + 1)
            if len(contents) > limit:
                raise ValueError("release response exceeds its admitted artifact size")
            return contents
        except (OSError, ValueError) as error:
            if attempt:
                raise RuntimeError(
                    f"step=fetch public guest tool: {error}; "
                    f"recovery: curl --fail --location --max-time 60 {shlex.quote(url)}"
                ) from error


def digest(contents):
    return hashlib.sha256(contents).hexdigest()


def prepare(output, recovery):
    root = output / "root"
    for directory in ["bin", "dev", "proc", "sys", "modules", "mnt/acyclic"]:
        (root / directory).mkdir(parents=True, exist_ok=True)

    release = platform.release()
    kernel = Path("/boot") / f"vmlinuz-{release}"
    kernel_bytes = run("read distribution kernel", ["sudo", "-n", "cat", "--", str(kernel)])
    (output / "kernel").write_bytes(kernel_bytes)
    busybox = Path(shutil.which("busybox") or "/usr/bin/busybox")
    # This guest deliberately copies no host initrd, configuration or secrets.
    headers = run("inspect BusyBox linkage", ["readelf", "--program-headers", str(busybox)])
    if b"INTERP" in headers:
        raise RuntimeError(f"step=select static BusyBox: dynamic interpreter unsupported; recovery: {recovery}")
    shutil.copyfile(busybox, root / "bin/busybox")
    (root / "bin/busybox").chmod(0o755)
    for applet in ["sh", "mount", "mkdir", "cat", "rm", "umount", "poweroff", "insmod", "sync"]:
        (root / "bin" / applet).symlink_to("busybox")

    dependencies = run("resolve actual virtio-fs module", ["modprobe", "--show-depends", "virtiofs"])
    modules = []
    for line in dependencies.decode().splitlines():
        parts = shlex.split(line)
        if not parts or parts[0] == "builtin":
            continue
        if len(parts) != 2 or parts[0] != "insmod":
            raise RuntimeError(f"step=resolve virtio-fs dependencies: unsupported module directive; recovery: {recovery}")
        source = Path(parts[1]).resolve()
        if not source.is_relative_to(Path("/lib/modules").resolve() / release):
            raise RuntimeError(f"step=select distribution module: path outside matching modules; recovery: {recovery}")
        contents = source.read_bytes()
        if source.suffix == ".zst":
            contents = run("decompress distribution module", ["zstd", "--decompress", "--stdout", str(source)])
        elif source.suffix != ".ko":
            raise RuntimeError(f"step=select module compression: unsupported {source.suffix}; recovery: {recovery}")
        name = f"modules/{len(modules)}.ko"
        (root / name).write_bytes(contents)
        modules.append({"source": str(source), "guest": "/" + name, "sha256": digest(contents)})

    init = """#!/bin/sh
PATH=/bin
export PATH
fail() {
    echo "step=$1; recovery: $2" >&2
    echo ACYCLIC_VIRTIOFS_FAIL
    poweroff -f
    exit 1
}
mount -t proc proc /proc || fail proc 'mount -t proc proc /proc'
mount -t sysfs sysfs /sys || fail sysfs 'mount -t sysfs sysfs /sys'
mount -t devtmpfs devtmpfs /dev || fail devtmpfs 'mount -t devtmpfs devtmpfs /dev'
"""
    for module in modules:
        command = f"insmod {module['guest']}"
        init += f"{command} || fail module '{command}'\n"
    init += """mount -t virtiofs acyclic /mnt/acyclic || fail mount 'mount -t virtiofs acyclic /mnt/acyclic'
printf 'actual guest bytes\\n' > /mnt/acyclic/qualification || fail write 'printf actual > /mnt/acyclic/qualification'
sync || fail sync sync
[ "$(cat /mnt/acyclic/qualification)" = 'actual guest bytes' ] || fail read 'cat /mnt/acyclic/qualification'
rm /mnt/acyclic/qualification || fail delete 'rm /mnt/acyclic/qualification'
[ ! -e /mnt/acyclic/qualification ] || fail absence 'test ! -e /mnt/acyclic/qualification'
umount /mnt/acyclic || fail unmount 'umount /mnt/acyclic'
echo ACYCLIC_VIRTIOFS_PASS
poweroff -f
"""
    (root / "init").write_text(init)
    (root / "init").chmod(0o755)
    names = sorted(str(path.relative_to(root)) for path in root.rglob("*"))
    archive = run("pack test initramfs", ["cpio", "--null", "-o", "--format=newc", "--owner=0:0"],
                  cwd=root, input=b"\0".join(name.encode() for name in names) + b"\0")
    initramfs = gzip.compress(archive, mtime=0)
    (output / "initramfs.gz").write_bytes(initramfs)

    metadata_url = "https://api.github.com/repos/cloud-hypervisor/cloud-hypervisor/releases/latest"
    metadata = json.loads(fetch(metadata_url, 2 * 1024 * 1024))
    asset = next(asset for asset in metadata["assets"] if asset["name"] == "cloud-hypervisor-static")
    expected = asset.get("digest", "")
    if not expected.startswith("sha256:"):
        raise RuntimeError(f"step=verify official VMM release: missing SHA256 digest; recovery: {recovery}")
    vmm = fetch(asset["browser_download_url"], 64 * 1024 * 1024)
    if "sha256:" + digest(vmm) != expected:
        raise RuntimeError(f"step=verify official VMM release: digest mismatch; recovery: {recovery}")
    executable = output / "cloud-hypervisor"
    executable.write_bytes(vmm)
    executable.chmod(0o755)
    manifest = {
        "kernel": {"source": str(kernel), "release": release, "sha256": digest(kernel_bytes)},
        "busybox": {"source": str(busybox), "sha256": digest(busybox.read_bytes())},
        "modules": modules,
        "initramfs_sha256": digest(initramfs),
        "cloud_hypervisor": {"metadata": metadata_url, "release": metadata["tag_name"],
                             "asset": asset["browser_download_url"], "digest": expected},
    }
    (output / "provenance.json").write_text(json.dumps(manifest, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    recovery = shlex.join([sys.executable, __file__, "--output", str(args.output)])
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        raise RuntimeError(f"step=select guest architecture: Linux x86_64 required; recovery: {recovery}")
    output = args.output.resolve()
    if output.exists():
        unused = output.with_name(output.name + "-new")
        raise RuntimeError("step=select unused guest artifact directory: destination exists; recovery: "
                           + shlex.join([sys.executable, __file__, "--output", str(unused)]))
    output.parent.mkdir(parents=True, exist_ok=True)
    # Failed preparation leaves no partially usable guest or downloaded VMM.
    with tempfile.TemporaryDirectory(prefix=".acyclic-guest-", dir=output.parent) as temporary:
        prepared = Path(temporary) / "guest"
        prepared.mkdir()
        prepare(prepared, recovery)
        if output.exists():
            raise RuntimeError(f"step=publish guest artifacts: destination appeared; recovery: {recovery}")
        prepared.rename(output)
    executable = output / "cloud-hypervisor"
    print(json.dumps({"kernel": str(output / "kernel"), "initramfs": str(output / "initramfs.gz"),
                      "cloud_hypervisor": str(executable), "provenance": str(output / "provenance.json")}))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"step=prepare virtio-fs test guest: {error}; recovery: "
              f"{shlex.join([sys.executable, *sys.argv])}", file=sys.stderr)
        sys.exit(1)
