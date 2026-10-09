"""Qualify installed transport bindings from an attested local archive."""

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import zipfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def qualify(args):
    archive = args.package.resolve(strict=True)
    authority = args.authority.resolve(strict=True)
    receipt_path = args.receipt.resolve(strict=True)
    go = args.go.resolve(strict=True)
    work = args.output.parent.resolve(strict=True) / args.output.name
    if work.exists() or work.is_symlink():
        raise ValueError("qualification output must be absent")
    for source in (archive, authority, receipt_path, Path(__file__).resolve().parent, go):
        if source == work or work in source.parents or source in work.parents:
            raise ValueError("qualification output overlaps an input")
    archive_bytes = archive.read_bytes()
    if digest(archive_bytes) != args.sha256:
        raise ValueError("package archive digest mismatch")
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    if (receipt["schema"] != "acyclic.sdk.go-producer-receipt.v1"
            or receipt["authority"] != "rust" or receipt["target"] != "go"):
        raise ValueError("unsupported generation receipt")
    manifest_bytes = (authority / "rust-authority.json").read_bytes()
    if digest(manifest_bytes) != receipt["authority_manifest_sha256"]:
        raise ValueError("Rust authority manifest digest mismatch")
    manifest = json.loads(manifest_bytes)
    if manifest["schema"] != "acyclic.sdk.rust-authority.v1" or manifest["authority"] != "rust":
        raise ValueError("unsupported authority manifest")
    if manifest["source_revision"] != receipt["source_revision"]:
        raise ValueError("Rust source revision mismatch")
    for family in manifest["families"]:
        for field in ("source", "descriptor"):
            relative = PurePosixPath(family[field])
            if relative.is_absolute() or ".." in relative.parts or "\\" in str(relative):
                raise ValueError("unsafe authority path")
            path = (authority / relative).resolve(strict=True)
            if not path.is_relative_to(authority):
                raise ValueError("authority input escapes its root")
            if digest(path.read_bytes()) != family[field + "_sha256"]:
                raise ValueError("Rust authority input digest mismatch")

    env = dict(os.environ, GOMAXPROCS="1", GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off",
               SDK_AUTHORITY_DIR=str(authority))
    version = subprocess.check_output([go, "version"], env=env, text=True).strip()
    if version.split()[2] != receipt["go_version"]:
        raise ValueError("Go toolchain differs from generation receipt")
    with zipfile.ZipFile(archive) as package:
        names = package.namelist()
        if len(names) != len(set(names)) or set(names) != set(receipt["outputs"]):
            raise ValueError("archive inventory differs from generation receipt")
        if len({os.path.normcase(name) for name in names}) != len(names):
            raise ValueError("package entries collide on this filesystem")
        for entry in package.infolist():
            path = PurePosixPath(entry.filename)
            if (path.is_absolute() or ".." in path.parts or "\\" in entry.filename
                    or ":" in entry.filename or str(path) != entry.filename
                    or (entry.external_attr >> 16) & 0o170000 not in (0, 0o100000)):
                raise ValueError("unsafe package entry")
            if os.name == "nt":
                devices = {"CON", "PRN", "AUX", "NUL"} | {
                    prefix + str(number) for prefix in ("COM", "LPT") for number in range(1, 10)}
                if any(part.endswith((".", " ")) or part.split(".")[0].upper() in devices for part in path.parts):
                    raise ValueError("unsafe Windows package entry")
            if digest(package.read(entry)) != receipt["output_sha256"][entry.filename]:
                raise ValueError("archive payload digest mismatch")
        work.mkdir()
        installed = work / "installed"
        package.extractall(installed)

    consumer = work / "consumer"
    consumer.mkdir()
    for name in ("go.mod", "go.sum"):
        shutil.copyfile(installed / name, consumer / name)
    controls = Path(__file__).parent / "testdata" / "consumer"
    for source in controls.glob("*.go"):
        shutil.copyfile(source, consumer / source.name)

    def run(name, arguments, expected_failure=False):
        result = subprocess.run([go, "-C", consumer, *arguments], env=env,
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=180)
        (work / (name + ".log")).write_text(result.stdout, encoding="utf-8")
        if bool(result.returncode) != expected_failure:
            raise RuntimeError(f"{name} returned {result.returncode}: {result.stdout}")
        return result.stdout

    run("module", ["mod", "edit", "-module=qualification.example/installed-go",
                   "-require=github.com/acyclic-labs/sdk/go@v0.2.0",
                   "-replace=github.com/acyclic-labs/sdk/go=../installed"])
    run("positive", ["test", "-mod=readonly", "-p=1", "-parallel=1", "-count=1", "-v", "./..."])
    negative = run("negative", ["test", "-mod=readonly", "-p=1", "-parallel=1", "-count=1",
                                "-tags=negative", "./..."], expected_failure=True)
    if negative.count("cannot use") != 3 or "as []byte" not in negative or "as *uint64" not in negative:
        raise RuntimeError("negative controls failed for an unrelated reason")
    result = {
        "scope": "installed-go-transport-bindings",
        "source_revision": receipt["source_revision"],
        "archive_sha256": args.sha256,
        "generation_receipt_sha256": digest(receipt_bytes),
        "authority_manifest_sha256": digest(manifest_bytes),
        "go_version": version,
        "go_executable_sha256": digest(go.read_bytes()),
        "qualifier_sha256": digest(Path(__file__).read_bytes()),
        "control_sha256": {source.name: digest(source.read_bytes()) for source in controls.glob("*.go")},
        "log_sha256": {path.name: digest(path.read_bytes()) for path in work.glob("*.log")},
        "descriptor_equality": "all API fields; source comments and Buf image tag 8042 excluded",
        "positive_controls_passed": True,
        "negative_type_controls_rejected": 3,
        "rust_backed_rpc_qualified": False,
        "embedded_runtime_qualified": False,
    }
    (work / "qualification.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--authority", type=Path, required=True)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    qualify(parser.parse_args())
