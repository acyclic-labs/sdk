#!/usr/bin/env python3
"""Shared Windows export admission, including resuming a completed producer.

This module owns archive membership, digest and disk-capacity admission. It
does not build, select Cargo artifacts, own processes or run product tests.
OpenAI Codex <noreply@openai.com>.
"""

from collections import namedtuple
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tarfile
from types import ModuleType
import uuid


materializer = ModuleType("acyclic_materializer")
materializer.__file__ = str(Path(__file__).with_name("graphcoder-e2e") / "materialize.py")
exec(compile(Path(materializer.__file__).read_bytes(), materializer.__file__, "exec"),
     materializer.__dict__)
AdmittedExport = namedtuple("AdmittedExport", "directory manifest")


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def require_capacity(directory, needed):
    """Reject an insufficient destination before fetch or extraction starts."""
    free = shutil.disk_usage(directory).free
    reserve = 64 * 1024 * 1024
    if needed + reserve > free:
        raise OSError(f"Insufficient export capacity: need {needed} bytes plus {reserve} reserve; have {free}")


def fetch_export(records, build_log):
    """Fetch a completed job's exact source-relative export, without rebuilding."""
    records = Path(records)
    matches = re.findall(r"^WIN_TEST_ARCHIVE ([0-9a-f]{64}) (\.bb-win-test-[a-f0-9]+\.tar\.gz)\s*$",
                         build_log, re.MULTILINE)
    if len(matches) != 1:
        raise ValueError("Build receipt must name exactly one successful Windows export")
    expected, name = matches[0]
    archive = records / name
    saved = records / "artifacts.tar.gz"
    for candidate in (archive, saved):
        if candidate.is_file() and digest(candidate) == expected:
            return candidate, expected
    sources = re.findall(r"; src (wt-sdk/[A-Za-z0-9_.-]+) target ", build_log)
    if len(sources) != 1:
        raise ValueError("Missing exact build-slot source receipt for immutable export")
    remote = "/mnt/bb/" + sources[0] + "/" + name
    inspect = ("import json,pathlib,sys,tarfile; p=pathlib.Path(sys.argv[1]); "
               "t=tarfile.open(p); print(json.dumps(dict(bytes=p.stat().st_size,"
               "expanded_bytes=sum(m.size for m in t if m.isfile()))))")
    command = ["python3", "-c", inspect, remote]
    transport = ["wsl.exe", "-d", "Ubuntu", "-e"] if os.name == "nt" else []
    config = ["-F", "/home/var/.ssh/buildbox/config", "-o", "BatchMode=yes"]
    sizes = json.loads(subprocess.check_output(
        [*transport, "ssh", *config, "bb2", shlex.join(command)], text=True))
    require_capacity(records, sizes["bytes"] + sizes["expanded_bytes"])
    incoming = records / ("incoming-" + uuid.uuid4().hex + ".tar.gz")
    # A failed transfer is retained at its exclusive path. A later --consume
    # uses this same immutable producer; it cannot rerun Cargo or overwrite it.
    # SCP writes to its stdout pipe, never reopening/truncating the destination.
    # The sole materializer owns both reservation and bounded digest copying.
    command = ["scp", *config, "bb2:" + remote, "/dev/stdout"]
    if transport:
        # WSL inherits a Windows-owned stdout pipe whose /proc fd cannot be
        # reopened by its unprivileged user. A POSIX-owned pipe lets SCP open
        # /dev/stdout; cat only forwards that pipe to the inherited descriptor.
        # "$@" preserves argv literally, and pipefail retains SCP's status.
        command = [*transport, "bash", "-o", "pipefail", "-c",
                   '"$@" | cat', "windows-export", *command]
    errors = incoming.with_suffix(".stderr")
    step = "reserve-transfer"
    received = 0
    try:
        with materializer.reserved_file(incoming, sizes["bytes"]) as output:
            step = "copy-reserved-transfer"
            with errors.open("xb") as stderr:
                with subprocess.Popen(command, stdin=subprocess.DEVNULL,
                                      stdout=subprocess.PIPE, stderr=stderr) as process:
                    try:
                        materializer.write_reserved(process.stdout, output, sizes["bytes"], expected)
                    finally:
                        received = output.tell()
                        process.stdout.close()
                    status = process.wait()
                    if status:
                        raise subprocess.CalledProcessError(status, command)
    except Exception as error:
        with incoming.with_suffix(".failure.json").open("x", encoding="utf-8") as receipt:
            json.dump({
            "step": step, "expected_bytes": sizes["bytes"], "received_bytes": received,
            "expected_sha256": expected, "path": str(incoming), "stderr": str(errors),
            }, receipt, indent=2)
        recovery = "bash scripts/windows-test.sh --consume " + shlex.quote(str(records))
        raise RuntimeError(f"Windows export step={step} failed: {error}; recovery={recovery}") from error
    if incoming.stat().st_size != sizes["bytes"] or digest(incoming) != expected:
        raise ValueError("Fetched archive SHA-256/size mismatch: " + str(incoming))
    if archive.exists():
        failed = records / "failed-transfers" / uuid.uuid4().hex
        failed.mkdir(parents=True)
        archive.rename(failed / archive.name)
        (failed / "receipt.json").write_text(json.dumps({
            "path": archive.name, "retained_sha256": digest(failed / archive.name),
            "expected_sha256": expected,
        }, indent=2), encoding="utf-8")
    incoming.rename(archive)
    return archive, expected


def destination(records, name):
    path = records.joinpath(*materializer.member_path(name).parts)
    # A retained partial extraction must never redirect subsequent admission.
    for part in (path, *path.parents):
        if part.is_symlink() or part.is_junction():
            raise ValueError("Archive destination traverses a link: " + str(part))
        if part == records:
            break
    return path


def admit_archive(records, archive, expected_sha256, *, required_members=None):
    """Return an owned, fully reserved export closure and its admitted manifest.

    A complete closure is verified and reused. Incomplete historical extraction
    bytes stay untouched; consumers use only the returned directory. The shared
    materializer reserves every destination extent before the first payload
    write, and publishes no fixture if capacity admission fails.

    Callers selecting a case matrix supply its independent archive-member to
    SHA-256 map in required_members (source/... for generated runtime inputs,
    bin/... for companions). The producer cannot satisfy that selection by
    omitting a member and its manifest row, or by declaring stale bytes valid.
    This contract is OS-neutral and is checked before reuse or materialization.
    """
    records, archive = Path(records), Path(archive)
    requirements = {} if required_members is None else dict(required_members)
    folded = set()
    for name, checksum in requirements.items():
        materializer.member_path(name)
        if name.casefold() in folded or not isinstance(checksum, str) or not re.fullmatch("[0-9a-f]{64}", checksum):
            raise ValueError("Export step=selected-input-admission invalid requirement: " + str(name)
                             + "; recovery=regenerate the selected case member/hash map from its source receipt")
        folded.add(name.casefold())
    if not re.fullmatch("[0-9a-f]{64}", expected_sha256) or digest(archive) != expected_sha256:
        raise ValueError("Fetched archive SHA-256 mismatch")
    # Gzip seeks restart decompression. Read each member exactly once rather
    # than enumerating to EOF and seeking back for manifests and payloads.
    inventory, documents, names = {}, {}, set()
    with tarfile.open(archive, "r|gz") as tar:
        for member in tar:
            materializer.member_path(member.name)
            if not member.isfile():
                raise ValueError("Unexpected non-file archive member: " + member.name)
            folded_name = member.name.casefold()
            if folded_name in names:
                raise ValueError("Duplicate member or missing export manifest")
            names.add(folded_name)
            with tar.extractfile(member) as stream:
                if member.name in ("manifest.json", "host-receipt.json"):
                    data = stream.read()
                    actual = hashlib.sha256(data).hexdigest()
                    documents[member.name] = json.loads(data)
                else:
                    actual = hashlib.file_digest(stream, "sha256").hexdigest()
            inventory[member.name] = dict(sha256=actual, bytes=member.size)
        if "manifest.json" not in documents:
            raise ValueError("Duplicate member or missing export manifest")
        manifest = documents["manifest.json"]
        allowed = {"bin/" + row["name"]: row["sha256"] for row in manifest["artifacts"]}
        allowed.update({"bin/" + name: row["sha256"] for name, row in manifest["dlls"].items()})
        allowed.update({"source/" + name: value for name, value in manifest["source"].items()})
        if "process_host" in manifest:
            host = manifest["process_host"]
            name = "bin/" + host["name"]
            if name in allowed:
                raise ValueError("Process host aliases a test or fixture artifact")
            allowed[name] = host["sha256"]
            allowed["host-receipt.json"] = host["receipt_sha256"]
            # The composable host factory's real Cargo stream is a required
            # producer input, independent of the enclosing helper's stem.
            # Admit its exact receipt-bound bytes, never an unverified log alias.
            producer = host["receipt"].get("producer")
            if producer is not None:
                checksum = producer["cargo_json_sha256"]
                if not isinstance(checksum, str) or not re.fullmatch(r"[0-9a-f]{64}", checksum):
                    raise ValueError("Export step=native-host-cargo-admission: invalid log digest; "
                                     "recovery=fetch the complete source-matched native host producer export")
                allowed["logs/cargo.jsonl"] = checksum
            for path, checksum in host["receipt"]["source"].items():
                if manifest["source"].get(path) != checksum:
                    raise ValueError("Process host source differs from the exported graph: " + path)
        if not set(allowed).issubset(inventory):
            raise ValueError("Incomplete artifact/fixture fetch")
        for name, expected in requirements.items():
            if allowed.get(name) != expected:
                raise ValueError("Export step=selected-input-admission missing or stale member: " + name
                                 + "; recovery=fetch the complete source-matched producer export before launching cases")
        if "process_host" in manifest and documents.get("host-receipt.json") != manifest["process_host"]["receipt"]:
            raise ValueError("Process host receipt differs from the export manifest")
        for name, row in inventory.items():
            if name != "manifest.json" and name not in allowed and not re.fullmatch(
                    r"logs/\.bb-win-test-[a-f0-9]+-[A-Za-z0-9_-]+\.jsonl", name):
                raise ValueError("Unexpected archive member: " + name)
            if name in allowed and row["sha256"] != allowed[name]:
                raise ValueError("Fetched file SHA-256 mismatch: " + name)
    # Recovery validates a complete immutable closure; it never adopts a
    # partially written file or resumes payload writes without reservation.
    for closure in sorted(records.glob("closure-*")):
        if not re.fullmatch(r"closure-[a-f0-9]{32}", closure.name):
            continue
        if closure.is_symlink() or closure.is_junction() or not closure.is_dir():
            raise ValueError("Export closure is not an owned directory: " + str(closure))
        if all((path := destination(closure, name)).is_file()
               and path.stat().st_size == row["bytes"] and digest(path) == row["sha256"]
               for name, row in inventory.items()):
            print("WIN_TEST_FETCH_VERIFIED", len(manifest["artifacts"]), "executables,",
                  len(manifest["source"]), "source/fixture files; retained complete closure", flush=True)
            return AdmittedExport(closure, manifest)
    require_capacity(records, sum(row["bytes"] for row in inventory.values()))
    closure = materializer.materialize(archive, expected_sha256, inventory, records)
    print("WIN_TEST_FETCH_VERIFIED", len(manifest["artifacts"]), "executables,",
          len(manifest["source"]), "source/fixture files; fully reserved closure", flush=True)
    return AdmittedExport(closure, manifest)
