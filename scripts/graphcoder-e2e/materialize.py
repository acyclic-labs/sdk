"""Admit an archive, reserve its complete destination, then publish its closure.

All files exist with paid physical extents before any payload is copied. An
allocation failure cannot expose a partly materialized executable fixture.
The host adapters only reserve extents; inventory and publication are shared.
"""
import ctypes
from contextlib import contextmanager
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import tarfile
import uuid


def member_path(value):
    path = PurePosixPath(value)
    if (not value or path.is_absolute() or "\\" in value or ":" in value
            or any(part in ("", ".", "..") for part in value.split("/"))):
        raise ValueError("unsafe archive member: " + value)
    return path


def _hash(stream):
    return hashlib.file_digest(stream, "sha256").hexdigest()


def _reserve_windows(stream, size):
    import msvcrt

    class Allocation(ctypes.Structure):
        _fields_ = [("bytes", ctypes.c_longlong)]

    class Standard(ctypes.Structure):
        _fields_ = [("allocated", ctypes.c_longlong), ("end", ctypes.c_longlong),
                    ("links", ctypes.c_ulong), ("pending", ctypes.c_ubyte),
                    ("directory", ctypes.c_ubyte)]

    class Attributes(ctypes.Structure):
        _fields_ = [("flags", ctypes.c_ulong), ("tag", ctypes.c_ulong)]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.SetFileInformationByHandle.argtypes = [ctypes.c_void_p, ctypes.c_int,
                                                ctypes.c_void_p, ctypes.c_ulong]
    kernel.GetFileInformationByHandleEx.argtypes = [ctypes.c_void_p, ctypes.c_int,
                                                  ctypes.c_void_p, ctypes.c_ulong]
    kernel.DeviceIoControl.argtypes = [ctypes.c_void_p, ctypes.c_ulong, ctypes.c_void_p,
                                      ctypes.c_ulong, ctypes.c_void_p, ctypes.c_ulong,
                                      ctypes.c_void_p, ctypes.c_void_p]
    handle = msvcrt.get_osfhandle(stream.fileno())
    attributes = Attributes()
    if not kernel.GetFileInformationByHandleEx(handle, 9, ctypes.byref(attributes), ctypes.sizeof(attributes)):
        raise ctypes.WinError(ctypes.get_last_error())
    # Scratch directories can inherit compression. Normalize only this new,
    # empty, exclusively owned file, before allocating its physical extents.
    for flag, control, value in [(0x800, 0x9C040, ctypes.c_ushort(0)),
                                 (0x200, 0x900C4, ctypes.c_ubyte(0))]:
        if attributes.flags & flag:
            returned = ctypes.c_ulong()
            if not kernel.DeviceIoControl(handle, control, ctypes.byref(value), ctypes.sizeof(value),
                                          None, 0, ctypes.byref(returned), None):
                raise ctypes.WinError(ctypes.get_last_error())
    if not kernel.GetFileInformationByHandleEx(handle, 9, ctypes.byref(attributes), ctypes.sizeof(attributes)):
        raise ctypes.WinError(ctypes.get_last_error())
    if attributes.flags & (0x200 | 0x400 | 0x800):
        raise ValueError("reservation cannot retain sparse/compressed/reparse attributes: " + hex(attributes.flags))
    allocation = Allocation(size)
    if not kernel.SetFileInformationByHandle(handle, 5, ctypes.byref(allocation), ctypes.sizeof(allocation)):
        raise ctypes.WinError(ctypes.get_last_error())
    stream.truncate(size)
    standard = Standard()
    if not kernel.GetFileInformationByHandleEx(handle, 1, ctypes.byref(standard), ctypes.sizeof(standard)):
        raise ctypes.WinError(ctypes.get_last_error())
    if standard.allocated < size or standard.end != size or standard.links != 1:
        raise ValueError("destination extents were not exclusively reserved")


@contextmanager
def reserved_file(path, size):
    """Own a new file and its physical extents through every payload write.

    Consumers receive the already allocated handle; reopening with truncation
    would release the capacity they just admitted. The exclusive path remains
    available to the caller for retaining a failed transfer's evidence.
    """
    with path.open("xb+") as stream:
        if os.name == "nt":
            _reserve_windows(stream, size)
        elif size:
            os.posix_fallocate(stream.fileno(), 0, size)
            stream.truncate(size)
            if os.fstat(stream.fileno()).st_blocks * 512 < size:
                raise ValueError("destination extents were not physically reserved")
        yield stream


def _reserve_file(path, size):
    with reserved_file(path, size):
        pass


def write_reserved(source, output, size, expected):
    """Copy exactly the admitted bytes without truncating or growing backing."""
    digest = hashlib.sha256()
    remaining = size
    while remaining:
        chunk = source.read(min(remaining, 128 * 1024))
        if not chunk:
            raise ValueError("truncated admitted payload")
        output.write(chunk)
        digest.update(chunk)
        remaining -= len(chunk)
    if source.read(1) or digest.hexdigest() != expected:
        raise ValueError("payload changed after admission")


def _write_member(source, path, size, expected):
    with path.open("r+b") as output:
        write_reserved(source, output, size, expected)


def materialize(archive, expected_sha256, inventory, parent, identity=None):
    """Return one complete, exclusively owned directory or leave no fixture.

    inventory maps canonical member names to {sha256, bytes}, supplied by the
    producer admission. No retry or overwrite of an earlier identity occurs.
    """
    archive, parent = Path(archive), Path(parent).resolve(strict=True)
    if not parent.is_dir() or not inventory or not re.fullmatch(r"[a-f0-9]{64}", expected_sha256):
        raise ValueError("archive, inventory and owned parent are required")
    if os.name == "nt" and not parent.is_relative_to(Path("D:/lane-scratch").resolve()):
        raise ValueError("Windows materialization belongs under D:/lane-scratch")
    for name, row in inventory.items():
        member_path(name)
        if (set(row) != {"sha256", "bytes"} or type(row["bytes"]) is not int
                or row["bytes"] < 0 or not re.fullmatch(r"[a-f0-9]{64}", row["sha256"])):
            raise ValueError("invalid admitted archive inventory")
    identity = uuid.uuid4().hex if identity is None else identity
    if not re.fullmatch(r"[a-f0-9]{32}", identity):
        raise ValueError("invalid fixture identity")
    destination = parent / ("closure-" + identity)
    # Keep the same input handle through admission and consumption. Member
    # digests are checked again while writing, before publishing the result.
    with archive.open("rb") as input_file:
        if _hash(input_file) != expected_sha256:
            raise ValueError("archive differs from its producer receipt")
        input_file.seek(0)
        seen = set()
        with tarfile.open(fileobj=input_file, mode="r:gz") as bundle:
            for member in bundle:
                member_path(member.name)
                if (not member.isfile() or member.name in seen or member.name not in inventory
                        or member.size != inventory[member.name]["bytes"]):
                    raise ValueError("archive differs from its admitted inventory")
                if _hash(bundle.extractfile(member)) != inventory[member.name]["sha256"]:
                    raise ValueError("archive member differs from its admitted digest")
                seen.add(member.name)
        if seen != set(inventory):
            raise ValueError("incomplete archive")
        destination.mkdir()  # Existing identities fail before any mutation.
        published = False
        try:
            # This is the capacity admission barrier. Create every directory
            # and reserve every file before starting the first payload write.
            for name, row in inventory.items():
                path = destination.joinpath(*member_path(name).parts)
                path.parent.mkdir(parents=True, exist_ok=True)
                _reserve_file(path, row["bytes"])
            input_file.seek(0)
            seen = set()
            with tarfile.open(fileobj=input_file, mode="r:gz") as bundle:
                for member in bundle:
                    if (not member.isfile() or member.name in seen or member.name not in inventory
                            or member.size != inventory[member.name]["bytes"]):
                        raise ValueError("archive changed after admission")
                    row = inventory[member.name]
                    _write_member(bundle.extractfile(member), destination.joinpath(*member_path(member.name).parts),
                                  row["bytes"], row["sha256"])
                    seen.add(member.name)
            if seen != set(inventory):
                raise ValueError("archive lost members after admission")
            published = True
            return destination
        finally:
            if not published:
                # Only this call's exclusively created identity may be removed.
                if destination.parent != parent or not destination.name.startswith("closure-"):
                    raise ValueError("cleanup escaped the fixture owner")
                shutil.rmtree(destination)
