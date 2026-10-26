#!/usr/bin/env python3
"""The native fleet lease must survive the death of its Python consumer.

Use ACTUAL_OLD_RUNNER as argv[2] for the unchanged former-consumer control.
The same real native host is suspended at the observation cut, so drain cannot
race the lease assertion. Teardown resumes that original host and awaits its
own physical retirement through handles opened while every process was live.
No replacement process killer, reaper or fixture profile policy is used.
Attribution: Codex <noreply@openai.com>.
"""

import ctypes
import errno
import json
import msvcrt
import os
from pathlib import Path
import subprocess
import sys
import time
import types
import uuid


def load(path):
    module = types.ModuleType(str(path))
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
    return module


def kernel_api():
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateEventW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_bool, ctypes.c_wchar_p]
    kernel.CreateEventW.restype = ctypes.c_void_p
    kernel.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_bool, ctypes.c_uint32]
    kernel.OpenProcess.restype = ctypes.c_void_p
    kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    kernel.WaitForMultipleObjects.argtypes = [ctypes.c_uint32, ctypes.POINTER(ctypes.c_void_p), ctypes.c_bool, ctypes.c_uint32]
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    return kernel


def owner(records, module_path, host, event, source):
    module = load(module_path)
    python = str(Path(sys.executable).resolve())
    child = (
        "import ctypes,json,os,pathlib,subprocess,sys,threading; "
        "p=subprocess.Popen([sys.executable,'-c','import threading; threading.Event().wait()']); "
        f"pathlib.Path({str(records / 'members.json')!r}).write_text(json.dumps([os.getpid(),p.pid])); "
        "k=ctypes.WinDLL('kernel32',use_last_error=True); "
        "k.OpenEventW.argtypes=[ctypes.c_uint32,ctypes.c_bool,ctypes.c_wchar_p]; "
        "k.OpenEventW.restype=ctypes.c_void_p; k.SetEvent.argtypes=[ctypes.c_void_p]; "
        "k.CloseHandle.argtypes=[ctypes.c_void_p]; "
        f"h=k.OpenEventW(2,False,{event!r}); assert h; assert k.SetEvent(h); assert k.CloseHandle(h); "
        "threading.Event().wait()"
    )
    original = module.subprocess.Popen

    def capture(argv, **kwargs):
        process = original(argv, **kwargs)
        if child in argv:
            (records / "native-host.json").write_text(json.dumps(process.pid))
            kernel = kernel_api()
            captured = kernel.CreateEventW(None, True, False, event + "Capture")
            assert captured, ctypes.get_last_error()
            kernel.SetEvent.argtypes = [ctypes.c_void_p]
            assert kernel.SetEvent(captured)
            assert kernel.CloseHandle(captured)
        return process

    module.subprocess.Popen = capture
    with module.Commands(records, source, host) as commands:
        commands.run([python, "-c", child], cwd=source,
                     name="owner-death-blocked-output", sha256=module.digest(python))


def excluded(path):
    # A one-byte test probe conflicts with the native whole-file lock. This
    # is an OS observation only; production locking remains FileLock alone.
    if not path.exists():
        return False
    with path.open("r+b") as file:
        try:
            msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as error:
            assert error.errno in (errno.EACCES, errno.EDEADLK), error
            return True
        else:
            msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
            return False


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--owner":
        owner(Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]), sys.argv[5], Path(sys.argv[6]))
        return
    if os.name != "nt":
        raise OSError("step=owner-death-host; recovery=run this qualification on real Windows with an admitted MSVC host")
    source = Path(__file__).resolve().parent.parent
    candidate_path = source / "scripts/windows-runner.py"
    candidate = load(candidate_path)
    module_path = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else candidate_path
    # An actual former consumer must retain its matched native source/host
    # pair. Never shim a v1 consumer onto the current v2 completion protocol.
    if len(sys.argv) > 3:
        if len(sys.argv) != 6:
            raise ValueError("step=old-control-input; recovery=pass RECORDS OLD_RUNNER OLD_SOURCE OLD_HOST OLD_HOST_RECEIPT")
        source = Path(sys.argv[3]).resolve()
        host_path, host_receipt = sys.argv[4:6]
    else:
        host_path = os.environ["E2E_PROCESS_HOST"]
        host_receipt = os.environ["WIN_TEST_PROCESS_HOST_RECEIPT"]
    records = Path(sys.argv[1]).resolve()
    records.mkdir(parents=True)
    host = candidate.admitted_host(source, host_path, host_receipt)
    kernel = kernel_api()
    native_api = ctypes.WinDLL("ntdll")
    native_api.NtSuspendProcess.argtypes = [ctypes.c_void_p]
    native_api.NtResumeProcess.argtypes = [ctypes.c_void_p]
    event_name = "Local\\AcyclicRunnerOwnerDeath" + uuid.uuid4().hex
    ready = kernel.CreateEventW(None, True, False, event_name)
    assert ready, ctypes.get_last_error()
    captured = kernel.CreateEventW(None, True, False, event_name + "Capture")
    assert captured, ctypes.get_last_error()
    handles = []
    native = None
    suspended = False
    observed_exclusion = False
    started = time.monotonic()
    owner_log = (records / "owner.log").open("x", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, "-X", "utf8", str(Path(__file__).resolve()), "--owner",
         str(records / "owner"), str(module_path), str(host), event_name, str(source)],
        stdout=owner_log, stderr=subprocess.STDOUT,
    )
    try:
        waits = (ctypes.c_void_p * 2)(ready, int(process._handle))
        assert kernel.WaitForMultipleObjects(2, waits, False, 0xFFFFFFFF) == 0, "owner exited before child admission; inspect owner.log"
        waits[0] = captured
        assert kernel.WaitForMultipleObjects(2, waits, False, 0xFFFFFFFF) == 0, "owner exited before publishing its native host"
        native_pid = json.loads((records / "owner/native-host.json").read_text())
        native = kernel.OpenProcess(0x100000 | 0x800, False, native_pid)
        assert native, ctypes.get_last_error()
        for pid in json.loads((records / "owner/members.json").read_text()):
            handle = kernel.OpenProcess(0x100000, False, pid)
            assert handle, ctypes.get_last_error()
            handles.append(handle)
        assert native_api.NtSuspendProcess(native) == 0
        suspended = True
        # Kill only the fixture consumer. Its original native host remains
        # alive and frozen, so early lease release is a deterministic failure.
        process.kill()
        process.wait()
        observed_exclusion = excluded(candidate.Commands(
            records / "probe", source, host).lease_root / "owner.lock")
    finally:
        if suspended:
            assert native_api.NtResumeProcess(native) == 0
        if process.poll() is None:
            process.kill()
        process.wait()
        # The original native host observes control EOF and owns all drain.
        for handle in [*handles, *([native] if native else [])]:
            try:
                assert kernel.WaitForSingleObject(handle, 0xFFFFFFFF) == 0
            finally:
                assert kernel.CloseHandle(handle)
        assert kernel.CloseHandle(ready)
        assert kernel.CloseHandle(captured)
        owner_log.close()
    result = dict(seconds=time.monotonic() - started,
                  consumer_sha256=candidate.digest(module_path),
                  native_host_sha256=candidate.digest(host),
                  native_host_receipt_sha256=candidate.digest(host_receipt),
                  source=str(source),
                  excluded_after_consumer_death=observed_exclusion,
                  stable_members=len(handles), physical_retirement=True)
    (records / "qualification.json").write_text(json.dumps(result, indent=2))
    assert observed_exclusion, "former consumer released fleet admission while its native tree was alive"
    assert len(handles) == 2 and result["seconds"] < 10, result
    print(json.dumps(result))


if __name__ == "__main__":
    main()
