#!/usr/bin/env python3
"""Real blocked-output cancellation through generic argv and libtest adapters.

The fixture uses the installed Python and admitted native host, never a compiler
or fabricated provider output. Pass OLD_RUNNER as argv[2] for the actual previous
consumer negative, and optionally "libtest" as argv[3] to select that adapter.
Its unmodified run loop ignores the cancellation subscription.
Teardown closes the same host control pipe so that negative cannot strand a tree.
"""

import ctypes
import json
import os
from pathlib import Path
import sys
import threading
import time
import types
import uuid


def load(path):
    module = types.ModuleType(str(path))
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), "exec"), module.__dict__)
    return module


def main():
    source = Path(__file__).resolve().parent.parent
    candidate_path = source / "scripts/windows-runner.py"
    candidate = load(candidate_path)
    path = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else candidate_path
    module = load(path)
    records = Path(sys.argv[1]).resolve()
    records.mkdir(parents=True)
    host = candidate.admitted_host(source, os.environ["E2E_PROCESS_HOST"],
                                   os.environ["WIN_TEST_PROCESS_HOST_RECEIPT"])
    python = str(Path(sys.executable).resolve())
    checksum = candidate.digest(python)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateEventW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_bool, ctypes.c_wchar_p]
    kernel.CreateEventW.restype = ctypes.c_void_p
    kernel.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_bool, ctypes.c_uint32]
    kernel.OpenProcess.restype = ctypes.c_void_p
    kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    observations = []
    started = time.monotonic()
    waits = 0
    adapters = (True,) if len(sys.argv) > 3 and sys.argv[3] == "libtest" else (False, True)
    for libtest in adapters:
        event_name = "Local\\AcyclicRunnerCancellation" + uuid.uuid4().hex
        ready = kernel.CreateEventW(None, True, False, event_name)
        assert ready, ctypes.get_last_error()
        pid_file = records / ("libtest-pids.json" if libtest else "argv-pids.json")
        read_entered = threading.Event()
        processes = []
        handles = []
        failures = []
        completed_lines = []
        cancellation = candidate.Cancellation()
        original_popen = module.subprocess.Popen

        class Output:
            def __init__(self, stream):
                self.stream = stream

            def __iter__(self):
                return self

            def __next__(self):
                read_entered.set()
                line = next(self.stream)
                completed_lines.append(line)
                return line

            def __getattr__(self, name):
                return getattr(self.stream, name)

        def capture(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            # Profile bootstrap uses the same owner but is not the blocked
            # command under observation. Its completed line is unrelated.
            if "-c" in args[0] and child in args[0]:
                processes.append(process)
                process.stdout = Output(process.stdout)
            return process

        def cancel_blocked_read():
            read_entered.wait()
            assert kernel.WaitForSingleObject(ready, 0xFFFFFFFF) == 0
            stream = processes[0].stdin
            try:
                assert not completed_lines, "cancellation arrived after a complete output line"
                for pid in json.loads(pid_file.read_text(encoding="utf-8")):
                    handle = kernel.OpenProcess(0x100000, False, pid)
                    assert handle, ctypes.get_last_error()
                    handles.append(handle)
                cancellation.cancel()
                assert stream.closed, "blocked stdout prevented cancellation from closing native host control"
            except BaseException as error:
                failures.append(repr(error))
            finally:
                # Actual-old negative cleanup uses its existing host's EOF
                # contract; this is not a second process owner or a timer.
                stream.close()

        child = (
            "import ctypes,json,os,pathlib,subprocess,sys,threading; "
            "child=subprocess.Popen([sys.executable,'-c','import threading; threading.Event().wait()']); "
            "pathlib.Path(sys.argv[1]).write_text(json.dumps([os.getpid(),child.pid]),encoding='utf-8'); "
            "sys.stdout.write('BLOCKED_WITHOUT_NEWLINE'); sys.stdout.flush(); "
            "k=ctypes.WinDLL('kernel32',use_last_error=True); "
            "k.OpenEventW.argtypes=[ctypes.c_uint32,ctypes.c_bool,ctypes.c_wchar_p]; "
            "k.OpenEventW.restype=ctypes.c_void_p; k.SetEvent.argtypes=[ctypes.c_void_p]; "
            "k.CloseHandle.argtypes=[ctypes.c_void_p]; "
            "h=k.OpenEventW(2,False,sys.argv[2]); assert h; assert k.SetEvent(h); "
            "assert k.CloseHandle(h); threading.Event().wait()"
        )
        module.subprocess.Popen = capture
        try:
            with module.Commands(records / ("libtest" if libtest else "argv"), source, host) as commands:
                # Setting the public operation token also exercises the actual
                # old run loop unchanged: it has no subscription and fails the
                # same synchronous close oracle, rather than a new-API TypeError.
                commands.cancellation = cancellation
                controller = threading.Thread(target=cancel_blocked_read)
                controller.start()
                try:
                    rc = commands.run([python, "-c", child, str(pid_file), event_name],
                                      cwd=source, name="blocked-output-cancellation",
                                      sha256=checksum, libtest=libtest)
                finally:
                    controller.join()
                assert len(handles) == 2, failures
                for handle in handles:
                    assert kernel.WaitForSingleObject(handle, 0) == 0, "fleet lease released before physical death"
                assert not failures, failures
                assert rc == 130, rc
                result = commands.results[-1]
                assert result["cancelled"] and result["exit"] == 130
                assert result["verification_error"] is None
                assert "BLOCKED_WITHOUT_NEWLINE" in Path(result["log"]).read_text(encoding="utf-8")
                assert result["under_10s"], result
                # A consumed token refuses a successor before fixture allocation
                # and process creation, so cancellation cannot cross invocations.
                count = len(processes)
                try:
                    commands.run([python, "-c", "raise AssertionError('must not launch')"],
                                 cwd=source, name="already-cancelled", sha256=checksum)
                except RuntimeError as error:
                    assert "cancelled before launch" in str(error), error
                else:
                    raise AssertionError("already-cancelled operation launched a successor")
                assert len(processes) == count
                observations.append(result)
                waits += commands.lease_wait_seconds
        finally:
            module.subprocess.Popen = original_popen
            for handle in handles:
                assert kernel.WaitForSingleObject(handle, 0xFFFFFFFF) == 0
                assert kernel.CloseHandle(handle)
            assert kernel.CloseHandle(ready)
    elapsed = time.monotonic() - started
    result = dict(exit=0, seconds=elapsed - waits, wall_seconds=elapsed,
                  lease_wait_seconds=waits, under_10s=elapsed - waits < 10,
                  runner_sha256=candidate.digest(path), observations=observations)
    (records / "qualification.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    assert result["under_10s"], result
    print(json.dumps(result))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise RuntimeError("Windows runner step=blocked-output-cancellation failed: " + str(error)
                           + "; recovery=python -X utf8 scripts/windows-runner-cancellation-test.py "
                           "D:/lane-scratch/windows/NEW_RECORDS") from error
