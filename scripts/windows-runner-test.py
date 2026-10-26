#!/usr/bin/env python3
"""Exercise the real Windows runner with the installed Python, without builds."""

import ast
import ctypes
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time


def load(path, helper):
    embedded = helper.read_text(encoding="utf-8").split("<<'WIN_TEST_PY'\n", 1)[1].rsplit("\nWIN_TEST_PY", 1)[0]
    tree = ast.parse(embedded)
    loader = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "runner_module")
    namespace = {}
    exec(compile(ast.Module(body=[loader], type_ignores=[]), str(helper), "exec"), namespace)
    return namespace["runner_module"](path)


def selection_probes(module, records, source, python, checksum):
    """Run the actual manifest adapter with two admitted Python fixture entries."""
    for label, counts, exits in (
        ("empty-first", [0, 1], [0, 0]),
        ("empty-last", [1, 0], [0, 0]),
        ("all-empty", [0, 0], [0, 0]),
        ("failure", [0, 1], [17, 0]),
        ("listing", [0, 0], [0, 0]),
        ("malformed-empty", [0, 1], [0, 0]),
    ):
        directory = records / label
        # All selections consume one already-admitted immutable source export.
        directory.mkdir(parents=True)
        counter = directory / "invocations"
        # These are protocol fixtures, not product tests. Both manifest entries
        # invoke the same hash-admitted installed executable with different
        # deterministic outputs. The production adapter owns every invocation.
        probe = (
            "from pathlib import Path; import sys; "
            f"p=Path({str(counter)!r}); n=int(p.read_text()) if p.exists() else 0; "
            "p.write_text(str(n+1)); "
            f"print('test result: ok. '+str({counts!r}[n])+' passed; 0 failed; 0 ignored;'); "
            f"sys.exit({exits!r}[n])"
        )
        if label == "malformed-empty":
            probe = probe.replace("0 failed; 0 ignored;", "0 failed;")
        manifest = dict(crate_dir=".", artifacts=[
            dict(name=python, target=f"{label}-{index}", test=True, sha256=checksum)
            for index in range(2)
        ])
        try:
            runtime = ["-c", probe, *(["--list"] if label == "listing" else [])]
            rc = module.run_tests(directory, manifest, runtime, source=source)
        except RuntimeError as error:
            assert label in ("all-empty", "malformed-empty") and "ZERO" in str(error), (label, error)
        else:
            assert label not in ("all-empty", "malformed-empty"), "accepted an empty or malformed selection"
            assert rc == (17 if label == "failure" else 0), (label, rc)
        actual = json.loads((directory / "results.json").read_text(encoding="utf-8"))
        expected = 1 if label in ("failure", "malformed-empty") else 2
        assert int(counter.read_text()) == expected
        assert [row["target"] for row in actual] == [f"{label}-{index}" for index in range(expected)]
        assert [row["passed_tests"] for row in actual] == counts[:expected]


def main():
    if os.name != "nt":
        raise OSError("Windows runner qualification requires real Windows")
    source = Path(__file__).resolve().parent.parent
    module_path = Path(sys.argv[2]) if len(sys.argv) > 2 else source / "scripts/windows-runner.py"
    helper = Path(sys.argv[3]) if len(sys.argv) > 3 else source / "scripts/windows-test.sh"
    module = load(module_path, helper)
    records = Path(sys.argv[1]).resolve()
    records.mkdir(parents=True)
    host = module.admitted_host(
        source, os.environ["E2E_PROCESS_HOST"], os.environ["WIN_TEST_PROCESS_HOST_RECEIPT"]
    )
    python = str(Path(sys.executable).resolve())
    checksum = module.digest(python)
    probe = (
        "import os,pathlib,sys; "
        "home=pathlib.Path(os.environ['HOME']); "
        "marker=home/'state'; assert not marker.exists(), 'retained fixture state'; "
        "marker.write_text('owned'); "
        "assert home==pathlib.Path(os.environ['USERPROFILE']); "
        "assert pathlib.Path(os.environ['CODEX_HOME']).is_relative_to(home); "
        "assert os.environ['RUST_TEST_THREADS']=='4'; "
        "assert 'RUST_MIN_STACK' not in os.environ; "
        "assert not any(k.upper().endswith('API_KEY') or 'AUTH_TOKEN' in k.upper() for k in os.environ); "
        "assert '--test-threads=4' not in sys.argv; print('generic command passed')"
    )
    start = time.monotonic()
    old_key = os.environ.get("RUNNER_PROBE_API_KEY")
    os.environ["RUNNER_PROBE_API_KEY"] = "fixture-only-no-secret"
    try:
        with module.Commands(records, source, host) as commands:
            for label in ("shipping-first", "shipping-second"):
                assert commands.run([python, "-c", probe], cwd=source, name=label, sha256=checksum) == 0
            libtest = "import sys; assert sys.argv.count('--test-threads=4')==1; print('test result: ok. 1 passed; 0 failed;')"
            assert commands.run([python, "-c", libtest], cwd=source, name="libtest-adapter", sha256=checksum, libtest=True) == 0
            lease = (
                "import msvcrt,sys; "
                "f=open(sys.argv[1],'r+b'); "
                "exec(\"try:\\n msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)\\n\"
                "\"except OSError:\\n pass\\nelse:\\n raise AssertionError('fleet lease escaped')\"); "
                "f.close(); print('fleet exclusion passed')"
            )
            assert commands.run([python, "-c", lease, str(commands.lease_root / "owner.lock")], cwd=source, name="cross-command-fleet-exclusion", sha256=checksum) == 0
            import_source = records / "import-source"
            import_source.mkdir()
            imported = import_source / "runner.py"
            imported.write_bytes(module_path.read_bytes())
            immutable_import = (
                "import ast,pathlib,sys; sys.dont_write_bytecode=False; "
                "path=pathlib.Path(sys.argv[1]); helper=pathlib.Path(sys.argv[2]); "
                "embedded=helper.read_text().split(\"<<'WIN_TEST_PY'\\n\",1)[1].rsplit('\\nWIN_TEST_PY',1)[0]; "
                "tree=ast.parse(embedded); loader=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='runner_module'); "
                "ns={}; exec(compile(ast.Module(body=[loader],type_ignores=[]),str(helper),'exec'),ns); "
                "ns['runner_module'](path); "
                "assert sorted(p.name for p in path.parent.iterdir())==['runner.py'], 'import mutated admitted source'; "
                "print('immutable runner import passed')"
            )
            assert commands.run([python, "-c", immutable_import, str(imported), str(helper)], cwd=source, name="immutable-source-import", sha256=checksum) == 0
            # Force the same identity: the second call must reject before a child.
            fixed = module.uuid.uuid4()
            original_uuid = module.uuid.uuid4
            module.uuid.uuid4 = lambda: fixed
            try:
                assert commands.run([python, "-c", probe], cwd=source, name="exclusive-first", sha256=checksum) == 0
                count = len(commands.results)
                try:
                    commands.run([python, "-c", probe], cwd=source, name="exclusive-collision", sha256=checksum)
                except FileExistsError:
                    pass
                else:
                    raise AssertionError("reused a mutable fixture identity")
                assert len(commands.results) == count
            finally:
                module.uuid.uuid4 = original_uuid
            try:
                commands.run([python, "-c", "print('no libtest results')"], cwd=source, name="zero-tests", sha256=checksum, libtest=True)
            except RuntimeError as error:
                assert "ZERO" in str(error)
            else:
                raise AssertionError("accepted missing libtest cases")
            count = len(commands.results)
            try:
                commands.run([python, "-c", probe], cwd=source, name="wrong-artifact", sha256="0" * 64)
            except ValueError:
                pass
            else:
                raise AssertionError("ran an unadmitted executable")
            assert len(commands.results) == count
            # Observe physical death through handles opened while both processes
            # are live, rather than looking up potentially recycled PIDs later.
            kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_bool, ctypes.c_uint32]
            kernel.OpenProcess.restype = ctypes.c_void_p
            kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
            kernel.CloseHandle.argtypes = [ctypes.c_void_p]
            handles = []
            def interrupt(*values, **kwargs):
                line = str(values[0]) if values else ""
                if line.startswith("DRAIN_READY "):
                    for pid in json.loads(line.removeprefix("DRAIN_READY ")):
                        handle = kernel.OpenProcess(0x100000, False, pid)
                        assert handle, ctypes.get_last_error()
                        handles.append(handle)
                    raise RuntimeError("fixture interruption")
                print(*values, **kwargs)
            module.print = interrupt
            tree = (
                "import json,os,subprocess,sys,threading; "
                "child=subprocess.Popen([sys.executable,'-c','import threading; threading.Event().wait()']); "
                "print('DRAIN_READY '+json.dumps([os.getpid(),child.pid]),flush=True); "
                "threading.Event().wait()"
            )
            try:
                try:
                    commands.run([python, "-c", tree], cwd=source, name="interruption-drain", sha256=checksum)
                except RuntimeError as error:
                    assert str(error) == "fixture interruption"
                else:
                    raise AssertionError("interruption was lost")
                assert len(handles) == 2
                for handle in handles:
                    assert kernel.WaitForSingleObject(handle, 0) == 0, "released before physical process death"
            finally:
                del module.print
                for handle in handles:
                    # A negative control may detect early release. Its teardown
                    # still waits for the existing host's EOF-driven drain before
                    # another qualification process can acquire the fleet lease.
                    try:
                        assert kernel.WaitForSingleObject(handle, 0xFFFFFFFF) == 0
                    finally:
                        assert kernel.CloseHandle(handle)
            assert commands.run([python, "-c", probe], cwd=source, name="after-drain", sha256=checksum) == 0
        assert len({result["fixture"] for result in commands.results}) == len(commands.results)
        plan = dict(source=str(source), records=str(records / "cli"), commands=[dict(
            argv=[python, "-c", probe], cwd=str(source), name="shipping-plan", sha256=checksum,
        )])
        plan_path = records / "command-plan.json"
        plan_path.write_text(json.dumps(plan), encoding="utf-8")
        assert subprocess.call([python, "-X", "utf8", str(module_path), str(plan_path)]) == 0
        cli_results = json.loads((records / "cli/results.json").read_text(encoding="utf-8"))
        # A second independent invocation can update the convenience summary,
        # but every first invocation receipt must remain byte-for-byte intact.
        receipts = records / "receipts"
        retained = {path.name: path.read_bytes() for path in receipts.glob("*.json")}
        assert len(retained) == len(commands.results)
        with module.Commands(records, source, host) as successor:
            assert successor.run([python, "-c", probe], cwd=source,
                                 name="successor-invocation", sha256=checksum) == 0
        for name, original in retained.items():
            assert (receipts / name).read_bytes() == original, "earlier receipt was overwritten"
        assert len(list(receipts.glob("*.json"))) == len(retained) + 1
        for result in [*commands.results, *successor.results]:
            path = receipts / (result["fixture"] + ".json")
            assert json.loads(path.read_text(encoding="utf-8")) == result
        lease_wait_seconds = (commands.lease_wait_seconds + successor.lease_wait_seconds
                              + cli_results[0]["lease_wait_seconds"])
        selection_probes(module, records, source, python, checksum)
        for label in ("empty-first", "empty-last", "all-empty", "failure", "listing", "malformed-empty"):
            rows = json.loads((records / label / "results.json").read_text(encoding="utf-8"))
            lease_wait_seconds += sum(row["lease_wait_seconds"] for row in rows)
    finally:
        if old_key is None:
            os.environ.pop("RUNNER_PROBE_API_KEY", None)
        else:
            os.environ["RUNNER_PROBE_API_KEY"] = old_key
    wall_seconds = time.monotonic() - start
    # Fleet admission may queue behind another lane. Preserve that latency,
    # while measuring the suite from ownership through physical drain.
    seconds = wall_seconds - lease_wait_seconds
    result = dict(exit=0, seconds=seconds, wall_seconds=wall_seconds,
                  lease_wait_seconds=lease_wait_seconds, under_10s=seconds < 10,
                  commands=len(commands.results), runner_sha256=module.digest(module_path))
    (records / "qualification.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    assert result["under_10s"], result
    print(json.dumps(result))


if __name__ == "__main__":
    main()
