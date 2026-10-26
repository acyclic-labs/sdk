#!/usr/bin/env python3
"""One Windows fixture and fleet lease for libtest and shipping commands.

Compilation and artifact admission belong to win-test.sh. Process containment
belongs to the admitted external process host; this module never owns a Job or
uses PID-based tree killing. OpenAI Codex <noreply@openai.com>.
"""

from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def admitted_host(source, executable, receipt):
    """Bind the cross-built host to its source and producer receipt."""
    executable = Path(executable)
    receipt = json.loads(Path(receipt).read_text(encoding="utf-8"))
    if not executable.is_absolute() or not executable.is_file():
        raise ValueError("E2E_PROCESS_HOST must name an absolute cross-built executable")
    if receipt["exit"] != 0 or receipt["executable_sha256"] != digest(executable):
        raise ValueError("process host executable differs from its successful build receipt")
    sources = receipt["source"]
    entry = "rust/crates/native-runtime/src/bin/acyclic-process-host.rs"
    if receipt["target"] != "x86_64-pc-windows-msvc" or entry not in sources:
        raise ValueError("process host needs the shared native-runtime MSVC build receipt")
    source = Path(source).resolve()
    for name, expected in sources.items():
        path = source / name
        if not path.resolve().is_relative_to(source) or path.is_symlink() or digest(path) != expected:
            raise ValueError("process host needs a fresh bb2 build for this source: " + name)
    return executable.resolve()


class Cancellation:
    """Caller-owned cancellation, independent of command output readiness.

    Subscribers retain only their own invocation's control pipe. Cancellation
    requests the existing native host's drain; it never kills or reaps a child.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._requested = False
        self._callbacks = set()

    @property
    def requested(self):
        with self._lock:
            return self._requested

    def cancel(self):
        with self._lock:
            self._requested = True
            callbacks = tuple(self._callbacks)
        for callback in callbacks:
            callback()

    def subscribe(self, callback):
        with self._lock:
            self._callbacks.add(callback)
            requested = self._requested
        if requested:
            callback()

        def unsubscribe():
            with self._lock:
                self._callbacks.discard(callback)
        return unsubscribe


class _ControlInput:
    """One idempotent EOF notification to the admitted host, including races."""

    def __init__(self, stream):
        self._lock = threading.Lock()
        self._stream = stream
        self.cancelled = False

    def close(self, *, cancelled=False):
        with self._lock:
            if self._stream is not None:
                self.cancelled |= cancelled
                try:
                    self._stream.close()
                except BrokenPipeError:
                    # The host has already closed its end; close still retires
                    # our descriptor. No other close failure is suppressed.
                    pass
                self._stream = None

    def cancel(self):
        self.close(cancelled=True)


class Commands:
    """Exclusive fixtures; the native command owner holds the fleet lease."""

    def __init__(self, records, source, host, binary=None, *, cancellation=None):
        self.records = Path(records).resolve()
        self.source = Path(source).resolve()
        if not self.records.is_relative_to(Path("D:/lane-scratch").resolve()):
            raise ValueError("Windows runner records must stay under D:/lane-scratch")
        self.host = Path(host).resolve()
        self.binary = None if binary is None else Path(binary).resolve()
        self.entered = False
        self.lease_root = Path("D:/lane-scratch/.acyclic-windows-command-custody")
        self.lease_wait_seconds = 0
        self.results = []
        self.selections = []
        self._selection = None
        self.cancellation = cancellation

    def __enter__(self):
        if os.name != "nt":
            raise OSError("this runtime adapter requires Windows")
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
        self.entered = True
        return self

    def __exit__(self, *_):
        # Each synchronous native invocation owns its lease through drain.
        # Python death cannot release that native owner's file lock.
        self.entered = False

    def _environment(self):
        identity = uuid.uuid4().hex
        fixture = self.records / "profiles" / identity
        fixture.mkdir(parents=True)
        bridge = self.source / "scripts/graphcoder-e2e/scratch.mjs"
        node = shutil.which("node")
        if not bridge.is_file() or node is None:
            raise RuntimeError("Windows command step=shared-profile-input: missing node or scratch.mjs; "
                               "recovery=admit Node and the source-matched shared scratch.mjs profile bridge")
        # Bootstrap has only OS launch inputs and its own temporary directory.
        # All product profile selectors and their directories come from the
        # shared factory, through this same process owner and fleet lease.
        inherited = {key: value for key, value in os.environ.items() if key.upper() in (
            "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "LANG", "LC_ALL",
            "NUMBER_OF_PROCESSORS", "PROCESSOR_ARCHITECTURE", "PROGRAMFILES", "PROGRAMFILES(X86)",
        )}
        temporary = fixture / "bootstrap"
        temporary.mkdir()
        result = self._run(
            [str(Path(node).resolve()), str(bridge), "profile", str(fixture)],
            cwd=self.source, name="shared-profile", sha256=digest(node),
            prepared_environment=(identity + "-profile", dict(inherited, TEMP=str(temporary), TMP=str(temporary))),
            profile=True,
        )
        if result["exit"]:
            raise RuntimeError("Windows command step=shared-profile-bootstrap failed: " + str(result["exit"])
                               + "; recovery=node scripts/graphcoder-e2e/scratch.mjs profile ABSOLUTE_SETTINGS")
        try:
            profile = json.loads(Path(result["log"]).read_text(encoding="utf-8"))
            if not isinstance(profile, dict) or "HOME" not in profile or not all(
                    isinstance(key, str) and isinstance(value, str) for key, value in profile.items()):
                raise ValueError("profile bridge must emit a nonempty string environment")
        except (OSError, ValueError) as error:
            raise RuntimeError("Windows command step=shared-profile-protocol: " + str(error)
                               + "; recovery=adopt the source-matched scratch.mjs profile CLI") from error
        # Merge explicitly selected non-profile test inputs only. Inherited
        # HOME/auth/provider settings never enter the bootstrap or product.
        selected = {key: value for key, value in os.environ.items()
                    if key.upper().startswith(("ACYCLIC_", "E2E_", "WIN_TEST_"))
                    and not (key.upper().endswith("API_KEY") or "AUTH_TOKEN" in key.upper()
                             or key.upper().endswith("ACCESS_TOKEN"))}
        env = dict(inherited, **selected, **profile)
        home = Path(profile["HOME"])
        env.update(
            HOMEDRIVE=home.drive, HOMEPATH=str(home)[len(home.drive):], RUST_TEST_THREADS="4",
            ACYCLIC_TEST_SOURCE_ROOT=str(self.source), GIT_CONFIG_NOSYSTEM="1",
            GIT_CONFIG_GLOBAL=str(home / ".gitconfig"), E2E_PROCESS_HOST=str(self.host),
            PYTHONDONTWRITEBYTECODE="1",
        )
        if self.binary is not None:
            env["ACYCLIC_TEST_BINARY"] = str(self.binary)
        else:
            env.pop("ACYCLIC_TEST_BINARY", None)
        return identity, env

    @contextmanager
    def _libtest_selection(self):
        """The libtest adapter owns nonvacuity across its whole selection."""
        if self._selection is not None:
            raise RuntimeError("a libtest selection already owns this runner")
        selected = []
        self._selection = selected
        try:
            yield
        except BaseException:
            raise
        else:
            result = dict(commands=len(selected), passed=sum(row["passed_tests"] for row in selected),
                          exits=[row["exit"] for row in selected],
                          listing=bool(selected) and all(row["listing"] for row in selected))
            self.selections.append(result)
            (self.records / "libtest-selections.json").write_text(
                json.dumps(self.selections, indent=2), encoding="utf-8")
            if not selected or (all(row["exit"] == 0 for row in selected)
                                and result["passed"] == 0 and not result["listing"]):
                raise RuntimeError("libtest selection reported ZERO passing tests")
        finally:
            self._selection = None

    def run(self, argv, *, cwd, name, sha256, libtest=False):
        """Run admitted argv, including shared profile preparation in timing."""
        return self._run(argv, cwd=cwd, name=name, sha256=sha256, libtest=libtest,
                         started=time.monotonic())

    def _run(self, argv, *, cwd, name, sha256, libtest=False,
             prepared_environment=None, profile=False, started=None):
        """The same native process lifecycle for profile bootstrap and commands."""
        if not self.entered:
            raise RuntimeError("Windows command step=runner-entry; recovery=use Commands as a context manager")
        wait_before = self.lease_wait_seconds
        start = time.monotonic() if started is None else started
        if self.cancellation is not None and self.cancellation.requested:
            raise RuntimeError("Windows command step=admit cancelled before launch; "
                               "recovery=create a new Cancellation for a new operation")
        argv = [str(value) for value in argv]
        executable = Path(argv[0])
        cwd = Path(cwd).resolve()
        if not executable.is_absolute() or digest(executable) != sha256:
            raise ValueError("command executable differs from its admission")
        if not cwd.is_relative_to(self.source) or not cwd.is_dir():
            raise ValueError("command cwd must be inside the admitted source")
        if libtest:
            if any(value.startswith("--test-threads") for value in argv[1:]):
                raise ValueError("the runner owns --test-threads=4")
            argv.append("--test-threads=4")
        identity, env = prepared_environment or self._environment()
        completions = self.records / "native-completions"
        completions.mkdir(exist_ok=True)
        completion_path = completions / (identity + ".json")
        summaries = []
        log_path = self.records / (identity + ".log")
        proc = None
        control = None
        unsubscribe = None
        try:
            with log_path.open("x", encoding="utf-8") as log:
                proc = subprocess.Popen(
                    [str(self.host), "--lease", str(self.lease_root),
                     "--completion", str(completion_path), *argv], cwd=cwd, env=env,
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace",
                )
                control = _ControlInput(proc.stdin)
                if self.cancellation is not None:
                    unsubscribe = self.cancellation.subscribe(control.cancel)
                for line in proc.stdout:
                    log.write(line)
                    log.flush()
                    print(line, end="", flush=True)
                    if "test result:" in line:
                        summaries.append(line.strip())
                rc = proc.wait()
        finally:
            if proc is not None:
                # EOF asks the existing host to terminate and drain its tree.
                # Keep the fleet lease while it drains, including on exceptions.
                if unsubscribe is not None:
                    unsubscribe()
                control.close()
                proc.stdin = None
                proc.communicate()
        wall_seconds = time.monotonic() - start
        cancelled = control.cancelled
        outcome = 130 if cancelled else rc
        verification_error = None
        native_completion = None
        try:
            native_completion = json.loads(completion_path.read_text(encoding="utf-8"))
            if (not isinstance(native_completion, dict)
                    or set(native_completion) != {"schema", "process_tree_retired", "exit_code", "cancelled", "lease_wait_seconds"}
                    or native_completion["schema"] != "acyclic.process-custody-completion.v2"
                    or native_completion["process_tree_retired"] is not True
                    or type(native_completion["exit_code"]) is not int
                    or native_completion["exit_code"] != rc
                    or type(native_completion["cancelled"]) is not bool
                    or type(native_completion["lease_wait_seconds"]) not in (int, float)
                    or not math.isfinite(native_completion["lease_wait_seconds"])
                    or native_completion["lease_wait_seconds"] < 0
                    or native_completion["lease_wait_seconds"] > wall_seconds):
                raise ValueError("native completion schema or exit differs")
            self.lease_wait_seconds += native_completion["lease_wait_seconds"]
        except (OSError, ValueError, TypeError, KeyError) as error:
            verification_error = ("Windows command step=native-custody-completion: " + str(error)
                                  + "; recovery=build the source-matched acyclic-process-host through scripts/windows-test.sh")
        lease_wait_seconds = self.lease_wait_seconds - wait_before
        elapsed = wall_seconds - lease_wait_seconds
        passed = sum(int(match.group(1)) for value in summaries
                     if (match := re.search(r"\b([0-9]+) passed\b", value)))
        if libtest and outcome == 0 and "--list" not in argv:
            empty = bool(summaries) and all(re.match(
                r"^test result: ok\. 0 passed; 0 failed; 0 ignored;", value) for value in summaries)
            if not summaries or (passed == 0 and (self._selection is None or not empty)):
                verification_error = verification_error or "libtest selected ZERO passing tests: " + repr(argv)
        result = dict(
            target=name, executable=str(executable), argv=argv,
            exit=outcome if outcome else (1 if verification_error else 0), process_exit=rc, cancelled=cancelled,
            seconds=round(elapsed, 6), under_10s=elapsed < 10,
            lease_wait_seconds=lease_wait_seconds, wall_seconds=round(wall_seconds, 6),
            summaries=summaries, sha256=sha256, fixture=identity,
            log=str(log_path), process_host_sha256=digest(self.host),
            verification_error=verification_error,
            native_completion=native_completion, native_completion_path=str(completion_path),
            passed_tests=passed,
            listing=libtest and "--list" in argv,
        )
        # Each completed command owns one immutable structured receipt. The
        # invocation summary below is only a view: reusing a producer/records
        # directory must never erase an earlier invocation's evidence.
        receipts = self.records / ("profile-receipts" if profile else "receipts")
        receipts.mkdir(exist_ok=True)
        if profile:
            result["profile_source_sha256"] = digest(self.source / "scripts/graphcoder-e2e/scratch.mjs")
        else:
            result["profile_receipt"] = str(self.records / "profile-receipts" / (identity + "-profile.json"))
        with (receipts / (identity + ".json")).open("x", encoding="utf-8") as receipt:
            json.dump(result, receipt, indent=2)
        if not profile:
            self.results.append(result)
            if libtest and self._selection is not None:
                self._selection.append(result)
            (self.records / "results.json").write_text(json.dumps(self.results, indent=2), encoding="utf-8")
        print("WIN_TEST_PROFILE" if profile else "WIN_TEST_RESULT", json.dumps(result), flush=True)
        if verification_error:
            raise RuntimeError(verification_error)
        return result if profile else (outcome if 0 <= outcome < 256 else 1)


def fixture_binary(records, manifest):
    """Select the single admitted normal binary independently of its target name."""
    fixtures = [row for row in manifest["artifacts"] if not row["test"]]
    if len(fixtures) > 1:
        raise RuntimeError("Ambiguous normal executable fixture")
    if not fixtures:
        return None
    binary = (Path(records) / "bin" / fixtures[0]["name"]).resolve()
    if not binary.is_file():
        raise RuntimeError("Missing admitted normal executable fixture")
    return binary


def run_tests(records, manifest, runtime, *, cancellation=None, source=None):
    """The libtest adapter over the same generic command custody API."""
    source = Path(source).resolve() if source is not None else records / "source"
    binary = fixture_binary(records, manifest)
    if "process_host" in manifest:
        host = admitted_host(source, records / "bin" / manifest["process_host"]["name"],
                             records / "host-receipt.json")
    else:
        host = admitted_host(source, os.environ["E2E_PROCESS_HOST"], os.environ["WIN_TEST_PROCESS_HOST_RECEIPT"])
    with Commands(records, source, host, binary, cancellation=cancellation) as commands:
        with commands._libtest_selection():
            for row in manifest["artifacts"]:
                if row["test"]:
                    rc = commands.run(
                        [str((records / "bin" / row["name"]).resolve()), *runtime],
                        cwd=source / manifest["crate_dir"], name=row["target"],
                        sha256=row["sha256"], libtest=True,
                    )
                    if rc:
                        return rc
    if not commands.results:
        raise RuntimeError("no admitted test executables")
    print("WIN_TEST_COMPLETE", len(commands.results), "suites;", records / "results.json", flush=True)
    return 0


def main():
    """Run a JSON command plan through this same API (including Node drivers)."""
    if len(sys.argv) != 2:
        raise ValueError("usage: windows-runner.py COMMAND_PLAN.json")
    plan = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    source = Path(plan["source"]).resolve()
    records = Path(plan["records"]).resolve()
    rows = plan["commands"]
    if not isinstance(rows, list) or not rows:
        raise ValueError("command plan must contain at least one command")
    host = admitted_host(source, os.environ["E2E_PROCESS_HOST"], os.environ["WIN_TEST_PROCESS_HOST_RECEIPT"])
    records.mkdir(parents=True)
    with Commands(records, source, host, plan.get("binary")) as commands:
        for row in rows:
            rc = commands.run(
                row["argv"], cwd=row["cwd"], name=row["name"],
                sha256=row["sha256"], libtest=row.get("libtest", False),
            )
            if rc:
                return rc
    return 0


if __name__ == "__main__":
    sys.exit(main())
