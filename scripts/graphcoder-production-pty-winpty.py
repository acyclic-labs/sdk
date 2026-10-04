"""Drive the installed GraphCoder entrypoint through a real Windows PTY.

The Node conhost --headless fallback exposes redirected streams to its child,
so Node's readline layer correctly reports isTTY=false and emits no prompt.
Python winpty creates the actual PTY boundary required by the generic terminal
adapter. The bridge remains explicitly configured through the environment.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import sys
import threading
import time
from pathlib import Path

try:
    from winpty import PtyProcess
except ImportError as error:  # pragma: no cover - platform dependency
    print(f"graphcoder-production-pty-winpty: winpty is unavailable: {error}", file=sys.stderr)
    raise SystemExit(2)


PROMPT = "graphcoder>"
PROMPT_TIMEOUT_SECONDS = 10.0
REQUIRED_MARKERS = (
    '"selectedSession"',
    '"activity"',
    '"messages"',
    '"approvals"',
    '"generation"',
    '"unifiedDiff"',
    '"mediaType"',
    '"applied":true',
    '"state":"cancelled"',
    '"exited":true',
)
EXPLICIT_ENVIRONMENT = {
    "GRAPHCODER_PACKAGE_ROOT",
    "GRAPHCODER_PACKAGE_ARTIFACT",
    "GRAPHCODER_BRIDGE_EXECUTABLE",
    "GRAPHCODER_BRIDGE_ARGS_JSON",
    "GRAPHCODER_BRIDGE_ENV_JSON",
    "GRAPHCODER_BRIDGE_CWD",
    "GRAPHCODER_IDENTITY_PATH",
    "GRAPHCODER_REQUIRE_PACKAGE_IDENTITY",
    "GRAPHCODER_LAZY_OBSERVATION_PATH",
    "GRAPHCODER_REQUIRE_LAZY_COUNTERS",
}
ANSI = re.compile(r"(?:\x1b\][^\x07]*(?:\x07|\x1b\\)|\x1b\[[0-9;?]*[ -/]*[@-~])")


def fail(message: str) -> None:
    raise RuntimeError(f"graphcoder-production-pty-winpty: {message}")


def clean(value: str) -> str:
    return ANSI.sub("", value)


def prompt_count(value: str) -> int:
    return clean(value).count("graphcoder>")


def context(transcript: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw_line in clean(transcript).splitlines():
        start = raw_line.find("{")
        if start < 0:
            continue
        try:
            value = json.loads(raw_line[start:])
        except json.JSONDecodeError:
            continue
        payload = value.get("value") if isinstance(value, dict) else None
        if not isinstance(payload, (dict, list)):
            continue
        if isinstance(payload, dict):
            selected = payload.get("selectedSession")
            if isinstance(selected, dict):
                selected_id = selected.get("id")
                if isinstance(selected_id, str):
                    result["session_id"] = selected_id
            for field in ("generation", "changesGeneration"):
                item = payload.get(field)
                if isinstance(item, (str, int)):
                    result["workspace_generation"] = str(item)
            writeback = payload.get("writeback")
            if isinstance(writeback, dict) and isinstance(writeback.get("operationId"), str):
                result["writeback_operation_id"] = writeback["operationId"]
            approvals = payload.get("approvals")
        else:
            approvals = payload
        if isinstance(approvals, list):
            for approval in approvals:
                if not isinstance(approval, dict) or approval.get("state") != "pending":
                    continue
                if isinstance(approval.get("id"), str):
                    result["approval_id"] = approval["id"]
                if isinstance(approval.get("operationId"), str):
                    result["writeback_operation_id"] = approval["operationId"]
    return result


def expand(command: str, transcript: str) -> str:
    values = context(transcript)

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in values:
            fail(f"unresolved command substitution: {{{{{name}}}}}")
        return values[name]

    return re.sub(r"\{\{([a-z_]+)\}\}", replace, command)


def terminal_records(transcript: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for raw_line in clean(transcript).splitlines():
        start = raw_line.find("{")
        if start < 0:
            continue
        try:
            value = json.loads(raw_line[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and isinstance(value.get("ok"), bool):
            records.append(value)
    return records


def json_record_count(transcript: str) -> int:
    """Count terminal result frames before PTY line wrapping is parsed.

    Long activity and message pages wrap at the PTY width and acquire cursor
    movement bytes, so they are not guaranteed to remain one JSON line. Every
    terminal result still begins with this stable framed prefix.
    """
    return clean(transcript).count('{"ok":')


def assert_typed_records(transcript: str) -> None:
    records = terminal_records(transcript)
    values = [record.get("value") for record in records if record.get("ok") is True]
    if not any(isinstance(value, dict) and isinstance(value.get("selectedSession"), dict) for value in values):
        fail("PTY transcript omitted typed selected-session projection")
    if not any(isinstance(value, list) for value in values):
        fail("PTY transcript omitted typed page projection")
    if not any(isinstance(value, dict) and (value.get("changeBody", {}).get("unifiedDiff") if isinstance(value.get("changeBody"), dict) else value.get("unifiedDiff")) for value in values):
        fail("PTY transcript omitted typed diff projection")
    if not any(isinstance(value, dict) and (value.get("fileBody", {}).get("mediaType") if isinstance(value.get("fileBody"), dict) else value.get("mediaType")) for value in values):
        fail("PTY transcript omitted typed file projection")
    if not any(isinstance(value, dict) and ((value.get("writeback", {}).get("applied") if isinstance(value.get("writeback"), dict) else value.get("applied")) is True) for value in values):
        fail("PTY transcript omitted applied writeback receipt")
    if not any(isinstance(value, dict) and ((value.get("selectedSession", {}).get("state") if isinstance(value.get("selectedSession"), dict) else value.get("state")) == "cancelled") for value in values):
        fail("PTY transcript omitted cancelled session state")
    if not any(record.get("exited") is True for record in records):
        fail("PTY transcript omitted typed exit record")


def explicit_environment() -> dict[str, str]:
    platform = {key: os.environ[key] for key in ("PATH", "SystemRoot", "WINDIR", "COMSPEC", "PATHEXT") if os.environ.get(key)}
    return {**platform, **{key: os.environ[key] for key in EXPLICIT_ENVIRONMENT if os.environ.get(key)}}


def verify_package_identity() -> None:
    """Verify that the PTY process consumed the requested installed exports."""
    if os.environ.get("GRAPHCODER_REQUIRE_PACKAGE_IDENTITY") != "1":
        return
    identity_name = os.environ.get("GRAPHCODER_IDENTITY_PATH")
    if not identity_name:
        fail("package identity is required but GRAPHCODER_IDENTITY_PATH is missing")
    identity_path = Path(identity_name)
    if not identity_path.is_file() or identity_path.is_symlink():
        fail(f"package identity was not durably recorded: {identity_path}")
    try:
        identity = json.loads(identity_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"package identity is unreadable: {error}")
    package_root = Path(os.environ.get("GRAPHCODER_PACKAGE_ROOT", "")).resolve()
    if identity.get("root") != str(package_root):
        fail("package identity root does not match GRAPHCODER_PACKAGE_ROOT")
    artifact = identity.get("artifact")
    archive_name = os.environ.get("GRAPHCODER_PACKAGE_ARTIFACT")
    if not isinstance(artifact, dict) or not archive_name:
        fail("package identity omitted the installed archive")
    archive = Path(archive_name)
    if not archive.is_file() or archive.is_symlink():
        fail(f"installed package archive is not a regular file: {archive}")
    observed = hashlib.sha256(archive.read_bytes()).hexdigest()
    if artifact.get("sha256") != observed:
        fail("package identity archive digest does not match the consumed archive")
    exports = identity.get("exports")
    required_exports = {
        "@acyclic-labs/graphcoder/bridge",
        "@acyclic-labs/graphcoder/node",
        "@acyclic-labs/graphcoder/terminal",
    }
    if not isinstance(exports, dict) or not required_exports.issubset(exports):
        fail("installed package identity omitted a required public export")
    for name in required_exports:
        export_path = Path(exports[name])
        if not export_path.is_file() or export_path.is_symlink():
            fail(f"installed package export is not a regular file: {name}")


def write_lifecycle(path_name: str | None, lifecycle: dict[str, object]) -> None:
    if not path_name:
        return
    path = Path(path_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"{json.dumps(lifecycle, indent=2, sort_keys=True)}\n", encoding="utf-8")


def wait_for_prompt(process: PtyProcess, output: list[str], previous: int) -> None:
    deadline = time.monotonic() + PROMPT_TIMEOUT_SECONDS
    while prompt_count("".join(output)) <= previous:
        if not process.isalive():
            fail("PTY process closed before the next framed prompt")
        if time.monotonic() >= deadline:
            fail("CLI did not present the next framed prompt")
        time.sleep(0.05)


def wait_for_record(process: PtyProcess, output: list[str], previous: int) -> None:
    """Wait for the command's JSON result, since readline redraws prompts eagerly.

    GraphCoder intentionally prints a fresh prompt while an interactive command
    is still awaiting its owner. Prompt count therefore cannot establish that a
    command completed; the typed terminal record is the completion boundary.
    """
    deadline = time.monotonic() + PROMPT_TIMEOUT_SECONDS
    while json_record_count("".join(output)) <= previous:
        if not process.isalive():
            fail("PTY process closed before the command result")
        if time.monotonic() >= deadline:
            fail("CLI did not emit the command result")
        time.sleep(0.05)


def wait_for_exit_record(output: list[str]) -> None:
    """Allow the reader to drain the final typed quit receipt after EOF."""
    deadline = time.monotonic() + 2.0
    while '"exited":true' not in clean("".join(output)):
        if time.monotonic() >= deadline:
            fail("PTY transcript omitted the typed exit record after process exit")
        time.sleep(0.05)


def run(commands: list[str], smoke: bool = False) -> int:
    if os.name != "nt":
        fail("Windows PTY qualification requires a Windows host")
    if not commands:
        fail("at least one terminal command is required")
    sdk_root = Path(__file__).resolve().parents[1]
    node = os.environ.get("GRAPHCODER_NODE", sys.executable.replace("python.exe", "node.exe"))
    entrypoint = sdk_root / "scripts" / "graphcoder-production-entrypoint.mjs"
    process = PtyProcess.spawn([node, str(entrypoint)], cwd=str(sdk_root), env=explicit_environment())
    # winpty's reader socket is blocking by default. A bounded timeout lets
    # the owned reader observe the stop event after close instead of leaving a
    # daemon thread behind on an otherwise successful run.
    process.fileobj.settimeout(0.2)
    lifecycle: dict[str, object] = {
        "pid": getattr(process, "pid", None),
        "smoke": smoke,
        "command_count": len(commands),
        "quit_sent": False,
        "natural_exit_observed": False,
        "termination_requested": False,
        "forced_kill": False,
        "alive_after_cleanup": None,
        "pty_closed": False,
        "close_error": None,
        "reader_alive_after_join": None,
    }
    output: list[str] = []
    stop = threading.Event()

    def drain() -> None:
        while not stop.is_set():
            try:
                output.append(process.read(4096))
            except (EOFError, socket.timeout):
                if stop.is_set():
                    return
                continue

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    try:
        wait_for_prompt(process, output, 0)
        for raw_command in commands:
            previous = json_record_count("".join(output))
            process.write(expand(raw_command, "".join(output)) + "\r")
            wait_for_record(process, output, previous)
        process.write("quit\r")
        lifecycle["quit_sent"] = True
        deadline = time.monotonic() + PROMPT_TIMEOUT_SECONDS
        while process.isalive() and time.monotonic() < deadline:
            time.sleep(0.05)
        if process.isalive():
            fail("PTY process did not exit after quit")
        lifecycle["natural_exit_observed"] = True
        wait_for_exit_record(output)
        transcript = "".join(output)
        records = terminal_records(transcript)
        if smoke:
            # The stage runtime deliberately exercises the durable start/read
            # path without a pending approval or root writeback. This mode
            # proves the installed package crossed a real PTY boundary and
            # completed every supplied command; strict marker qualification
            # remains the default for a fixture that supports the full flow.
            required = (PROMPT, '"selectedSession"', '"activity"', '"messages"', '"approvals"', '"changes"', '"mediaType"')
            missing = [marker for marker in required if marker not in transcript]
            if missing:
                fail(f"missing smoke transcript markers: {', '.join(missing)}")
            frame_count = json_record_count(transcript)
            if frame_count < len(commands) + 1:
                fail(f"PTY transcript contained {frame_count} terminal frames for {len(commands)} commands plus quit")
            if '"exited":true' not in clean(transcript):
                fail("PTY transcript omitted typed exit record")
        else:
            missing = [marker for marker in (PROMPT, *REQUIRED_MARKERS) if marker not in transcript]
            if missing:
                fail(f"missing transcript markers: {', '.join(missing)}")
            assert_typed_records(transcript)
        verify_package_identity()
        sys.stdout.write(transcript)
        return 0
    finally:
        stop.set()
        if process.isalive():
            lifecycle["termination_requested"] = True
            try:
                process.terminate()
            except (OSError, PermissionError):
                # winpty can report an already-closing child as inaccessible.
                # Try the stronger cleanup operation while preserving the
                # original qualification failure for the caller.
                try:
                    process.kill()
                    lifecycle["forced_kill"] = True
                except (OSError, PermissionError):
                    pass
        deadline = time.monotonic() + 2.0
        while process.isalive() and time.monotonic() < deadline:
            time.sleep(0.05)
        lifecycle["alive_after_cleanup"] = process.isalive()
        try:
            # Closing winpty's socket is required to release the reader thread
            # after the child has exited; isalive() alone does not unblock recv.
            process.close()
            lifecycle["pty_closed"] = True
        except Exception as error:  # pragma: no cover - platform failure
            lifecycle["close_error"] = str(error)
        reader.join(timeout=2)
        lifecycle["reader_alive_after_join"] = reader.is_alive()
        transcript_path = os.environ.get("GRAPHCODER_PTY_TRANSCRIPT_PATH")
        if transcript_path:
            Path(transcript_path).write_text("".join(output), encoding="utf-8")
        write_lifecycle(os.environ.get("GRAPHCODER_PTY_LIFECYCLE_PATH"), lifecycle)
        if lifecycle["alive_after_cleanup"]:
            fail("PTY child remained alive after bounded cleanup")
        if lifecycle["close_error"] is not None:
            fail(f"PTY socket close failed: {lifecycle['close_error']}")
        if lifecycle["reader_alive_after_join"]:
            fail("PTY reader thread remained alive after socket close")


if __name__ == "__main__":
    try:
        arguments = sys.argv[1:]
        smoke = arguments[:1] == ["--smoke"]
        raise SystemExit(run(arguments[1:] if smoke else arguments, smoke=smoke))
    except Exception as error:  # pragma: no cover - process boundary
        print(str(error), file=sys.stderr)
        raise SystemExit(1)
