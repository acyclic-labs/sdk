#!/usr/bin/env python3
"""Run repeated real-Codex behavioral evaluations of agent-workspaces."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from typing import Any
from urllib.parse import quote

from grade import canonical_digest, grade_attempt, sha256_file, tree_manifest

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
CONFIG_PATH = HERE / "cases.json"
PLUGIN_SOURCE = REPO / "plugins" / "acyclic-agent-workspaces"
CONTROL_MANIFEST = PLUGIN_SOURCE / "control" / "Cargo.toml"
WORKSPACE_MANIFEST = REPO / "Cargo.toml"


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run(command: list[str], *, cwd: Path, env: dict[str, str], input_text: str | None = None,
        timeout: int = 300) -> subprocess.CompletedProcess[str]:
    options: dict[str, Any] = {"start_new_session": True} if os.name != "nt" else {"creationflags": subprocess.CREATE_NEW_PROCESS_GROUP}
    process = subprocess.Popen(command, cwd=cwd, env=env, text=True, encoding="utf-8", errors="replace",
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **options)
    try:
        stdout, stderr = process.communicate(input_text, timeout=timeout)
    except subprocess.TimeoutExpired as error:
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        else:
            os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
        raise subprocess.TimeoutExpired(command, timeout, output=(error.output or "") + stdout, stderr=(error.stderr or "") + stderr)
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)


def control_binary() -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    return CONTROL_MANIFEST.parent / "target" / "release" / f"acyclic-agent-workspaces-control{suffix}"


def cli_binary() -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    return REPO / "target" / "release" / f"acyclic{suffix}"


def resolve_codex_binary(source_home: Path, explicit: Path | None) -> str:
    if explicit:
        return str(explicit.resolve())
    # On Windows, PATHEXT may make an unrelated codex.exe win over the npm
    # launcher selected by PowerShell. Prefer the versioned npm .cmd shim.
    discovered = shutil.which("codex.cmd") if os.name == "nt" else shutil.which("codex")
    discovered = discovered or shutil.which("codex")
    if not discovered:
        raise SystemExit("codex executable not found; pass --codex-bin")
    return discovered


def build_binaries() -> tuple[Path, Path]:
    commands = [
        ["cargo", "build", "--release", "--locked", "--manifest-path", str(CONTROL_MANIFEST)],
        ["cargo", "build", "--release", "--locked", "--manifest-path", str(WORKSPACE_MANIFEST), "-p", "acyclic-cli"],
    ]
    for command in commands:
        result = subprocess.run(command, cwd=REPO)
        if result.returncode:
            raise SystemExit(f"release build failed: {' '.join(command)}")
    control, cli = control_binary(), cli_binary()
    for binary in (control, cli):
        if not binary.is_file():
            raise SystemExit(f"binary missing after build: {binary}")
    return control, cli


def initialize_git_fixture(path: Path, case: dict[str, Any], sentinel: Path) -> None:
    path.mkdir(parents=True)
    (path / "README.md").write_text("# Disposable agent-workspaces evaluation fixture\n", encoding="utf-8")
    for relative, content in case.get("seed_files", {}).items():
        target = path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
    commands = [
        ["git", "init", "--quiet"],
        ["git", "add", "."],
        ["git", "-c", "user.name=Acyclic Eval", "-c", "user.email=eval@invalid", "commit", "--quiet", "-m", "seed"],
    ]
    for command in commands:
        result = subprocess.run(command, cwd=path, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode:
            raise RuntimeError(f"fixture command failed: {command}: {result.stderr}")
    sentinel.write_text(case.get("outside_sentinel", "outside remains unchanged\n"), encoding="utf-8")


def stage_marketplace(root: Path, control: Path, cli: Path) -> None:
    plugin = root / "plugins" / "acyclic-agent-workspaces"
    (plugin / ".codex-plugin").mkdir(parents=True)
    (plugin / "hooks").mkdir(parents=True)
    (plugin / "bin").mkdir(parents=True)
    shutil.copy2(PLUGIN_SOURCE / ".codex-plugin" / "plugin.json", plugin / ".codex-plugin" / "plugin.json")
    shutil.copy2(PLUGIN_SOURCE / "hooks" / "hooks.json", plugin / "hooks" / "hooks.json")
    shutil.copy2(cli, plugin / "bin" / cli.name)
    mcp = {
        "mcpServers": {
            "acyclic_agent_workspaces": {
                "command": sys.executable,
                "args": [str(HERE / "launch_control.py"), str(control)],
                "env_vars": ["PLUGIN_ROOT", "ACYCLIC_EVAL_PLUGIN_DATA", "ACYCLIC_EVAL_CRASH_AFTER_POST_TOOL", "ACYCLIC_AGENT_WORKSPACE_PROCESS_SANDBOX", "PATH"],
                "startup_timeout_sec": 180,
                "tool_timeout_sec": 3600,
                "default_tools_approval_mode": "approve"
            }
        }
    }
    write_json(plugin / ".mcp.json", mcp)
    marketplace = {
        "name": "agent-workspaces-eval",
        "plugins": [{
            "name": "acyclic-agent-workspaces",
            "source": {"source": "local", "path": "./plugins/acyclic-agent-workspaces"},
            "policy": {"installation": "AVAILABLE", "authentication": "ON_INSTALL"},
            "category": "Developer Tools"
        }]
    }
    write_json(root / ".agents" / "plugins" / "marketplace.json", marketplace)


def credential_markers(auth: Path) -> list[str]:
    document = json.loads(auth.read_text(encoding="utf-8"))
    secrets: set[str] = set()

    def visit(value: Any, key: str = "") -> None:
        if isinstance(value, dict):
            for child_key, child in value.items():
                visit(child, str(child_key).lower())
        elif isinstance(value, list):
            for child in value:
                visit(child, key)
        elif isinstance(value, str) and len(value) >= 8 and ("token" in key or "api_key" in key):
            secrets.add(value)

    visit(document)
    markers = set(secrets)
    for secret in secrets:
        markers.add(json.dumps(secret)[1:-1])
        markers.add(quote(secret, safe=""))
        markers.add(base64.b64encode(secret.encode()).decode())
    return sorted(markers, key=len, reverse=True)


def redact_credentials(text: str, markers: list[str]) -> tuple[str, bool]:
    leaked = False
    for marker in markers:
        if marker and marker in text:
            leaked = True
            text = text.replace(marker, "[REDACTED_EVAL_CREDENTIAL]")
    return text, leaked


def credential_files(roots: dict[str, Path], markers: list[str]) -> list[str]:
    encoded = [marker.encode() for marker in markers if marker]
    matches: list[str] = []
    for label, root in roots.items():
        if not root.exists():
            continue
        root_resolved = root.resolve()
        pending = [root]
        while pending:
            path = pending.pop()
            relative = path.name if root.is_file() else path.relative_to(root).as_posix()
            if path.is_symlink():
                try:
                    resolved = path.resolve(strict=True)
                    confined = resolved.is_relative_to(root_resolved)
                except (OSError, RuntimeError):
                    confined = False
                if not confined:
                    matches.append(f"{label}/{relative}")
                continue
            if path.is_dir():
                try:
                    pending.extend(path.iterdir())
                except OSError:
                    matches.append(f"{label}/{relative}")
                continue
            if not path.is_file():
                matches.append(f"{label}/{relative}")
                continue
            try:
                contents = path.read_bytes()
            except OSError:
                matches.append(f"{label}/{relative}")
                continue
            if any(marker in contents for marker in encoded):
                matches.append(f"{label}/{relative}")
    return sorted(matches)


def final_response(events: str) -> Any | None:
    for line in reversed(events.splitlines()):
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        item = event.get("item") if isinstance(event, dict) else None
        if not isinstance(item, dict) or item.get("type") != "agent_message":
            continue
        text = item.get("text")
        if not isinstance(text, str):
            continue
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return {"text": text}
    return None


def redact_json(value: Any, markers: list[str]) -> tuple[Any, bool]:
    redacted, leaked = redact_credentials(json.dumps(value), markers)
    return json.loads(redacted), leaked


def purge_credential_attempt(attempt: Path, grade: dict[str, Any]) -> None:
    shutil.rmtree(attempt)
    if attempt.exists():
        raise RuntimeError(f"credential-bearing attempt could not be purged: {attempt}")
    attempt.mkdir(parents=True, exist_ok=True)
    grade["artifacts"].update({"prompt": None, "events": None, "stderr": None, "final": None})
    write_json(attempt / "grade.json", grade)


def prepare_codex_home(codex: str, source_home: Path, destination: Path, marketplace: Path, control: Path, cli: Path,
                       expected_version: str) -> tuple[dict[str, str], dict[str, Any], list[str]]:
    auth = source_home / "auth.json"
    if not auth.is_file():
        raise SystemExit(f"Codex authentication file not found: {auth}")
    destination.mkdir(parents=True)
    markers = credential_markers(auth)
    shutil.copy2(auth, destination / "auth.json")
    stage_marketplace(marketplace, control, cli)
    allowed = {"PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "PROGRAMDATA", "PROGRAMFILES", "PROGRAMFILES(X86)", "PROCESSOR_ARCHITECTURE", "NUMBER_OF_PROCESSORS", "LANG", "LC_ALL"}
    env = {key: value for key, value in os.environ.items() if key.upper() in allowed}
    for key, relative in {"HOME":"home", "USERPROFILE":"home", "APPDATA":"appdata/roaming", "LOCALAPPDATA":"appdata/local"}.items():
        isolated = destination / relative
        isolated.mkdir(parents=True, exist_ok=True)
        env[key] = str(isolated)
    env["CODEX_HOME"] = str(destination)
    env["ACYCLIC_AGENT_WORKSPACE_PROCESS_SANDBOX"] = "1"
    version = run([codex, "--version"], cwd=REPO, env=env)
    actual_version = version.stdout.strip()
    if actual_version != expected_version:
        raise SystemExit(f"expected {expected_version!r}, got {actual_version!r}")
    add_market = run([codex, "plugin", "marketplace", "add", str(marketplace), "--json"], cwd=REPO, env=env)
    if add_market.returncode:
        raise SystemExit(f"cannot add disposable marketplace: {add_market.stderr or add_market.stdout}")
    add_plugin = run([codex, "plugin", "add", "acyclic-agent-workspaces@agent-workspaces-eval", "--json"], cwd=REPO, env=env)
    if add_plugin.returncode:
        raise SystemExit(f"cannot install disposable plugin: {add_plugin.stderr or add_plugin.stdout}")
    return env, {"binary": codex, "version": actual_version, "marketplace": json.loads(add_market.stdout), "plugin": json.loads(add_plugin.stdout)}, markers


def render_prompt(case: dict[str, Any], sentinel: Path) -> str:
    prompt = (HERE / case["prompt"]).read_text(encoding="utf-8")
    prompt = prompt.replace("{{OUTSIDE_SENTINEL}}", str(sentinel.resolve()))
    return (
        f"Behavioral evaluation case `{case['id']}`. Follow the task literally. Use native subagents and the "
        "Acyclic agent-workspaces MCP tools; do not imitate them with ordinary root filesystem edits. "
        "Finish with JSON conforming to the supplied output schema.\n\n" + prompt
    )


def run_attempt(codex: str, case: dict[str, Any], index: int, out: Path, env_base: dict[str, str], config: dict[str, Any],
                keep: str, timeout: int, markers: list[str]) -> dict[str, Any]:
    attempt = out / case["id"] / f"run-{index:02d}"
    fixture = attempt / "fixture"
    plugin_data = attempt / "plugin-data"
    sentinel = attempt / "outside-sentinel.txt"
    attempt.mkdir(parents=True, exist_ok=True)
    initialize_git_fixture(fixture, case, sentinel)
    before = tree_manifest(fixture)
    write_json(attempt / "fixture-before.json", before)
    git_before = canonical_digest(tree_manifest(fixture / ".git", include_git=True))
    prompt = render_prompt(case, sentinel)
    (attempt / "prompt.md").write_text(prompt, encoding="utf-8")
    events_path = attempt / "events.jsonl"
    final_path = attempt / "final.json"
    stderr_path = attempt / "stderr.log"
    env = env_base.copy()
    env["ACYCLIC_EVAL_PLUGIN_DATA"] = str(plugin_data)
    env["ACYCLIC_EVAL_CRASH_AFTER_POST_TOOL"] = "1" if case.get("require_crash_marker") else "0"
    command = [
        codex, "exec", "--json", "--ignore-rules",
        "-s", "workspace-write", "-c", 'approval_policy="never"', "--dangerously-bypass-hook-trust",
        "-C", str(fixture), "-m", config["model"],
        "-c", f'model_reasoning_effort="{config["reasoning_effort"]}"',
        "-c", 'service_tier="default"', "--output-schema", str(HERE / "final.schema.json"),
        "-"
    ]
    started = time.monotonic()
    timed_out = False
    try:
        result = run(command, cwd=fixture, env=env, input_text=prompt, timeout=timeout)
        exit_code = result.returncode
        stdout, stdout_leak = redact_credentials(result.stdout, markers)
        stderr, stderr_leak = redact_credentials(result.stderr, markers)
        events_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")
    except subprocess.TimeoutExpired as error:
        timed_out = True
        exit_code = 124
        stdout = error.stdout or ""
        stderr = error.stderr or ""
        raw_stdout = stdout if isinstance(stdout, str) else stdout.decode(errors="replace")
        raw_stderr = stderr if isinstance(stderr, str) else stderr.decode(errors="replace")
        stdout, stdout_leak = redact_credentials(raw_stdout, markers)
        stderr, stderr_leak = redact_credentials(raw_stderr, markers)
        events_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")
    final = final_response(stdout)
    if final is not None:
        write_json(final_path, final)
    grade = grade_attempt(case, fixture, plugin_data, events_path, sentinel, git_before)
    grade["codex"] = {"exit_code": exit_code, "timed_out": timed_out, "duration_seconds": round(time.monotonic() - started, 3)}
    grade["assertions"].append({"name": "codex-exit", "passed": exit_code == 0, "evidence": exit_code})
    leaked_streams = [name for name, leaked in (("events", stdout_leak), ("stderr", stderr_leak)) if leaked]
    grade["assertions"].append({"name": "credential-not-emitted", "passed": not leaked_streams, "evidence": leaked_streams})
    leaked_files = credential_files({"attempt": attempt}, markers)
    grade["assertions"].append({"name": "credential-not-retained", "passed": not leaked_files, "evidence": leaked_files})
    grade["passed"] = all(item["passed"] for item in grade["assertions"])
    after = tree_manifest(fixture)
    write_json(attempt / "fixture-after.json", after)
    grade["artifacts"] = {
        "prompt": sha256_file(attempt / "prompt.md"),
        "events": sha256_file(events_path),
        "stderr": sha256_file(stderr_path),
        "final": sha256_file(final_path) if final_path.is_file() else None,
        "fixture_before": canonical_digest(before),
        "fixture_after": canonical_digest(after),
    }
    retain = (keep == "all" or (keep == "failures" and not grade["passed"])) and not leaked_files
    grade["retained_state"] = retain
    grade["credential_purge"] = bool(leaked_files)
    grade, _ = redact_json(grade, markers)
    if leaked_files:
        purge_credential_attempt(attempt, grade)
    else:
        write_json(attempt / "grade.json", grade)
    if not retain and not leaked_files:
        shutil.rmtree(fixture, ignore_errors=True)
        shutil.rmtree(plugin_data, ignore_errors=True)
        sentinel.unlink(missing_ok=True)
    return grade


def write_sums(root: Path) -> None:
    lines = []
    for path in sorted(item for item in root.rglob("*") if item.is_file() and item.name != "SHA256SUMS"):
        lines.append(f"{sha256_file(path).removeprefix('sha256:')}  {path.relative_to(root).as_posix()}")
    (root / "SHA256SUMS").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", dest="cases", help="case id; repeatable")
    parser.add_argument("--runs", type=int, default=None)
    parser.add_argument("--model")
    parser.add_argument("--reasoning-effort")
    parser.add_argument("--source-codex-home", type=Path, required=True, help="dedicated evaluation Codex home containing auth.json")
    parser.add_argument("--acknowledge-readable-eval-credential", action="store_true", help="confirm execution is inside a dedicated OS/container account with a short-lived credential")
    parser.add_argument("--codex-bin", type=Path, help="exact Codex CLI binary; defaults to CODEX_CLI_PATH from source config")
    parser.add_argument("--out", type=Path, default=HERE / "artifacts" / "behavior")
    parser.add_argument("--keep", choices=("none", "failures", "all"), default="failures")
    parser.add_argument("--timeout", type=int, default=1800)
    args = parser.parse_args()
    if not args.acknowledge_readable_eval_credential:
        raise SystemExit("refusing to copy authentication without --acknowledge-readable-eval-credential; use a dedicated OS/container account and short-lived evaluation credential")
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    selected = [case for case in config["cases"] if not args.cases or case["id"] in args.cases]
    unknown = set(args.cases or []) - {case["id"] for case in config["cases"]}
    if unknown:
        raise SystemExit(f"unknown cases: {', '.join(sorted(unknown))}")
    runs = args.runs if args.runs is not None else config["minimum_runs"]
    if runs < 1:
        raise SystemExit("--runs must be positive")
    if args.model:
        config["model"] = args.model
    if args.reasoning_effort:
        config["reasoning_effort"] = args.reasoning_effort
    args.out = args.out.resolve()
    args.out.mkdir(parents=True, exist_ok=True)
    control, cli = build_binaries()
    codex = resolve_codex_binary(args.source_codex_home.resolve(), args.codex_bin)
    started = time.monotonic()
    results = []
    with tempfile.TemporaryDirectory(prefix="acyclic-codex-home-") as home_tmp:
        bootstrap = Path(home_tmp) / "marketplace"
        env, setup, markers = prepare_codex_home(codex, args.source_codex_home.resolve(), Path(home_tmp) / "home", bootstrap, control, cli, config["codex_version"])
        for case in selected:
            for index in range(1, runs + 1):
                result = run_attempt(codex, case, index, args.out, env, config, args.keep, args.timeout, markers)
                results.append(result)
                print(json.dumps({"case": case["id"], "run": index, "passed": result["passed"]}), flush=True)

    case_results = []
    for case in selected:
        attempts = [item for item in results if item["case"] == case["id"]]
        passed = sum(bool(item["passed"]) for item in attempts)
        rate = passed / len(attempts)
        case_results.append({"case": case["id"], "passed_attempts": passed, "attempts": len(attempts), "pass_rate": rate, "threshold": config["case_pass_rate"], "passed": rate >= config["case_pass_rate"]})
    passed_total = sum(bool(item["passed"]) for item in results)
    overall_rate = passed_total / len(results) if results else 0.0
    enough = runs >= config["minimum_runs"]
    suite_passed = enough and all(item["passed"] for item in case_results) and overall_rate >= config["suite_pass_rate"]
    report = {
        "schema": "acyclic.agent-workspaces.behavior-report.v1",
        "status": "passed" if suite_passed else ("insufficient-samples" if not enough else "failed"),
        "passed": suite_passed,
        "configuration": {
            "codex_version": config["codex_version"], "model": config["model"],
            "reasoning_effort": config["reasoning_effort"], "runs_per_case": runs,
            "minimum_runs": config["minimum_runs"], "case_pass_rate": config["case_pass_rate"],
            "suite_pass_rate": config["suite_pass_rate"], "retention": args.keep,
            "sampling_note": "Repeated model runs are reliability samples, not bit-for-bit deterministic replays."
        },
        "host": {"platform": platform.platform(), "python": platform.python_version()},
        "setup": setup,
        "source": {"config": sha256_file(CONFIG_PATH), "control_binary": sha256_file(control), "cli_binary": sha256_file(cli)},
        "duration_seconds": round(time.monotonic() - started, 3),
        "overall": {"passed_attempts": passed_total, "attempts": len(results), "pass_rate": overall_rate},
        "cases": case_results,
        "attempts": results,
    }
    write_json(args.out / "report.json", report)
    write_sums(args.out)
    print(json.dumps({"status": report["status"], "report": str(args.out / "report.json")}, sort_keys=True))
    return 0 if suite_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
