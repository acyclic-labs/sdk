#!/usr/bin/env python3
"""Deterministically verify the production control and authenticated workspace CLI."""

from __future__ import annotations

import argparse, hashlib, json, os, platform, re, shutil, subprocess, tempfile, time
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
CONTROL_MANIFEST = REPO / "plugins" / "acyclic-agent-workspaces" / "control" / "Cargo.toml"
EXPECTED_TOOLS = [
    {"name":"agent_changes","description":"Inspect an agent workspace without publishing it. The root may inspect any descendant; a subagent may inspect itself or its descendants. Returns bounded change counts and generation identity; path is an optional presentation hint.","inputSchema":{"type":"object","properties":{"agent":{"type":"string","description":"Agent identifier to inspect."},"path":{"type":"string","description":"Optional path hint for the inspection UI; it does not change authorization or publication."}},"required":["agent"],"additionalProperties":False}},
    {"name":"agent_merge","description":"Publish the current workspace of one direct child into the caller's workspace. Only that child's direct parent is authorized. The child remains available, so call again to publish later incremental changes. Descendants must first be merged into their own direct parent. Reports applied, no-changes, stale, fenced, or typed-conflict outcomes without bypassing Acyclic filesystem join semantics.","inputSchema":{"type":"object","properties":{"agent":{"type":"string","description":"Direct child agent to publish."}},"required":["agent"],"additionalProperties":False}},
    {"name":"agent_discard","description":"Permanently discard one direct child's unpublished workspace and recursively discard all of its unpublished descendants. Only the direct parent is authorized; inspect or merge desired work first.","inputSchema":{"type":"object","properties":{"agent":{"type":"string","description":"Direct child agent whose subtree should be discarded."}},"required":["agent"],"additionalProperties":False}},
]
CONTEXT_SUFFIX = "Tool paths are redirected automatically; do not access the parent checkout by a hard-coded path. For optional local Git-shaped operations use the authenticated workspace CLI, for example: `acyclic git status`, `acyclic git diff --cached`, `acyclic git commit -m \"msg\"`, or `acyclic git switch -c branch`. This is an ergonomic facade over this Acyclic workspace, not system Git; transport and object-database commands are unsupported. Ordinary `git` remains system Git. Your parent can inspect your unpublished changes with agent_changes, publish repeated incremental updates with agent_merge, or recursively discard your workspace and every unpublished descendant with agent_discard. Merge and discard are authorized only for the direct parent, so publish descendants into you before asking your parent to publish you."


def digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def built(name: str, root: Path) -> Path:
    configured = os.environ.get("CARGO_TARGET_DIR")
    target = Path(configured) if configured else root / "target"
    if not target.is_absolute():
        target = REPO / target
    return target / "release" / f"{name}{'.exe' if os.name == 'nt' else ''}"


class Rpc:
    def __init__(self, executable: Path, data: Path, plugin: Path) -> None:
        env = os.environ.copy()
        env.update({"PLUGIN_DATA": str(data), "PLUGIN_ROOT": str(plugin), "ACYCLIC_AGENT_WORKSPACE_PROCESS_SANDBOX": "1"})
        self.process = subprocess.Popen([str(executable)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True, encoding="utf-8", env=env)
        self.next_id = 1

    def call(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        assert self.process.stdin and self.process.stdout
        request = {"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params}
        self.next_id += 1
        self.process.stdin.write(json.dumps(request, separators=(",", ":")) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError("control exited before responding")
        return json.loads(line)

    def hook(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return self.call("tools/call", {"name": name, "arguments": arguments})

    def kill(self) -> tuple[int, str]:
        self.process.kill()
        _, stderr = self.process.communicate(timeout=10)
        return self.process.returncode, stderr


def hook_value(response: dict[str, Any]) -> dict[str, Any]:
    content = response.get("result", {}).get("content", [])
    text = content[0].get("text") if content and isinstance(content[0], dict) else None
    if not isinstance(text, str):
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return {"_error": text}


def scoped_context(command: str) -> dict[str, str]:
    values = {}
    for name in ("ACYCLIC_CONTEXT_VERSION", "ACYCLIC_CONTROL_ENDPOINT", "ACYCLIC_WORKSPACE_TOKEN"):
        match = re.search(rf"(?:\$env:)?{name}='([^']+)'", command)
        if match:
            values[name] = match.group(1).replace("''", "'")
    return values


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=HERE / "artifacts" / "protocol")
    parser.add_argument("--skip-rust-tests", action="store_true")
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    started, checks = time.monotonic(), []

    def check(name: str, passed: bool, evidence: object) -> None:
        checks.append({"name": name, "passed": bool(passed), "evidence": evidence})

    build_commands = [
        ["cargo", "build", "--release", "--locked", "--manifest-path", str(CONTROL_MANIFEST)],
        ["cargo", "build", "--release", "--locked", "--manifest-path", str(REPO / "Cargo.toml"), "-p", "acyclic-cli"],
    ]
    logs = []
    for index, command in enumerate(build_commands, 1):
        result = subprocess.run(command, cwd=REPO, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=1800)
        logs.append(result.stdout)
        check(f"locked-release-build:{index}", result.returncode == 0, result.returncode)
    (args.out / "cargo-build.log").write_text("\n".join(logs), encoding="utf-8")
    control, cli = built("acyclic-agent-workspaces-control", CONTROL_MANIFEST.parent), built("acyclic", REPO)
    check("release-binaries-present", control.is_file() and cli.is_file(), {"control": str(control), "cli": str(cli)})
    if not args.skip_rust_tests:
        tests = subprocess.run(["cargo", "test", "--locked", "--manifest-path", str(CONTROL_MANIFEST)], cwd=REPO,
                               text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=1800)
        (args.out / "cargo-test.log").write_text(tests.stdout, encoding="utf-8")
        check("locked-control-tests", tests.returncode == 0, tests.returncode)

    transcript: dict[str, Any] = {}
    if control.is_file() and cli.is_file():
        with tempfile.TemporaryDirectory(prefix="acyclic-protocol-") as temporary:
            root = Path(temporary)
            fixture, data, plugin = root / "fixture", root / "plugin-data", root / "plugin"
            fixture.mkdir(); (fixture / "seed.txt").write_text("protocol fixture\n", encoding="utf-8")
            (plugin / "bin").mkdir(parents=True)
            staged_cli = plugin / "bin" / cli.name; shutil.copy2(cli, staged_cli)
            first = Rpc(control, data, plugin)
            initialize = first.call("initialize", {}); tools = first.call("tools/list", {})
            session = first.hook("_hook_session_start", {"session_id":"protocol-eval","cwd":str(fixture)})
            prompt = first.hook("_hook_user_prompt", {"session_id":"protocol-eval","turn_id":"root-turn"})
            spawn = first.hook("_hook_pre_tool", {"session_id":"protocol-eval","turn_id":"root-turn","tool_use_id":"spawn-child","tool_name":"spawn_agent","tool_input":{}})
            child = first.hook("_hook_subagent_start", {"session_id":"protocol-eval","turn_id":"child-turn","agent_id":"child","agent_type":"explorer"})
            context = hook_value(child).get("hookSpecificOutput", {}).get("additionalContext", "")
            child_path = Path(context.removeprefix("Your filesystem is an isolated Acyclic workspace mounted at ").split(". ", 1)[0])
            acyclic = first.hook("_hook_pre_tool", {"session_id":"protocol-eval","turn_id":"child-turn","tool_use_id":"git-status","tool_name":"exec_command","tool_input":{"cmd":"acyclic git status","workdir":str(child_path)}})
            updated = hook_value(acyclic).get("hookSpecificOutput", {}).get("updatedInput", {})
            rewritten = updated.get("cmd", "")
            scoped = scoped_context(rewritten); cli_env = os.environ.copy(); cli_env.update(scoped)
            shell_command = ([shutil.which("pwsh") or shutil.which("powershell") or "powershell", "-NoProfile", "-NonInteractive", "-Command", rewritten]
                             if os.name == "nt" else ["bash", "-lc", rewritten])
            cli_run = subprocess.run(shell_command, cwd=Path(updated.get("workdir", child_path)), env=os.environ.copy(), text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            missing_env = {k:v for k,v in os.environ.items() if not k.startswith("ACYCLIC_")}
            missing = subprocess.run([str(staged_cli), "git", "status"], cwd=child_path, env=missing_env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            invalid_env = cli_env.copy(); invalid_env["ACYCLIC_WORKSPACE_TOKEN"] = "invalid"
            invalid = subprocess.run([str(staged_cli), "git", "status"], cwd=child_path, env=invalid_env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            first.hook("_hook_post_tool", {"session_id":"protocol-eval","turn_id":"child-turn","tool_use_id":"git-status","tool_name":"exec_command","tool_input":{"cmd":"acyclic git status"},"tool_response":{"exit_code":cli_run.returncode}})
            bare = first.hook("_hook_pre_tool", {"session_id":"protocol-eval","turn_id":"child-turn","tool_use_id":"system-git","tool_name":"exec_command","tool_input":{"cmd":"git --version","workdir":str(child_path)}})
            bare_command = hook_value(bare).get("hookSpecificOutput", {}).get("updatedInput", {}).get("cmd", "")
            bare_run = subprocess.run(bare_command, cwd=child_path, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
            first.hook("_hook_post_tool", {"session_id":"protocol-eval","turn_id":"child-turn","tool_use_id":"system-git","tool_name":"exec_command","tool_input":{"cmd":"git --version"},"tool_response":{"exit_code":bare_run.returncode}})
            stopped = first.hook("_hook_subagent_stop", {"session_id":"protocol-eval","turn_id":"child-turn","agent_id":"child"})
            discard = first.call("tools/call", {"name":"agent_discard","arguments":{"agent":"child","_caller_turn_id":"root-turn"}})
            discard_value = hook_value(discard)
            state = data / "adapter-state.json"; state_before = digest(state) if state.is_file() else None
            first_exit, first_stderr = first.kill()
            second = Rpc(control, data, plugin)
            restart_initialize = second.call("initialize", {}); restart_tools = second.call("tools/list", {})
            restart_session = second.hook("_hook_session_start", {"session_id":"protocol-eval","cwd":str(fixture)})
            state_after = digest(state) if state.is_file() else None; second_exit, second_stderr = second.kill()
            transcript = {"initialize":initialize,"tools":tools,"session":session,"prompt":prompt,"spawn":spawn,"child":child,"acyclic":{"rewritten":bool(rewritten),"scoped_variables":sorted(scoped)},"bare":{"command":bare_command},"stopped":stopped,"discard":discard,"restart_initialize":restart_initialize,"restart_tools":restart_tools,"restart_session":restart_session}
            expected_initialize = {"protocolVersion":"2025-06-18","capabilities":{"tools":{"listChanged":False}},"serverInfo":{"name":"acyclic-agent-workspaces","version":"0.1.0"}}
            check("initialize-contract", initialize.get("result") == expected_initialize, initialize)
            check("exact-public-tools", tools.get("result", {}).get("tools") == EXPECTED_TOOLS, tools.get("result", {}).get("tools"))
            check("exact-subagent-context", context == f"Your filesystem is an isolated Acyclic workspace mounted at {child_path}. {CONTEXT_SUFFIX}", context)
            check("authenticated-cli-rewrite", set(scoped) == {"ACYCLIC_CONTEXT_VERSION","ACYCLIC_CONTROL_ENDPOINT","ACYCLIC_WORKSPACE_TOKEN"} and str(staged_cli) in rewritten and "PLUGIN_DATA" not in rewritten, {"scoped_variables":sorted(scoped),"packaged_cli":str(staged_cli) in rewritten,"forbidden_secrets_absent":"PLUGIN_DATA" not in rewritten})
            check("authenticated-cli-success", cli_run.returncode == 0 and cli_run.stdout.strip().startswith("{"), {"exit_code":cli_run.returncode,"stdout":cli_run.stdout,"stderr":cli_run.stderr})
            check("missing-token-fails-closed", missing.returncode != 0 and "ACYCLIC_CONTEXT_VERSION" in missing.stderr, {"exit_code":missing.returncode,"stderr":missing.stderr})
            check("invalid-token-fails-closed", invalid.returncode != 0, {"exit_code":invalid.returncode,"stderr":invalid.stderr})
            check("bare-git-is-system-git", bare_command == "git --version" and bare_run.returncode == 0 and bare_run.stdout.startswith("git version ") and not (plugin / "bin" / ("git.exe" if os.name == "nt" else "git")).exists(), {"command":bare_command,"stdout":bare_run.stdout})
            check("discard-before-restart", discard_value.get("status") == "discarded", discard_value)
            check("restart-initialize-contract", restart_initialize.get("result") == expected_initialize, restart_initialize)
            check("restart-exact-public-tools", restart_tools.get("result", {}).get("tools") == EXPECTED_TOOLS, restart_tools)
            check("unclean-restart-reopens-state", state_before is not None and state_after is not None and not restart_session.get("result", {}).get("isError", False), {"first_exit":first_exit,"second_exit":second_exit,"state_before":state_before,"state_after":state_after,"stderr":first_stderr+second_stderr})

    (args.out / "rpc-transcript.json").write_text(json.dumps(transcript, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report = {"schema":"acyclic.agent-workspaces.protocol-report.v1","passed":all(c["passed"] for c in checks),"duration_seconds":round(time.monotonic()-started,3),"host":{"platform":platform.platform(),"python":platform.python_version()},"source":{"control_manifest":str(CONTROL_MANIFEST.relative_to(REPO)),"sha256":digest(CONTROL_MANIFEST)},"checks":checks}
    report_path = args.out / "report.json"; report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    sums = [f"{digest(p).removeprefix('sha256:')}  {p.relative_to(args.out).as_posix()}" for p in sorted(i for i in args.out.rglob("*") if i.is_file() and i.name != "SHA256SUMS")]
    (args.out / "SHA256SUMS").write_text("\n".join(sums) + "\n", encoding="utf-8")
    failed = [{"name": c["name"], "evidence": c["evidence"]} for c in checks if not c["passed"]]
    print(json.dumps({"passed":report["passed"],"report":str(report_path),"failed_checks":failed}, sort_keys=True))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
