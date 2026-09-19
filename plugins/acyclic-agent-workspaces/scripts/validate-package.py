#!/usr/bin/env python3
"""Validate a staged plugin using only the files in that artifact."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile


def rpc_message(payload: dict[str, object]) -> bytes:
    return (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")


def managed_child_smoke(executable: Path, server: dict[str, object], plugin_root: Path, checkout: Path, plugin_data: Path) -> None:
    environment = os.environ.copy()
    environment.update({"PLUGIN_ROOT": str(plugin_root), "PLUGIN_DATA": str(plugin_data)})
    process = subprocess.Popen(
        [str(executable), *server.get("args", [])],
        cwd=checkout,
        env=environment,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.stdin is None or process.stdout is None:
        raise RuntimeError("packaged control process lacks RPC pipes")
    sequence = 0

    def request(method: str, params: dict[str, object]) -> dict[str, object]:
        nonlocal sequence
        sequence += 1
        process.stdin.write(
            rpc_message({"jsonrpc": "2.0", "id": sequence, "method": method, "params": params})
        )
        process.stdin.flush()
        line = process.stdout.readline()
        if not line:
            exit_code = process.poll()
            stderr = ""
            if exit_code is not None and process.stderr is not None:
                stderr = process.stderr.read().decode("utf-8", errors="replace")[-2000:]
            raise RuntimeError(
                f"packaged control process closed before its RPC response (exit {exit_code}): {stderr}"
            )
        response = json.loads(line)
        if response.get("id") != sequence:
            raise RuntimeError(f"unexpected RPC response identity: {response}")
        return response

    def tool(name: str, arguments: dict[str, object]) -> dict[str, object]:
        response = request("tools/call", {"name": name, "arguments": arguments})
        result = response["result"]
        if result.get("isError"):
            raise RuntimeError(result["content"][0]["text"])
        return json.loads(result["content"][0]["text"])

    request(
        "initialize",
        {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "package-validator", "version": "1"},
        },
    )
    tools = request("tools/list", {})
    if "tools" not in tools.get("result", {}):
        raise RuntimeError("packaged control tools/list omitted tools")
    tool("_hook_session_start", {"session_id": "package-validator", "cwd": str(checkout)})
    tool("_hook_user_prompt", {"session_id": "package-validator", "turn_id": "root-turn"})
    tool(
        "_hook_pre_tool",
        {
            "session_id": "package-validator",
            "turn_id": "root-turn",
            "tool_use_id": "spawn",
            "tool_name": "spawn_agent",
            "tool_input": {},
        },
    )
    tool(
        "_hook_subagent_start",
        {
            "session_id": "package-validator",
            "turn_id": "child-turn",
            "agent_id": "child",
            "agent_type": "explorer",
        },
    )
    pre = tool(
        "_hook_pre_tool",
        {
            "session_id": "package-validator",
            "turn_id": "child-turn",
            "tool_use_id": "status",
            "tool_name": "exec_command",
            "tool_input": {"cmd": "acyclic git status", "workdir": str(checkout)},
        },
    )
    updated = pre["hookSpecificOutput"]["updatedInput"]
    scoped_command = updated["cmd"]
    endpoint_match = re.search(r"ACYCLIC_CONTROL_ENDPOINT='([^']+)'", scoped_command)
    if endpoint_match is None:
        raise RuntimeError("managed child command omitted its opaque endpoint")
    endpoint = endpoint_match.group(1)
    shell = ["powershell", "-NoProfile", "-Command"] if os.name == "nt" else ["/bin/sh", "-c"]
    managed = subprocess.run(
        [*shell, scoped_command],
        cwd=updated["workdir"],
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    if managed.returncode != 0:
        raise RuntimeError(
            "managed acyclic git failed: " + managed.stderr.decode("utf-8", errors="replace")[-2000:]
        )
    managed_result = json.loads(managed.stdout)
    if "Status" not in managed_result:
        raise RuntimeError(f"managed acyclic git returned an unexpected result: {managed_result}")
    tool(
        "_hook_post_tool",
        {
            "session_id": "package-validator",
            "turn_id": "child-turn",
            "tool_use_id": "status",
            "tool_name": "exec_command",
        },
    )
    tool(
        "_hook_subagent_stop",
        {
            "session_id": "package-validator",
            "turn_id": "child-turn",
            "agent_id": "child",
        },
    )
    stale = subprocess.run(
        [*shell, scoped_command],
        cwd=updated["workdir"],
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
    )
    if stale.returncode == 0:
        raise RuntimeError("revoked managed-child capability remained usable after SubagentStop")
    process.stdin.close()
    try:
        exit_code = process.wait(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        raise
    if exit_code != 0:
        stderr = process.stderr.read().decode("utf-8", errors="replace") if process.stderr else ""
        raise RuntimeError(f"packaged control exited {exit_code}: {stderr[-2000:]}")
    if endpoint.startswith("unix://") and Path(endpoint.removeprefix("unix://")).exists():
        raise RuntimeError("Unix control socket remained after packaged control shutdown")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path, help="staged artifact directory")
    args = parser.parse_args()
    source = args.artifact.resolve()
    if not source.is_dir():
        parser.error(f"artifact is not a directory: {source}")

    with tempfile.TemporaryDirectory(prefix="acyclic-plugin-validation-") as temporary:
        artifact_root = (Path(temporary) / "artifact").resolve()
        plugin_data = Path(temporary) / "data"
        checkout = Path(temporary) / "checkout"
        shutil.copytree(source, artifact_root)
        marketplace = json.loads(
            (artifact_root / ".agents" / "plugins" / "marketplace.json").read_text(encoding="utf-8")
        )
        entries = [item for item in marketplace["plugins"] if item["name"] == "acyclic-agent-workspaces"]
        if len(entries) != 1:
            raise RuntimeError("artifact marketplace must contain exactly one plugin entry")
        relative_plugin = entries[0]["source"]["path"]
        plugin_root = (artifact_root / relative_plugin).resolve()
        if artifact_root not in plugin_root.parents:
            raise RuntimeError("marketplace plugin source escapes the artifact")
        if entries[0].get("policy", {}).get("installation") != "AVAILABLE":
            raise RuntimeError("packaged marketplace does not advertise installation")
        plugin_manifest = plugin_root / ".codex-plugin" / "plugin.json"
        if not plugin_manifest.is_file():
            raise FileNotFoundError(f"packaged plugin manifest is missing: {plugin_manifest}")
        installed_root = (Path(temporary) / "installed-plugin-cache").resolve()
        shutil.copytree(plugin_root, installed_root)
        plugin_root = installed_root
        for forbidden in ("control", "scripts", "Cargo.toml", "Cargo.lock"):
            if (plugin_root / forbidden).exists():
                raise RuntimeError(f"installed plugin contains development source: {forbidden}")
        plugin_data.mkdir()
        checkout.mkdir()

        config_text = (plugin_root / ".mcp.json").read_text(encoding="utf-8")
        if "../" in config_text or "rust/crates" in config_text or "cargo" in config_text:
            raise RuntimeError("runtime MCP config is not artifact-local")
        config = json.loads(config_text)
        server = config["mcpServers"]["acyclic_agent_workspaces"]
        command = server["command"].replace("${PLUGIN_ROOT}", str(plugin_root))
        executable = Path(command).resolve(strict=True)
        if plugin_root != executable.parent and plugin_root not in executable.parents:
            raise RuntimeError("MCP command escapes the staged plugin root")
        if not executable.is_file():
            raise FileNotFoundError(f"packaged executable is missing: {executable}")
        suffix = ".exe" if os.name == "nt" else ""
        acyclic_cli = plugin_root / "bin" / f"acyclic{suffix}"
        if not acyclic_cli.is_file():
            raise FileNotFoundError(f"packaged acyclic CLI is missing: {acyclic_cli}")
        if any(path.name in {"git", "git.exe"} for path in (plugin_root / "bin").iterdir()):
            raise RuntimeError("artifact must not package or shadow system git")
        cli_help = subprocess.run(
            [str(acyclic_cli), "--help"],
            cwd=checkout,
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=10,
        )
        cli_help_text = cli_help.stdout.decode("utf-8", errors="replace")
        if cli_help.returncode != 0 or "git" not in cli_help_text.lower():
            raise RuntimeError("packaged acyclic CLI help does not expose the git namespace")

        managed_child_smoke(executable, server, plugin_root, checkout, plugin_data)
        print(f"validated artifact-only marketplace, installed-cache startup, and managed child: {artifact_root}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, RuntimeError, subprocess.TimeoutExpired) as error:
        print(f"validate-package.py: {error}", file=sys.stderr)
        raise SystemExit(1) from error
