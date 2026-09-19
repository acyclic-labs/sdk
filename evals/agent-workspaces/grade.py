#!/usr/bin/env python3
"""Grade one agent-workspaces attempt from JSONL events and host artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any

IGNORED_PARTS = {".git", ".agents", ".codex", ".eval"}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def tree_manifest(root: Path, *, include_git: bool = False) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not root.exists():
        return rows
    for path in sorted(root.rglob("*"), key=lambda p: p.as_posix()):
        relative = path.relative_to(root)
        if not include_git and any(part in IGNORED_PARTS for part in relative.parts):
            continue
        if path.is_symlink():
            rows.append({"path": relative.as_posix(), "type": "symlink", "target": os.readlink(path)})
        elif path.is_file():
            rows.append({"path": relative.as_posix(), "type": "file", "size": path.stat().st_size, "sha256": sha256_file(path)})
    return rows


def canonical_digest(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def read_events(path: Path) -> tuple[list[Any], list[str]]:
    events: list[Any] = []
    errors: list[str] = []
    if not path.exists():
        return events, ["events file is absent"]
    for line_number, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        if not line.strip():
            continue
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError as error:
            errors.append(f"line {line_number} is not JSON: {error.msg}")
    return events, errors


def normalized_tool(value: str) -> str | None:
    candidates = ("spawn_agent", "agent_changes", "agent_merge", "agent_discard")
    lowered = value.lower()
    if lowered == "spawn":
        return "spawn_agent"
    for candidate in candidates:
        if lowered == candidate or lowered.endswith(f"__{candidate}") or lowered.endswith(f"/{candidate}"):
            return candidate
    return None


def tool_succeeded(item: dict[str, Any]) -> bool:
    result = item.get("result")
    return (
        item.get("status") == "completed"
        and not item.get("error")
        and not (isinstance(result, dict) and result.get("isError") is True)
    )


def event_summary(events: list[Any]) -> dict[str, Any]:
    tools: dict[str, int] = {}
    attempts: dict[str, int] = {}
    event_types: dict[str, int] = {}
    for event in events:
        if isinstance(event, dict) and isinstance(event.get("type"), str):
            name = event["type"]
            event_types[name] = event_types.get(name, 0) + 1
        if isinstance(event, dict) and event.get("type") in {"item.started", "item.completed"}:
            item = event.get("item")
            if isinstance(item, dict) and item.get("type") in {"collab_tool_call", "mcp_tool_call"}:
                value = item.get("tool")
                if isinstance(value, str):
                    tool = normalized_tool(value)
                    if tool and event.get("type") == "item.started":
                        attempts[tool] = attempts.get(tool, 0) + 1
                    if tool and event.get("type") == "item.completed" and tool_succeeded(item):
                        tools[tool] = tools.get(tool, 0) + 1
    return {"events": len(events), "event_types": dict(sorted(event_types.items())), "tools": dict(sorted(tools.items())), "tool_attempts": dict(sorted(attempts.items()))}


def event_evidence_text(events: list[Any]) -> str:
    values: list[str] = []
    for event in events:
        if not isinstance(event, dict) or event.get("type") not in {"item.started", "item.completed"}:
            continue
        item = event.get("item")
        if not isinstance(item, dict) or item.get("type") not in {"command_execution", "mcp_tool_call", "collab_tool_call"}:
            continue
        for key in ("command", "aggregated_output", "error", "result", "arguments", "tool"):
            value = item.get(key)
            if value is not None:
                values.append(json.dumps(value, sort_keys=True) if not isinstance(value, str) else value)
    return "\n".join(values).lower()


def completed_commands(events: list[Any]) -> list[dict[str, Any]]:
    commands = []
    for event in events:
        if not isinstance(event, dict) or event.get("type") != "item.completed":
            continue
        item = event.get("item")
        if not isinstance(item, dict) or item.get("type") != "command_execution" or item.get("status") != "completed":
            continue
        command = item.get("command")
        exit_code = item.get("exit_code")
        if isinstance(command, str) and isinstance(exit_code, int):
            commands.append({"command": command, "exit_code": exit_code})
    return commands


def rejected_commands(events: list[Any]) -> list[dict[str, Any]]:
    commands = []
    for event in events:
        if not isinstance(event, dict) or event.get("type") != "item.completed":
            continue
        item = event.get("item")
        if not isinstance(item, dict) or item.get("type") != "command_execution":
            continue
        command, exit_code = item.get("command"), item.get("exit_code")
        rejected = item.get("status") == "failed" or (isinstance(exit_code, int) and exit_code != 0)
        if isinstance(command, str) and rejected:
            commands.append({"command": command, "exit_code": exit_code, "status": item.get("status")})
    return commands


def command_invokes(command: str, expected: str) -> bool:
    wanted = [token.strip("'\"").lower() for token in re.findall(r"'[^']*'|\"[^\"]*\"|\S+", expected)]
    if wanted:
        wanted[0] = Path(wanted[0]).name.removesuffix(".exe")
    for segment in reversed(re.split(r"\s*(?:&&|\|\||;)\s*", command.strip())):
        tokens = [token.strip("'\"") for token in re.findall(r"'[^']*'|\"[^\"]*\"|\S+", segment)]
        while tokens and (tokens[0] == "&" or ("=" in tokens[0] and tokens[0].split("=", 1)[0].replace("$env:", "").startswith("ACYCLIC_"))):
            tokens.pop(0)
        if not tokens:
            continue
        program = Path(tokens[0]).name.lower().removesuffix(".exe")
        actual = [program, *[token.lower() for token in tokens[1:]]]
        return actual[:len(wanted)] == wanted
    return False


def successful_tool_results(events: list[Any], wanted: str) -> list[str]:
    def strings(value: Any) -> list[str]:
        if isinstance(value, str):
            return [value]
        if isinstance(value, dict):
            return [text for child in value.values() for text in strings(child)]
        if isinstance(value, list):
            return [text for child in value for text in strings(child)]
        return []

    results: list[str] = []
    for event in events:
        if not isinstance(event, dict) or event.get("type") != "item.completed":
            continue
        item = event.get("item")
        if not isinstance(item, dict) or not tool_succeeded(item) or normalized_tool(str(item.get("tool", ""))) != wanted:
            continue
        results.append("\n".join(strings(item.get("result"))))
    return results


def normalized_path(path: str | Path) -> str:
    value = str(path)
    if os.name == "nt" and value.startswith("\\\\?\\"):
        value = value[4:]
    return os.path.normcase(os.path.abspath(value))


def find_adapter_state(plugin_data: Path) -> tuple[Path | None, Any | None, str | None]:
    matches = sorted(plugin_data.rglob("adapter-state.json")) if plugin_data.exists() else []
    if len(matches) != 1:
        return None, None, f"expected one adapter-state.json, found {len(matches)}"
    try:
        return matches[0], json.loads(matches[0].read_text(encoding="utf-8")), None
    except (OSError, json.JSONDecodeError) as error:
        return matches[0], None, f"cannot parse adapter state: {error}"


def grade_attempt(case: dict[str, Any], fixture: Path, plugin_data: Path, events_path: Path,
                  outside_sentinel: Path, git_before: str) -> dict[str, Any]:
    events, parse_errors = read_events(events_path)
    summary = event_summary(events)
    assertions: list[dict[str, Any]] = []

    def check(name: str, passed: bool, evidence: Any) -> None:
        assertions.append({"name": name, "passed": bool(passed), "evidence": evidence})

    check("jsonl-valid", not parse_errors and bool(events), parse_errors or f"{len(events)} events")
    for tool, minimum in case.get("required_tools", {}).items():
        actual = summary["tools"].get(tool, 0)
        check(f"tool:{tool}", actual >= minimum, {"minimum": minimum, "actual": actual})
    for tool, expected_calls in case.get("tool_results", {}).items():
        actual_results = successful_tool_results(events, tool)
        for index, alternatives in enumerate(expected_calls):
            passed = index < len(actual_results) and any(term in actual_results[index] for term in alternatives)
            check(f"tool-result:{tool}:{index + 1}", passed, {"alternatives": alternatives, "actual": actual_results[index] if index < len(actual_results) else None})

    raw_events = event_evidence_text(events)
    for term in case.get("required_event_terms", []):
        check(f"event-term:{term}", term.lower() in raw_events, term)
    commands = completed_commands(events)
    for requirement in case.get("required_commands", []):
        term = requirement["term"].lower()
        exit_codes = requirement["exit_codes"]
        matching = [item for item in commands if command_invokes(item["command"], term) and item["exit_code"] in exit_codes]
        check(f"completed-command:{requirement['term']}", bool(matching), {"exit_codes": exit_codes, "matching": matching})
    rejected = rejected_commands(events)
    for expected in case.get("required_rejected_commands", []):
        resolved = expected.replace("$OUTSIDE_SENTINEL", f'"{outside_sentinel.resolve()}"')
        matching = [item for item in rejected if command_invokes(item["command"], resolved)]
        check(f"rejected-command:{expected}", bool(matching), {"expected": resolved, "matching": matching})

    for relative, expected in case.get("files", {}).items():
        target = fixture / relative
        actual = target.read_text(encoding="utf-8", errors="replace") if target.is_file() else None
        check(f"file:{relative}", actual == expected and not target.is_symlink(), {"expected": expected, "actual": actual, "symlink": target.is_symlink()})
    for relative in case.get("absent", []):
        check(f"absent:{relative}", not os.path.lexists(fixture / relative), str(fixture / relative))
    if "outside_sentinel" in case:
        actual = outside_sentinel.read_text(encoding="utf-8", errors="replace") if outside_sentinel.is_file() else None
        check("outside-sentinel", actual == case["outside_sentinel"] and not outside_sentinel.is_symlink(), {"expected": case["outside_sentinel"], "actual": actual, "symlink": outside_sentinel.is_symlink()})

    state_path, state, state_error = find_adapter_state(plugin_data)
    check("adapter-state-present", state_error is None, state_error or str(state_path))
    if isinstance(state, dict):
        required = {"version", "root_session_id", "root_path", "root_workspace_name", "routes", "turns", "leases"}
        check("adapter-state-shape", required <= set(state), sorted(state))
        check("adapter-state-root", normalized_path(state.get("root_path", "")) == normalized_path(fixture), state.get("root_path"))
        routes = state.get("routes") if isinstance(state.get("routes"), dict) else {}
        if "minimum_routes" in case:
            check("adapter-state-routes", len(routes) >= case["minimum_routes"], {"minimum": case["minimum_routes"], "actual": len(routes)})
        if "route_count" in case:
            check("adapter-state-routes", len(routes) == case["route_count"], {"expected": case["route_count"], "actual": len(routes)})

    if case.get("require_crash_marker"):
        markers = list(plugin_data.rglob("eval-control-crashed-once")) if plugin_data.exists() else []
        check("control-crash-injected", len(markers) == 1, [str(path) for path in markers])

    git_after = canonical_digest(tree_manifest(fixture / ".git", include_git=True))
    if case.get("git_unchanged"):
        check("system-git-unchanged", git_after == git_before, {"before": git_before, "after": git_after})

    return {
        "schema": "acyclic.agent-workspaces.attempt-grade.v1",
        "case": case["id"],
        "passed": all(item["passed"] for item in assertions),
        "event_summary": summary,
        "assertions": assertions,
        "fixture_digest": canonical_digest(tree_manifest(fixture)),
        "plugin_state_digest": canonical_digest(tree_manifest(plugin_data, include_git=True)),
        "git_before": git_before,
        "git_after": git_after,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("events", type=Path, help="raw codex exec JSONL")
    args = parser.parse_args()
    events, errors = read_events(args.events)
    print(json.dumps({"errors": errors, **event_summary(events)}, indent=2, sort_keys=True))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
