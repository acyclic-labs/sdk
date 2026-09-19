from __future__ import annotations

import json
import base64
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from grade import canonical_digest, command_invokes, completed_commands, event_summary, grade_attempt, rejected_commands, tree_manifest  # noqa: E402
from run_eval import credential_files, credential_markers, final_response, purge_credential_attempt, redact_credentials, redact_json  # noqa: E402


class HarnessTests(unittest.TestCase):
    def test_tool_names_are_normalized_from_jsonl_events(self) -> None:
        events = [
            {"type": "item.started", "item": {"type": "collab_tool_call", "tool": "spawn", "status": "in_progress"}},
            {"type": "item.completed", "item": {"type": "collab_tool_call", "tool": "spawn", "status": "completed"}},
            {"type": "item.started", "item": {"type": "mcp_tool_call", "tool": "agent_merge", "status": "in_progress"}},
            {"type": "item.completed", "item": {"type": "mcp_tool_call", "tool": "agent_merge", "status": "completed"}},
        ]
        self.assertEqual(event_summary(events)["tools"], {"agent_merge": 1, "spawn_agent": 1})

    def test_manifest_is_stable_and_excludes_harness_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "visible.txt").write_text("value\n", encoding="utf-8")
            (root / ".git").mkdir()
            (root / ".git" / "index").write_bytes(b"ignored")
            first = tree_manifest(root)
            second = tree_manifest(root)
            self.assertEqual(first, second)
            self.assertEqual([row["path"] for row in first], ["visible.txt"])
            self.assertEqual(canonical_digest(first), canonical_digest(second))

    def test_grade_uses_host_state_not_final_claim(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture = root / "fixture"
            plugin = root / "plugin"
            fixture.mkdir()
            (fixture / ".git").mkdir()
            plugin.mkdir()
            (plugin / "adapter-state.json").write_text(json.dumps({
                "version": 1, "root_session_id": "s", "root_path": str(fixture.resolve()),
                "root_workspace_name": "root", "routes": {}, "turns": {}, "leases": {}
            }), encoding="utf-8")
            events = root / "events.jsonl"
            events.write_text(json.dumps({"type": "item.completed", "item": {"type": "mcp_tool_call", "tool": "agent_merge", "status": "completed"}}) + "\n", encoding="utf-8")
            sentinel = root / "outside.txt"
            sentinel.write_text("outside remains unchanged\n", encoding="utf-8")
            case = {"id": "host-state", "required_tools": {"agent_merge": 1}, "files": {"required.txt": "real\n"}}
            grade = grade_attempt(case, fixture, plugin, events, sentinel, canonical_digest([]))
            self.assertFalse(grade["passed"])
            self.assertFalse(next(item for item in grade["assertions"] if item["name"] == "file:required.txt")["passed"])

    def test_failed_tool_result_does_not_count(self) -> None:
        events = [
            {"type": "item.completed", "item": {
            "type": "mcp_tool_call", "tool": "agent_merge", "status": "failed",
            "result": {"content": [{"text": "unknown agent"}]},
            }},
            {"type": "item.completed", "item": {
                "type": "mcp_tool_call", "tool": "agent_changes", "status": "completed",
                "result": {"isError": True, "content": [{"text": "unknown agent"}]},
            }},
        ]
        self.assertEqual(event_summary(events)["tools"], {})

    def test_command_evidence_requires_completed_event_and_exit_code(self) -> None:
        events = [
            {"type": "item.started", "item": {"type": "command_execution", "command": "acyclic git status", "status": "in_progress"}},
            {"type": "item.completed", "item": {"type": "command_execution", "command": "acyclic git commit", "status": "completed", "exit_code": 1}},
            {"type": "item.completed", "item": {"type": "command_execution", "command": "git --version", "status": "completed", "exit_code": 0}},
        ]
        self.assertEqual(completed_commands(events), [
            {"command": "acyclic git commit", "exit_code": 1},
            {"command": "git --version", "exit_code": 0},
        ])
        self.assertTrue(command_invokes("acyclic git status", "acyclic git status"))
        self.assertTrue(command_invokes("$env:ACYCLIC_CONTEXT_VERSION='1';& 'C:/plugin/bin/acyclic.exe' 'git' 'status'", "acyclic git status"))
        self.assertFalse(command_invokes("echo acyclic git status", "acyclic git status"))

    def test_prompt_text_cannot_fake_required_command(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fixture, plugin = root / "fixture", root / "plugin"
            fixture.mkdir(); (fixture / ".git").mkdir(); plugin.mkdir()
            (plugin / "adapter-state.json").write_text(json.dumps({
                "version": 1, "root_session_id": "s", "root_path": str(fixture.resolve()),
                "root_workspace_name": "root", "routes": {}, "turns": {}, "leases": {}
            }), encoding="utf-8")
            events = root / "events.jsonl"
            events.write_text(json.dumps({"type": "item.completed", "item": {
                "type": "agent_message", "text": "I ran acyclic git status", "status": "completed"
            }}) + "\n", encoding="utf-8")
            sentinel = root / "outside.txt"; sentinel.write_text("ok\n", encoding="utf-8")
            case = {"id": "no-false-pass", "required_commands": [{"term": "acyclic git status", "exit_codes": [0]}]}
            grade = grade_attempt(case, fixture, plugin, events, sentinel, canonical_digest([]))
            assertion = next(item for item in grade["assertions"] if item["name"] == "completed-command:acyclic git status")
            self.assertFalse(assertion["passed"])

    def test_path_escape_requires_two_actual_rejected_commands(self) -> None:
        events = [
            {"type": "item.completed", "item": {"type": "command_execution", "command": "echo escape", "status": "completed", "exit_code": 0}},
            {"type": "item.completed", "item": {"type": "command_execution", "command": "touch ../escaped.txt", "status": "failed"}},
        ]
        self.assertEqual([item["command"] for item in rejected_commands(events)], ["touch ../escaped.txt"])
        self.assertTrue(command_invokes("touch ../escaped.txt", "touch ../escaped.txt"))
        self.assertFalse(command_invokes("echo touch ../escaped.txt", "touch ../escaped.txt"))
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); fixture = root / "fixture"; plugin = root / "plugin"
            fixture.mkdir(); (fixture / ".git").mkdir(); plugin.mkdir()
            (plugin / "adapter-state.json").write_text(json.dumps({
                "version": 1, "root_session_id": "s", "root_path": str(fixture.resolve()),
                "root_workspace_name": "root", "routes": {}, "turns": {}, "leases": {}
            }), encoding="utf-8")
            events_path = root / "events.jsonl"
            events_path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")
            sentinel = root / "outside.txt"; sentinel.write_text("unchanged\n", encoding="utf-8")
            case = {"id": "escape", "required_rejected_commands": ["touch ../escaped.txt", "touch $OUTSIDE_SENTINEL"]}
            grade = grade_attempt(case, fixture, plugin, events_path, sentinel, canonical_digest([]))
            self.assertFalse(grade["passed"])

    def test_credentials_are_redacted_before_artifact_persistence(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            auth = Path(temporary) / "auth.json"
            secret = "sensitive-access-token-value"
            auth.write_text(json.dumps({"tokens": {"access_token": secret}, "account_id": "safe-id"}), encoding="utf-8")
            markers = credential_markers(auth)
            encoded = base64.b64encode(secret.encode()).decode()
            redacted, leaked = redact_credentials(f"raw={secret} encoded={encoded} account=safe-id", markers)
            self.assertTrue(leaked)
            self.assertNotIn(secret, redacted)
            self.assertNotIn(encoded, redacted)
            self.assertIn("safe-id", redacted)

    def test_credentials_in_retained_state_are_detected_without_persisting_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            auth = root / "auth.json"
            secret = "sensitive-retained-token-value"
            auth.write_text(json.dumps({"access_token": secret}), encoding="utf-8")
            attempt = root / "attempt"; attempt.mkdir()
            (attempt / "copied-auth.json").write_text(auth.read_text(encoding="utf-8"), encoding="utf-8")
            markers = credential_markers(auth)
            self.assertEqual(credential_files({"attempt": attempt}, markers), ["attempt/copied-auth.json"])
            sanitized, leaked = redact_json({"evidence": secret}, markers)
            self.assertTrue(leaked)
            self.assertEqual(sanitized, {"evidence": "[REDACTED_EVAL_CREDENTIAL]"})
            grade = {"artifacts": {"prompt": "sha256:old", "events": "sha256:old", "stderr": "sha256:old", "final": "sha256:old"}}
            purge_credential_attempt(attempt, grade)
            self.assertEqual([path.name for path in attempt.iterdir()], ["grade.json"])
            self.assertNotIn(secret, (attempt / "grade.json").read_text(encoding="utf-8"))
            self.assertTrue(all(value is None for value in grade["artifacts"].values()))

    def test_credential_purge_fails_closed_when_removal_does_not_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            attempt = Path(temporary) / "attempt"; attempt.mkdir()
            (attempt / "leak.json").write_text("secret", encoding="utf-8")
            grade = {"artifacts": {"prompt": "sha256:old", "events": None, "stderr": None, "final": None}}
            with mock.patch("run_eval.shutil.rmtree", return_value=None):
                with self.assertRaisesRegex(RuntimeError, "could not be purged"):
                    purge_credential_attempt(attempt, grade)
            self.assertFalse((attempt / "grade.json").exists())

    def test_credential_scan_rejects_symlinks_outside_attempt(self) -> None:
        if sys.platform == "win32":
            self.skipTest("creating symlinks requires optional Windows privilege")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            auth = root / "auth.json"; auth.write_text(json.dumps({"access_token": "sensitive-link-token"}), encoding="utf-8")
            attempt = root / "attempt"; attempt.mkdir()
            (attempt / "linked-auth.json").symlink_to(auth)
            self.assertEqual(credential_files({"attempt": attempt}, credential_markers(auth)), ["attempt/linked-auth.json"])

    def test_credential_scan_treats_unreadable_files_as_unsafe(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            attempt = Path(temporary) / "attempt"; attempt.mkdir()
            blocked = attempt / "blocked.bin"; blocked.write_bytes(b"contents")
            original = Path.read_bytes

            def read_bytes(path: Path) -> bytes:
                if path == blocked:
                    raise PermissionError("blocked")
                return original(path)

            with mock.patch("pathlib.Path.read_bytes", autospec=True, side_effect=read_bytes):
                self.assertEqual(credential_files({"attempt": attempt}, ["credential-marker"]), ["attempt/blocked.bin"])

    def test_credential_scan_treats_unreadable_directories_as_unsafe(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            attempt = Path(temporary) / "attempt"; attempt.mkdir()
            blocked = attempt / "blocked"; blocked.mkdir()
            original = Path.iterdir

            def iterdir(path: Path):
                if path == blocked:
                    raise PermissionError("blocked")
                return original(path)

            with mock.patch("pathlib.Path.iterdir", autospec=True, side_effect=iterdir):
                self.assertEqual(credential_files({"attempt": attempt}, ["credential-marker"]), ["attempt/blocked"])

    def test_final_response_is_derived_from_redacted_events(self) -> None:
        events = "\n".join([
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": "earlier"}}),
            json.dumps({"type": "item.completed", "item": {"type": "agent_message", "text": json.dumps({"status": "done"})}}),
        ])
        self.assertEqual(final_response(events), {"status": "done"})


if __name__ == "__main__":
    unittest.main()
