"""Negative and lifecycle tests for the installed winpty qualification runner."""

from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path


SDK_ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = SDK_ROOT / "scripts" / "graphcoder-production-pty-winpty.py"
FIXTURE_PATH = SDK_ROOT / "scripts" / "fixtures" / "graphcoder-production-pty-negative.mjs"
SPEC = importlib.util.spec_from_file_location("graphcoder_production_pty_winpty", RUNNER_PATH)
if SPEC is None or SPEC.loader is None:  # pragma: no cover - test loader failure
    raise RuntimeError(f"could not load {RUNNER_PATH}")
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


@unittest.skipUnless(os.name == "nt", "winpty qualification is Windows-only")
class ProductionPtyRunnerNegativeTests(unittest.TestCase):
    def run_fixture(self, mode: str, *, expect_success: bool) -> dict[str, object]:
        work = Path(tempfile.mkdtemp(prefix="graphcoder-pty-runner-test-"))
        names = {
            "GRAPHCODER_PTY_ENTRYPOINT": str(FIXTURE_PATH),
            "GRAPHCODER_PTY_ALLOW_FIXTURE": "1",
            "GRAPHCODER_PTY_FIXTURE_MODE": mode,
            "GRAPHCODER_PTY_PROMPT_TIMEOUT_SECONDS": "2" if expect_success else "0.25",
            "GRAPHCODER_PTY_COMMAND_EXPECTATIONS_JSON": json.dumps(['"selectedSession"']),
            "GRAPHCODER_NODE": shutil.which("node") or "",
            "GRAPHCODER_PTY_TRANSCRIPT_PATH": str(work / "transcript.txt"),
            "GRAPHCODER_PTY_LIFECYCLE_PATH": str(work / "lifecycle.json"),
        }
        previous = {name: os.environ.get(name) for name in names}
        os.environ.update(names)
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                if expect_success:
                    self.assertEqual(runner.run(["probe"], smoke=True), 0)
                else:
                    with self.assertRaises(RuntimeError):
                        runner.run(["probe"], smoke=True)
            lifecycle = json.loads((work / "lifecycle.json").read_text(encoding="utf-8"))
            self.assertFalse(lifecycle["alive_after_cleanup"], mode)
            self.assertTrue(lifecycle["pty_closed"], mode)
            self.assertFalse(lifecycle["reader_alive_after_join"], mode)
            return lifecycle
        finally:
            for name, value in previous.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value
            shutil.rmtree(work, ignore_errors=True)

    def test_success_closes_child_socket_and_reader(self) -> None:
        lifecycle = self.run_fixture("success", expect_success=True)
        self.assertTrue(lifecycle["natural_exit_observed"])
        self.assertFalse(lifecycle["forced_kill"])

    def test_missing_result_fails_and_cleans_up(self) -> None:
        self.run_fixture("missing", expect_success=False)

    def test_truncated_result_fails_and_cleans_up(self) -> None:
        self.run_fixture("truncated", expect_success=False)

    def test_wrong_result_fails_and_cleans_up(self) -> None:
        self.run_fixture("wrong", expect_success=False)

    def test_timeout_fails_and_cleans_up(self) -> None:
        self.run_fixture("timeout", expect_success=False)

    def test_bridge_crash_fails_and_cleans_up(self) -> None:
        self.run_fixture("bridge-crash", expect_success=False)


if __name__ == "__main__":
    raise SystemExit(unittest.main())
