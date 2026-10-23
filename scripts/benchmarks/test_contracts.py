"""Offline production-path proofs; no model, Docker, credential or build access."""

import hashlib
import itertools
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from .contracts import Artifact, catalog, run_command
from .bootstrap import verified_uv
from .plan import STAGES, inventory, manifest, paired_plan


class BenchmarkContracts(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="predictor-contracts-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.binary = self.root / "graphcoder"
        # Stand-in process records the actual OS argv/env, without a model.
        self.binary.write_text("#!/usr/bin/python3\nimport json,os,sys\n"
                               "print(json.dumps({'argv':sys.argv[1:],'env':dict(os.environ)}))\n"
                               "sys.exit(int(os.environ.get('FIXTURE_EXIT','0')))\n")
        self.binary.chmod(0o755)
        self.sha = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        self.source = "a" * 40
        self.artifact = Artifact.read(str(self.binary), self.sha, self.source)

    def dataset(self, label="terminal-bench@2.0", count=89):
        return manifest(label, "https://github.com/example/benchmark.git", "b" * 40,
                        [f"tasks/task-{index:03}" for index in range(count)])

    def plan(self, stage="tb2-20", dataset=None, model="openai/fixture"):
        return paired_plan(stage, dataset or self.dataset(), self.artifact, str(self.binary),
                           model, "http://127.0.0.1:9999/v1", 100, 10, "0.164.0")

    def test_artifact_identity_is_verified_at_every_construction_entry(self):
        with self.assertRaisesRegex(ValueError, "immutable bytes"):
            Artifact(bytearray(self.artifact.contents), self.sha, self.source)
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            Artifact(b"modified", self.sha, self.source)
        self.binary.write_bytes(b"modified after verification")
        self.assertNotEqual(self.artifact.contents, self.binary.read_bytes())
        with self.assertRaisesRegex(ValueError, "hash mismatch"):
            Artifact.read(str(self.binary), self.sha, self.source)
        for field in ("latest", "a" * 39, "A" * 40, "a" * 41):
            with self.assertRaises(ValueError):
                Artifact(self.artifact.contents, self.sha, field)

    def test_endpoint_has_no_credential_bearing_shape(self):
        for endpoint in ("https://name:password@example.test/v1", "https://example.test/?token=x",
                         "https://example.test/#secret", "file:///etc/passwd", "http:///v1"):
            with self.subTest(endpoint=endpoint), self.assertRaises(ValueError):
                catalog("openai/fixture", endpoint, 100, 10)
        for context, output in ((0, 1), (10, 11), (True, 1), (10, 0)):
            with self.assertRaises(ValueError):
                catalog("openai/fixture", "http://localhost/v1", context, output)
        self.assertEqual(catalog("openai/fixture", "http://localhost/v1", 100, 10)
                         ["benchmark"]["authentication"], "optional")

    def test_bootstrap_rejects_unpinned_bytes_before_archive_parsing(self):
        with patch("scripts.benchmarks.bootstrap.zipfile.ZipFile",
                   side_effect=AssertionError("unverified archive parsed")):
            with self.assertRaisesRegex(ValueError, "wheel hash mismatch"):
                verified_uv(b"untrusted replacement; not a zip archive")

    def test_all_stages_pair_identical_pinned_tasks_without_execution(self):
        # Unexpected calls turn a dry plan into a test failure, including auth
        # subprocesses, remote git discovery or starting the runner.
        with patch("subprocess.run", side_effect=AssertionError("offline plan executed a process")):
            for stage, (label, count, selected) in STAGES.items():
                result = self.plan(stage, self.dataset(label, count))
                self.assertEqual(len(result["task_names"]), selected)
                self.assertEqual(result["graphcoder"]["tasks"], result["codex"]["tasks"])
                self.assertEqual(result["graphcoder"]["agents"][0]["kwargs"]["base_url"],
                                 result["codex"]["agents"][0]["env"]["OPENAI_BASE_URL"])
                self.assertEqual(result["readiness"]["status"], "BLOCKED")
                for name in ("graphcoder", "codex"):
                    self.assertEqual(result[name]["retry"], {"max_retries": 0})
                    self.assertTrue(all(t["git_commit_id"] == "b" * 40 for t in result[name]["tasks"]))
        reverse = manifest("terminal-bench@2.0", "https://github.com/example/benchmark.git",
                           "b" * 40, list(reversed([task["path"] for task in self.dataset()["tasks"]])))
        self.assertEqual(self.plan(), self.plan(dataset=reverse))
        self.assertLessEqual(set(self.plan()["task_names"]), set(self.plan("tb2-full")["task_names"]))

    def test_incomplete_or_ambiguous_task_universes_cannot_be_planned(self):
        for count in (0, 19, 20, 88, 90):
            with self.assertRaises(ValueError):
                self.plan(dataset=self.dataset(count=count))
        with self.assertRaises(ValueError):
            manifest("terminal-bench@2.0", "https://github.com/example/benchmark", "b" * 40,
                     ["one/task", "two/task"])
        for path in ("../escape", "/absolute", "a/../b", "a//b", "./b", "b*", ""):
            with self.assertRaises(ValueError):
                manifest("terminal-bench@2.0", "https://github.com/example/benchmark", "b" * 40, [path])

    def test_inventory_and_plan_cli_use_pinned_local_metadata_only(self):
        repository = self.root / "dataset"
        repository.mkdir()
        git_env = {"PATH": os.environ["PATH"], "HOME": str(self.root),
                   "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull}
        def git(*arguments):
            return subprocess.run(["git", "-C", str(repository), *arguments], env=git_env,
                                  capture_output=True, text=True, check=True).stdout.strip()
        git("init", "-q")
        (repository / "one").mkdir()
        (repository / "one/task.toml").write_text("# fixture tree metadata\n")
        git("add", ".")
        git("-c", "user.name=Codex Fixture", "-c", "user.email=codex@openai.com",
            "-c", "commit.gpgsign=false", "commit", "--no-gpg-sign", "-qm", "fixture")
        commit = git("rev-parse", "HEAD")
        metadata = inventory(str(repository), "terminal-bench@2.0",
                             "https://github.com/example/benchmark.git", commit)
        self.assertEqual(metadata["tasks"], [{"name": "one", "path": "one"}])
        self.assertEqual(metadata["git_commit_id"], commit)
        with self.assertRaises(ValueError):
            inventory(str(repository), "terminal-bench@2.0",
                      "https://github.com/example/benchmark.git", "main")
        manifest_file = self.root / "manifest.json"
        manifest_file.write_text(json.dumps(self.dataset()))
        result = subprocess.run([sys.executable, "-m", "scripts.benchmarks.plan", "plan",
            "--stage", "tb2-20", "--manifest", str(manifest_file), "--binary", str(self.binary),
            "--binary-sha256", self.sha, "--source-sha", self.source, "--model", "openai/fixture",
            "--base-url", "http://127.0.0.1:9999/v1", "--context-tokens", "100",
            "--output-tokens", "10", "--codex-version", "0.164.0"],
            capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(result.stdout)["task_names"], self.plan()["task_names"])

    def test_shell_metacharacters_roundtrip_as_prompt_data_and_env_is_empty(self):
        state = self.root / "state with spaces"
        logs = self.root / "logs with spaces"
        logs.mkdir()
        for child in ("home", "tmp", "fixture-codex"):
            (state / child).mkdir(parents=True)
        prompts = ["--model=wrong", "line1\nline2", "$(touch ESCAPED)", "`touch ESCAPED`"]
        prompts += ["a" + "".join(chars) + "z" for chars in itertools.product("'\";$", repeat=2)]
        for instruction in prompts:
            command = run_command(str(self.binary), str(state), str(logs), "openai/fixture", instruction)
            env = {"PATH": os.environ["PATH"], "OPENAI_API_KEY": "fixture-must-not-forward",
                   "CODEX_HOME": str(self.root / "never-read-login")}
            result = subprocess.run(["bash", "-c", command], env=env, cwd=self.root, check=False)
            self.assertEqual(result.returncode, 0, "prompt framing must launch the selected executable")
            observed = json.loads((logs / "graphcoder.jsonl").read_text())
            self.assertEqual(observed["argv"][-1], "--prompt=" + instruction)
            self.assertNotIn("OPENAI_API_KEY", observed["env"])
            self.assertEqual(observed["env"]["CODEX_HOME"], str(state / "fixture-codex"))
            self.assertEqual(observed["env"]["PATH"], env["PATH"])
            self.assertFalse((self.root / "ESCAPED").exists())

    def test_log_redirection_preserves_agent_failure_status(self):
        self.binary.write_text("#!/bin/sh\nexit 7\n")
        self.binary.chmod(0o755)
        result = subprocess.run(["bash", "-c", run_command(str(self.binary), str(self.root),
                    str(self.root), "openai/fixture", "hello")], check=False)
        self.assertEqual(result.returncode, 7)


if __name__ == "__main__":
    unittest.main()
