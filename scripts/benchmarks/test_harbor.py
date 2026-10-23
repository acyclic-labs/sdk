"""Exercise the actual pinned Harbor API; only environment I/O is a fixture."""

import hashlib
import importlib.metadata
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from harbor.models.job.config import JobConfig
from .graphcoder_harbor import GraphCoder
from . import test_contracts


class Environment:
    default_user = "fixture-agent"

    def __init__(self):
        self.commands = []
        self.uploads = {}
        self.reject = False

    async def exec(self, command, **kwargs):
        self.commands.append(command)
        if self.reject:
            return SimpleNamespace(return_code=7, stdout="", stderr="fixture install rejected")
        return SimpleNamespace(return_code=0, stdout="graphcoder fixture", stderr="")

    async def upload_file(self, source, target):
        self.uploads[target] = Path(source).read_bytes()


class HarborContracts(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="predictor-harbor-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.binary = self.root / "graphcoder"
        self.binary.write_bytes(b"immutable fixture executable")
        self.original = self.binary.read_bytes()
        self.kwargs = dict(logs_dir=self.root / "logs", model_name="openai/fixture",
            graphcoder_binary=str(self.binary), graphcoder_sha256=hashlib.sha256(self.original).hexdigest(),
            source_sha="a" * 40, base_url="http://127.0.0.1:9999/v1", context_tokens=100, output_tokens=10)

    async def test_install_run_and_failed_reinstall_fence_execution(self):
        self.assertEqual(importlib.metadata.version("harbor"), "0.24.0")
        agent = GraphCoder(**self.kwargs)
        environment = Environment()
        with self.assertRaisesRegex(RuntimeError, "not installed"):
            await agent.run("hello", environment, None)
        self.binary.write_bytes(b"replaced after construction")
        await agent.setup(environment)
        self.assertEqual(environment.uploads[agent.binary], self.original)
        self.assertEqual(agent.version(), "a" * 40)
        await agent.run("hello ; $(false)", environment, None)
        self.assertIn("--prompt=hello ; $(false)", environment.commands[-1])
        environment.reject = True
        with self.assertRaises(RuntimeError):
            await agent.install(environment)
        with self.assertRaisesRegex(RuntimeError, "not installed"):
            await agent.run("hello", environment, None)

    async def test_extra_environment_is_rejected_before_setup(self):
        with self.assertRaisesRegex(ValueError, "inherited agent env"):
            GraphCoder(**self.kwargs, extra_env={"FIXTURE_TOKEN": "not-a-real-key"})

    async def test_plans_validate_as_real_harbor_jobs(self):
        fixture = test_contracts.BenchmarkContracts()
        fixture.setUp()
        try:
            result = fixture.plan()
            for name in ("codex", "graphcoder"):
                job = JobConfig.model_validate(result[name])
                self.assertEqual(len(job.tasks), 20)
                self.assertEqual(job.n_concurrent_trials, 1)
                self.assertEqual(job.n_attempts, 1)
                self.assertEqual(job.retry.max_retries, 0)
        finally:
            fixture.doCleanups()


if __name__ == "__main__":
    unittest.main()
