"""Exercise the actual pinned Harbor API; only environment I/O is a fixture."""

import asyncio
import hashlib
import importlib.metadata
import shutil
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from harbor.models.job.config import JobConfig
from harbor.environments.base import ExecResult
from harbor.agents.installed.codex import Codex, CodexOptions
from .acyclic_graphcoder_harbor.graphcoder_harbor import GraphCoder
from . import test_contracts


class Environment:
    default_user = "fixture-agent"

    def __init__(self):
        self.commands = []
        self.uploads = {}
        self.reject = False
        self.transport_failure = False

    async def exec(self, command, **kwargs):
        if self.transport_failure:
            raise RuntimeError("fixture transport failure before install")
        self.commands.append(command)
        if self.reject:
            return SimpleNamespace(return_code=7, stdout="", stderr="fixture install rejected")
        return SimpleNamespace(return_code=0, stdout="graphcoder fixture", stderr="")

    async def upload_file(self, source, target):
        self.uploads[target] = Path(source).read_bytes()


class NativeEnvironment:
    """Execute the installed-agent contract against the actual Linux filesystem."""

    default_user = None

    def __init__(self, directory):
        self.directory = directory

    async def exec(self, command, **kwargs):
        process = await asyncio.create_subprocess_exec(
            "bash", "-c", command, cwd=kwargs.get("cwd") or self.directory,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await process.communicate()
        return ExecResult(return_code=process.returncode,
                          stdout=stdout.decode(), stderr=stderr.decode())

    async def upload_file(self, source, target):
        shutil.copyfile(source, target)


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
        other_environment = Environment()
        with self.assertRaisesRegex(RuntimeError, "not installed"):
            await agent.run("hello", other_environment, None)
        self.assertEqual(other_environment.commands, [])
        environment.reject = True
        with self.assertRaises(RuntimeError):
            await agent.install(environment)
        with self.assertRaisesRegex(RuntimeError, "not installed"):
            await agent.run("hello", environment, None)

        environment.reject = False
        await agent.setup(environment)
        environment.transport_failure = True
        with self.assertRaisesRegex(RuntimeError, "transport failure"):
            await agent.setup(environment)
        with self.assertRaisesRegex(RuntimeError, "not installed"):
            await agent.run("hello", environment, None)

    async def test_proxy_environment_is_consumed_without_forwarding_provider_keys(self):
        agent = GraphCoder(**self.kwargs, allow_http_host="172.17.0.1", extra_env={
            "OPENAI_BASE_URL": "http://172.17.0.1:9999/t/trial/openai/v1",
            "ACYCLIC_PROVIDER_AUTH_HEADER": "x-harbor-token",
            "ACYCLIC_PROVIDER_AUTH_TOKEN": "generated-proxy-token",
        })
        self.assertEqual(agent.provider_catalog["benchmark"]["headers"],
                         {"x-harbor-token": "generated-proxy-token"})
        self.assertEqual(agent.provider_catalog["benchmark"]["allow_http_host"], "172.17.0.1")
        self.assertFalse(agent.extra_env)
        for name in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY", "FIXTURE_TOKEN"):
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, "raw provider keys"):
                GraphCoder(**self.kwargs, extra_env={name: "not-a-real-key"})

    async def test_task_skills_are_native_volume_files_and_copy_failure_revokes_execution(self):
        # Capture a real native executable; this control exercises installation
        # and filesystem projection, without invoking a model or a fake process.
        self.binary.write_bytes(Path("/usr/bin/true").read_bytes())
        self.kwargs["graphcoder_sha256"] = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        skills = self.root / "task skills ; literal"
        for relative, contents in {"inspect/SKILL.md": "---\nname: inspect\ndescription: Inspect files\n---\nRead files.\n",
                                   "inspect/.notes": "hidden support file",
                                   "other/data.bin": "support data"}.items():
            path = skills / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(contents)
        workspace = self.root / "workspace"
        workspace.mkdir()
        existing = workspace / ".agents/skills/existing/SKILL.md"
        existing.parent.mkdir(parents=True)
        existing.write_text("existing project skill")
        agent = GraphCoder(**self.kwargs, skills_dir=str(skills))
        self.assertTrue(agent.capabilities.skills)
        # Keep the trial-owned installation inside this control's temporary root.
        agent.state = str(self.root / "installed agent")
        agent.binary = agent.state + "/graphcoder"
        environment = NativeEnvironment(workspace)
        await agent.install(environment)
        for source in skills.rglob("*"):
            if source.is_file():
                target = workspace / ".agents/skills" / source.relative_to(skills)
                self.assertTrue(target.is_file(), f"task skill was not projected: {source}")
                self.assertEqual(target.read_bytes(), source.read_bytes())
        self.assertEqual(existing.read_text(), "existing project skill")
        agent.skills_dir = str(skills / "missing")
        with self.assertRaisesRegex(RuntimeError, "cp"):
            await agent.install(environment)
        with self.assertRaisesRegex(RuntimeError, "not installed"):
            await agent.run("must not execute", environment, None)

    async def test_comparator_projection_cannot_change_the_effective_model(self):
        fixture = test_contracts.BenchmarkContracts()
        fixture.setUp()
        try:
            for model in ("openai/fixture", "proxy/org/fixture"):
                # Call only the actual pure command builder on public values.
                # Never construct Codex or enter its authentication discovery.
                subject = SimpleNamespace(model_name=model, options=CodexOptions(),
                    _REMOTE_ACP_CODEX_HOME=Codex._REMOTE_ACP_CODEX_HOME,
                    _acp_codex_executable=None)
                command = Codex.acp_command(subject)
                config = next(value.removeprefix("CODEX_CONFIG=") for value in command
                              if value.startswith("CODEX_CONFIG="))
                import json
                effective = json.loads(config)["model"]
                if model == "openai/fixture":
                    result = fixture.plan(model=model)
                    self.assertEqual(result["graphcoder"]["agents"][0]["model_name"].split("/", 1)[1],
                                     effective)
                else:
                    self.assertNotEqual(model.split("/", 1)[1], effective)
                    with self.assertRaisesRegex(ValueError, "truncates nested model"):
                        fixture.plan(model=model)
        finally:
            fixture.doCleanups()

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
