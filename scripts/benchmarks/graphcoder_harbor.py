"""Harbor installed agent using immutable, prebuilt GraphCoder artifacts."""

import json
import shlex
import tempfile
import uuid
from pathlib import Path

from harbor.agents.installed.base import BaseInstalledAgent, with_prompt_template
from .contracts import Artifact, catalog, run_command


class GraphCoder(BaseInstalledAgent):
    """Harbor 0.24 installed-agent interface, with no optional execution path."""

    @staticmethod
    def name() -> str:
        return "graphcoder"

    def __init__(self, *args, graphcoder_binary: str, graphcoder_sha256: str,
                 source_sha: str, base_url: str, context_tokens: int,
                 output_tokens: int, **kwargs):
        self.artifact = Artifact.read(graphcoder_binary, graphcoder_sha256, source_sha)
        if kwargs.get("version") not in (None, source_sha):
            raise ValueError("GraphCoder version must be the artifact source SHA")
        kwargs["version"] = source_sha
        super().__init__(*args, **kwargs)
        if self.extra_env:
            raise ValueError("GraphCoder benchmark execution accepts no inherited agent env")
        if self.mcp_servers or self.skills_dir:
            raise ValueError("this benchmark adapter does not project task MCP/skills")
        self.provider_catalog = catalog(self.model_name, base_url, context_tokens, output_tokens)
        self.state = "/installed-agent/graphcoder-" + uuid.uuid4().hex
        self.binary = self.state + "/graphcoder"
        self._installed = False

    async def install(self, environment):
        self._installed = False
        # Task images must provide the native mount prerequisites. Never alter
        # the task image, relax its isolation, or build a different executable.
        await self.exec_as_agent(environment, command=(
            "test -c /dev/fuse && test -r /dev/fuse && test -w /dev/fuse"
            " && command -v fusermount3 >/dev/null && command -v sha256sum >/dev/null"
            " && test \"$PWD\" != /"))
        await self.exec_as_root(environment, command="mkdir -p " + shlex.join([
            self.state + "/home", self.state + "/fixture-codex", self.state + "/tmp"]))
        with tempfile.TemporaryDirectory(prefix="graphcoder-artifact-") as temporary:
            root = Path(temporary)
            binary = root / "graphcoder"
            binary.write_bytes(self.artifact.contents)
            provider = root / "catalog.json"
            provider.write_text(json.dumps(self.provider_catalog) + "\n")
            await environment.upload_file(binary, self.binary)
            await environment.upload_file(provider, self.state + "/catalog.json")
        checksum = self.artifact.sha256 + "  " + self.binary + "\n"
        await self.exec_as_root(environment, command=(
            "printf %s " + shlex.quote(checksum) + " | sha256sum -c - && chmod 755 "
            + shlex.quote(self.binary)))
        if environment.default_user is not None:
            await self.exec_as_root(environment, command="chown -R " + shlex.join([
                str(environment.default_user), self.state]))
        # Even --version sees only this trial's fixture environment.
        await self.exec_as_agent(environment, command=shlex.join([
            "env", "-i", "PATH=/usr/bin:/bin", "HOME=" + self.state + "/home",
            "CODEX_HOME=" + self.state + "/fixture-codex", self.binary, "--version"]))
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        (self.logs_dir / "artifact.json").write_text(json.dumps({
            "source_sha": self.artifact.source_sha, "sha256": self.artifact.sha256,
            "runner": "harbor==0.24.0", "native_mount_prerequisite": "checked",
            "model_calls": "not part of setup",
        }, indent=2) + "\n")
        self._installed = True

    @with_prompt_template
    async def run(self, instruction, environment, context):
        if not self._installed:
            raise RuntimeError("GraphCoder artifact was not installed and verified")
        await self.exec_as_agent(environment, command=run_command(
            self.binary, self.state, str(self.environment_logs_dir), self.model_name, instruction))
