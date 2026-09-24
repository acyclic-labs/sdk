"""File and shell tools confined to the calling agent's workspace.

Coding hosts get their tool paths rewritten into a child's mount by the
engine; a Pydantic AI agent's tools are ordinary Python, so they have to be
pointed at ``ctx.deps.path`` instead. These are that, ready-made. Paths that
resolve outside the workspace are refused.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from pydantic_ai import FunctionToolset, RunContext

from ._forkjoin import workspace_of


def _inside(ctx: RunContext[Any], path: str) -> Path:
    root = workspace_of(ctx).path.resolve()
    target = (root / path).resolve()
    if target != root and root not in target.parents:
        raise ValueError(f"{path!r} is outside this agent's workspace")
    return target


def workspace_tools(*, shell: bool = True, shell_timeout: float = 300.0) -> FunctionToolset[Any]:
    """read_file, write_file, list_files and (optionally) run_shell, all
    relative to the workspace in the agent's deps."""
    toolset: FunctionToolset[Any] = FunctionToolset(id="acyclic-workspace")

    @toolset.tool
    async def read_file(ctx: RunContext[Any], path: str) -> str:
        """Read a text file, relative to your workspace."""
        return _inside(ctx, path).read_text(errors="replace")

    @toolset.tool
    async def write_file(ctx: RunContext[Any], path: str, content: str) -> str:
        """Create or overwrite a text file, relative to your workspace."""
        target = _inside(ctx, path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        return f"wrote {len(content)} characters to {path}"

    @toolset.tool
    async def list_files(ctx: RunContext[Any], directory: str = ".") -> list[str]:
        """List files under a directory of your workspace (skips .git)."""
        root = workspace_of(ctx).path.resolve()
        base = _inside(ctx, directory)
        return sorted(
            str(p.relative_to(root))
            for p in base.rglob("*")
            if p.is_file() and ".git" not in p.relative_to(root).parts and not p.name.startswith("._")
        )

    if shell:

        @toolset.tool
        async def run_shell(ctx: RunContext[Any], command: str) -> str:
            """Run a shell command with your workspace as the working directory."""
            proc = await asyncio.create_subprocess_shell(
                command,
                cwd=str(workspace_of(ctx).path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            try:
                out, _ = await asyncio.wait_for(proc.communicate(), shell_timeout)
            except asyncio.TimeoutError:
                proc.kill()
                return f"timed out after {shell_timeout:.0f}s"
            return f"exit {proc.returncode}\n{out.decode(errors='replace')[-8000:]}"

    return toolset
