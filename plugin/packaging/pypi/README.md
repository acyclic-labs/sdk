# acyclic-pydantic-ai

Recursive decomposition and speculative execution for
[Pydantic AI](https://ai.pydantic.dev) agents, on
[acyclic](https://acyclic.dev) forked workspaces.

- **`fork(ctx)`** gives a piece of delegated work its own fork of the calling
  agent's workspace. On success the fork is merged into its direct parent; if
  the work raises, the fork and everything it forked are discarded and the
  parent is untouched. Forks fork their own children, to any depth.
- **`speculate(agents, prompt, parent=..., check=...)`** runs several attempts
  at once, each in its own fork, merges the one that passes `check`, and
  discards the rest.

```python
from pydantic_ai import Agent, RunContext
from acyclic_pydantic_ai import Session, Workspace, fork, speculate, workspace_tools

worker = Agent("anthropic:claude-sonnet-5", deps_type=Workspace, toolsets=[workspace_tools()])
planner = Agent("anthropic:claude-opus-5", deps_type=Workspace)

@planner.tool
async def delegate(ctx: RunContext[Workspace], part: str) -> str:
    """Hand one independent part of the task to a worker in its own fork."""
    async with fork(ctx, name="part") as ws:
        result = await worker.run(part, deps=ws)
    return result.output

@planner.tool
async def attempt_risky_step(ctx: RunContext[Workspace], step: str) -> str:
    """Try a risky step three ways; keep the one whose tests pass."""
    outcome = await speculate([worker, worker, worker], step, parent=ctx, check="pytest -q")
    return "no attempt passed" if outcome.winner is None else outcome.output

async with Session.open(".") as session:
    await planner.run("Move billing to the v2 payments API", deps=session.root)
```

## How it works

The engine is the `acyclic` binary (0.1 or newer). Coding hosts such as
Claude Code fire its lifecycle hooks themselves; Pydantic AI has no subagent
lifecycle, so this package fires them explicitly as the `sdk` host:
`SessionStart`, then per fork `PreToolUse(Agent)` and `SubagentStart`, then
`SubagentStop`. Merge and discard are `acyclic git merge agents/<id>` and
`acyclic discard agents/<id>`, run from the parent's path; the engine refuses
them from anywhere else.

Each `Workspace` has a `path`. The engine does not rewrite a Pydantic AI
tool's paths for it, so tools must work relative to `ctx.deps.path`.
`workspace_tools()` provides `read_file`, `write_file`, `list_files` and
`run_shell` that do exactly that and refuse paths outside the workspace.

## Choosing the winner

`check` is a shell command run inside each fork (exit 0 passes) or a callable
taking the fork's `Workspace`. `choose` picks among the attempts; the default,
`fewest_changes`, takes the passing attempt that changed the fewest paths.
Pass your own to use a judge model:

```python
outcome = await speculate(agents, step, parent=ctx, check="pytest -q",
                          choose=lambda attempts: my_judge(attempts))
```

Every `Attempt` carries its output, error, check result and output, changed
paths, and the run's token usage.

## Limits

- One acyclic session per repository root at a time. If another session (a
  Claude Code session with acyclic installed, say) is active on the same
  root, merging into the root fails with "cwd belongs to multiple Acyclic
  sessions".
- On a conflicting merge `MergeConflict` is raised after trying
  `acyclic git merge --abort`. If the abort itself fails, `aborted` is False
  and the parent still has the merge pending.
- On macOS the engine serves forks over NFS, where macOS may add `._name`
  AppleDouble files inside a fork. The engine never merges them (unless the
  repository tracks such a path).

## Development

```sh
pip install -e ".[dev]"
pytest                                   # protocol tests against a fake engine
ACYCLIC_LIVE_BIN=/path/to/acyclic pytest # also the live tests against the real engine
```

The live tests default to this checkout's `target/debug/acyclic` when it
exists, and keep all engine state in a temporary `XDG_STATE_HOME`.
