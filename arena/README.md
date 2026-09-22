# Repo Arena

Race coding agents on your own repo, let a decision model referee, keep the winner, and get a leaderboard of which model wins which kind of task in your codebase, at what cost.

This is a separate product built on top of [acyclic](https://acyclic.dev) (forks, promote, checkpoints), [OpenCode](https://opencode.ai) (the workers), and TypeSafe's Jev via [OpenRouter](https://openrouter.ai/typesafe/jev-1.13) (the referee). It calls all three through their public interfaces and modifies none of them.

| Package | What | Docs |
|---|---|---|
| `packages/arena` | `@acyclic-labs/arena`, the Node CLI and library: `arena race`, `arena run`, `arena board` | [README](packages/arena/README.md) |
| `packages/jev` | `acyclic-jev`, the Python SDK: typed decisions, swarms, fork arena, gate, hash-chained log, evals | [README](packages/jev/README.md) |

```
cd packages/arena && bun install && bun x tsc -b
export OPENROUTER_API_KEY=sk-or-...
cd your-repo && acyclic init
arena race "add slugify() to util.py with a test" --models deepseek/deepseek-v4-flash,anthropic/claude-haiku-4.5 --test "pytest -q" --promote
arena board --badge badge.svg
```

## Requirements

acyclic and OpenCode on PATH, Node 20+, an OpenRouter key. Nothing here needs a modified acyclic or OpenCode.
