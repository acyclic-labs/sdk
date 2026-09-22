# @acyclic-labs/arena

Race OpenCode workers on your own repo, let a decision model referee, keep the winner, and get a leaderboard of which model wins which kind of task in your codebase, at what cost.

```
npm i -g @acyclic-labs/arena        # or: npx @acyclic-labs/arena
export OPENROUTER_API_KEY=sk-or-...  # one key: Jev (the referee) and every worker model
cd your-repo && acyclic init         # forks and promote come from acyclic
arena doctor
arena race "add slugify(title) to util.py with a docstring" --models deepseek/deepseek-v4-flash,anthropic/claude-haiku-4.5 --test "pytest -q" --promote
arena run tasks.json --test "npm test"
arena board --badge badge.svg
```

## What happens in a race

1. N forks of the working tree: `acyclic fork -n N` overlay mounts if acyclic is set up, otherwise git worktrees.
2. One headless `opencode run` per fork, on the model you chose, or on the tier Jev routed the task to.
3. Your test command runs in each fork; its tail is captured.
4. Every fork's diff and test output go into one state. Jev answers in one request: which fork wins, is each safe to land, how complete is each.
5. If the winner clears the confidence and safety thresholds and its tests pass, `acyclic promote` lands it. The rest evaporate.
6. Every fork, worker cost, verdict, and promotion is a line in a hash-chained JSONL log.

`arena run tasks.json` without `--models` asks Jev to route each task first: complexity, kind, risk, reasoning, and whether parallel attempts help, mapped to a tier and a fan-out by a policy table you can edit.

## The board

`arena board` reads the log and prints, per task kind and model: races, win rate, average safety, test pass rate, and cost per race. `--badge` writes an SVG for your README. `policyFromBoard()` turns the board into routing: per kind, the model that has been winning, ties broken by cost.

## Requirements

- Node 20+ and [OpenCode](https://opencode.ai) on PATH.
- [acyclic](https://acyclic.dev) is optional. With it, forks are O(1) overlay mounts and promote is a three-way merge that lands even if the mainline moved. Without it, `arena` uses git worktrees, carries your uncommitted changes into each fork, and promotes by syncing the winner's changed files back into your tree, unstaged. `--forks acyclic|git` forces one; the default detects.
- `OPENROUTER_API_KEY` in the environment or a `.env` in the working directory. Jev is `typesafe/jev-1.13` on OpenRouter's Decisions endpoint; workers use `openrouter/<model>` through OpenCode.
- Headless OpenCode reads piped stdin when it is not a terminal and will wait forever; this package closes stdin for every worker. If you drive OpenCode yourself, pass `< /dev/null`.

## Honest notes

Jev's probabilities measured overconfident on public classification sets. Treat the board, which is grounded in your tests, as the source of truth, and the referee's confidence as a tiebreak. The log is labelled data in the exact shape decision models train on; keep it.
