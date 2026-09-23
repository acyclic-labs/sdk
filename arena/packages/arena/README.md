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
5. The winner is landed if its tests pass and Jev's confidence clears the threshold. Tests are the ground truth: a passing probe overrides the referee's safety doubt, a failing one overrides its confidence, and the referee decides alone only when no test command was given. The rest evaporate.
6. Every fork, worker cost, verdict, and promotion is a line in a hash-chained JSONL log.

`arena run tasks.json` without `--models` asks Jev to route each task first: complexity, kind, risk, reasoning, and whether parallel attempts help, mapped to a tier and a fan-out by a policy table you can edit.

## The board

`arena board` reads the log and prints, per task kind and model: races, win rate, average safety, test pass rate, and cost per race. `--badge` writes an SVG for your README. `policyFromBoard()` turns the board into routing: per kind, the model that has been winning, ties broken by cost.

## Requirements

- Node 20+ and [OpenCode](https://opencode.ai) on PATH.
- [acyclic](https://acyclic.dev) is optional. With it, forks are O(1) overlay mounts and promote is a three-way merge that lands even if the mainline moved. Without it, `arena` uses git worktrees, carries your uncommitted changes into each fork, and promotes by syncing the winner's changed files back into your tree, unstaged. `--forks acyclic|git` forces one; the default detects.
- `OPENROUTER_API_KEY` in the environment or a `.env` in the working directory. Jev is `typesafe/jev-1.13` on OpenRouter's Decisions endpoint; workers use `openrouter/<model>` through OpenCode.
- Headless OpenCode reads piped stdin when it is not a terminal and will wait forever; this package closes stdin for every worker. If you drive OpenCode yourself, pass `< /dev/null`.

## What leaves your machine

Every race posts the rendered state to OpenRouter: the task text, each fork's changed paths, unified diffs of the changed files (capped at `maxDiffChars`, default 1500 per file), and the last 12 lines of test output. Credential-shaped strings (API keys, AWS and GitHub tokens, `token=`/`password=` pairs, private key blocks) are scrubbed first; pass `redact: false` to turn that off. The scrubber is pattern-based, so keep secrets out of files a worker might touch and out of test output rather than relying on it. `--save-states` writes the same rendered state to disk.

## Honest notes

Jev favours the fork it sees labelled "A": identical forks scored 0.95 / 0.05 in a self-race. The arena therefore asks twice, once per fork order with the forks relabelled by position, and averages. Identical forks now score 0.50 / 0.50 and real differences still come through at 0.99. Set `ARENA_ROUTE=aggressive` for the routing thresholds that beat a fixed cheap model on the twelve-task eval; the defaults over-escalate. See `arena/evals/REPORT.md` for every experiment.


Jev's probabilities measured overconfident on public classification sets. Treat the board, which is grounded in your tests, as the source of truth, and the referee's confidence as a tiebreak. The log is labelled data in the exact shape decision models train on; keep it.
