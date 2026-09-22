# Repo Arena evals

A reproducible comparison of three ways to run the same task list on the same seed repo:

- **routed**: Jev classifies each task and picks a tier and fan-out (cheap by default for easy work).
- **frontier**: every task on the frontier model, one attempt.
- **raced**: every task raced between the cheap model and the frontier model, Jev picks.

The seed is a small Python shop module with real unit tests. Each task has its own hidden test that also runs the base suite; a task counts as landed only if its tests pass in the fork and the winner is promoted.

```
export OPENROUTER_API_KEY=sk-or-...
./run.sh                       # writes results/<timestamp>/{routed,frontier,raced}.jsonl and compare.md
ARMS="routed cheap" ./run.sh   # any subset of routed frontier raced cheap
```

Models default to `deepseek/deepseek-v4-flash` (cheap) and `anthropic/claude-sonnet-5` (frontier); override with `CHEAP=` and `FRONTIER=`.

The point is not that the cheap model always wins. It is that a 300 ms referee lets you send easy work to the cheap model and only pay frontier prices where the referee or the tests say you must, and that the log makes every verdict auditable.
