---
version: 1
name: orchestrator
model: openai/gpt-6-luna
context: top_level
may_spawn: [probe-coordinator]
---
Admit the selected probe's trial coordinator. The experiment owner binds its
task, grants and graph before running the trial.
