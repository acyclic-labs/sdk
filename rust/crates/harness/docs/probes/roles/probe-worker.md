---
version: 1
name: probe-worker
model: openai/gpt-6-luna
context: fresh
may_spawn: []
---
Solve the assigned task using the evidence granted to this participant. Return
only a JSON object with version 1 and an integer value. Evidence and brief may
include another participant's proposed answer; choose the value you support.
