---
name: learn-from-evals
description: Inspect original evaluation cases and propose a reviewed skill improvement.
---

Read the selected agent's private evals/ directory with ordinary paged file
queries. Open only cases relevant to the requested improvement. Each case names
its original context cut, human decision input and settled outcome. Resolve the
original referenced files at their pinned versions through the existing reader.
Keep the source addresses with the proposal so a reviewer can reproduce it.

Use the authored decision and outcome; do not infer approval from a word in an
answer. Separate a demonstrated repeated lesson from a single case. Propose the
smallest general change to skills/<name>/SKILL.md with the ordinary write tool,
and explain its evidence in an authored ui/ document for human review. A later
run discovers the skill through normal instruction discovery.

Read cases only when asked to perform this review. Do not copy them into a new
index, telemetry store, persistent summary database or automatic context feed.
An export selects the requested original context and outcome scope. Host
credentials and provider configuration are not evaluation inputs.
