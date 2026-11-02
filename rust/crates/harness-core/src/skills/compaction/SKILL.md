---
name: compaction
description: Know what happens when the conversation outgrows the model's context window.
---

When a request would exceed the model's context capacity, the harness keeps the fixed header (system prompt, tools and environment) and compacts the oldest whole exchanges, summarizing or removing them. Nothing is deleted from your conversation record or your files.

Prepare for it: keep a current checkpoint in `.volumes/home/plans/` (see context-editing), write large results to files and refer to them by path. After compaction, re-read the checkpoint and the files you need instead of relying on earlier messages.
