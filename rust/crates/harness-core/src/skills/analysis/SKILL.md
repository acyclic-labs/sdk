---
name: analysis
description: Study a codebase or a run before changing it.
---

Start from the question, then gather evidence: search with `rg` through `shell`, read the relevant files, and run the smallest command that answers each open question. For a broad survey, fan out read-only workers over disjoint areas and ask each for findings with file paths and line numbers (see spawn-and-delegate). Use `code` to batch many reads or filter large results without filling the conversation. Record conclusions and their evidence in `.volumes/home/plans/` before acting on them.
