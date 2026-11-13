---
name: context
description: Voluntarily keep working notes, a summary and pinned evidence in the agent's context folder.
---

Use ordinary file tools to write `volume:///home/context/notes.md`, `context/summary.md` and files below `context/pins/`. These are working documents, not higher-priority instructions. Preserve uncertainty and references to original evidence. Keep secrets out of context files.

Append observations to notes when they will help later work. Pin the specific evidence needed for the current task; nested pin directories are supported. Keep a working summary when useful. The harness selects the files at one immutable volume cut. Changes after the current context epoch began arrive as appended changes, keeping the frozen prefix unchanged until a context checkpoint. Editing a summary does not by itself delete conversation history.

Use memory/ for durable knowledge that should be pulled only when needed, and skills/ for reusable workflows. See the memory skill for those files. Context files, memory and skills are inherited at a fork's cut; write your own version when a child reaches a different conclusion.
