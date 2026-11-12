---
name: memory
description: Retain evidence and reusable skills in the agent's private volume, and recover them on later turns or in a fork.
---

Use ordinary file tools to keep durable working knowledge under `volume:///home/memory/`. Record observations, decisions, uncertainty and references to the original evidence. Keep secrets out of memory and skill files. Read the relevant file when needed; storing a note does not insert it into the model's context.

For a repeatable workflow, write `volume:///home/skills/<name>/SKILL.md` with YAML frontmatter containing `name` and `description`. Use a lowercase hyphenated name. The description is discovered without loading the body; load the body explicitly when using the skill. Put supporting files beside it and reference them from the skill. Use the writing-skills skill for authoring guidance.

These are ordinary versioned volume files. A child inherits the files at its fork cut; later parent and child edits remain independent. Read inherited evidence before repeating work, check it against current state, and update your own files when the evidence changes. Private memory and skills stay outside the project and are not merged into its checkout.
