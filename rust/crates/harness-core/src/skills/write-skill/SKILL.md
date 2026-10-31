---
name: write-skill
description: Package a reusable workflow as an ordinary writable skill and scripts.
---
Create skills/<name>/SKILL.md in your private volume. Use YAML front matter with a concise description; add an input schema and runtime only when the skill needs a callable interface. Write short instructions describing when to use the skill, its required inputs and its observable result. Put executable details in scripts beside the file. Exercise scripts through the normal shell or code-mode tools and approval policy. Use relative paths. Do not store credentials or add a second tool registry. Share the skill by merging its files into the project, preserving unrelated work. Discovery uses filesystem changes; do not poll or restart the agent to refresh a catalog.
