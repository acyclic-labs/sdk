---
name: files-and-streams
description: Read, write and find files across the checkout, your home and the harness layers.
---

Paths are relative to the project checkout. `read` returns text, images, PDFs or notebook cells with an opaque version; pass that version to later reads and to `edit` to pin them or reject edits against a changed file. `write` creates or replaces a file and `edit` replaces exact matches. Use `shell` for search and builds (rg, git, test runners); `code` runs JavaScript that batches many tool calls and keeps intermediate results out of the conversation.

Volumes are mounted under `.volumes/`:
- `.volumes/home/` is your private, retained home. It is never merged into the project. Large tool output spills to `.volumes/home/tool-output/`.
- `.volumes/seed/` holds the harness defaults (skills, `AGENT.md`, `sys/`). It is read-only.
- `.volumes/effective/<path>` reads your home's copy of a path over the seed's (file tools only). It is read-only; write an override under `.volumes/home/<path>`.

A child agent works on its own fork of your project; inspect it with `fs diff agents/<name>` and take it with `fs merge agents/<name>` (see spawn-and-delegate). Every message and tool result is appended to your conversation: re-reading a file appends a new result rather than changing an earlier one.
