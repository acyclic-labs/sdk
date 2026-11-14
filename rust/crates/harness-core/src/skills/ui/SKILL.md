---
name: ui
description: Customise the human view through private ui/ files.
---
Use the ordinary file tools at `volume:///private/ui/<path>`. This is an authenticated tool address, not a directory mounted in the cwd; managed private outputs remain read-only.
The default ui/index.md, status.md and paged children/ need no maintenance. Write ui/<path> in your private agent volume (outside the repo) to override that path; delete it to restore the default. An empty fenced `ui:default` block embeds the default at that point. Approval blocks use the existing approval decision path and cannot be authored to grant authority. Do not re-read your own ui/index.md: it repeats your context. Pull source files when needed. No active content or automatically followed links.
Reads of your own UI files return a `self-projection: true` pointer by default.
Use `read` with `include_body: true` only when you explicitly need that body;
the result remains marked. Project files named `ui/` are ordinary source files.
In programs, `read(path)` is an explicit request for the complete file body.
