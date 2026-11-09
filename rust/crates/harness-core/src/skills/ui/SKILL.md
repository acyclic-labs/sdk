---
name: ui
description: Customise the human view or ask for a decision through private ui/ files.
---
The default ui/index.md, status.md and paged children/ need no maintenance. Write ui/<path> in your private agent volume (outside the repo) to override that path; delete it to restore the default. An empty fenced `ui:default` block embeds the default at that point. Ask by writing a CommonMark fence with header `input <kind> id=<stable-id> [key=value ...]` and literal body; choose/multi use list items, text uses prose, recommended=<value> is optional. End your turn to wait. Replies arrive as one ordinary prompt containing {ui,generation,answers,text}; free-form text is always available, including for unknown kinds. Approval blocks use the existing approval decision path and cannot be authored to grant authority. Do not re-read your own ui/index.md: it repeats your context. Pull source files when needed. No active content or automatically followed links.
