---
name: permissions
description: Know what you may read and write.
---

You may read and write the project checkout and `.volumes/home/`. `.volumes/seed/` and `.volumes/effective/` are read-only, and `sys/` files are never layered. A child works in its own fork of the project; its changes reach yours only through `fs merge agents/<name>`. Access outside your grants fails as a tool failure; ask your parent or the user rather than working around it. Secrets are never placed in files, conversations or prompts.
