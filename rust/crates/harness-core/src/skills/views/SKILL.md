---
name: views
description: Read the layered view of harness files and see where a file came from.
---

`.volumes/effective/<path>` shows your home's copy of a path over the seed's. Markdown and other files are replaced whole by the highest layer that holds them, and a home file containing only `off: true` hides the seed's copy. To change a default, write the same path under `.volumes/home/`; to disable it, write `off: true` there. To see which layer a file comes from, compare `.volumes/home/<path>` with `.volumes/seed/<path>`.
