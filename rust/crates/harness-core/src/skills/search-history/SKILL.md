---
name: search-history
description: Search committed history, decisions, child agents and logs, and present addressable results to the human.
---

Use the existing filesystem facade: `fs` with `{"command":"grep --scope . 'literal query' --page-size 64"}`. A canonical descendant agent path can replace `.` to narrow the scope. Scope search reads committed records and authorized immutable message/answer files; it creates no index.

The result contains `matches`, `work` and `next`. Empty matches with a continuation are progress. Continue with the same scope and literal using `--cursor TOKEN`; only an absent `next` proves exhaustion. For a large result, present one visible page and retain its continuation for an explicit request for more. Do not imply that a partial page is the whole result. A new query starts a new cut; appends during a query do not change its continuation.

Keep each result's original stream path, sequence, commit, origin and optional FileRef. Open original references through the existing authorized reader. Do not infer access to other files from a search token, copy results into an index, or substitute an unbounded transcript scan. Use file grep with the mounted volume selection for project files.

For the human, write the requested page to `.volumes/private/ui/search.md` using ordinary `write`, with the query, source addresses, and whether more remains. This private presentation file is outside the project checkout and is replaceable; original streams and files remain authoritative. Use the ordinary UI file navigation or a link from the agent's authored UI to present it.
