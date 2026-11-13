# Working context and memory

An agent owns `context/`, `memory/` and `skills/` in its home volume. Ordinary file operations address them through `volume:///home/`. A fork inherits the original immutable volume cut; subsequent home writes belong to the selected agent. Memory and authored skills are read on demand through the ordinary filesystem and skill discovery paths.

The context manifest selects `context/notes.md`, `context/summary.md` and all files under `context/pins/` at one home-volume generation. Pin directories are paged without a total-count limit. Configured byte or admission bounds cause an explicit error rather than a partial document selection. Context documents enter as assistant data and cannot grant authority or replace system instructions.

Between context checkpoints the initial prefix remains frozen. A changed document appends a `context.changed` notice identifying its immutable revision. A small textual change includes its contents; larger content remains an immutable file reference. Changes never mutate an earlier request. The optional `context` skill teaches the worker to append notes and pin evidence; no housekeeping worker turn is required.

Capacity management reserves the selected provider's verified maximum response size. Input admission uses `context_tokens - output_tokens`; unknown response capacity or an exhausted window is an error. There is no threshold percentage or fixed recent-message budget.

At capacity pressure a separate summary-purpose inference selects a rewrite of the eligible older prefix. Its response is versioned JSON:

```json
{"version":1,"drop_through":3,"summary":"Facts, decisions and evidence references."}
```

`drop_through` is an exclusive source-message index, positive and within the supplied source count. The cut must preserve complete native and tool dependencies. The current input, required instructions and pinned context remain protected. The continuing worker receives the selected summary and retained messages through the same context pipeline. The manager inference has its own usage and provenance and does not increment the worker's step count. Invalid or insufficient rewrites fail explicitly without authorizing truncation or a worker dispatch.

When the eligible history exceeds the manager window, the manager receives capacity-admitted windows of complete dependency groups. Later windows include the preceding rolling summary. Each part has an independent journal and usage identity at the same worker step; recovery reuses the original source cut and selected window. A summary-only rewrite must become smaller. Only a final rewrite that admits the continuing worker can publish the new context; an indivisible source group that cannot fit fails explicitly.

A successful managed rewrite conditionally publishes `context/summary.md` and `context/drops/selected.json` together. The version-one drops record identifies the operation, step, source digest, cut and immutable summary reference. Publication uses the file versions captured before inference; concurrent edits conflict rather than being overwritten. Recovery reuses the original staged intent and publication receipt, including after a later user edit. A failed file publication cannot commit a compacted conversation projection.

Editing a summary alone does not delete conversation history. The drop record is a receipt of a managed rewrite, not an executable instruction in arbitrary authored JSON. Conversation checkpoints and immutable journal provenance remain the source of history selection.
