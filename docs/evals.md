# Evaluation files

Evaluation cases are computed from original settled turns. They have no database,
background index or separate publication. `evals::StreamProvider` composes the
authenticated conversation provider with the filesystem's existing `Computed`
adapter. Bind that source at the private `evals/` route, outside the project cwd.

The root is a paged directory. Names are the canonical twenty-digit conversation
sequence followed by `.json`. Listing uses the pinned tail; it does not decode
turns or load their bodies. An exact lookup/open prepares only that turn. A file
range serializes directly into the requested output interval.

The version-one JSON case contains:

- `cut`: original conversation prefix plus its preceding history checkpoint;
- `source`: original sequence, origin and publication identity, including forks;
- `decision`: admitted inbox position, typed input and steering transcript;
- `outcome`: settled status, original output FileRef, tool-result directory and
  the original history checkpoint after settlement.

History checkpoints are the existing typed descriptors, not copies of volume
contents. They name the selected model-context stream/checkpoint and exact
retained project generations, including after a context restore. Opening one
case reads its conversation entry and at most two indexed history descriptors;
directory pages read no descriptors. Both stream heads share the selected cut,
and descriptor reads use its revision floor and the current authenticated reader.
Missing or malformed descriptors fail; current generations are never substituted.
The first case has no preceding settled checkpoint (`cut.checkpoint` is null).
An exporter must resolve that initial state from original creation custody; null
does not authorize using current project heads.

Human corrections and overrides retain their exact authored input. Approval
inputs retain their request identity. No classifier guesses acceptance from prose.
The output reference retains the original answer and its cost metadata; cases do
not create a second cost total. Resolve source files through current content
authority and their original immutable versions.

A selected cut excludes later appends. Every demand, including directory-only
requests, authenticates the retained tail through the current stream provider.
References and cursors confer no authority. Expired cuts fail rather than
substituting current history. A subscription starts one conversation follow;
there is no follow or task for an idle projection. Notifications are events and
retirement releases the original subscription owner.

Host credentials and provider configuration are outside the case input types.
Secret custody remains at the original execution boundary; exports must select
case/context/outcome sources explicitly and must not package an entire private
volume or host configuration. Text matching is not a substitute for custody.

Agents may pull cases, inspect original decisions and outcomes, and propose an
ordinary `skills/<name>/SKILL.md` write for review through `ui/`. Cases are never
automatically inserted into model context.

Scope export composes the existing immutable filesystem operations. Resolve the
selected original context/outcome generations with current authority, then use
`ForkOptions::from_generation(...).inherit_paths(...)` to select only the scope's
files. Export the selected fork's pinned checkout using `export_manifest` and
page its canonical closure with `export_generation_batch`. Do not export the
unfiltered source manifest: a post-export filename filter cannot remove bodies
already present in a transfer closure. Selection uses the original pinned
generation even when the source changes concurrently. Parent generation IDs are
provenance; omitted parents' file bodies are not part of the selected closure.
