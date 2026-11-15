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
case reads its conversation entry and two adjacent indexed history boundaries;
directory pages read no descriptors. Both stream heads share the selected cut,
and descriptor reads use its revision floor and the current authenticated reader.
Missing or malformed descriptors fail; current generations are never substituted.
History uses version-three boundary descriptors: sequence zero commits with
adoption, and each later sequence names its exclusive conversation cut. The first
case reads that original boundary, so its preceding checkpoint is present too.
Fresh and forked volumes use their original `PreparedGeneration` identities.
Existing project volumes are retained at the head selected by adoption, with a
head condition in the same commit; a racing write refuses the entire adoption.
A workspace may predate the agent. Its birth generation and its later current
head never substitute for the agent's selected adoption generation. Other history
formats are refused; no migration or inferred initial checkpoint is used.

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

For Computed case files, fork the captured lazy snapshot, observe the selected
paths in that witness, and use `exactify_cut` for exactly those paths. It returns
a plain immutable generation or refuses unavailable original source versions.
Apply `inherit_paths` to that generation before building its transfer manifest.
Unrelated private file bodies need not be opened or copied to export the cases.
