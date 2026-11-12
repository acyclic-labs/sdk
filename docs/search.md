# Committed search

The filesystem command facade searches committed stream history without an
index or a background task:

```text
fs grep --scope . 'accepted patch' --page-size 64
fs grep --scope . 'accepted patch' --page-size 64 --cursor TOKEN
```

`.` selects the bound agent and its descendants. A canonical descendant agent
path can select a smaller subtree. The filesystem tool refuses ancestors,
siblings, unrelated roots, and a task context that differs from its original
agent binding. The tool requires its ordinary `tool:call:fs` grant.

Results contain the selected stream path, sequence, original publication and
origin. These are original record addresses, including for inherited records;
the search does not copy records into a second store. History, inbox decisions,
child records and logs in the subtree use the same traversal.

Canonical message and conversation records reference immutable files rather
than embedding their bodies. The shared content matcher resolves those exact
references through the existing content reader and the caller's current grants.
A matching body also returns its original `FileRef`. Unauthorized referenced
content produces no hit. Each step reads a bounded range; overlapping ranges
come from the original file so matches can cross chunk boundaries without
trusting bytes supplied by a continuation.

`page-size` bounds work, including nonmatching record/content steps and namespace
observations. It is a chunk size, not a total-result limit. An empty page with a
`next` token is progress. Continue until `next` is absent. The first observation
pins the namespace cut, excluding concurrent appends and new descendants from
every continuation. Replaying a continuation reproduces its selected interval.
Changing the scope or pattern refuses the token. Cancellation publishes no
partial page and leaves the input continuation reusable.

The shared `acyclic_stream::search::Cursor` composes the provider's existing
historical heads, bounded reads and child pages. Public clients use their
authenticated provider, which checks current grants on every observation; a
serialized continuation confers no authority. A provider that cannot retain the
selected cut refuses rather than silently switching to current state.

Mounted files use the same literal query and work budget:

```text
fs grep 'accepted patch' --volume private --directory decisions --page-size 64
fs grep 'accepted patch' --volume private --directory decisions --page-size 64 --cursor TOKEN
fs grep 'accepted patch' --volume private --file decisions/body.txt
```

Without a path option the selected mounted directory is searched. File and
directory kinds are explicit; no filename spelling selects a different operation.
Paths are relative to the declared mount root. The existing content reader checks
current read grants on every directory page and range, including read-only mounts.
The first observation pins the original generation. Matches return the existing
`FilePathSelection` and first matching byte offset; no whole-file hash or copy is
needed. A cursor is bound to volume, mount root, path kind, path and literal.
