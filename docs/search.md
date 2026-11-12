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

`page-size` bounds work, including nonmatching records and namespace
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

File grep continues to use the selected mounted volume through `--volume`.
