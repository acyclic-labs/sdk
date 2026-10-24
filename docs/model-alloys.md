# Model alloys

An alloy names an ordered set of catalog models and a selection policy. Use its
name anywhere GraphCoder accepts a model selection:

```sh
graphcoder --catalog models.json --model mixed -p 'Review the changes'
graphcoder --catalog models.json --model mixed
```

The member identities must already exist in the catalog. Add an `alloys` object
beside the provider objects in `models.json`:

```json
{
  "alloys": {
    "mixed": {
      "models": ["a/planner", "b/worker"],
      "select": "round_robin"
    }
  }
}
```

Alternatively, define the alloy in the settings root's `.codex/config.toml`.
This example also makes it the default model:

```toml
model = "mixed"

[alloys.mixed]
models = ["a/planner", "b/worker"]
select = "round_robin"
```

Each model step selects a member. The operation identity, persisted step ordinal
and agent role determine the selection; the journal retains the selected model
before generation. A retry or resumed step uses that retained model. A new prompt
starts its own step sequence. Selecting `a/planner` directly creates a one-member
alloy with the same execution path.

The policies are:

| `select` value | Selection |
| --- | --- |
| `"first"` | Always use the first member. |
| `"round_robin"` | Cycle through the members in order. |
| `{"weighted":{"weights":[1,3]}}` | Repeat the weighted cycle, with its starting position derived from the operation identity. Zero weights exclude members from selection. |
| `{"per_role":{"roles":{"root":0,"child":1},"fallback":1}}` | Select the indexed member for the agent's role; use the fallback for other roles. Indices start at zero. |

Role defaults and policies can be configured together:

```toml
model = "team"

[roles]
root = "planner"
child = "worker"

[alloys.team]
models = ["a/planner", "b/worker"]

[alloys.team.select.per_role]
roles = { planner = 0, worker = 1 }
fallback = 1
```

The `spawn` tool's optional `role` argument overrides the child default. Roles
belong to agents, independently of user and assistant message roles.

All members must support the features required by the task. Admission and
compaction use the smallest declared context and output capacities across the
members. Tool calls and results retain their canonical identities across member
changes. Provider reasoning follows the normal provider and account scope rules.

The TUI shows the member serving each step. Headless JSONL publishes the member
with step completion metadata; the final result's `metadata.members` object
contains usage totals keyed by `provider/model`. Reopening reconstructs those
totals from the persisted steps.

The coordinator's live acceptance driver is `scripts/live/alloy.sh`. It requires
runtime credentials, a catalog and the shipping executable through environment
variables. Its Anthropic member defaults to `anthropic/claude-haiku-5-5`.
