# Forced continuation v1

The harness can start a decider continuation from a retained boundary in its
own conversation stream. GraphCoder exposes the existing historical agent
fork through these execution flags:

```sh
graphcoder --workspace ./project --model PROVIDER/MODEL \
  --fork-from 'harness/agents/AGENT_ID@EVENT_OFFSET' \
  --override override.json --operation fork-1 \
  --output-format jsonl exec 'Continue from this decision point'
```

`AGENT_ID` is the canonical run/agent identity from the source trace.
`EVENT_OFFSET` is the zero-based, inclusive `sequence` of a record on that
agent's `/conversation` resource, also available through `graphcoder history`.
Use a retained settled boundary, never an execution payload's observation
sequence or a position inside an outstanding tool call. The harness selects
the workspace generation recorded at that event; callers never choose a
different generation. The same workspace/state owner must retain the source.

`--override` is optional. Its file is a versioned request, for example:

```json
{
  "version": 1,
  "override_item": {
    "kind": "assistant",
    "text": "Delegate these independent investigations.",
    "calls": [
      {"name": "spawn", "arguments": {"name": "a", "role": "worker", "task": "Investigate A. Own a.txt."}},
      {"name": "spawn", "arguments": {"name": "b", "role": "worker", "task": "Investigate B. Own b.txt."}}
    ]
  }
}
```

An instruction before the next freely sampled action uses
`{"version":1,"override_item":{"kind":"instruction","text":"Do this inline."}}`.
An assistant action may contain text, tool calls, or both. Calls use the usual
registered tool schemas, approvals and execution journal. After the first
action, the original model continues freely. `override_item: null` makes an
ordinary continuation without an intervention. No earlier events are rewritten.

Launch K independent normal runs by using K distinct operations; there is no
batch endpoint or shared continuation state:

```sh
K=4
CUT='harness/agents/AGENT_ID@EVENT_OFFSET'
for i in $(seq 1 "$K"); do
  graphcoder --workspace ./project --model PROVIDER/MODEL \
    --fork-from "$CUT" --override override.json --operation "fork-$i" \
    --output-format jsonl --usage-jsonl "usage-$i.jsonl" \
    exec 'Continue from this decision point' > "trace-$i.jsonl"
done
```

Reuse an operation with the same inputs to recover its existing fork and
settled outputs. A different operation creates an independent fork. Every run
uses the usual text/JSONL, artifact, cursor and usage outputs.

The library entry is `acyclic_harness::local::continuation::fork_from`, returning
a normal `Connection`. It composes the existing `AgentCommand::Fork` with
`continuation: Some(ContinuationRequest { version: 1, override_item })`;
the ordinary role/brief fields are unused for continuation. The original
decider's directive and model selection remain the run's role. Ordinary child
agents cannot be selected as v1 sources.

The stream fork excludes children. The new adoption's `profile.continuation`
records version 1, `parent_run_id`, `stream_offset`, `workspace_generations`,
the immutable `override_item` file reference, `live_children_at_cut`, and
`non_replay_manifest`. The request file retains the exact override. Stream and
workspace forks publish in the existing single filesystem composition commit.
Workspace forks use immutable generations, not directory snapshots.

The non-replay manifest names `wall_clocks`, `web_network`,
`external_machine_state`, and `model_sampling`: these are not restored exactly.
The live-children flag is captured at the source boundary, rather than inferred
from the parent's current children. Persisted agent records are version 14 and
generation-linked history descriptors are version 3; other versions are refused
without migration. History sequence zero is the original adoption boundary;
sequence N retains the context and project generations after N settled turns.
A fork at conversation tail N includes history through boundary N.
