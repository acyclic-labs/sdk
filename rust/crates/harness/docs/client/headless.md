# Headless event output

`graphcoder exec` uses the ordinary conversation driver and authenticated session.
`--output-format jsonl` projects its original streams and execution journals.
There is no separate execution history or headless server.

```sh
graphcoder --operation review-17 --output-format jsonl \
  --event-cursor review-17.cursor.json exec 'Review the selected changes'
```

Select the same session when resuming, using its original connection and
`--agent` selection where needed. Keep the operation, prompt bytes and input IDs
unchanged. The cursor is bound to its operation, selected agent and followed
resources; applying it to a different selection fails before input dispatch.

## Input and completion

Without a positional task, `exec` reads one JSON request per stdin line and
requires `--operation`. Each request selects exactly one action:

- `{"prompt":"Review the changes","id":"review"}` delivers a prompt.
- An approval request supplies `approve` (the original request ID), `binding`
  (the shown operation and action digest) and `approved` (a boolean). Copy the
  binding from the complete original request; an ID alone cannot authorize it.
- `{"cancel":true}` requests cancellation through the same agent inbox.

Unknown fields, mixed actions, missing approval bindings and `cancel:false`
are refused. Omitted prompt IDs are assigned in input order; replay therefore
requires the same request order. Explicit IDs are preferable for resumed input.
A prompt's delivery identity is `operation:id`; exact retries return its original
committed result. A changed request under the same identity is a conflict.

With a positional task and `--input-format jsonl`, stdin can supply approval
decisions or cancellation. Without `--output-format jsonl`, each pending
approval prints `{"approval":ID,"binding":BINDING}` to answer it with. A
reconnecting text-input run answers the agent's current pending approval even
when its saved event cursor is already past the announcing status.
With JSONL input, EOF ends input admission and waits for accepted prompts to
settle. Completion also drains the finite opening cuts of the selected follows;
later appends do not extend those cuts. Exit status is zero only when every
settled prompt succeeded. Failed prompts and unresolved delivery outcomes return
a nonzero status.

## Version-one output

Each exec event is one compact JSON object followed by a newline. Its `version`
is `1`, and `operation` identifies the caller's invocation. Consumers dispatch
on `kind`; stream control observations remain observable alongside records.

| Kind | Contents and identity |
| --- | --- |
| `record`, `opened`, `watermark`, `moved`, `closed`, `control` | Original `resource`, optional record `sequence`, exclusive `after` cursor and standard padded base64 `frame` containing the canonical SessionFrame protobuf. `value` is decoded application JSON when available; binary records have no JSON value. |
| `execution` | Original `agent`, inbox `input`, canonical execution `record` and exclusive execution `after`. The record retains its original turn operation and sequence. |
| `execution_payload` | Original agent, input, turn, sequence, payload field and immutable file reference, with byte `offset`, standard padded base64 `data` and execution `after`. |
| `approval_payload` | Original status `resource` and `sequence`, complete request `length`, byte `offset`, standard padded base64 `data` and status `after`. |
| `result` | Prompt `id`, accepted `inbox` when available, and its settled output or error. A cancelled turn is an error; its partial output never becomes a result or final artifact. |
| `approval` | Identity of a request requiring a decision. Its complete request arrives through the original status and bounded approval payloads. |
| `completed` | Final `success` after accepted input and the selected finite cuts settle. |
| `error` | Terminal `code`, failing `step`, diagnostic `error` and `recovery` command. Argument-parser failures precede this protocol; a lost stdout reader may prevent delivery of the final diagnostic. |

Ordering is authoritative within each resource or original execution journal.
The multiplexed output does not imply a total commit order across resources.
The frame preserves binary application data and record headers; the readable
JSON value is a projection of that data.

## Acknowledgements and replay

An `after` position acknowledges complete flushed output. Execution observations
with referenced payloads retain the previous position until the last payload
chunk flushes. Approval statuses follow the same rule. An interrupted observation
may therefore repeat its header and any chunks already written.

Deduplicate stream records by resource and sequence, execution observations by
original turn and sequence, execution chunks by agent, turn, sequence, field and
offset, and approval chunks by status resource, sequence and offset. Result lines
can repeat when the caller retries an already settled prompt; correlate their
original prompt identity.

`--event-cursor PATH` loads exclusive provider cursors and one selected execution
position. After a complete acknowledgement, it replaces the cursor file through
a temporary sibling. The original owned output write completes before replacement, so interruption
can repeat output but cannot acknowledge an unwritten observation. Replacement
provides atomic file visibility, without a power-loss durability guarantee. The
native writer is unbuffered: the original descriptor and frame stay with its
producer through completion, and acknowledgement adds no flush or durability
operation. Losing the observer consumes that writer; it cannot reopen a second
writer and advance the cursor for an uncertain line.
Provider cursor bytes are opaque and must be returned unchanged.

Payloads are authenticated through their original publication and read in bounded
ranges under current authority. Missing retained data, revoked authority and a
closed producer fail explicitly. No full-payload fallback or readiness polling is
used. Backpressure waits on the inherited output's OS event.

## Final artifacts and browsing

`--output-schema schema.json` admits the schema before opening a harness, then
validates each final JSON value through the shared `model::OutputSchema` validator.
`--output answer.json` atomically replaces the destination only after successful
validation and complete serialization; a rejected result preserves the previous
artifact. This is final-result validation. Provider-native constrained generation
requires the selected provider's own enforcement contract.

`graphcoder sessions` reads one bounded page of the selected agent's direct
children through `children_page`. `graphcoder history` seeks directly to the
newest compact turn summaries. Its opaque `next` continuation visits older turns
under the same frozen opening cut; a fresh initial query includes new appends.
Pass either continuation unchanged using `--cursor`. Select and follow only
visible agents through the existing client; neither command constructs a whole
tree or walks preceding history pages to reach the newest turn.

History rows contain an exclusive `conversation` cut, `context_tail` and
`volume_count`; saved roots remain at the source. A selected turn is
`conversation - 1`, never its position in the displayed page. For a file restore,
capture expected current roots through `ProjectHeads`; saved target generations
cannot stand in for the current roots being approved.
