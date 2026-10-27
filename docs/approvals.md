# Approval policies and files

Approvals are part of the harness. Every tool invocation selected for approval
creates an immutable request in its task volume before dispatch. A decision must
name that request's exact SHA-256, current version and authenticated principal.
Changing the tool, call identity or arguments requires a new request. Accepted
storage writes do not authorize execution until both the interaction resolution
and its decision file have durable receipts.

`LocalHarnessConfig::approval_policy` selects an ordered chain. The harness
default is a human approver. GraphCoder loads its separate shipped JSON policy,
which contains one rule approver with an `allow` fallback. It uses the same
request, decision and durability path as a restrictive policy. Pass
`graphcoder --approval-policy policy.json` to select another policy:

```json
{
  "approvers": [
    {
      "kind": "rules",
      "principal": "rules",
      "rules": [
        {"tool": "url_fetch", "arguments": null, "verdict": "deny"}
      ],
      "fallback": "ask"
    },
    {
      "kind": "llm",
      "principal": "reviewer",
      "model": "review-model",
      "prompt": "Review the exact action. Ask a human when uncertain."
    },
    {"kind": "human", "principal": "acyclic.local"}
  ]
}
```

A human stage's `principal` is who may decide it: the decider's authenticated
principal. That is `acyclic.local` for a local GraphCoder run, and the
`principal` that `graphcoder serve` prints for the clients of an endpoint.

An `allow` or `deny` settles the request; `ask` advances to the next stage.
Exhausting the chain denies the action. Within one rule set, matching `deny`
wins over `ask`, which wins over `allow`. Omitted or null `tool` and `arguments` fields are
wildcards; supplied arguments must equal the complete arguments object.
Rules can also conjoin `selectors` against the exact argument object. Each uses an
RFC 6901 `pointer`: `{"kind":"pattern","pointer":"/command","pattern":"cargo test *"}`
uses an anchored pattern with `*` as its only wildcard;
`{"kind":"path_within","pointer":"/path","root":"/src"}` accepts that path or its
descendants using the filesystem's canonical portable path grammar; and
`{"kind":"equal","pointer":"/volume","value":"work"}` selects a declared volume.
Relative paths are anchored at the logical volume root. Traversal, repeated or
native separators, missing fields and wrong types refuse the match. Path and
volume selectors restrict approval; the normal workload capability and filesystem
policy must still authorize the effect. A matching approval never widens them.

An explicitly opaque subject exposes no tool metadata: only selector-free rules
match it, and the configured fallback still applies.
The policy identity, including stage order, is pinned at task admission and
inherited by children. Reopening with another policy cannot resume that task.

LLM stages use a separately bound provider/model and their own prompt, with no
tools. GraphCoder resolves the selection from its model catalog. Embedded
callers register it with `LocalHarnessConfig::approval_model`. The request is
presented as untrusted action data. Dispatch identity is persisted before the
model call; recovery reconciles it or reports an indeterminate result instead
of silently repeating the call. The admitted task's limits bound the request
and response.

The live task-file view includes:

```text
approvals/<request-id>/request.json
approvals/<request-id>/reviews/<principal-digest>.json
approvals/<request-id>/decisions/<principal-digest>.json
```

Decision filenames use `ApprovalDecisionFile::principal_file(principal)`.
`ApprovalDecisionFile::new` constructs a body containing `principal`,
`request_sha256`, `expected_version`, `verdict` (`approve` or `deny`) and
`reason`. Query `Query::Approvals(task)` or the task/session live view to follow
requests and decisions. Files are owner-published copies of the canonical
interaction, rather than a second settlement authority.

A human submits a decision through `LocalHarness::submit_approval_decision`
or the existing GraphCoder JSONL task-file input for that operation. The
connection can act only as the current configured human principal. Supplying
another principal in the body or filename does not authenticate it. Exact
retries are idempotent; stale versions, changed request digests and decisions
for cancelled tasks are refused. Closing input while waiting on a human
preserves the durable request and returns the indeterminate exit status, so
the same operation can be reopened.

`{"kind":"parent"}` escalates a child request through its retained parent
inbox. The parent sees the mirrored request and pending approval in the normal
child status/wait view. It sends an ordinary message whose JSON body is
`{"approval_decision": <FileValue>}`. The expected principal is
`parent:<parent-task-id>`. The child verifies the inbox's authenticated sender;
the message body cannot claim parent authority. If the parent completes or
fails, retained authenticated decisions are consumed first; an unanswered
request is denied so child cleanup does not await a departed approver.
A root task skips a parent
stage, and therefore needs a later stage if it should permit the action.

Embedded compositions must bind the same policy identity on their runtime and
local client. Automatic services require an explicit matching approval
authority; otherwise the embedding supplies signed decisions through the
runtime. Default tool bindings require an approval policy across ordinary and
native executor entry points.

Active approval queries and the retained session view keep only unsettled requests and reviews. Settled request, review and decision files remain available by canonical file paths or a per-request approval query. Approval history does not consume the per-message attachment allowance.
