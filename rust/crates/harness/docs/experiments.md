# Paired populations

An experiment is a version-one arms file in a volume. Every arm and replicate
forks the same settled source turn, through the ordinary history fork. Project
generations and model context come from that saved turn. This is live execution;
there is no claim that independent provider responses are deterministic.

The initial topology is `fork_join`. Topology changes require the shared topology
owner to bind the actual communication graph before execution. A topology name
in a prompt would not establish that graph. Session-shared volumes remain live,
as they do for an ordinary history fork; a history fork is not whole-scope
isolation.

```json
{
  "version": 1,
  "experiment": "check-the-premise",
  "cut": {
    "agent": "harness/agents/00000000000000000000000000000000",
    "turn": 0
  },
  "replicates": 20,
  "arms": [
    {
      "name": "baseline",
      "topology": "fork_join",
      "role": "worker",
      "model": "openai/gpt-6-luna",
      "prompt": "Solve the task in the inherited context."
    },
    {
      "name": "check-premise",
      "topology": "fork_join",
      "role": "worker",
      "model": "openai/gpt-6-luna",
      "prompt": "Check the premise against the evidence, then solve the inherited task."
    }
  ]
}
```

Replace the example source with a real agent and saved zero-based turn. The
first arm is the comparison baseline. Use pilots of 20–50 total assigned trials
per arm across independent cuts, and obtain approval before scaling up. The
format has no hidden population cap;
the replicate count is the user's reservation. Arm order rotates between
replicates to reduce a fixed temporal ordering effect.

`experiments::Population::admit` checks the version, identities, role context and
declared model selection before execution. All executable roles in the host's
snapshot must use `openai/gpt-6-luna`, `anthropic/claude-haiku-5-5`, or
`google/gemini-3.8-flash`. This includes descendant roles, so the stock profile
snapshot with its more expensive advisor/reviewer is refused. The parent must
admit every arm role, and arm roles must inherit context. Each fork carries the
admitted whole-role snapshot identity. The host compares it before history,
staging or activation and refuses a mismatch with a recovery instruction.
Every adopted role pin also binds that snapshot: changing only a future
descendant's policy refuses resumed turns and spawns under the new snapshot.
This admission validates role files. The executor must also bind each admitted
role to its actual provider/model before activation; a host-wide provider alone
does not enforce the declared cheap-model policy. That shared binding remains
a prerequisite for running these populations.

`population::run` (features `filesystem` and `hosting`) returns released runs.
Supply authenticated `AgentControls`, explicit nonzero in-flight credit and an
async `retain(&CompletedRun)` callback. Read and score the selected original
output, then durably append its run ticket and score through the volume owner
before returning from that callback. Its wait belongs to that owner's commit
event and must return a typed error if the owner closes. The runner adds no
command tool, transport, background thread, directory scan or timer.

Each trial has an intent derived from the complete arms file, role snapshot,
arm position and replicate. Retrying a fork reuses its committed child. Changing
an intervention creates a different experiment
identity. Dropping the stream releases observations; already committed agents
retain their ordinary lifecycle. Record failures as well as completed rows.
An uncertain control failure retains its exact prepared invocation for explicit
replay. Save the original status sequence with the run ticket. Recover pending
releases by reading those tickets and calling `population::release`; an archive's
latest status cannot stand in for the original output. This API does not infer
that sequence from a settlement query or conversation length. The first failure
stops admission and drops other active futures; reconcile any outstanding fork
intents and retained tickets before restarting a population.

Completion is a committed `Done` status, or `Idle` after at least one new
conversation entry beyond the inherited prefix. The adoption's initial idle
status and a wait for children cannot finish a trial. Status observation uses
one latest-status follow, so a resumed observer does not replay status history
or transfer the transcript. Producer/session loss returns the session adapter's
typed error. The producer of each subsequent wake is the fork's own actor step,
which commits status with its turn.

After retention, one `Archive` command fences the exact original status. Credit
stays held until `Released` proves the parent consumed the terminal report and
removed membership. The returned status, inbox cursor and conversation cut must
all match the retained result. Merely observing the child's archive publication
cannot complete a trial. Retention failure leaves the child open and returns its
original ticket; release failure preserves that ticket and any uncertain command.
The runner remains a draft until the shared membership/layout primitives are
complete, including paused-turn reservations. Do not launch populations yet.

## Results and cost

Keep raw completed rows, including the actual child identity and committed
status. Join scores from a declared scorer and cost from that child's original
`ModelTokenUsage` and `ModelPriceRecord` observations. Settlement is not a
successful score. Missing price or usage leaves cost unknown; never turn missing
accounting into zero. Do not exclude failed runs when reporting paid cost.

| Population | Arm | Replicate | Agent | Phase | Score | Cost USD/run |
| --- | --- | --- | --- | --- | --- | --- |
| Not run | — | — | — | — | — | Unmeasured |

`experiments::Observation` is a version-one scored row. Its `difference` method
requires the same population and replicate and different arms, and refuses
nonfinite scores or invalid paid costs. Pair a treatment with the declared
baseline, then feed the difference to `stats::Cluster`.

An independent problem/source cut is one cluster. All replicates sharing that
cut belong to that cluster; creating more agents does not create independent
clusters. Group all rows for each cluster, finish it once, and pass it to
`PairedStats`. Keep a separate accumulator per treatment comparison. A single
cut cannot estimate clustered uncertainty and is refused.

The report gives the equally weighted mean paired difference, ordinary paired
SE, and intercept-only CR1 clustered SE:

`SE² = G/(G-1) × Σ_g (Σ_i d_gi - n_g × mean(d))² / N²`.

The accumulator uses centered online QR so large common offsets do not require
subtracting nearly equal covariance sums. Updates use constant space and work;
no row, agent, history or cluster-key collection grows in the accumulator.

`stats::required_clusters` calculates
`ceil(((z_(1-alpha/2) + z_power) × cluster_sd / effect)²)`, with at least two
clusters to estimate uncertainty. Quantiles are supplied explicitly. This is a
normal approximation for equal-sized independent cluster means, not a
small-sample guarantee or a count of dependent replicates. For unequal sizes,
plan with cluster-level simulation or independent pilot clusters.

## Work and validation

Profile admission hashes the sorted role files once and caches the snapshot
identity. Population admission reads the arms configuration once, checks each
executable role once and borrows the cached identity without sorting again.
Trial enumeration and rotation take O(1) work, allocate nothing, and
never multiply the replicate count by the arm count. Intent derivation hashes
fixed-size identity fields. Streaming statistics allocate nothing per update.

Per trial, the orchestration now prepares two commands and invocations (fork
and exact-cut release) and makes two control exchanges, up from one each. The
added exchange buys parent-acknowledged membership release before credit reuse.
It follows one status stream and transfers no transcript. It copies
the input brief once into the command and returns one result identity and arm
name. It adds zero direct syscalls, durability barriers, extra commits or model
calls. Each command owns its agent/intent identity; the release command adds
one agent-path and one intent copy. Retention commits the caller's output once,
before closure; it is required for crash recovery. The shared fork/release owners
retain their existing commit costs. The retention callback's future is inlined,
with zero wrapper allocations; only failures box retained custody.
The snapshot precondition copies one 32-byte identity into each trial command,
serialized in its existing JSON exchange. Its field adds 85–149 JSON bytes,
depending on the digest's decimal digits. The host performs one fixed-width
comparison with zero allocations, I/O, round trips or waits. Existing fixed-width
role pins bind the full epoch without growing the adopted record. A failed
precondition performs zero provider operations.
Using ordinary full agent observation would open two follows and transfer
conversation records; lifecycle-only observation opens one and transfers zero
conversation records.

The shared fork receipt is one immutable outcome followed directly from its
original spawn commit. Release adds one original-status read, its fenced archive
commit and one parent-owned receipt follow in that shared implementation. These
receipts add no separate commit or durability barrier. Complete fork complexity
still depends on the unfinished membership/layout primitives.

Focused validation commands:

```sh
cargo check --locked -p acyclic-harness --features hosting --lib
cargo test --locked -p acyclic-harness-core --features filesystem experiments::
cargo test --locked -p acyclic-harness-client --features agents lifecycle_observation_bootstraps_latest_then_waits_for_the_committed_step
cargo kani -p acyclic-harness-core --features filesystem --harness rotation_is_bounded_and_always_advances
```

The tests cover mismatched/expensive models, incompatible formats, mismatched
pairs, a population too large to enumerate eagerly, reproducible unique intents,
unequal cluster sizes, large common offsets, and false independence. The rotation
proof checks safety and progress over arbitrary machine-sized counts. The
lifecycle integration test uses the real in-memory Streams provider and session
server; status records are produced by the canonical encoder, not hand-written
provider fixtures.
