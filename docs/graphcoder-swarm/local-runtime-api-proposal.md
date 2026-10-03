# Durable local recursive runtime API proposal

The production composition should be a thin ownership layer over the existing
`DurableHarnessStorage`, `FilesystemForkPreparer`, `StreamAggregate`, and
`HarnessBuilder` paths. It should own the durable local providers and session
descriptor, while keeping model execution behind an explicitly supplied mock
`ModelProvider`.

```rust
pub struct PersistentLocalSwarm {
    root: PersistentLocalHarness,
    registry: DurableSwarmRegistry,
}

impl PersistentLocalSwarm {
    pub async fn open(root: impl AsRef<Path>, config: LocalSwarmConfig) -> Result<Self>;
    pub async fn run_root(&self, operation: OperationId, prompt: &str) -> Result<TurnOutput>;
    pub async fn request_fork(&self, parent: TaskId, request: ForkIntent) -> Result<ForkHandle>;
    pub async fn resume(&self) -> Result<SwarmRecovery>;
    pub async fn session(&self, task: TaskId) -> Result<LocalSwarmSession>;
}

pub struct LocalSwarmSession {
    pub task: TaskId,
    pub parent: Option<TaskId>,
    pub run_limits: TaskRunLimits,
    pub phase: SwarmSessionPhase,
}
```

`request_fork` only persists intent and admission. The batch publisher is the
activation barrier: it receives one `CompletedModelBoundary`, verifies the
immutable prefix and provider-owned workspace generations, publishes the typed
fork, and only then constructs a child builder with the parent prefix plus an
explicit notification/task/identity/fresh-scratch suffix. Child model dispatch
cannot occur before that publication succeeds. Child records remain in the
existing task registry and scheduler, so reopening replays the registry and
recovers pending publication without starting a model implicitly.

The first black-box test should drive root -> two children -> grandchild with a
deterministic mock provider. It must inspect provider requests, real local
filesystem effects, durable message/wait records, exact diffs, and completion
outcomes after closing and reopening the host. A second test should interrupt
publication and prove the parent batch remains blocked until reconciliation.

## Integration seam with the native fork helper

The application constructor should call the filesystem helper after it has
loaded the parent `CompletedModelBoundary` and before it builds a child model
bundle. The helper owns `FilesystemForkPreparer`/`StreamAggregate` and returns
the immutable boundary plus child scoped inherited `FileRef` grants. The local
composition then uses `HarnessStorage::inherited_builder` with the returned
boundary and a suffix containing task identity, notification, and fresh
scratch. It must not use a root-private grant to read an inherited reference;
the child scope and helper-produced grants are the only source of inherited
content authority.

The narrowest useful seam is an async `activate(parent, request)` operation
that returns `{ child_task, child_operation, boundary, inherited_files }` after
the typed fork publication is committed. The caller persists `ForkAdmitted`
before invoking it, and persists `ForkCompleted` only after child model output
is durably recorded. Recovery can therefore inspect the registry without
starting a worker and retry only an explicitly supplied original request.

The current composition prototype in `filesystem/swarm_local.rs` uses the
shared boundary and inherited-builder APIs and is intentionally ready for this
seam. Its test proves recursive prefix bytes and durable lazy reopening. Before
qualification, wire its activation step to `verified_model_fork_boundary` and
`spawn_from_report`: the child conversation must contain the helper's scoped
inherited references before `run_conversation` selects context. Building a
child bundle directly from an empty child conversation would exercise the
provider prefix guard but would not qualify authoritative recursive history.

Implementation checkpoint: `03002645` (`harness: compose durable local recursive swarm sessions`).

The production activation adapter must receive the typed `ForkSeed` produced by
`spawn_from_report` and construct the child through
`HarnessStorage::from_published_fork`. That call binds the published child
conversation and installs only the seed's exact `reference_capabilities`; the
swarm registry must not treat an empty child bundle plus
`InheritedModelContext` as a substitute. A recursive child therefore carries
its authoritative aggregate and seed provenance into the next activation.

The local production constructor consequently needs one shared local Stream
client and Filesystem host for the registry, parent aggregate, and child
aggregate. Per-session isolated providers cannot publish a child seed that the
next descendant can reopen. Its activation sequence is:

1. verify the parent's admitted completed boundary;
2. prepare the exact `ForkRequest` with `FilesystemForkPreparer`;
3. publish and bind it with `StreamAggregate::spawn_from_report`;
4. create the child storage with `HarnessStorage::from_published_fork`;
5. append the child task suffix and execute its fresh operation.

The task registry retains the typed seed, parent aggregate identity, child
issuer descriptor, and original request so recovery can repeat the same
operation without widening workspace or reference scope.
