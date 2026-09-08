//! Customer-hostable Stream-backed coordinator and pull-worker admission.

use crate::{
    Error, IdempotencyKey, OperationId, Result,
    core::{Authority, AuthorityVerifier, Scope},
    scheduler::{
        DurableOwner, EntrypointRef, LeaseFence, OperationSpec, OperationState,
        OrchestrationDecision, Reservation, ResourceSnapshot, Scheduler, SchedulerEvent,
        reduction_invocation_digest,
    },
    wire,
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream,
    StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use prost::Message as _;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc};

const COORDINATOR_PATH: &str = "harness/coordinator/events";
const READ_PAGE_SIZE: u32 = 1_024;
const COORDINATOR_WIRE_VERSION: &str = "1";
const COORDINATOR_WIRE_CONTRACT: &[u8] = b"acyclic.harness.coordinator.scheduler-event-envelope.v1";
const LEGACY_HARNESS_DESCRIPTOR_DIGEST: &str =
    "b7506282912690d6a9cd875ca426b3b4f3c9dd937b457377f6865d83c1d2b3d9";

/// Pull worker capacity and placement identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Worker {
    /// Stable worker identity.
    pub id: String,
    /// Currently available logical capacity.
    pub available: ResourceSnapshot,
}

/// Work atomically claimed from the coordinator.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkLease {
    /// Complete immutable operation declaration.
    pub operation: OperationSpec,
    /// Pinned execution allocation.
    pub reservation: Reservation,
    /// Latest durable resumable checkpoint, if any.
    pub checkpoint: Option<crate::resources::CheckpointRef>,
    /// Operation revision after this lease was admitted.
    pub operation_revision: u64,
}

/// Synchronous deterministic implementation of one pinned reducer contract.
pub trait DurableReducer: Send + Sync {
    /// Exact immutable reducer identity.
    fn entrypoint(&self) -> &EntrypointRef;
    /// Reduces values in stable child-slot order.
    fn reduce(
        &self,
        values: &[(String, serde_json::Value)],
    ) -> Result<crate::Outcome<serde_json::Value>>;
}

/// Code-first registry for exact durable reducer implementations.
#[derive(Default)]
pub struct ReducerRegistry(BTreeMap<(String, String, [u8; 32]), Arc<dyn DurableReducer>>);

impl ReducerRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub const fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Registers one exact reducer without ambiguous replacement.
    pub fn register(&mut self, reducer: Arc<dyn DurableReducer>) -> Result<()> {
        let entrypoint = reducer.entrypoint();
        jsonschema::validator_for(&entrypoint.result_schema)
            .map_err(|error| Error::Invalid(format!("invalid reducer result schema: {error}")))?;
        let key = (
            entrypoint.name.clone(),
            entrypoint.version.clone(),
            entrypoint.digest,
        );
        if self.0.contains_key(&key) {
            return Err(Error::Conflict("reducer is already registered".into()));
        }
        self.0.insert(key, reducer);
        Ok(())
    }

    fn get(&self, entrypoint: &EntrypointRef) -> Option<&Arc<dyn DurableReducer>> {
        self.0.get(&(
            entrypoint.name.clone(),
            entrypoint.version.clone(),
            entrypoint.digest,
        ))
    }
}

/// Outcome of a coordinator append.
#[derive(Clone, Debug, PartialEq)]
pub enum CoordinatorApply {
    /// New state was durably appended.
    Applied,
    /// Exact retry was already durably appended.
    Replayed,
}

/// One logical coordinator whose entire semantic history is a Stream.
pub struct DistributedCoordinator<P> {
    client: StreamClient<P>,
    stream: Stream<P>,
    scheduler: Scheduler,
    revision: u64,
    intents: BTreeMap<String, ([u8; 32], SchedulerEvent)>,
}

impl<P: StreamProvider> DistributedCoordinator<P> {
    /// Opens and replays the public coordinator implementation.
    pub async fn open(client: &StreamClient<P>) -> Result<Self> {
        let stream = client
            .stream(COORDINATOR_PATH)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let mut value = Self {
            client: client.clone(),
            stream,
            scheduler: Scheduler::new(),
            revision: 0,
            intents: BTreeMap::new(),
        };
        let mut from = 0;
        loop {
            let records = match value.stream.read(from, READ_PAGE_SIZE).await {
                Ok(records) => records,
                Err(StreamError::NotFound) if from == 0 => break,
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            let page = records
                .try_collect::<Vec<_>>()
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            if page.is_empty() {
                break;
            }
            for record in &page {
                let (revision, operation_id, key, digest, event) = decode(&record.value)?;
                if revision != record.sequence + 1 {
                    return Err(Error::Storage("coordinator revision is not gapless".into()));
                }
                value.apply_committed(revision, operation_id, key, digest, event)?;
            }
            from = from
                .checked_add(page.len() as u64)
                .ok_or_else(|| Error::Storage("coordinator cursor exhausted".into()))?;
        }
        Ok(value)
    }

    /// Returns the deterministic scheduler projection.
    #[must_use]
    pub const fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    /// Returns one operation only after verifying the owner's signed scope.
    pub fn observe_operation(
        &self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
    ) -> Result<OperationState> {
        self.authorize_operation(owner, scope, verifier, operation_id, "operation:observe")
            .cloned()
    }

    /// Durably requests cancellation with exact retry and optional subtree propagation.
    pub async fn cancel_operation(
        &mut self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        recursive: bool,
    ) -> Result<(CoordinatorApply, OperationState)> {
        self.authorize_operation(owner, scope, verifier, operation_id, "operation:cancel")?;
        let applied = self
            .apply(
                operation_id,
                idempotency_key,
                SchedulerEvent::CancellationRequested {
                    operation_id,
                    recursive,
                },
            )
            .await?;
        let state = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?
            .clone();
        Ok((applied, state))
    }

    fn authorize_operation<'a>(
        &'a self,
        owner: &Authority,
        scope: &Scope,
        verifier: &AuthorityVerifier,
        operation_id: OperationId,
        capability: &str,
    ) -> Result<&'a OperationState> {
        verifier.verify_audience(owner)?;
        verifier.verify(scope)?;
        if !scope.capabilities().contains(capability) {
            return Err(Error::Unauthorized(format!(
                "scope {} lacks capability {capability}",
                scope.id()
            )));
        }
        let operation = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?;
        let declared_owner = match &operation.spec.owner {
            DurableOwner::Attached { authority } | DurableOwner::Detached { authority } => {
                authority
            }
        };
        if declared_owner != owner {
            return Err(Error::NotFound(format!("operation {operation_id}")));
        }
        Ok(operation)
    }

    /// Durably applies one scheduler event with exact retry semantics.
    pub async fn apply(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
    ) -> Result<CoordinatorApply> {
        if matches!(
            &event,
            SchedulerEvent::Orchestrated {
                reducer: Some(_),
                ..
            }
        ) {
            return Err(Error::Unauthorized(
                "reduce decisions must execute through the reducer registry".into(),
            ));
        }
        self.apply_internal(operation_id, idempotency_key, event)
            .await
    }

    async fn apply_internal(
        &mut self,
        operation_id: OperationId,
        idempotency_key: IdempotencyKey,
        event: SchedulerEvent,
    ) -> Result<CoordinatorApply> {
        IdempotencyKey::new(idempotency_key.0.clone())?;
        if scheduler_event_operation(&event) != operation_id {
            return Err(Error::Invalid(
                "scheduler event belongs to another operation".into(),
            ));
        }
        let canonical =
            serde_json::to_vec(&event).map_err(|error| Error::Invalid(error.to_string()))?;
        let digest = *blake3::hash(&canonical).as_bytes();
        if let Some((existing_digest, existing)) = self.intents.get(idempotency_key.as_str()) {
            return if existing_digest == &digest && existing == &event {
                Ok(CoordinatorApply::Replayed)
            } else {
                Err(Error::Conflict("coordinator retry identity reused".into()))
            };
        }
        let mut projected = self.scheduler.clone();
        projected.apply(event.clone())?;
        let revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("coordinator revision exhausted".into()))?;
        let bytes = encode(
            revision,
            operation_id,
            idempotency_key.as_str(),
            digest,
            canonical,
        );
        let stream_key = stream_key(idempotency_key.as_str())?;
        let outcome = match self
            .stream
            .append_batch(
                vec![Bytes::copy_from_slice(&bytes)],
                Some(self.revision),
                Some(stream_key.clone()),
            )
            .await
        {
            Ok(outcome) => outcome,
            Err(StreamError::Unavailable) => {
                match self.client.inspect_idempotency(stream_key).await {
                    Ok(Some(observation)) => match observation.outcome {
                        IdempotencyOutcome::Append(outcome) => outcome,
                        _ => {
                            return Err(Error::Conflict(
                                "coordinator retry identity has another operation kind".into(),
                            ));
                        }
                    },
                    Ok(None) | Err(_) => return Err(Error::Indeterminate(operation_id)),
                }
            }
            Err(StreamError::IdempotencyMismatch) => {
                return Err(Error::Conflict("coordinator retry identity reused".into()));
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        match outcome {
            AppendOutcome::Committed(receipt)
                if receipt.start == self.revision
                    && receipt.end == revision
                    && receipt.tail >= revision =>
            {
                let records = self
                    .stream
                    .read(receipt.start, 1)
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                let record = records.first().ok_or_else(|| {
                    Error::Storage("committed coordinator record is unavailable".into())
                })?;
                if record.sequence != receipt.start || record.value.as_ref() != bytes.as_slice() {
                    return Err(Error::Conflict(
                        "coordinator observation does not match submitted event".into(),
                    ));
                }
                self.apply_committed(revision, operation_id, idempotency_key.0, digest, event)?;
                Ok(CoordinatorApply::Applied)
            }
            AppendOutcome::Committed(_) => {
                Err(Error::Storage("invalid coordinator append receipt".into()))
            }
            AppendOutcome::TailConflict { actual_tail } => Err(Error::Conflict(format!(
                "coordinator tail is {actual_tail}"
            ))),
        }
    }

    /// Pulls and atomically admits one dependency- and resource-ready operation.
    pub async fn pull(&mut self, worker: &Worker) -> Result<Option<WorkLease>> {
        if worker.id.trim().is_empty() {
            return Err(Error::Invalid("worker identity is empty".into()));
        }
        if let Some(operation_id) = self.scheduler.blocked_by_dependencies().first().copied() {
            self.apply(
                operation_id,
                IdempotencyKey::new(format!("dependency-rejected:{operation_id}"))?,
                SchedulerEvent::Rejected {
                    operation_id,
                    reason: "a required dependency did not succeed".into(),
                },
            )
            .await?;
            return Ok(None);
        }
        let available = self.scheduler.available_for(&worker.id, &worker.available);
        let Some(operation_id) = self.scheduler.ready(&available).first().copied() else {
            return Ok(None);
        };
        let state = self
            .scheduler
            .operation(operation_id)
            .ok_or_else(|| Error::NotFound(format!("operation {operation_id}")))?
            .clone();
        let operation = state.spec.clone();
        let reservation = Reservation {
            id: format!("{}:{operation_id}:{}", worker.id, self.revision + 1),
            placement: worker.id.clone(),
            admitted: operation.resources.clone(),
        };
        self.apply(
            operation_id,
            IdempotencyKey::new(format!(
                "pull:{}:{operation_id}:{}",
                worker.id,
                self.revision + 1
            ))?,
            SchedulerEvent::Admitted {
                operation_id,
                reservation: reservation.clone(),
            },
        )
        .await?;
        Ok(Some(WorkLease {
            operation,
            reservation,
            checkpoint: state.checkpoint,
            operation_revision: self
                .scheduler
                .operation(operation_id)
                .map_or(0, |value| value.revision),
        }))
    }

    /// Returns a crashed worker's exact lease to admission without losing its checkpoint.
    pub async fn release_lease(
        &mut self,
        lease: &WorkLease,
        idempotency_key: IdempotencyKey,
    ) -> Result<CoordinatorApply> {
        self.apply(
            lease.operation.operation_id,
            idempotency_key,
            SchedulerEvent::LeaseReleased {
                operation_id: lease.operation.operation_id,
                fence: LeaseFence::from(&lease.reservation),
            },
        )
        .await
    }

    /// Atomically commits a ready structured-concurrency decision and loser cancellation.
    pub async fn orchestrate(
        &mut self,
        parent: OperationId,
        idempotency_key: IdempotencyKey,
    ) -> Result<bool> {
        let expected_revision = self
            .scheduler
            .operation(parent)
            .ok_or_else(|| Error::NotFound(format!("operation {parent}")))?
            .revision;
        let OrchestrationDecision::Complete { outcome, cancel } =
            self.scheduler.orchestration(parent)
        else {
            return Ok(false);
        };
        self.apply(
            parent,
            idempotency_key,
            SchedulerEvent::Orchestrated {
                operation_id: parent,
                expected_revision,
                outcome,
                cancel,
                reducer: None,
                reduction_digest: None,
            },
        )
        .await?;
        Ok(true)
    }

    /// Commits the result of the exact versioned reducer invocation currently planned.
    pub async fn complete_reduction(
        &mut self,
        reducers: &ReducerRegistry,
        parent: OperationId,
        idempotency_key: IdempotencyKey,
    ) -> Result<CoordinatorApply> {
        let state = self
            .scheduler
            .operation(parent)
            .ok_or_else(|| Error::NotFound(format!("operation {parent}")))?;
        let expected_revision = state.revision;
        let OrchestrationDecision::Reduce { reducer, values } =
            self.scheduler.orchestration(parent)
        else {
            return Err(Error::Conflict("no reducer invocation is ready".into()));
        };
        let implementation = reducers.get(&reducer).ok_or_else(|| {
            Error::Unsupported("exact reducer implementation is not registered".into())
        })?;
        let outcome = implementation.reduce(&values)?;
        if matches!(outcome, crate::Outcome::Indeterminate { .. }) {
            return Err(Error::Invalid(
                "a synchronous reducer cannot return an indeterminate outcome".into(),
            ));
        }
        let reduction_digest = reduction_invocation_digest(&reducer, &values)?;
        self.apply_internal(
            parent,
            idempotency_key,
            SchedulerEvent::Orchestrated {
                operation_id: parent,
                expected_revision,
                outcome,
                cancel: Vec::new(),
                reducer: Some(reducer),
                reduction_digest: Some(reduction_digest),
            },
        )
        .await
    }

    fn apply_committed(
        &mut self,
        revision: u64,
        operation_id: OperationId,
        key: String,
        digest: [u8; 32],
        event: SchedulerEvent,
    ) -> Result<()> {
        if revision != self.revision + 1 || scheduler_event_operation(&event) != operation_id {
            return Err(Error::Conflict(
                "invalid committed coordinator event".into(),
            ));
        }
        IdempotencyKey::new(key.clone())?;
        if self.intents.contains_key(&key) {
            return Err(Error::Conflict(
                "coordinator history repeats an idempotency key".into(),
            ));
        }
        self.scheduler.apply(event.clone())?;
        self.revision = revision;
        self.intents.insert(key, (digest, event));
        Ok(())
    }
}

fn scheduler_event_operation(event: &SchedulerEvent) -> OperationId {
    match event {
        SchedulerEvent::Declared { spec } => spec.operation_id,
        SchedulerEvent::WaitingForCapacity { operation_id }
        | SchedulerEvent::Admitted { operation_id, .. }
        | SchedulerEvent::PartiallyAdmitted { operation_id, .. }
        | SchedulerEvent::Rejected { operation_id, .. }
        | SchedulerEvent::Started { operation_id, .. }
        | SchedulerEvent::Checkpointed { operation_id, .. }
        | SchedulerEvent::WaitingForChildren { operation_id, .. }
        | SchedulerEvent::LeaseReleased { operation_id, .. }
        | SchedulerEvent::CancellationRequested { operation_id, .. }
        | SchedulerEvent::Completed { operation_id, .. }
        | SchedulerEvent::Orchestrated { operation_id, .. } => *operation_id,
    }
}

fn encode(
    revision: u64,
    operation_id: OperationId,
    key: &str,
    digest: [u8; 32],
    canonical: Vec<u8>,
) -> Vec<u8> {
    wire::SchedulerEventEnvelope {
        protocol: Some(coordinator_protocol_identity()),
        revision,
        operation_id: operation_id.to_string(),
        idempotency_key: key.into(),
        canonical_event_json: canonical,
        event_digest: digest.to_vec(),
    }
    .encode_to_vec()
}

fn decode(bytes: &[u8]) -> Result<(u64, OperationId, String, [u8; 32], SchedulerEvent)> {
    let envelope = wire::SchedulerEventEnvelope::decode(bytes)
        .map_err(|error| Error::Storage(error.to_string()))?;
    validate_coordinator_protocol(envelope.protocol.as_ref())?;
    let digest: [u8; 32] = envelope
        .event_digest
        .try_into()
        .map_err(|_| Error::Storage("scheduler digest must be 32 bytes".into()))?;
    if *blake3::hash(&envelope.canonical_event_json).as_bytes() != digest {
        return Err(Error::Storage("scheduler event digest mismatch".into()));
    }
    let event = serde_json::from_slice(&envelope.canonical_event_json)
        .map_err(|error| Error::Storage(error.to_string()))?;
    Ok((
        envelope.revision,
        OperationId::parse(&envelope.operation_id)?,
        envelope.idempotency_key,
        digest,
        event,
    ))
}

fn coordinator_protocol_identity() -> wire::ProtocolIdentity {
    wire::ProtocolIdentity {
        version: COORDINATOR_WIRE_VERSION.into(),
        descriptor_digest: blake3::hash(COORDINATOR_WIRE_CONTRACT).to_hex().to_string(),
    }
}

fn validate_coordinator_protocol(protocol: Option<&wire::ProtocolIdentity>) -> Result<()> {
    let actual =
        protocol.ok_or_else(|| Error::Storage("coordinator event protocol is missing".into()))?;
    let current = coordinator_protocol_identity();
    if actual.version == COORDINATOR_WIRE_VERSION
        && (actual.descriptor_digest == current.descriptor_digest
            || actual.descriptor_digest == LEGACY_HARNESS_DESCRIPTOR_DIGEST)
    {
        Ok(())
    } else {
        Err(Error::Unsupported(
            "unsupported coordinator event wire version".into(),
        ))
    }
}

fn stream_key(key: &str) -> Result<StreamIdempotencyKey> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-harness-coordinator-v1");
    hasher.update(key.as_bytes());
    StreamIdempotencyKey::new(Bytes::copy_from_slice(hasher.finalize().as_bytes()))
        .map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Capabilities,
        core::{AggregateKind, Authority, AuthorityIssuer},
        scheduler::{
            DurableOwner, EntrypointRef, OperationPhase, Orchestration, ParentLink, ResourceRequest,
        },
    };
    use acyclic_stream::MemoryStream;
    use serde_json::Value;
    use std::{collections::BTreeSet, sync::Arc};

    struct TestReducer {
        entrypoint: EntrypointRef,
        value: Value,
    }

    impl DurableReducer for TestReducer {
        fn entrypoint(&self) -> &EntrypointRef {
            &self.entrypoint
        }

        fn reduce(&self, _: &[(String, Value)]) -> Result<crate::Outcome<Value>> {
            Ok(crate::Outcome::Succeeded(self.value.clone()))
        }
    }

    fn spec(operation_id: OperationId, cpu: u64) -> OperationSpec {
        OperationSpec {
            operation_id,
            parent: None,
            owner: DurableOwner::Detached {
                authority: Authority {
                    kind: AggregateKind::Task,
                    id: "owner".into(),
                },
            },
            entrypoint: EntrypointRef {
                name: "example.task".into(),
                version: "1".into(),
                digest: [2; 32],
                result_schema: Value::Object(Default::default()),
            },
            dependencies: BTreeSet::new(),
            resources: ResourceRequest(BTreeMap::from([("cpu".into(), cpu)])),
            placement: Value::Null,
            orchestration: Orchestration::Leaf,
            state: Value::Null,
        }
    }

    fn historical_coordinator_record(
        revision: u64,
        operation_id: OperationId,
        key: &str,
        event: &SchedulerEvent,
    ) -> Result<Bytes> {
        let canonical =
            serde_json::to_vec(event).map_err(|error| Error::Invalid(error.to_string()))?;
        let digest = *blake3::hash(&canonical).as_bytes();
        Ok(Bytes::from(
            wire::SchedulerEventEnvelope {
                protocol: Some(wire::ProtocolIdentity {
                    version: COORDINATOR_WIRE_VERSION.into(),
                    descriptor_digest: LEGACY_HARNESS_DESCRIPTOR_DIGEST.into(),
                }),
                revision,
                operation_id: operation_id.to_string(),
                idempotency_key: key.into(),
                canonical_event_json: canonical,
                event_digest: digest.to_vec(),
            }
            .encode_to_vec(),
        ))
    }

    #[tokio::test]
    async fn authenticated_recursive_cancel_is_atomic_durable_and_exactly_replayable() -> Result<()>
    {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let owner = Authority {
            kind: AggregateKind::Task,
            id: "owner".into(),
        };
        let issuer = AuthorityIssuer::new("runtime", [9; 32], owner.clone());
        let scope = issuer.root(
            "operation-control",
            Capabilities::new(["operation:observe", "operation:cancel"]),
        );
        let parent = OperationId::from_bytes([31; 16]);
        let child = OperationId::from_bytes([32; 16]);
        let mut coordinator = DistributedCoordinator::open(&client).await?;
        coordinator
            .apply(
                parent,
                IdempotencyKey::new("declare-control-parent")?,
                coordinator.scheduler().declare(spec(parent, 0))?,
            )
            .await?;
        let mut child_spec = spec(child, 0);
        child_spec.parent = Some(ParentLink {
            operation_id: parent,
            slot: "child".into(),
        });
        coordinator
            .apply(
                child,
                IdempotencyKey::new("declare-control-child")?,
                coordinator.scheduler().declare(child_spec)?,
            )
            .await?;

        assert_eq!(
            coordinator
                .observe_operation(&owner, &scope, &issuer.verifier(), parent)?
                .phase,
            OperationPhase::WaitingForDependencies
        );
        let (applied, status) = coordinator
            .cancel_operation(
                &owner,
                &scope,
                &issuer.verifier(),
                parent,
                IdempotencyKey::new("cancel-control-tree")?,
                true,
            )
            .await?;
        assert_eq!(applied, CoordinatorApply::Applied);
        assert_eq!(status.outcome, Some(crate::Outcome::Cancelled));
        assert_eq!(
            coordinator
                .scheduler()
                .operation(child)
                .map(|value| value.outcome.clone()),
            Some(Some(crate::Outcome::Cancelled))
        );

        let (replayed, _) = coordinator
            .cancel_operation(
                &owner,
                &scope,
                &issuer.verifier(),
                parent,
                IdempotencyKey::new("cancel-control-tree")?,
                true,
            )
            .await?;
        assert_eq!(replayed, CoordinatorApply::Replayed);
        assert!(matches!(
            coordinator
                .cancel_operation(
                    &owner,
                    &scope,
                    &issuer.verifier(),
                    parent,
                    IdempotencyKey::new("cancel-control-tree")?,
                    false,
                )
                .await,
            Err(Error::Conflict(_))
        ));
        let reopened = DistributedCoordinator::open(&client).await?;
        assert_eq!(
            reopened
                .observe_operation(&owner, &scope, &issuer.verifier(), child)?
                .outcome,
            Some(crate::Outcome::Cancelled)
        );

        let observe_only = issuer.root("observe-only", Capabilities::new(["operation:observe"]));
        let mut reopened = reopened;
        assert!(matches!(
            reopened
                .cancel_operation(
                    &owner,
                    &observe_only,
                    &issuer.verifier(),
                    child,
                    IdempotencyKey::new("unauthorized-cancel")?,
                    false,
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn legacy_non_recursive_cancel_reopens_and_retries_exactly() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let operation_id = OperationId::from_bytes([33; 16]);
        let declaration = Scheduler::new().declare(spec(operation_id, 0))?;
        let cancellation = SchedulerEvent::CancellationRequested {
            operation_id,
            recursive: false,
        };
        let stream = client
            .stream(COORDINATOR_PATH)
            .map_err(|error| Error::Storage(error.to_string()))?;
        stream
            .append_batch(
                vec![
                    historical_coordinator_record(
                        1,
                        operation_id,
                        "declare-legacy-cancel",
                        &declaration,
                    )?,
                    historical_coordinator_record(2, operation_id, "legacy-cancel", &cancellation)?,
                ],
                Some(0),
                Some(stream_key("seed-legacy-history")?),
            )
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let mut reopened = DistributedCoordinator::open(&client).await?;
        assert_eq!(
            reopened
                .apply(
                    operation_id,
                    IdempotencyKey::new("legacy-cancel")?,
                    SchedulerEvent::CancellationRequested {
                        operation_id,
                        recursive: false,
                    },
                )
                .await?,
            CoordinatorApply::Replayed
        );
        Ok(())
    }

    #[tokio::test]
    async fn coordinator_replays_and_pull_admission_is_exclusive() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let operation_id = OperationId::from_bytes([1; 16]);
        let spec = OperationSpec {
            operation_id,
            parent: None,
            owner: DurableOwner::Detached {
                authority: Authority {
                    kind: AggregateKind::Task,
                    id: "owner".into(),
                },
            },
            entrypoint: EntrypointRef {
                name: "example.task".into(),
                version: "1".into(),
                digest: [2; 32],
                result_schema: Value::Object(Default::default()),
            },
            dependencies: BTreeSet::new(),
            resources: ResourceRequest::default(),
            placement: Value::Null,
            orchestration: Orchestration::Leaf,
            state: Value::Null,
        };
        let mut coordinator = DistributedCoordinator::open(&client).await?;
        coordinator
            .apply(
                operation_id,
                IdempotencyKey::new("declare-1")?,
                coordinator.scheduler().declare(spec)?,
            )
            .await?;
        let mut reopened = DistributedCoordinator::open(&client).await?;
        let worker = Worker {
            id: "worker-1".into(),
            available: ResourceSnapshot::default(),
        };
        let lease = reopened
            .pull(&worker)
            .await?
            .ok_or_else(|| Error::NotFound("lease".into()))?;
        assert_eq!(lease.operation.operation_id, operation_id);
        assert!(reopened.pull(&worker).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn active_worker_reservations_are_subtracted_from_capacity() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let first = OperationId::from_bytes([3; 16]);
        let second = OperationId::from_bytes([4; 16]);
        let mut coordinator = DistributedCoordinator::open(&client).await?;
        for (index, operation_id) in [first, second].into_iter().enumerate() {
            let declaration = coordinator.scheduler().declare(spec(operation_id, 1))?;
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new(format!("declare-capacity-{index}"))?,
                    declaration,
                )
                .await?;
        }
        let worker = Worker {
            id: "worker-one".into(),
            available: ResourceSnapshot(BTreeMap::from([("cpu".into(), 1)])),
        };
        let first_lease = coordinator
            .pull(&worker)
            .await?
            .ok_or_else(|| Error::NotFound("first lease".into()))?;
        assert_eq!(first_lease.operation.operation_id, first);
        assert!(coordinator.pull(&worker).await?.is_none());
        coordinator
            .apply(
                first,
                IdempotencyKey::new("start-capacity-first")?,
                SchedulerEvent::Started {
                    operation_id: first,
                    fence: LeaseFence::from(&first_lease.reservation),
                },
            )
            .await?;
        coordinator
            .apply(
                first,
                IdempotencyKey::new("complete-capacity-first")?,
                SchedulerEvent::Completed {
                    operation_id: first,
                    outcome: crate::Outcome::Succeeded(Value::Null),
                    fence: Some(LeaseFence::from(&first_lease.reservation)),
                },
            )
            .await?;
        assert_eq!(
            coordinator
                .pull(&worker)
                .await?
                .map(|lease| lease.operation.operation_id),
            Some(second)
        );
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_work_does_not_starve_the_pull_queue() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = DistributedCoordinator::open(&client).await?;
        let cancelled = OperationId::from_bytes([5; 16]);
        let runnable = OperationId::from_bytes([6; 16]);
        for (index, operation_id) in [cancelled, runnable].into_iter().enumerate() {
            let declaration = coordinator.scheduler().declare(spec(operation_id, 0))?;
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new(format!("declare-cancel-{index}"))?,
                    declaration,
                )
                .await?;
        }
        coordinator
            .apply(
                cancelled,
                IdempotencyKey::new("cancel-before-pull")?,
                SchedulerEvent::CancellationRequested {
                    operation_id: cancelled,
                    recursive: false,
                },
            )
            .await?;
        assert_eq!(
            coordinator
                .scheduler()
                .operation(cancelled)
                .and_then(|state| state.outcome.as_ref()),
            Some(&crate::Outcome::Cancelled)
        );
        let worker = Worker {
            id: "worker".into(),
            available: ResourceSnapshot::default(),
        };
        assert_eq!(
            coordinator
                .pull(&worker)
                .await?
                .map(|lease| lease.operation.operation_id),
            Some(runnable)
        );
        Ok(())
    }

    #[tokio::test]
    async fn leases_are_fenced_and_recoverable() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = DistributedCoordinator::open(&client).await?;
        let operation_id = OperationId::from_bytes([7; 16]);
        let declaration = coordinator.scheduler().declare(spec(operation_id, 0))?;
        coordinator
            .apply(
                operation_id,
                IdempotencyKey::new("declare-recovery")?,
                declaration,
            )
            .await?;
        let worker = Worker {
            id: "worker".into(),
            available: ResourceSnapshot::default(),
        };
        let first = coordinator
            .pull(&worker)
            .await?
            .ok_or_else(|| Error::NotFound("lease".into()))?;
        let forged = LeaseFence {
            reservation_id: "foreign".into(),
            placement: worker.id.clone(),
        };
        assert!(matches!(
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("forged-start")?,
                    SchedulerEvent::Started {
                        operation_id,
                        fence: forged
                    }
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        coordinator
            .apply(
                operation_id,
                IdempotencyKey::new("valid-start")?,
                SchedulerEvent::Started {
                    operation_id,
                    fence: LeaseFence::from(&first.reservation),
                },
            )
            .await?;
        let checkpoint = crate::resources::CheckpointRef::new(
            crate::resources::ProviderRef::new("example", "machines", "1")?,
            vec![1],
            Some("checkpoint-1".into()),
        )?;
        coordinator
            .apply(
                operation_id,
                IdempotencyKey::new("checkpoint-before-crash")?,
                SchedulerEvent::Checkpointed {
                    operation_id,
                    checkpoint: checkpoint.clone(),
                    fence: LeaseFence::from(&first.reservation),
                },
            )
            .await?;
        coordinator
            .release_lease(&first, IdempotencyKey::new("release-crashed-worker")?)
            .await?;
        let second = coordinator
            .pull(&worker)
            .await?
            .ok_or_else(|| Error::NotFound("replacement lease".into()))?;
        assert_ne!(first.reservation.id, second.reservation.id);
        assert_eq!(second.checkpoint, Some(checkpoint));
        assert!(second.operation_revision > first.operation_revision);
        assert!(matches!(
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("stale-complete")?,
                    SchedulerEvent::Completed {
                        operation_id,
                        outcome: crate::Outcome::Succeeded(Value::Null),
                        fence: Some(LeaseFence::from(&first.reservation))
                    }
                )
                .await,
            Err(Error::Conflict(_) | Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[test]
    fn duplicate_reducer_registration_never_replaces_the_original() -> Result<()> {
        let entrypoint = EntrypointRef {
            name: "example.reducer".into(),
            version: "1".into(),
            digest: [3; 32],
            result_schema: serde_json::json!({"type": "number"}),
        };
        let original: Arc<dyn DurableReducer> = Arc::new(TestReducer {
            entrypoint: entrypoint.clone(),
            value: serde_json::json!(1),
        });
        let replacement: Arc<dyn DurableReducer> = Arc::new(TestReducer {
            entrypoint: entrypoint.clone(),
            value: serde_json::json!(2),
        });
        let mut registry = ReducerRegistry::new();
        registry.register(Arc::clone(&original))?;
        assert!(matches!(
            registry.register(replacement),
            Err(Error::Conflict(_))
        ));
        let retained = registry
            .get(&entrypoint)
            .ok_or_else(|| Error::NotFound("reducer".into()))?;
        assert!(Arc::ptr_eq(retained, &original));
        Ok(())
    }

    #[tokio::test]
    async fn generic_coordinator_apply_cannot_bypass_reducer_execution() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut coordinator = DistributedCoordinator::open(&client).await?;
        let operation_id = OperationId::from_bytes([13; 16]);
        let event = SchedulerEvent::Orchestrated {
            operation_id,
            expected_revision: 0,
            outcome: crate::Outcome::Succeeded(Value::Null),
            cancel: Vec::new(),
            reducer: Some(EntrypointRef {
                name: "example.reducer".into(),
                version: "1".into(),
                digest: [3; 32],
                result_schema: Value::Object(Default::default()),
            }),
            reduction_digest: Some([1; 32]),
        };
        assert!(matches!(
            coordinator
                .apply(
                    operation_id,
                    IdempotencyKey::new("forged-reduction")?,
                    event
                )
                .await,
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }
}
