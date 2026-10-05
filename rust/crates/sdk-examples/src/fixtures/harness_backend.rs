//! Stateful Harness fixture backend used by the generated transport examples.
//!
//! The fixture deliberately keeps the journal in the same Rust-owned port as the
//! production wire adapter.  It is small enough to be deterministic, while still
//! exercising the properties that consumers rely on: immutable admission identity,
//! idempotent retries, ordered replay, and cancellation events.

use std::{collections::BTreeMap, sync::Arc};

use acyclic_harness::{
    Error, Result as HarnessResult, wire,
    wire_api::{
        HarnessWireApi, OperationControlRequest, current_protocol, negotiate,
        validate_command_protocol, validate_resume_protocol,
    },
};
use futures::{
    FutureExt as _,
    stream::{self, BoxStream},
};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
struct OperationRecord {
    command: wire::CommandEnvelope,
    status: wire::OperationStatus,
    events: Vec<wire::EventEnvelope>,
}

/// Deterministic Harness journal for real fixture-server and consumer tests.
#[derive(Clone, Default)]
pub struct StatefulHarnessFixtureBackend {
    operations: Arc<Mutex<BTreeMap<String, OperationRecord>>>,
}

impl std::fmt::Debug for StatefulHarnessFixtureBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("StatefulHarnessFixtureBackend").finish_non_exhaustive()
    }
}

impl StatefulHarnessFixtureBackend {
    /// Creates an empty journal.  No operation history is fabricated at startup.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn status_for(
        operation: wire::OperationIdentity,
        protocol: wire::ProtocolIdentity,
        owner: wire::Authority,
        state: wire::CompletionState,
        revision: u64,
        cancellation_requested: bool,
    ) -> wire::OperationStatus {
        wire::OperationStatus {
            operation: Some(operation),
            state: state as i32,
            error: None,
            protocol: Some(protocol),
            owner: Some(owner),
            cancellation_requested,
            revision,
        }
    }

    fn immutable_command(command: &wire::CommandEnvelope) -> wire::CommandEnvelope {
        let mut command = command.clone();
        // Scope proofs authenticate a retry but are not the operation identity.
        // A refreshed proof must not make an otherwise identical idempotent retry
        // look like a conflicting command.
        command.scope = None;
        command
    }

    fn same_identity(left: &wire::CommandEnvelope, right: &wire::CommandEnvelope) -> bool {
        Self::immutable_command(left) == Self::immutable_command(right)
    }

    fn accepted_event(
        command: &wire::CommandEnvelope,
        protocol: &wire::ProtocolIdentity,
        owner: &wire::Authority,
    ) -> wire::EventEnvelope {
        let operation_id = command
            .operation
            .as_ref()
            .map_or_else(String::new, |operation| operation.operation_id.clone());
        wire::EventEnvelope {
            protocol: Some(protocol.clone()),
            authority: Some(owner.clone()),
            revision: 1,
            operation_id,
            intent_digest: command.intent_digest.clone(),
            scope: command.scope.as_ref().map(|scope| wire::RecordedScope {
                id: scope.id.clone(),
                capabilities: scope.capabilities.clone(),
                issuer: scope.issuer.clone(),
                agent_id: scope.agent_id.clone(),
            }),
            causal_parent: command.causal_parent.clone(),
            event_type: "fixture.command.accepted".into(),
            canonical_payload_json: br#"{"status":"accepted"}"#.to_vec(),
            attestation: vec![1; 32],
        }
    }

    fn cancelled_event(record: &OperationRecord) -> wire::EventEnvelope {
        let operation_id = record
            .status
            .operation
            .as_ref()
            .map_or_else(String::new, |operation| operation.operation_id.clone());
        wire::EventEnvelope {
            protocol: record.status.protocol.clone(),
            authority: record.status.owner.clone(),
            revision: 2,
            operation_id,
            intent_digest: record.command.intent_digest.clone(),
            scope: record.command.scope.as_ref().map(|scope| wire::RecordedScope {
                id: scope.id.clone(),
                capabilities: scope.capabilities.clone(),
                issuer: scope.issuer.clone(),
                agent_id: scope.agent_id.clone(),
            }),
            causal_parent: record.command.causal_parent.clone(),
            event_type: "fixture.command.cancelled".into(),
            canonical_payload_json: br#"{"status":"cancelled"}"#.to_vec(),
            attestation: vec![2; 32],
        }
    }
}

fn wire_authority(owner: &acyclic_harness::core::Authority) -> wire::Authority {
    let kind = match owner.kind {
        acyclic_harness::core::AggregateKind::Agent => wire::AggregateKind::Agent,
        acyclic_harness::core::AggregateKind::Conversation => wire::AggregateKind::Conversation,
        acyclic_harness::core::AggregateKind::Session => wire::AggregateKind::Session,
        acyclic_harness::core::AggregateKind::Turn => wire::AggregateKind::Turn,
        acyclic_harness::core::AggregateKind::Task => wire::AggregateKind::Task,
    };
    wire::Authority {
        kind: kind as i32,
        id: owner.id.clone(),
    }
}

impl HarnessWireApi for StatefulHarnessFixtureBackend {
    fn handshake<'a>(
        &'a self,
        request: wire::HandshakeRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<wire::HandshakeResponse>> {
        async move {
            negotiate(
                &request,
                &wire::CapabilitySet {
                    capabilities: vec![
                        wire::Capability { name: "submit".into(), version: "1".into() },
                        wire::Capability { name: "replay".into(), version: "1".into() },
                        wire::Capability { name: "observe".into(), version: "1".into() },
                        wire::Capability { name: "cancel".into(), version: "1".into() },
                    ],
                },
            )
        }
        .boxed()
    }

    fn submit<'a>(
        &'a self,
        command: wire::CommandEnvelope,
    ) -> futures::future::BoxFuture<'a, HarnessResult<wire::Admission>> {
        async move {
            validate_command_protocol(&command)?;
            let operation = command
                .operation
                .clone()
                .ok_or_else(|| Error::Invalid("fixture command operation is missing".into()))?;
            let owner = command
                .authority
                .clone()
                .ok_or_else(|| Error::Invalid("fixture command authority is missing".into()))?;
            let protocol = command.protocol.clone().unwrap_or_else(current_protocol);
            let operation_id = operation.operation_id.clone();
            if operation_id.is_empty() {
                return Err(Error::Invalid("fixture operation id is missing".into()));
            }

            let mut operations = self.operations.lock().await;
            if let Some(existing) = operations.get(&operation_id) {
                if Self::same_identity(&existing.command, &command) {
                    return Ok(wire::Admission {
                        operation: existing.status.operation.clone(),
                        state: wire::AdmissionState::Accepted as i32,
                        error: None,
                    });
                }
                return Err(Error::Conflict(format!(
                    "fixture operation {operation_id} was admitted with a different identity"
                )));
            }

            let event = Self::accepted_event(&command, &protocol, &owner);
            let status = Self::status_for(
                operation.clone(),
                protocol,
                owner,
                wire::CompletionState::Succeeded,
                1,
                false,
            );
            operations.insert(
                operation_id,
                OperationRecord {
                    command: Self::immutable_command(&command),
                    status,
                    events: vec![event],
                },
            );
            Ok(wire::Admission {
                operation: Some(operation),
                state: wire::AdmissionState::Accepted as i32,
                error: None,
            })
        }
        .boxed()
    }

    fn replay<'a>(
        &'a self,
        request: wire::ResumeRequest,
    ) -> futures::future::BoxFuture<
        'a,
        HarnessResult<BoxStream<'static, HarnessResult<wire::Delivery>>>,
    > {
        async move {
            validate_resume_protocol(&request)?;
            let cursor = request.cursors.into_iter().next();
            let authority = cursor.as_ref().and_then(|value| value.authority.clone());
            let generation = cursor
                .as_ref()
                .map_or_else(|| "fixture-generation".into(), |value| value.generation.clone());
            let revision = cursor.as_ref().map_or(0, |value| value.revision);
            let deliveries: Vec<_> = self
                .operations
                .lock()
                .await
                .values()
                .filter(|record| {
                    authority.as_ref().map_or(true, |expected| {
                        record.status.owner.as_ref() == Some(expected)
                    })
                })
                .filter_map(|record| {
                    let events: Vec<_> = record
                        .events
                        .iter()
                        .filter(|event| event.revision > revision)
                        .cloned()
                        .collect();
                    let first = events.first()?.revision;
                    let last = events.last()?.revision;
                    Some(Ok(wire::Delivery {
                        authority: record.status.owner.clone(),
                        generation: generation.clone(),
                        from_revision: first,
                        through_revision: last,
                        events,
                        live: true,
                    }))
                })
                .collect();
            Ok(Box::pin(stream::iter(deliveries)) as BoxStream<'static, HarnessResult<wire::Delivery>>)
        }
        .boxed()
    }

    fn authorize_operation_control<'a>(
        &'a self,
        request: &'a OperationControlRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<()>> {
        let operation_id = request.operation_id.to_string();
        let owner = wire_authority(&request.owner);
        async move {
            let operations = self.operations.lock().await;
            let record = operations.get(&operation_id).ok_or_else(|| {
                Error::NotFound(format!("fixture operation {operation_id} is unknown"))
            })?;
            if record.status.owner.as_ref() != Some(&owner) {
                return Err(Error::Unauthorized(format!(
                    "fixture operation {operation_id} owner does not match"
                )));
            }
            Ok(())
        }
        .boxed()
    }

    fn observe<'a>(
        &'a self,
        request: wire::ObserveRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<wire::OperationStatus>> {
        async move {
            let record = self
                .operations
                .lock()
                .await
                .get(&request.operation_id)
                .cloned()
                .ok_or_else(|| Error::NotFound(format!("fixture operation {} is unknown", request.operation_id)))?;
            if request.owner.as_ref() != record.status.owner.as_ref() {
                return Err(Error::Unauthorized(format!(
                    "fixture operation {} owner does not match",
                    request.operation_id
                )));
            }
            Ok(record.status)
        }
        .boxed()
    }

    fn cancel<'a>(
        &'a self,
        request: wire::CancelRequest,
    ) -> futures::future::BoxFuture<'a, HarnessResult<wire::CancelResponse>> {
        async move {
            let mut operations = self.operations.lock().await;
            let record = operations.get_mut(&request.operation_id).ok_or_else(|| {
                Error::NotFound(format!("fixture operation {} is unknown", request.operation_id))
            })?;
            if request.owner.as_ref() != record.status.owner.as_ref() {
                return Err(Error::Unauthorized(format!(
                    "fixture operation {} owner does not match",
                    request.operation_id
                )));
            }
            if record.status.state != wire::CompletionState::Cancelled as i32 {
                record.status.state = wire::CompletionState::Cancelled as i32;
                record.status.cancellation_requested = true;
                record.status.revision = 2;
                let event = Self::cancelled_event(record);
                record.events.push(event);
            }
            let operation = wire::OperationIdentity {
                operation_id: request.operation_id.clone(),
                idempotency_key: request.idempotency_key.clone(),
            };
            Ok(wire::CancelResponse {
                status: Some(record.status.clone()),
                operation: Some(operation),
            })
        }
        .boxed()
    }
}

/// Builds a Harness tonic server around one Rust-owned stateful journal.
///
/// Keeping the backend injection point explicit makes the loopback server and
/// the in-process collector use exactly the same implementation and state
/// semantics. The convenience constructor below is what normal fixture
/// binaries use when they do not need to inspect the journal directly.
pub fn harness_server_with_backend(
    backend: StatefulHarnessFixtureBackend,
) -> acyclic_harness::grpc::transport::harness_service_server::HarnessServiceServer<
    acyclic_harness::grpc::HarnessGrpcService,
> {
    acyclic_harness::grpc::HarnessGrpcService::new(Arc::new(backend)).into_server()
}

/// Builds the Harness tonic server with a fresh stateful Rust fixture journal.
pub fn harness_server()
-> acyclic_harness::grpc::transport::harness_service_server::HarnessServiceServer<
    acyclic_harness::grpc::HarnessGrpcService,
> {
    harness_server_with_backend(StatefulHarnessFixtureBackend::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;

    fn owner() -> wire::Authority {
        wire::Authority { kind: wire::AggregateKind::Task as i32, id: "fixture-task".into() }
    }

    fn command(operation_id: &str, idempotency_key: &str, action: &str) -> wire::CommandEnvelope {
        wire::CommandEnvelope {
            protocol: Some(current_protocol()),
            authority: Some(owner()),
            operation: Some(wire::OperationIdentity {
                operation_id: operation_id.into(),
                idempotency_key: idempotency_key.into(),
            }),
            action_type: action.into(),
            canonical_action_json: format!(r#"{{"action":"{action}"}}"#).into_bytes(),
            intent_digest: vec![action.as_bytes().first().copied().unwrap_or_default(); 32],
            ..wire::CommandEnvelope::default()
        }
    }

    fn resume(revision: u64) -> wire::ResumeRequest {
        wire::ResumeRequest {
            protocol: Some(current_protocol()),
            cursors: vec![wire::ReplayCursor {
                authority: Some(owner()),
                generation: "fixture-generation".into(),
                revision,
            }],
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn submit_is_idempotent_and_rejects_conflicting_identity() {
        let backend = StatefulHarnessFixtureBackend::new();
        let first = command("fixture-op", "key-a", "run");
        let repeated = backend.submit(first.clone()).await.expect("first admission");
        let retry = backend.submit(first).await.expect("idempotent retry");
        assert_eq!(repeated, retry);

        let conflicting = command("fixture-op", "key-b", "run");
        assert!(matches!(backend.submit(conflicting).await, Err(Error::Conflict(_))));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replay_returns_all_operations_and_cancel_progression() {
        let backend = StatefulHarnessFixtureBackend::new();
        backend.submit(command("fixture-a", "key-a", "run")).await.expect("a");
        backend.submit(command("fixture-b", "key-b", "run")).await.expect("b");

        let mut deliveries = backend.replay(resume(0)).await.expect("initial replay");
        let mut initial = Vec::new();
        while let Some(delivery) = deliveries.next().await {
            initial.push(delivery.expect("delivery"));
        }
        assert_eq!(initial.len(), 2);
        assert_eq!(initial.iter().map(|d| d.events.len()).sum::<usize>(), 2);
        assert!(initial.iter().all(|d| d.through_revision == 1));

        let cancelled = backend
            .cancel(wire::CancelRequest {
                operation_id: "fixture-a".into(),
                owner: Some(owner()),
                protocol: Some(current_protocol()),
                idempotency_key: "cancel-a".into(),
                ..wire::CancelRequest::default()
            })
            .await
            .expect("cancel");
        assert_eq!(cancelled.status.expect("status").revision, 2);

        let mut deliveries = backend.replay(resume(1)).await.expect("incremental replay");
        let mut incremental = Vec::new();
        while let Some(delivery) = deliveries.next().await {
            incremental.push(delivery.expect("delivery"));
        }
        assert_eq!(incremental.len(), 1);
        assert_eq!(incremental[0].events[0].event_type, "fixture.command.cancelled");
        assert_eq!(incremental[0].from_revision, 2);
        assert_eq!(incremental[0].through_revision, 2);

        let repeated_cancel = backend
            .cancel(wire::CancelRequest {
                operation_id: "fixture-a".into(),
                owner: Some(owner()),
                protocol: Some(current_protocol()),
                idempotency_key: "cancel-a".into(),
                ..wire::CancelRequest::default()
            })
            .await
            .expect("idempotent cancel");
        assert_eq!(repeated_cancel.status.expect("status").revision, 2);
        let mut deliveries = backend.replay(resume(1)).await.expect("replay after retry");
        assert_eq!(deliveries.next().await.expect("delivery").expect("ok").events.len(), 1);
        assert!(deliveries.next().await.is_none());
    }
}
