//! Pure public message, provisional dequeue/claim and tool-continuation trace.
//! The existing Harness owns admission/reduction. This adapter consumes only
//! snapshots captured by this in-process trusted host after those reducers run.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "example assertions qualify the public consumer"
)]

use acyclic_client::*;
use acyclic_harness::{
    AgentId, Capabilities, IdempotencyKey, OperationId,
    conversation::{
        ConversationMessage, FileDescriptor, FileRef, MessageKind, ReferencedAttachments,
        VolumeClass, VolumeOwner, VolumeRef,
    },
    core::{Action, AggregateKind, Authority, AuthorityIssuer, Command, Reducer, SchemaRegistry},
    resources::ProviderRef,
    scheduler::{
        DurableOwner, EntrypointRef, LeaseFence, OperationPhase, OperationSpec, Orchestration,
        Reservation, ResourceRequest, Scheduler, SchedulerEvent,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
enum Projection {
    Message(Option<Arc<ConversationMessage>>),
    AwaitingToolResult(Arc<ConversationMessage>),
    Queue {
        available: bool,
        owner: Option<LeaseFence>,
    },
    // Pure continuation input references, never a model run or KV commitment.
    Continuation {
        call: Uuid,
        result: Arc<FileRef>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct Basis {
    authority: Authority,
    generation: [u8; 16],
    revision: u64,
    content: FileDescriptor,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum Assumption {
    MessageAbsent,
    QueueAvailable,
    ToolCall { call: Uuid, call_id: String },
    Continuation { call: Uuid },
}

// Private host envelope, not a public `verified: true` flag. It has no network
// deserializer; a real remote host must authenticate/reduce journal evidence.
struct TrustedSnapshot {
    key: u128,
    basis: Basis,
    value: Arc<Projection>,
    operation: Option<(OperationId, OperationOutcome)>,
}

fn snapshot(
    key: u128,
    authority: Authority,
    revision: u64,
    value: Projection,
    operation: Option<(OperationId, OperationOutcome)>,
) -> TrustedSnapshot {
    let content =
        FileDescriptor::from_bytes(&serde_json::to_vec(&value).unwrap(), "application/json")
            .unwrap();
    TrustedSnapshot {
        key,
        basis: Basis {
            authority,
            generation: [1; 16],
            revision,
            content,
        },
        value: Arc::new(value),
        operation,
    }
}

struct HarnessViews;
impl Domain for HarnessViews {
    type Key = u128;
    type Basis = Basis;
    type Operation = OperationId;
    type Assumption = Assumption;
    type Value = Projection;
    type Evidence = TrustedSnapshot;

    // Pin both semantic adapter implementation and revision, separate from source basis.
    fn identity(&self) -> u128 {
        0x4841524e4553530001
    }
    fn validate(
        &self,
        fact: &Fact<u128, Basis, Projection>,
        predicted: &Projection,
        assumption: &Assumption,
        work: usize,
    ) -> Result<(usize, usize), Error> {
        if work < 2 {
            return Err(Error::Budget);
        }
        match (&*fact.value, predicted, assumption) {
            (
                Projection::Message(None),
                Projection::Message(Some(message)),
                Assumption::MessageAbsent,
            ) => {
                message.validate().map_err(|_| Error::Conflict)?;
                if message.kind != MessageKind::User {
                    return Err(Error::Conflict);
                }
            }
            (
                Projection::Queue {
                    available: true,
                    owner: None,
                },
                Projection::Queue {
                    available: false, ..
                },
                Assumption::QueueAvailable,
            ) => {}
            (
                Projection::AwaitingToolResult(canonical_call),
                Projection::Message(Some(message)),
                Assumption::ToolCall { call, call_id },
            ) => {
                message.validate().map_err(|_| Error::Conflict)?;
                if message.kind != MessageKind::ToolResult
                    || message.reply_to != Some(*call)
                    || message.tool_call_id.as_ref() != Some(call_id)
                    || canonical_call.id != *call
                    || canonical_call.tool_call_id.as_ref() != Some(call_id)
                    || canonical_call.kind != MessageKind::ToolCall
                {
                    return Err(Error::Conflict);
                }
                // Canonical call identity/content is part of the exact source basis.
            }
            (
                _,
                Projection::Continuation { call, result },
                Assumption::Continuation { call: expected },
            ) => {
                result.validate().map_err(|_| Error::Conflict)?;
                if call != expected {
                    return Err(Error::Conflict);
                }
            }
            _ => return Err(Error::Unsupported),
        }
        let bytes = serde_json::to_vec(&(predicted, assumption, &fact.basis))
            .map_err(|_| Error::Conflict)?
            .len();
        if bytes > 4096 {
            return Err(Error::Budget);
        }
        Ok((bytes * 3, 2))
    }
    fn observe(
        &self,
        evidence: &TrustedSnapshot,
        current: Option<&Fact<u128, Basis, Projection>>,
        work: usize,
    ) -> Result<Observation<u128, Basis, OperationId, Projection>, Error> {
        if work < 2 {
            return Err(Error::Budget);
        }
        let encoded = serde_json::to_vec(&*evidence.value).map_err(|_| Error::Conflict)?;
        if encoded.len() > 4096 {
            return Err(Error::Budget);
        }
        evidence
            .basis
            .content
            .verify(&encoded)
            .map_err(|_| Error::Conflict)?;
        if let Some(old) = current
            && (old.basis.authority != evidence.basis.authority
                || old.basis.generation != evidence.basis.generation
                || evidence.basis.revision < old.basis.revision
                || (evidence.basis.revision == old.basis.revision && old.basis != evidence.basis))
        {
            return Err(Error::Conflict);
        }
        Ok(Observation {
            fact: Some(Fact {
                key: evidence.key,
                basis: evidence.basis.clone(),
                value: Arc::clone(&evidence.value),
                bytes: encoded.len() * 3 + 512,
            }),
            operation: evidence.operation,
            work: 2,
        })
    }
    fn corresponds(
        &self,
        predicted: &Projection,
        canonical: &Projection,
        work: usize,
    ) -> Result<(Correspondence, usize), Error> {
        if work < 1 {
            return Err(Error::Budget);
        }
        // This consumer's contract requires exact ref/linkage equality. Display
        // text equality is insufficient, and no KV equivalence is claimed.
        Ok((
            if predicted == canonical {
                Correspondence::Match
            } else {
                Correspondence::Different
            },
            1,
        ))
    }
}

fn file(label: &str, bytes: &[u8]) -> FileRef {
    let volume = VolumeRef::new(
        ProviderRef::new("example", "filesystem", "2").unwrap(),
        "project",
        VolumeClass::Project,
        VolumeOwner::Project("project".into()),
    )
    .unwrap();
    FileRef::new(
        volume,
        label,
        "generation-1",
        FileDescriptor::from_bytes(bytes, "text/plain").unwrap(),
        label,
    )
    .unwrap()
}

fn message(
    id: u128,
    sequence: u64,
    kind: MessageKind,
    body: &[u8],
    reply_to: Option<Uuid>,
    call_id: Option<&str>,
) -> ConversationMessage {
    ConversationMessage {
        id: Uuid::from_u128(id),
        sequence,
        kind,
        content: file("body.txt", body),
        attachments: ReferencedAttachments::Inline { items: vec![] },
        reply_to,
        tool_call_id: call_id.map(str::to_owned),
        extensions: BTreeMap::new(),
    }
}

fn op(id: u8) -> OperationId {
    OperationId::from_bytes([id; 16])
}

/// Execute the same reducer-backed public consumer trace on native and WASM.
///
/// # Panics
/// Panics if a public production transition violates the asserted contract.
#[allow(
    clippy::too_many_lines,
    reason = "one sequential trace makes canonical and speculative transitions directly comparable"
)]
pub fn trace() {
    let limits = Limits {
        records: 8,
        branches: 8,
        edges: 8,
        bytes: 262_144,
        work: 128,
        retention: 100,
        visible: 8,
    };
    let mut client = Client::new(HarnessViews, 900, 0, limits).unwrap();
    let authority = Authority {
        kind: AggregateKind::Conversation,
        id: "conversation".into(),
    };
    let issuer = AuthorityIssuer::new("example", [7; 32], authority.clone());
    let scope = issuer.root(
        "host",
        Capabilities::new(["conversation:bind", "conversation:append"]),
    );
    let mut reducer = Reducer::new(authority.clone(), issuer.verifier(), SchemaRegistry::new());
    let command = |id, revision, action| Command {
        operation_id: op(id),
        idempotency_key: IdempotencyKey(format!("{id}")),
        expected_revision: revision,
        scope: scope.clone(),
        causal_parent: None,
        action,
    };
    reducer
        .apply(command(
            1,
            0,
            Action::BindConversation {
                agent: AgentId::from_bytes([1; 16]),
            },
        ))
        .unwrap();
    let absent = snapshot(1, authority.clone(), 0, Projection::Message(None), None);
    client.observe(1, &absent).unwrap();
    let predicted = message(1, 1, MessageKind::User, b"hello", None, None);
    let branch = client
        .begin(Begin {
            key: 1,
            basis: absent.basis,
            operation: Some(op(2)),
            assumption: Assumption::MessageAbsent,
            predicted: Arc::new(Projection::Message(Some(Arc::new(predicted.clone())))),
            dependencies: vec![],
            expires: 50,
        })
        .unwrap();
    assert!(matches!(
        client.view(&1, &[branch]).unwrap().provenance,
        Provenance::Hypothesis { .. }
    ));
    assert!(
        reducer
            .conversation()
            .unwrap()
            .message(predicted.id)
            .is_none()
    );
    reducer
        .apply(command(
            2,
            1,
            Action::AppendConversationMessage {
                message: Box::new(predicted.clone()),
            },
        ))
        .unwrap();
    // Concurrent authoritative message is retained independently of the predicted key.
    let unrelated = message(2, 2, MessageKind::User, b"concurrent", None, None);
    reducer
        .apply(command(
            3,
            2,
            Action::AppendConversationMessage {
                message: Box::new(unrelated.clone()),
            },
        ))
        .unwrap();
    let canonical = reducer
        .conversation()
        .unwrap()
        .message(predicted.id)
        .unwrap();
    assert_eq!(reducer.operation_revision(op(2)), Some(2));
    client
        .observe(
            1,
            &snapshot(
                1,
                authority.clone(),
                1,
                Projection::Message(Some(Arc::new(canonical.clone()))),
                Some((op(2), OperationOutcome::Completed)),
            ),
        )
        .unwrap();
    client
        .observe(
            2,
            &snapshot(
                2,
                authority.clone(),
                2,
                Projection::Message(Some(Arc::new(unrelated))),
                None,
            ),
        )
        .unwrap();
    assert_eq!(
        client.hypothesis(branch).unwrap().prediction,
        PredictionOutcome::Confirmed
    );
    client.discard(branch).unwrap();
    assert!(matches!(
        &*client.view(&2, &[]).unwrap().value,
        Projection::Message(Some(_))
    ));

    // Dequeue is a local view of Scheduler's ready work, not a queue authority.
    let task_authority = Authority {
        kind: AggregateKind::Task,
        id: "task".into(),
    };
    let mut scheduler = Scheduler::default();
    let spec = OperationSpec {
        operation_id: op(20),
        parent: None,
        owner: DurableOwner::Attached {
            authority: task_authority.clone(),
        },
        entrypoint: EntrypointRef {
            name: "example.work".into(),
            version: "1".into(),
            digest: [1; 32],
            result_schema: serde_json::json!({}),
        },
        dependencies: BTreeSet::new(),
        resources: ResourceRequest::default(),
        placement: BTreeMap::new(),
        orchestration: Orchestration::Leaf,
        state: file("state.json", b"{}"),
    };
    scheduler.apply(scheduler.declare(spec).unwrap()).unwrap();
    let queue = snapshot(
        20,
        task_authority.clone(),
        1,
        Projection::Queue {
            available: true,
            owner: None,
        },
        None,
    );
    client.observe(20, &queue).unwrap();
    let predicted_claim = client
        .begin(Begin {
            key: 20,
            basis: queue.basis,
            operation: Some(op(20)),
            assumption: Assumption::QueueAvailable,
            predicted: Arc::new(Projection::Queue {
                available: false,
                owner: Some(LeaseFence {
                    reservation_id: "guess".into(),
                    placement: "worker-a".into(),
                }),
            }),
            dependencies: vec![],
            expires: 50,
        })
        .unwrap();
    let reservation = Reservation {
        id: "real".into(),
        placement: "worker-b".into(),
        admitted: ResourceRequest::default(),
    };
    scheduler
        .apply(SchedulerEvent::Admitted {
            operation_id: op(20),
            reservation,
        })
        .unwrap();
    let state = scheduler.operation(op(20)).unwrap();
    assert_eq!(state.phase, OperationPhase::Admitted);
    client
        .observe(
            20,
            &snapshot(
                20,
                task_authority,
                state.revision,
                Projection::Queue {
                    available: false,
                    owner: state.reservation.as_ref().map(LeaseFence::from),
                },
                Some((op(20), OperationOutcome::Admitted)),
            ),
        )
        .unwrap();
    assert_eq!(
        client.hypothesis(predicted_claim).unwrap().prediction,
        PredictionOutcome::Invalidated
    );
    assert_eq!(
        client.hypothesis(predicted_claim).unwrap().outcome,
        OperationOutcome::Admitted
    );
    assert!(
        matches!(&*client.view(&20, &[predicted_claim]).unwrap().value,
        Projection::Queue { owner: Some(fence), .. } if fence.placement == "worker-b")
    );

    // Tool result uses the existing canonical call/result pair and FileRef.
    let call = message(3, 3, MessageKind::ToolCall, b"{}", None, Some("call-1"));
    reducer
        .apply(command(
            4,
            3,
            Action::AppendConversationMessage {
                message: Box::new(call.clone()),
            },
        ))
        .unwrap();
    let absent_result = snapshot(
        30,
        authority.clone(),
        3,
        Projection::AwaitingToolResult(Arc::new(call.clone())),
        None,
    );
    let continuation_base = snapshot(
        31,
        authority.clone(),
        0,
        Projection::Continuation {
            call: call.id,
            result: Arc::new(call.content.clone()),
        },
        None,
    );
    client.observe(30, &absent_result).unwrap();
    client.observe(31, &continuation_base).unwrap();
    let predicted_result = message(
        4,
        4,
        MessageKind::ToolResult,
        b"predicted",
        Some(call.id),
        Some("call-1"),
    );
    let result_branch = client
        .begin(Begin {
            key: 30,
            basis: absent_result.basis,
            operation: Some(op(5)),
            assumption: Assumption::ToolCall {
                call: call.id,
                call_id: "call-1".into(),
            },
            predicted: Arc::new(Projection::Message(Some(Arc::new(
                predicted_result.clone(),
            )))),
            dependencies: vec![],
            expires: 50,
        })
        .unwrap();
    let continuation = client
        .begin(Begin {
            key: 31,
            basis: continuation_base.basis,
            operation: None,
            assumption: Assumption::Continuation { call: call.id },
            predicted: Arc::new(Projection::Continuation {
                call: call.id,
                result: Arc::new(predicted_result.content),
            }),
            dependencies: vec![Dependency::prediction(result_branch)],
            expires: 50,
        })
        .unwrap();
    let actual_result = message(
        4,
        4,
        MessageKind::ToolResult,
        b"actual",
        Some(call.id),
        Some("call-1"),
    );
    reducer
        .apply(command(
            5,
            4,
            Action::AppendConversationMessage {
                message: Box::new(actual_result.clone()),
            },
        ))
        .unwrap();
    client
        .observe(
            30,
            &snapshot(
                30,
                authority,
                4,
                Projection::Message(Some(Arc::new(actual_result))),
                Some((op(5), OperationOutcome::Completed)),
            ),
        )
        .unwrap();
    assert_eq!(
        client.hypothesis(result_branch).unwrap().prediction,
        PredictionOutcome::Replaced
    );
    assert_eq!(
        client.hypothesis(continuation).unwrap().prediction,
        PredictionOutcome::Invalidated
    );
    assert!(matches!(
        client.view(&31, &[continuation]).unwrap().provenance,
        Provenance::Authoritative(_)
    ));
    // No provider invocation, continuation admission, inference/KV reuse or IO occurred.
}

#[cfg(not(test))]
fn main() {
    trace();
    println!("message/claim/tool continuation trace passed");
}
