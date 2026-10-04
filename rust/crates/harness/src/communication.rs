//! Durable, host-neutral contracts for task messages and waits.
//!
//! The stream-backed host owns persistence and capability checks.  This module
//! owns the part that must be identical for every host: target authorization,
//! bounded cursors, stable identities, and wait state transitions.  Keeping
//! these checks here prevents a terminal adapter or a future remote host from
//! quietly inventing a different communication protocol.

use crate::{
    Error, OperationId, Outcome, Result, TaskId, conversation::FileRef, runtime::DurableTaskHost,
    scheduler::InboxItem,
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use futures::future::{BoxFuture, join_all};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, sync::Arc, time::Duration};

/// Maximum entries returned by one durable inbox observation.
pub const MAX_INBOX_PAGE: usize = 1_024;
/// Maximum task outcomes observed by one wait request.
pub const MAX_WAIT_TASKS: usize = 64;
/// Maximum time a caller may ask one wait operation to remain pending.
///
/// The deadline is an absolute Unix timestamp.  This bound keeps a malformed
/// request from becoming an effectively unbounded durable record while still
/// allowing long-running coding agents to wait for children.
pub const MAX_WAIT_DURATION_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const MESSAGE_CONTRACT: &str = "harness.message.v1";
const WAIT_CONTRACT: &str = "harness.wait.v1";

/// Derives the stream idempotency identity for one message endpoint pair.
///
/// The caller supplied message operation remains the model-visible delivery
/// identity.  Persistence must additionally namespace it by both endpoints so
/// two senders can safely use the same operation value for their own message
/// sequence without colliding in the recipient's mailbox stream.
#[must_use]
pub fn message_endpoint_operation(
    sender: TaskId,
    recipient: TaskId,
    message_id: OperationId,
) -> OperationId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"harness:message-endpoints:v1\0");
    hasher.update(&sender.into_bytes());
    hasher.update(&recipient.into_bytes());
    hasher.update(&message_id.into_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

fn nonzero_task(task: TaskId, label: &str) -> Result<()> {
    if task.into_bytes() == [0; 16] {
        return Err(Error::Invalid(format!("{label} identity is nil")));
    }
    Ok(())
}

fn nonzero_operation(operation: OperationId, label: &str) -> Result<()> {
    if operation.into_bytes() == [0; 16] {
        return Err(Error::Invalid(format!("{label} identity is nil")));
    }
    Ok(())
}

/// Authorization relationship selected by the caller when sending a message.
///
/// Parent and child targets are derived from the two immutable admissions.
/// There is no implicit sibling or session-wide broadcast target; a future
/// cross-branch grant must carry an owner-attested capability rather than a
/// model-selected recipient list.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MessageTarget {
    /// The recipient must be the sender's direct parent.
    Parent,
    /// The recipient must be a direct child of the sender.
    Child,
}

impl MessageTarget {
    /// Validates that the target grant is well formed.
    pub fn validate(&self, recipient: TaskId, sender: TaskId) -> Result<()> {
        nonzero_task(sender, "sender")?;
        nonzero_task(recipient, "recipient")?;
        if sender == recipient {
            return Err(Error::Invalid(
                "message recipient must differ from sender".into(),
            ));
        }
        Ok(())
    }

    /// Checks the target against the immutable parent links of both tasks.
    pub fn authorize(
        &self,
        sender: TaskId,
        recipient: TaskId,
        sender_parent: Option<TaskId>,
        recipient_parent: Option<TaskId>,
    ) -> Result<()> {
        self.validate(recipient, sender)?;
        match self {
            Self::Parent if sender_parent == Some(recipient) => Ok(()),
            Self::Child if recipient_parent == Some(sender) => Ok(()),
            Self::Parent => Err(Error::Unauthorized(
                "message recipient is not the sender's direct parent".into(),
            )),
            Self::Child => Err(Error::Unauthorized(
                "message recipient is not the sender's direct child".into(),
            )),
        }
    }
}

/// One ref-only durable message request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageRequest {
    /// Task that owns the sending capability.
    pub sender: TaskId,
    /// Task that owns the receiving inbox.
    pub recipient: TaskId,
    /// Caller-retained idempotency identity.
    pub message_id: OperationId,
    /// Explicit target authorization recorded with this request.
    pub target: MessageTarget,
    /// Version-pinned message content.
    pub payload: FileRef,
}

impl MessageRequest {
    /// Validates identities, target authorization shape, and content refs.
    pub fn validate(&self) -> Result<()> {
        self.target.validate(self.recipient, self.sender)?;
        nonzero_operation(self.message_id, "message")?;
        self.payload.validate()
    }

    /// Returns the versioned canonical request retained by an admission
    /// journal. The host may store only a content reference to these bytes.
    #[must_use]
    pub fn canonical_value(&self) -> Value {
        serde_json::json!({
            "contract": MESSAGE_CONTRACT,
            "sender": self.sender,
            "recipient": self.recipient,
            "message_id": self.message_id,
            "target": self.target,
            "payload": self.payload,
        })
    }

    /// Encodes the exact canonical request bytes.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        self.validate()?;
        crate::contract::canonical_json_bytes(&self.canonical_value())
    }

    /// Decodes and validates a versioned canonical request.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self> {
        let value: Value =
            serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        let canonical = value.clone();
        let mut body = value
            .as_object()
            .cloned()
            .ok_or_else(|| Error::Invalid("message request must be an object".into()))?;
        if body.remove("contract") != Some(Value::String(MESSAGE_CONTRACT.into())) {
            return Err(Error::Invalid(
                "unsupported message request contract".into(),
            ));
        }
        let request: Self = serde_json::from_value(Value::Object(body))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        request.validate()?;
        if request.canonical_value() != canonical
            || crate::contract::canonical_json_bytes(&canonical)? != bytes
        {
            return Err(Error::Invalid("message request is not canonical".into()));
        }
        Ok(request)
    }

    /// Returns a stable operation key that includes both endpoint identities.
    ///
    /// The stream operation ID remains the caller's idempotency key.  This
    /// digest is useful to journals and diagnostics because it prevents two
    /// endpoint pairs from being confused when inspecting a delivery.
    #[must_use]
    pub fn endpoint_digest(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"harness:message-endpoints:v1\0");
        hasher.update(&self.sender.into_bytes());
        hasher.update(&self.recipient.into_bytes());
        hasher.update(&self.message_id.into_bytes());
        *hasher.finalize().as_bytes()
    }

    /// Returns the endpoint-scoped stream idempotency identity used by the
    /// durable host while retaining the caller's message ID in the inbox.
    #[must_use]
    pub fn endpoint_operation(&self) -> OperationId {
        message_endpoint_operation(self.sender, self.recipient, self.message_id)
    }
}

/// Validates a bounded ordered inbox page returned by a host.
pub fn validate_inbox_page(
    task_id: TaskId,
    after: u64,
    limit: usize,
    items: &[InboxItem],
) -> Result<()> {
    nonzero_task(task_id, "inbox task")?;
    if limit == 0 || limit > MAX_INBOX_PAGE || items.len() > limit {
        return Err(Error::Invalid("inbox page bound is invalid".into()));
    }
    let mut expected = after
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("inbox cursor exhausted".into()))?;
    let mut identities = BTreeSet::new();
    for item in items {
        if item.task_id != task_id || item.sequence != expected {
            return Err(Error::Conflict(
                "inbox delivery sequence is not contiguous".into(),
            ));
        }
        nonzero_task(item.sender, "inbox sender")?;
        if item.delivered_at_epoch_ms == 0 {
            return Err(Error::Invalid(
                "inbox delivery timestamp is invalid".into(),
            ));
        }
        let identity = OperationId::parse(&item.message_id)
            .map_err(|_| Error::Invalid("inbox message identity is not canonical".into()))?;
        nonzero_operation(identity, "inbox message")?;
        // Message IDs are caller-scoped. Distinct authenticated senders may
        // legitimately reuse one ID in the same recipient mailbox, while a
        // repeated sender/ID pair still indicates duplicate delivery.
        if !identities.insert((item.sender, identity)) {
            return Err(Error::Conflict(
                "inbox page repeats a message identity".into(),
            ));
        }
        item.payload.validate()?;
        expected = expected
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("inbox sequence exhausted".into()))?;
    }
    Ok(())
}

/// Exact durable target of a wait operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WaitTarget {
    /// Observe terminal outcomes for these specifically named tasks.
    Tasks {
        /// Exact task identities to observe.
        task_ids: Vec<TaskId>,
    },
    /// Observe newly committed messages after a task-local cursor.
    Messages {
        /// Inbox owner; it must equal the waiter.
        task_id: TaskId,
        /// Last sequence already observed.
        after: u64,
        /// Maximum number of messages to return.
        limit: usize,
    },
    /// Suspend at this absolute Unix timestamp.
    Deadline {
        /// Absolute Unix deadline in milliseconds.
        deadline_epoch_ms: u64,
    },
}

impl WaitTarget {
    fn validate(&self, waiter: TaskId) -> Result<()> {
        match self {
            Self::Tasks { task_ids } => {
                if task_ids.is_empty() || task_ids.len() > MAX_WAIT_TASKS {
                    return Err(Error::Invalid("wait task target set is invalid".into()));
                }
                let mut seen = BTreeSet::new();
                for task in task_ids {
                    nonzero_task(*task, "wait target")?;
                    if *task == waiter || !seen.insert(*task) {
                        return Err(Error::Invalid(
                            "wait task targets must be distinct and external".into(),
                        ));
                    }
                }
                Ok(())
            }
            Self::Messages {
                task_id,
                after,
                limit,
            } => {
                nonzero_task(*task_id, "wait inbox")?;
                if *task_id != waiter {
                    return Err(Error::Unauthorized(
                        "wait may only observe the caller's inbox".into(),
                    ));
                }
                if *limit == 0 || *limit > MAX_INBOX_PAGE {
                    return Err(Error::Invalid("wait inbox page bound is invalid".into()));
                }
                if *after == u64::MAX {
                    return Err(Error::Invalid("wait inbox cursor is exhausted".into()));
                }
                Ok(())
            }
            Self::Deadline { deadline_epoch_ms } if *deadline_epoch_ms == 0 => {
                Err(Error::Invalid("wait deadline is invalid".into()))
            }
            Self::Deadline { .. } => Ok(()),
        }
    }
}

/// One persisted wait declaration.  The request is immutable; only the
/// terminal status can advance, which makes recovery a replay of a known wait
/// identity rather than a second subscription.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitRequest {
    /// Stable identity used to reconcile a lost wait admission.
    pub operation_id: OperationId,
    /// Task that owns this wait.
    pub waiter: TaskId,
    /// One explicit task, message, or deadline target.
    pub target: WaitTarget,
    /// Optional absolute bound for the wait's lifetime.
    pub timeout_epoch_ms: Option<u64>,
    /// Optional caller-owned cancellation identity.
    pub cancellation_id: Option<OperationId>,
}

impl WaitRequest {
    /// Validates the immutable wait declaration.
    pub fn validate(&self, now_epoch_ms: Option<u64>) -> Result<()> {
        nonzero_operation(self.operation_id, "wait")?;
        nonzero_task(self.waiter, "waiter")?;
        self.target.validate(self.waiter)?;
        if let WaitTarget::Deadline { deadline_epoch_ms } = &self.target {
            if let Some(now) = now_epoch_ms {
                let duration = deadline_epoch_ms.saturating_sub(now);
                if *deadline_epoch_ms > now && duration > MAX_WAIT_DURATION_MS {
                    return Err(Error::Invalid(
                        "wait deadline is outside the permitted window".into(),
                    ));
                }
            }
        }
        if self.cancellation_id == Some(self.operation_id) {
            return Err(Error::Invalid(
                "wait cancellation identity must differ from wait".into(),
            ));
        }
        if let Some(cancellation) = self.cancellation_id {
            nonzero_operation(cancellation, "wait cancellation")?;
        }
        if let Some(timeout) = self.timeout_epoch_ms {
            if timeout == 0 {
                return Err(Error::Invalid("wait timeout is invalid".into()));
            }
            if let Some(now) = now_epoch_ms {
                let duration = timeout.saturating_sub(now);
                if timeout <= now || duration > MAX_WAIT_DURATION_MS {
                    return Err(Error::Invalid(
                        "wait timeout is outside the permitted window".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Durable wait lifecycle.  Terminal states are deliberately explicit so a
/// timeout or cancellation can never be mistaken for a successful observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitStatus {
    /// The declaration has been persisted and is awaiting its target.
    Pending,
    /// The requested target was observed.
    Completed,
    /// The caller explicitly cancelled the wait.
    Cancelled,
    /// The caller's wait bound elapsed before the target was observed.
    TimedOut,
}

/// Persisted wait record used for restart and reconciliation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WaitRecord {
    /// Immutable wait declaration.
    pub request: WaitRequest,
    /// Current lifecycle state.
    pub status: WaitStatus,
}

/// Result of observing one explicit wait target.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WaitCompletion {
    /// Every requested task has a terminal observation.
    Tasks {
        /// Outcomes in the same order as the request's task IDs.
        outcomes: Vec<(TaskId, Outcome<Value>)>,
    },
    /// One or more newly committed messages were observed.
    Messages {
        /// Ordered inbox records after the request cursor.
        items: Vec<InboxItem>,
    },
    /// The durable deadline was reached.
    Deadline,
    /// The caller cancelled the observation before completion.
    Cancelled,
    /// The caller's absolute timeout elapsed before completion.
    TimedOut,
}

impl WaitRequest {
    /// Validates that a retained terminal result belongs to this exact wait
    /// declaration.  Replay must not reinterpret a completion from another
    /// target, cursor, or page shape merely because it has the same operation
    /// identity.
    pub fn validate_completion(&self, completion: &WaitCompletion) -> Result<()> {
        self.validate_completion_at(completion, None)
    }

    /// Validates a retained terminal result against the current clock when
    /// the completion is a caller timeout. A durable store must not replay a
    /// timeout before the declared bound has elapsed.
    pub fn validate_completion_at(
        &self,
        completion: &WaitCompletion,
        now_epoch_ms: Option<u64>,
    ) -> Result<()> {
        match (&self.target, completion) {
            (WaitTarget::Tasks { task_ids }, WaitCompletion::Tasks { outcomes }) => {
                if outcomes.len() != task_ids.len()
                    || outcomes
                        .iter()
                        .zip(task_ids)
                        .any(|((actual, _), expected)| actual != expected)
                {
                    return Err(Error::Conflict(
                        "wait task completion does not match its requested task order".into(),
                    ));
                }
            }
            (
                WaitTarget::Messages {
                    task_id,
                    after,
                    limit,
                },
                WaitCompletion::Messages { items },
            ) => {
                validate_inbox_page(*task_id, *after, *limit, items)?;
            }
            (WaitTarget::Deadline { deadline_epoch_ms }, WaitCompletion::Deadline) => {
                if let Some(completed_at) = now_epoch_ms
                    && completed_at < *deadline_epoch_ms
                {
                    return Err(Error::Conflict(
                        "deadline completion arrived before its declared deadline".into(),
                    ));
                }
            }
            (_, WaitCompletion::Cancelled) => {}
            (_, WaitCompletion::TimedOut) if self.timeout_epoch_ms.is_some() => {
                if let (Some(timeout), Some(now)) = (self.timeout_epoch_ms, now_epoch_ms)
                    && timeout > now
                {
                    return Err(Error::Conflict(
                        "wait timeout completion arrived before its declared bound".into(),
                    ));
                }
            }
            (_, WaitCompletion::TimedOut) => {
                return Err(Error::Conflict(
                    "wait timeout completion has no declared timeout".into(),
                ));
            }
            (WaitTarget::Tasks { .. }, _)
            | (WaitTarget::Messages { .. }, _)
            | (WaitTarget::Deadline { .. }, _) => {
                return Err(Error::Conflict(
                    "wait completion kind does not match its requested target".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Host-backed communication and wait adapter.
///
/// This adapter only composes the existing durable host boundary.  It does
/// not own a second journal or a process-local mailbox.  The host remains the
/// authority for admission, grants, idempotent publication, and recovery.
pub trait DurableWaitStore: Send + Sync {
    /// Persists one immutable wait admission before observation begins.
    /// Returns a previously retained terminal completion during recovery.
    fn open<'a>(&'a self, request: WaitRequest) -> BoxFuture<'a, Result<Option<WaitCompletion>>>;

    /// Persists a terminal result before exposing it to the caller. A
    /// concurrent owner must return the already retained winner.
    fn complete<'a>(
        &'a self,
        request: WaitRequest,
        completion: WaitCompletion,
    ) -> BoxFuture<'a, Result<WaitCompletion>>;

    /// Persists an explicit cancellation declaration before the caller is
    /// allowed to observe the cancelled outcome. Hosts may invoke this from a
    /// task cancellation endpoint before a process restart; the retained
    /// completion is then returned by [`Self::open`] during recovery.
    fn cancel<'a>(&'a self, request: WaitRequest) -> BoxFuture<'a, Result<WaitCompletion>> {
        Box::pin(async move {
            if request.cancellation_id.is_none() {
                return Err(Error::Invalid(
                    "durable wait cancellation requires its declared cancellation identity".into(),
                ));
            }
            self.complete(request, WaitCompletion::Cancelled).await
        })
    }
}

// Completion records carry an owner-clock timestamp.  This is a new journal
// shape; old records are rejected at the contract boundary instead of being
// decoded with a fabricated timestamp.
const WAIT_EVENT_CONTRACT: &str = "harness.wait-event.v2";
const WAIT_STREAM_PAGE: u32 = 1_024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum PersistedWaitEvent {
    Admission {
        contract: String,
        request: WaitRequest,
    },
    Completion {
        contract: String,
        request: WaitRequest,
        completion: WaitCompletion,
        /// Owner-clock timestamp captured when the terminal result was
        /// durably appended. Timeout validity is checked against this value
        /// during replay rather than against a later recovery clock.
        completed_at_epoch_ms: u64,
    },
}

impl PersistedWaitEvent {
    fn request(&self) -> &WaitRequest {
        match self {
            Self::Admission { request, .. } | Self::Completion { request, .. } => request,
        }
    }

    fn canonical_bytes(&self) -> Result<Vec<u8>> {
        crate::contract::canonical_json_bytes(self)
    }

    fn from_canonical_bytes(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)
            .map_err(|error| Error::Storage(format!("wait event is not JSON: {error}")))?;
        if crate::contract::canonical_json_bytes(&value)? != bytes {
            return Err(Error::Storage("wait event is not canonical JSON".into()));
        }
        let event: Self = serde_json::from_value(value)
            .map_err(|error| Error::Storage(format!("wait event is invalid: {error}")))?;
        let contract = match &event {
            Self::Admission { contract, .. } | Self::Completion { contract, .. } => contract,
        };
        if contract != WAIT_EVENT_CONTRACT {
            return Err(Error::Storage("wait event contract is unsupported".into()));
        }
        event.request().validate(None)?;
        Ok(event)
    }
}

#[derive(Clone)]
struct RetainedWait {
    completion: Option<WaitCompletion>,
}

/// Stream-backed owner journal for wait admissions and terminal results.
///
/// The stream is partitioned by waiter identity and uses stable idempotency
/// keys for the admission and completion phases. This adapter can be bound to
/// the same `StreamClient` used by `CoordinatorTaskHost`; it does not create a
/// second coordinator or local recovery cache.
pub struct StreamWaitStore<P> {
    stream: StreamClient<P>,
}

impl<P: StreamProvider> StreamWaitStore<P> {
    /// Binds wait persistence to an existing owner stream provider.
    #[must_use]
    pub fn new(stream: StreamClient<P>) -> Self {
        Self { stream }
    }

    fn wait_stream(&self, waiter: TaskId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/waits/{waiter}"))
            .map_err(|error| Error::Storage(error.to_string()))
    }

    fn event_key(kind: &str, request: &WaitRequest) -> Result<StreamKey> {
        let identity = format!(
            "harness/v2/waits/{kind}/{}/{}",
            request.waiter, request.operation_id
        );
        StreamKey::new(Bytes::copy_from_slice(
            blake3::hash(identity.as_bytes()).as_bytes(),
        ))
        .map_err(|error| Error::Storage(error.to_string()))
    }

    async fn read_events(
        &self,
        stream: &acyclic_stream::Stream<P>,
        waiter: TaskId,
    ) -> Result<Vec<PersistedWaitEvent>> {
        let bounds = match stream.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(Vec::new()),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let mut events = Vec::new();
        let mut from = 0_u64;
        while from < bounds.tail {
            let limit = (bounds.tail - from).min(u64::from(WAIT_STREAM_PAGE)) as u32;
            let records = stream
                .read(from, limit)
                .await
                .map_err(|error| Error::Storage(error.to_string()))?
                .try_collect::<Vec<_>>()
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            if records.is_empty() {
                return Err(Error::Storage("wait journal read made no progress".into()));
            }
            for record in records {
                if record.sequence != from {
                    return Err(Error::Storage("wait journal has a sequence gap".into()));
                }
                let event = PersistedWaitEvent::from_canonical_bytes(&record.value)?;
                if event.request().waiter != waiter {
                    return Err(Error::Storage("wait journal crosses waiter scope".into()));
                }
                events.push(event);
                from = from.saturating_add(1);
            }
        }
        Ok(events)
    }

    fn retained(
        events: &[PersistedWaitEvent],
        request: &WaitRequest,
        now_epoch_ms: u64,
    ) -> Result<Option<RetainedWait>> {
        let mut retained = None;
        for event in events {
            if event.request().operation_id != request.operation_id {
                continue;
            }
            if event.request() != request {
                return Err(Error::Conflict(
                    "wait operation identity was reused with another request".into(),
                ));
            }
            match event {
                PersistedWaitEvent::Admission { .. } => {
                    if retained.is_some() {
                        return Err(Error::Storage(
                            "wait journal has duplicate admission".into(),
                        ));
                    }
                    retained = Some(RetainedWait { completion: None });
                }
                PersistedWaitEvent::Completion {
                    completion,
                    completed_at_epoch_ms,
                    ..
                } => {
                    let Some(current) = retained.as_mut() else {
                        return Err(Error::Storage(
                            "wait completion is missing its admission".into(),
                        ));
                    };
                    if current.completion.is_some() {
                        return Err(Error::Storage(
                            "wait journal has duplicate completion".into(),
                        ));
                    }
                    if *completed_at_epoch_ms == 0 {
                        return Err(Error::Storage(
                            "wait completion is missing its durable timestamp".into(),
                        ));
                    }
                    if *completed_at_epoch_ms > now_epoch_ms {
                        return Err(Error::Conflict(
                            "wait completion timestamp is in the future".into(),
                        ));
                    }
                    request.validate_completion_at(completion, Some(*completed_at_epoch_ms))?;
                    current.completion = Some(completion.clone());
                }
            }
        }
        Ok(retained)
    }

    async fn append(
        &self,
        stream: &acyclic_stream::Stream<P>,
        request: &WaitRequest,
        event: PersistedWaitEvent,
        kind: &str,
    ) -> Result<()> {
        let bytes = event.canonical_bytes()?;
        let key = Self::event_key(kind, request)?;
        match stream
            .append_batch(vec![Bytes::from(bytes)], None, Some(key))
            .await
        {
            Ok(AppendOutcome::Committed(_)) => Ok(()),
            Ok(AppendOutcome::TailConflict { .. }) => Err(Error::Storage(
                "wait journal append unexpectedly conflicted on tail".into(),
            )),
            Err(StreamError::IdempotencyMismatch) => Err(Error::Conflict(
                "wait journal operation identity was reused".into(),
            )),
            Err(error) => Err(Error::Storage(error.to_string())),
        }
    }
}

impl<P: StreamProvider> DurableWaitStore for StreamWaitStore<P> {
    fn open<'a>(&'a self, request: WaitRequest) -> BoxFuture<'a, Result<Option<WaitCompletion>>> {
        Box::pin(async move {
            request.validate(None)?;
            let now_epoch_ms = unix_millis()?;
            let stream = self.wait_stream(request.waiter)?;
            let events = self.read_events(&stream, request.waiter).await?;
            if let Some(retained) = Self::retained(&events, &request, now_epoch_ms)? {
                return Ok(retained.completion);
            }
            let append = self
                .append(
                    &stream,
                    &request,
                    PersistedWaitEvent::Admission {
                        contract: WAIT_EVENT_CONTRACT.into(),
                        request: request.clone(),
                    },
                    "admission",
                )
                .await;
            if let Err(error @ Error::Conflict(_)) = &append {
                let events = self.read_events(&stream, request.waiter).await?;
                if let Some(retained) = Self::retained(&events, &request, unix_millis()?)? {
                    return Ok(retained.completion);
                }
                return Err(error.clone());
            }
            append?;
            let events = self.read_events(&stream, request.waiter).await?;
            let Some(retained) = Self::retained(&events, &request, unix_millis()?)? else {
                return Err(Error::Storage(
                    "wait admission disappeared after append".into(),
                ));
            };
            Ok(retained.completion)
        })
    }

    fn complete<'a>(
        &'a self,
        request: WaitRequest,
        completion: WaitCompletion,
    ) -> BoxFuture<'a, Result<WaitCompletion>> {
        Box::pin(async move {
            request.validate(None)?;
            if matches!(completion, WaitCompletion::Cancelled) && request.cancellation_id.is_none()
            {
                return Err(Error::Invalid(
                    "durable wait cancellation requires its declared cancellation identity".into(),
                ));
            }
            let stream = self.wait_stream(request.waiter)?;
            let events = self.read_events(&stream, request.waiter).await?;
            if let Some(retained) = Self::retained(&events, &request, unix_millis()?)? {
                if let Some(completion) = retained.completion {
                    return Ok(completion);
                }
            } else {
                return Err(Error::Conflict(
                    "wait completion has no retained admission".into(),
                ));
            }
            let completed_at_epoch_ms = unix_millis()?;
            request.validate_completion_at(&completion, Some(completed_at_epoch_ms))?;
            let append = self
                .append(
                    &stream,
                    &request,
                    PersistedWaitEvent::Completion {
                        contract: WAIT_EVENT_CONTRACT.into(),
                        request: request.clone(),
                        completion,
                        completed_at_epoch_ms,
                    },
                    "completion",
                )
                .await;
            if let Err(error @ Error::Conflict(_)) = &append {
                let events = self.read_events(&stream, request.waiter).await?;
                if let Some(retained) = Self::retained(&events, &request, unix_millis()?)? {
                    if let Some(completion) = retained.completion {
                        return Ok(completion);
                    }
                }
                return Err(error.clone());
            }
            append?;
            let events = self.read_events(&stream, request.waiter).await?;
            let Some(retained) = Self::retained(&events, &request, unix_millis()?)? else {
                return Err(Error::Storage(
                    "wait completion disappeared after append".into(),
                ));
            };
            retained.completion.ok_or_else(|| {
                Error::Storage("wait completion was not retained after append".into())
            })
        })
    }

    fn cancel<'a>(&'a self, request: WaitRequest) -> BoxFuture<'a, Result<WaitCompletion>> {
        Box::pin(async move {
            if request.cancellation_id.is_none() {
                return Err(Error::Invalid(
                    "durable wait cancellation requires its declared cancellation identity".into(),
                ));
            }
            let stream = self.wait_stream(request.waiter)?;
            let events = self.read_events(&stream, request.waiter).await?;
            let Some(retained) = Self::retained(&events, &request, unix_millis()?)? else {
                return Err(Error::Conflict(
                    "wait cancellation has no retained admission".into(),
                ));
            };
            if let Some(completion) = retained.completion {
                return Ok(completion);
            }
            self.complete(request, WaitCompletion::Cancelled).await
        })
    }
}

#[derive(Clone)]
/// Host-backed task messages and restart-safe waits.
pub struct DurableCommunication {
    host: Arc<dyn DurableTaskHost>,
    waits: Option<Arc<dyn DurableWaitStore>>,
}

impl DurableCommunication {
    /// Binds communication to one owner-retained durable host.
    #[must_use]
    pub fn new(host: Arc<dyn DurableTaskHost>) -> Self {
        Self { host, waits: None }
    }

    /// Binds owner-retained wait admission and completion persistence.
    #[must_use]
    pub fn with_wait_store(mut self, waits: Arc<dyn DurableWaitStore>) -> Self {
        self.waits = Some(waits);
        self
    }

    /// Validates target authorization from owner-retained admissions before
    /// publishing the ref-only message through the host.
    pub async fn send(&self, request: MessageRequest) -> Result<()> {
        request.validate()?;
        let sender = self.host.observe_admission(request.sender).await?;
        let recipient = self.host.observe_admission(request.recipient).await?;
        request.target.authorize(
            request.sender,
            request.recipient,
            sender.parent,
            recipient.parent,
        )?;
        self.host
            .send(
                request.sender,
                request.recipient,
                request.message_id,
                request.payload,
            )
            .await
    }

    /// Reads and validates one ordered, bounded inbox page.
    pub async fn inbox(&self, task_id: TaskId, after: u64, limit: usize) -> Result<Vec<InboxItem>> {
        let items = self.host.inbox(task_id, after, limit).await?;
        validate_inbox_page(task_id, after, limit, &items)?;
        Ok(items)
    }

    /// Runs a restart-safe wait with explicit cancellation and timeout.
    ///
    /// A timer target is persisted by `wait_until`; task and inbox targets are
    /// observations over owner-retained state.  Cancellation and timeout are
    /// returned as typed completions and never reported as successful waits.
    pub async fn wait(
        &self,
        request: WaitRequest,
        mut cancellation: Option<tokio::sync::watch::Receiver<bool>>,
    ) -> Result<WaitCompletion> {
        let initial_now = unix_millis()?;
        // A request whose timeout has already elapsed is a valid replay of a
        // previously admitted wait. Preserve the typed terminal result rather
        // than turning recovery into an invalid-input error. Zero remains
        // invalid through the ordinary validation path.
        let expired = request
            .timeout_epoch_ms
            .is_some_and(|deadline| deadline <= initial_now);
        let deadline_expired = matches!(
            &request.target,
            WaitTarget::Deadline { deadline_epoch_ms } if *deadline_epoch_ms <= initial_now
        );
        if expired || deadline_expired {
            request.validate(None)?;
        } else {
            request.validate(Some(initial_now))?;
        }
        if cancellation.is_some() && request.cancellation_id.is_none() {
            return Err(Error::Invalid(
                "live wait cancellation requires its declared cancellation identity".into(),
            ));
        }
        if cancellation.is_some() && self.waits.is_none() {
            return Err(Error::Unsupported(
                "live wait cancellation requires an owner-retained durable wait store".into(),
            ));
        }
        self.authorize_wait(&request).await?;
        if let Some(waits) = &self.waits {
            if let Some(completion) = waits.open(request.clone()).await? {
                request.validate_completion_at(&completion, Some(unix_millis()?))?;
                return Ok(completion);
            }
        }

        // Authorization and durable admission may cross the caller's
        // deadline. Re-read the owner clock after admission so an operation
        // that became due while being admitted is retained as a typed
        // terminal result instead of entering the observation path with a
        // stale duration.
        let now = unix_millis()?;
        let expired = request
            .timeout_epoch_ms
            .is_some_and(|deadline| deadline <= now);
        let deadline_expired = matches!(
            &request.target,
            WaitTarget::Deadline { deadline_epoch_ms } if *deadline_epoch_ms <= now
        );
        if expired {
            return self.finish(request, WaitCompletion::TimedOut).await;
        }
        if deadline_expired {
            return self.finish(request, WaitCompletion::Deadline).await;
        }
        if cancellation
            .as_ref()
            .is_some_and(|receiver| *receiver.borrow())
        {
            return self.finish(request, WaitCompletion::Cancelled).await;
        }
        let timeout = request
            .timeout_epoch_ms
            .map(|deadline| Duration::from_millis(deadline.saturating_sub(now)));
        let wait = self.observe(request.clone());
        tokio::pin!(wait);
        let timeout_sleep = async {
            if let Some(duration) = timeout {
                tokio::time::sleep(duration).await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(timeout_sleep);
        loop {
            tokio::select! {
                result = &mut wait => return self.finish(request.clone(), result?).await,
                () = &mut timeout_sleep => return self.finish(request.clone(), WaitCompletion::TimedOut).await,
                changed = async {
                    match cancellation.as_mut() {
                        Some(receiver) => receiver.changed().await.map_err(|_| ()),
                        None => std::future::pending::<std::result::Result<(), ()>>().await,
                    }
                } => {
                    if changed.is_ok() && cancellation.as_ref().is_some_and(|receiver| *receiver.borrow()) {
                        return self.finish(request.clone(), WaitCompletion::Cancelled).await;
                    }
                    // A dropped sender is not cancellation. Remove the
                    // receiver so the select loop does not spin on an error
                    // forever while the durable observation remains pending.
                    if changed.is_err() {
                        cancellation = None;
                    }
                }
            }
        }
    }

    /// Durably declares cancellation for one admitted wait. This endpoint is
    /// intentionally separate from the live watch bridge so a cancellation
    /// request survives process loss and can be replayed after restart.
    pub async fn cancel(&self, request: WaitRequest) -> Result<WaitCompletion> {
        request.validate(None)?;
        if request.cancellation_id.is_none() {
            return Err(Error::Invalid(
                "durable wait cancellation requires its declared cancellation identity".into(),
            ));
        }
        self.authorize_wait(&request).await?;
        let waits = self.waits.as_ref().ok_or_else(|| {
            Error::Unsupported(
                "wait cancellation requires an owner-retained durable wait store".into(),
            )
        })?;
        request.validate_completion(&WaitCompletion::Cancelled)?;
        let completion = waits.cancel(request.clone()).await?;
        request.validate_completion(&completion)?;
        Ok(completion)
    }

    async fn finish(
        &self,
        request: WaitRequest,
        completion: WaitCompletion,
    ) -> Result<WaitCompletion> {
        request.validate_completion_at(&completion, Some(unix_millis()?))?;
        match &self.waits {
            Some(waits) => {
                let retained = waits.complete(request.clone(), completion).await?;
                request.validate_completion_at(&retained, Some(unix_millis()?))?;
                Ok(retained)
            }
            None => Ok(completion),
        }
    }

    async fn authorize_wait(&self, request: &WaitRequest) -> Result<()> {
        // A wait is an owner-scoped observation just like message delivery.
        // Authenticate the waiter before returning a retained timeout or
        // cancellation so a caller cannot use a known operation identity as a
        // bearer credential after restart.
        self.host.observe_admission(request.waiter).await?;
        let WaitTarget::Tasks { task_ids } = &request.target else {
            return Ok(());
        };
        for task_id in task_ids {
            let admission = self.host.observe_admission(*task_id).await?;
            if admission.parent != Some(request.waiter) {
                return Err(Error::Unauthorized(
                    "wait target is not a direct child of the waiter".into(),
                ));
            }
        }
        Ok(())
    }

    async fn observe(&self, request: WaitRequest) -> Result<WaitCompletion> {
        match request.target {
            WaitTarget::Tasks { task_ids } => {
                let futures = task_ids.into_iter().map(|task_id| async move {
                    Ok::<_, Error>((task_id, self.host.wait_outcome(task_id).await?))
                });
                let outcomes = join_all(futures)
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;
                Ok(WaitCompletion::Tasks { outcomes })
            }
            WaitTarget::Messages {
                task_id,
                after,
                limit,
            } => {
                let mut delay = 25_u64;
                loop {
                    let items = self.inbox(task_id, after, limit).await?;
                    if !items.is_empty() {
                        return Ok(WaitCompletion::Messages { items });
                    }
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    delay = delay.saturating_mul(2).min(1_000);
                }
            }
            WaitTarget::Deadline { deadline_epoch_ms } => {
                self.host
                    .wait_until(request.waiter, request.operation_id, deadline_epoch_ms)
                    .await?;
                Ok(WaitCompletion::Deadline)
            }
        }
    }
}

fn unix_millis() -> Result<u64> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| Error::Invalid(error.to_string()))?
        .as_millis();
    u64::try_from(millis).map_err(|_| Error::Invalid("system time is not representable".into()))
}

impl WaitRecord {
    /// Creates a validated pending record.
    pub fn pending(request: WaitRequest, now_epoch_ms: Option<u64>) -> Result<Self> {
        request.validate(now_epoch_ms)?;
        Ok(Self {
            request,
            status: WaitStatus::Pending,
        })
    }

    /// Advances a pending record to one terminal state.
    pub fn finish(&mut self, status: WaitStatus) -> Result<()> {
        if !matches!(
            status,
            WaitStatus::Completed | WaitStatus::Cancelled | WaitStatus::TimedOut
        ) {
            return Err(Error::Invalid(
                "wait may only finish with a terminal status".into(),
            ));
        }
        if self.status != WaitStatus::Pending {
            return Err(Error::Conflict("wait is already terminal".into()));
        }
        self.status = status;
        Ok(())
    }

    /// Returns canonical bytes suitable for a durable content record.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        crate::contract::canonical_json_bytes(&self.canonical_value())
    }

    /// Returns the versioned canonical record envelope.
    #[must_use]
    pub fn canonical_value(&self) -> Value {
        serde_json::json!({
            "contract": WAIT_CONTRACT,
            "request": self.request,
            "status": self.status,
        })
    }

    /// Reconstructs a persisted wait record and rejects noncanonical bytes.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self> {
        let value: Value =
            serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        let mut body = value
            .as_object()
            .cloned()
            .ok_or_else(|| Error::Invalid("wait record must be an object".into()))?;
        if body.remove("contract") != Some(Value::String(WAIT_CONTRACT.into())) {
            return Err(Error::Invalid("unsupported wait record contract".into()));
        }
        let record: Self = serde_json::from_value(Value::Object(body))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        record.request.validate(None)?;
        if record.canonical_value() != value
            || crate::contract::canonical_json_bytes(&value)? != bytes
        {
            return Err(Error::Invalid("wait record is not canonical".into()));
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Capabilities,
        conversation::{FileDescriptor, Limits, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
        runtime::{DurableTaskHost, TaskAdmissionRecord, TaskRunLimits},
    };
    use serde_json::json;
    use std::{collections::BTreeMap, sync::Mutex};

    fn task(value: u8) -> TaskId {
        TaskId::from_bytes([value; 16])
    }

    fn operation(value: u8) -> OperationId {
        OperationId::from_bytes([value; 16])
    }

    fn payload() -> Result<FileRef> {
        let volume = VolumeRef::new(
            ProviderRef::new("test", "filesystem", "2")?,
            "private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(crate::AgentId::from_bytes([9; 16])),
        )?;
        FileRef::new(
            volume,
            "message.json",
            "v1",
            FileDescriptor::from_bytes(b"{}", "application/json")?,
            "message.json",
        )
    }

    fn admission(task_id: u8, parent: Option<TaskId>) -> Result<TaskAdmissionRecord> {
        let schema = json!({"type": "object"});
        TaskAdmissionRecord::from_parts(
            operation(task_id),
            "test.communication",
            "1",
            json!({}),
            schema.clone(),
            schema,
            &BTreeSet::new(),
            &[task_id; 32],
            parent,
            Capabilities::new(["mail:send", "mail:read", "timer:wait"]),
            Limits::default(),
            TaskRunLimits::default(),
            None,
            None,
            None,
        )
    }

    struct RecordingHost {
        admissions: BTreeMap<TaskId, TaskAdmissionRecord>,
        sent: Mutex<Vec<MessageRequest>>,
        inbox: Vec<InboxItem>,
        outcomes: BTreeMap<TaskId, Outcome<Value>>,
        observed_outcomes: Mutex<Vec<TaskId>>,
        observe_delay: Duration,
    }

    impl DurableTaskHost for RecordingHost {
        fn observe_admission<'a>(
            &'a self,
            task_id: TaskId,
        ) -> futures::future::BoxFuture<'a, Result<TaskAdmissionRecord>> {
            let delay = self.observe_delay;
            Box::pin(async move {
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                self.admissions
                    .get(&task_id)
                    .cloned()
                    .ok_or_else(|| Error::NotFound("task admission".into()))
            })
        }

        fn outcome<'a>(
            &'a self,
            task_id: TaskId,
        ) -> futures::future::BoxFuture<'a, Result<Option<Outcome<Value>>>> {
            Box::pin(async move { Ok(self.outcomes.get(&task_id).cloned()) })
        }

        fn wait_outcome<'a>(
            &'a self,
            task_id: TaskId,
        ) -> futures::future::BoxFuture<'a, Result<Outcome<Value>>> {
            Box::pin(async move {
                self.observed_outcomes
                    .lock()
                    .map_err(|_| Error::Storage("recording host lock poisoned".into()))?
                    .push(task_id);
                self.outcomes
                    .get(&task_id)
                    .cloned()
                    .ok_or_else(|| Error::NotFound("task outcome".into()))
            })
        }

        fn cancel<'a>(&'a self, _task_id: TaskId) -> futures::future::BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }

        fn send<'a>(
            &'a self,
            sender: TaskId,
            recipient: TaskId,
            message_id: OperationId,
            payload: FileRef,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.sent
                    .lock()
                    .map_err(|_| Error::Storage("recording host lock poisoned".into()))?
                    .push(MessageRequest {
                        sender,
                        recipient,
                        message_id,
                        target: MessageTarget::Child,
                        payload,
                    });
                Ok(())
            })
        }

        fn inbox<'a>(
            &'a self,
            _task_id: TaskId,
            _after: u64,
            _limit: usize,
        ) -> futures::future::BoxFuture<'a, Result<Vec<InboxItem>>> {
            Box::pin(async move { Ok(self.inbox.clone()) })
        }

        fn wait_until<'a>(
            &'a self,
            _task_id: TaskId,
            _operation_id: OperationId,
            _deadline_unix_ms: u64,
        ) -> futures::future::BoxFuture<'a, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    fn host(outcomes: BTreeMap<TaskId, Outcome<Value>>) -> Result<Arc<RecordingHost>> {
        let root = task(9);
        let mut admissions = BTreeMap::new();
        admissions.insert(task(1), admission(1, Some(root))?);
        admissions.insert(task(2), admission(2, Some(task(1)))?);
        admissions.insert(task(3), admission(3, Some(task(1)))?);
        admissions.insert(root, admission(9, None)?);
        Ok(Arc::new(RecordingHost {
            admissions,
            sent: Mutex::new(Vec::new()),
            inbox: Vec::new(),
            outcomes,
            observed_outcomes: Mutex::new(Vec::new()),
            observe_delay: Duration::ZERO,
        }))
    }

    fn item(sequence: u64, id: u8) -> Result<InboxItem> {
        Ok(InboxItem {
            task_id: task(2),
            sender: task(1),
            delivered_at_epoch_ms: 1,
            sequence,
            message_id: operation(id).to_string(),
            payload: payload()?,
        })
    }

    #[test]
    fn parent_and_child_targets_are_closed_world() -> Result<()> {
        let sender = task(1);
        let child = task(2);
        MessageTarget::Parent.authorize(sender, task(3), Some(task(3)), None)?;
        MessageTarget::Child.authorize(sender, child, None, Some(sender))?;
        assert!(
            MessageTarget::Child
                .authorize(sender, task(3), None, Some(task(4)))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn inbox_validation_requires_gapless_canonical_delivery() -> Result<()> {
        let first = item(1, 10)?;
        let second = item(2, 11)?;
        validate_inbox_page(task(2), 0, 2, &[first.clone(), second])?;
        assert!(validate_inbox_page(task(2), 0, 2, &[first.clone(), first.clone()]).is_err());
        assert!(validate_inbox_page(task(2), 1, 2, &[first.clone()]).is_err());
        assert!(validate_inbox_page(task(3), 0, 2, &[first]).is_err());
        let mut invalid_sender = item(1, 12)?;
        invalid_sender.sender = TaskId::from_bytes([0; 16]);
        assert!(validate_inbox_page(task(2), 0, 1, &[invalid_sender]).is_err());
        let mut invalid_timestamp = item(1, 13)?;
        invalid_timestamp.delivered_at_epoch_ms = 0;
        assert!(validate_inbox_page(task(2), 0, 1, &[invalid_timestamp]).is_err());
        Ok(())
    }

    #[test]
    fn wait_records_are_canonical_and_terminal_once() -> Result<()> {
        let request = WaitRequest {
            operation_id: operation(20),
            waiter: task(1),
            target: WaitTarget::Tasks {
                task_ids: vec![task(2), task(3)],
            },
            timeout_epoch_ms: Some(10_000),
            cancellation_id: Some(operation(21)),
        };
        let mut record = WaitRecord::pending(request, Some(1_000))?;
        let bytes = record.canonical_bytes()?;
        assert_eq!(WaitRecord::from_canonical_bytes(&bytes)?, record);
        let mut tampered: Value = serde_json::from_slice(&bytes).expect("canonical wait");
        tampered["contract"] = Value::String("harness.wait.v0".into());
        assert!(WaitRecord::from_canonical_bytes(&serde_json::to_vec(&tampered).unwrap()).is_err());
        record.finish(WaitStatus::TimedOut)?;
        assert!(record.finish(WaitStatus::Cancelled).is_err());
        Ok(())
    }

    #[test]
    fn waits_reject_self_duplicate_and_oversized_targets() {
        let self_target = WaitRequest {
            operation_id: operation(20),
            waiter: task(1),
            target: WaitTarget::Tasks {
                task_ids: vec![task(1)],
            },
            timeout_epoch_ms: None,
            cancellation_id: None,
        };
        assert!(self_target.validate(None).is_err());
        let duplicate_target = WaitRequest {
            target: WaitTarget::Tasks {
                task_ids: vec![task(2), task(2)],
            },
            ..self_target.clone()
        };
        assert!(duplicate_target.validate(None).is_err());
        let mut oversized = self_target;
        oversized.target = WaitTarget::Tasks {
            task_ids: (1..=MAX_WAIT_TASKS as u8 + 1).map(task).collect(),
        };
        assert!(oversized.validate(None).is_err());
    }

    #[test]
    fn retained_wait_completion_is_bound_to_target_shape() -> Result<()> {
        let request = WaitRequest {
            operation_id: operation(22),
            waiter: task(1),
            target: WaitTarget::Tasks {
                task_ids: vec![task(2), task(3)],
            },
            timeout_epoch_ms: None,
            cancellation_id: None,
        };
        let reversed = WaitCompletion::Tasks {
            outcomes: vec![
                (task(3), Outcome::Succeeded(json!(3))),
                (task(2), Outcome::Succeeded(json!(2))),
            ],
        };
        assert!(matches!(
            request.validate_completion(&reversed),
            Err(Error::Conflict(message)) if message.contains("task order")
        ));
        assert!(matches!(
            request.validate_completion(&WaitCompletion::Deadline),
            Err(Error::Conflict(message)) if message.contains("kind")
        ));
        let inbox = WaitRequest {
            target: WaitTarget::Messages {
                task_id: task(1),
                after: 2,
                limit: 1,
            },
            ..request
        };
        assert!(
            inbox
                .validate_completion(&WaitCompletion::Messages {
                    items: vec![item(1, 23)?],
                })
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn timeout_completion_must_not_precede_its_declared_bound() -> Result<()> {
        let request = WaitRequest {
            operation_id: operation(23),
            waiter: task(1),
            target: WaitTarget::Messages {
                task_id: task(1),
                after: 0,
                limit: 1,
            },
            timeout_epoch_ms: Some(2_000),
            cancellation_id: None,
        };
        assert!(matches!(
            request.validate_completion_at(&WaitCompletion::TimedOut, Some(1_999)),
            Err(Error::Conflict(message)) if message.contains("before")
        ));
        request.validate_completion_at(&WaitCompletion::TimedOut, Some(2_000))?;
        Ok(())
    }

    #[test]
    fn deadline_completion_must_not_precede_its_declared_deadline() -> Result<()> {
        let request = WaitRequest {
            operation_id: operation(24),
            waiter: task(1),
            target: WaitTarget::Deadline {
                deadline_epoch_ms: 2_000,
            },
            timeout_epoch_ms: None,
            cancellation_id: None,
        };
        assert!(matches!(
            request.validate_completion_at(&WaitCompletion::Deadline, Some(1_999)),
            Err(Error::Conflict(message)) if message.contains("before")
        ));
        request.validate_completion_at(&WaitCompletion::Deadline, Some(2_000))?;
        Ok(())
    }

    #[test]
    fn message_identity_binds_both_endpoints() -> Result<()> {
        let first = MessageRequest {
            sender: task(1),
            recipient: task(2),
            message_id: operation(3),
            target: MessageTarget::Child,
            payload: payload()?,
        };
        let mut second = first.clone();
        second.recipient = task(4);
        assert_ne!(first.endpoint_digest(), second.endpoint_digest());
        assert_eq!(
            first.endpoint_operation(),
            message_endpoint_operation(first.sender, first.recipient, first.message_id)
        );
        assert_ne!(first.endpoint_operation(), second.endpoint_operation());
        first.validate()?;
        let bytes = first.canonical_bytes()?;
        assert_eq!(MessageRequest::from_canonical_bytes(&bytes)?, first);
        let mut tampered: Value = serde_json::from_slice(&bytes).expect("canonical message");
        tampered["contract"] = Value::String("harness.message.v0".into());
        assert!(
            MessageRequest::from_canonical_bytes(&serde_json::to_vec(&tampered).unwrap()).is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn durable_send_checks_direct_target_before_publishing() -> Result<()> {
        let host = host(BTreeMap::new())?;
        let communication = DurableCommunication::new(host.clone());
        let request = MessageRequest {
            sender: task(1),
            recipient: task(3),
            message_id: operation(30),
            target: MessageTarget::Parent,
            payload: payload()?,
        };
        assert!(matches!(
            communication.send(request).await,
            Err(Error::Unauthorized(_))
        ));
        assert!(host.sent.lock().expect("test lock").is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn durable_send_allows_only_the_admitted_direct_child() -> Result<()> {
        let host = host(BTreeMap::new())?;
        let communication = DurableCommunication::new(host.clone());
        communication
            .send(MessageRequest {
                sender: task(1),
                recipient: task(2),
                message_id: operation(31),
                target: MessageTarget::Child,
                payload: payload()?,
            })
            .await?;
        assert_eq!(host.sent.lock().expect("test lock").len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn wait_requires_an_admitted_waiter_before_replaying_terminal_state() -> Result<()> {
        let host = host(BTreeMap::new())?;
        let communication = DurableCommunication::new(host);
        let now = unix_millis()?;
        assert!(matches!(
            communication
                .wait(
                    WaitRequest {
                        operation_id: operation(42),
                        waiter: task(8),
                        target: WaitTarget::Deadline {
                            deadline_epoch_ms: now.saturating_sub(1),
                        },
                        timeout_epoch_ms: None,
                        cancellation_id: None,
                    },
                    None,
                )
                .await,
            Err(Error::NotFound(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn wait_preserves_requested_task_order_and_requires_direct_children() -> Result<()> {
        let outcomes = BTreeMap::from([
            (task(2), Outcome::Succeeded(json!("child-2"))),
            (
                task(3),
                Outcome::Failed {
                    message: "child-3 failed".into(),
                },
            ),
        ]);
        let host = host(outcomes)?;
        let communication = DurableCommunication::new(host);
        let completion = communication
            .wait(
                WaitRequest {
                    operation_id: operation(32),
                    waiter: task(1),
                    target: WaitTarget::Tasks {
                        task_ids: vec![task(3), task(2)],
                    },
                    timeout_epoch_ms: None,
                    cancellation_id: None,
                },
                None,
            )
            .await?;
        let WaitCompletion::Tasks { outcomes } = completion else {
            panic!("unexpected wait completion");
        };
        assert_eq!(outcomes[0].0, task(3));
        assert_eq!(outcomes[1].0, task(2));
        Ok(())
    }

    #[tokio::test]
    async fn inbox_wait_can_resume_with_explicit_cancellation() -> Result<()> {
        let host = host(BTreeMap::new())?;
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let waits = Arc::new(StreamWaitStore::new(acyclic_stream::StreamClient::new(
            provider,
        )));
        let communication = DurableCommunication::new(host).with_wait_store(waits);
        let (sender, receiver) = tokio::sync::watch::channel(false);
        let wait = tokio::spawn(async move {
            communication
                .wait(
                    WaitRequest {
                        operation_id: operation(33),
                        waiter: task(1),
                        target: WaitTarget::Messages {
                            task_id: task(1),
                            after: 0,
                            limit: 10,
                        },
                        timeout_epoch_ms: None,
                        cancellation_id: Some(operation(34)),
                    },
                    Some(receiver),
                )
                .await
        });
        sender
            .send(true)
            .map_err(|_| Error::Storage("cancel channel closed".into()))?;
        assert_eq!(
            wait.await
                .map_err(|error| Error::Storage(error.to_string()))??,
            WaitCompletion::Cancelled
        );
        Ok(())
    }

    #[tokio::test]
    async fn wait_replays_an_expired_timeout_as_typed_terminal_state() -> Result<()> {
        let host = host(BTreeMap::new())?;
        let now = unix_millis()?;
        let completion = DurableCommunication::new(host)
            .wait(
                WaitRequest {
                    operation_id: operation(35),
                    waiter: task(1),
                    target: WaitTarget::Messages {
                        task_id: task(1),
                        after: 0,
                        limit: 10,
                    },
                    timeout_epoch_ms: Some(now.saturating_sub(1)),
                    cancellation_id: None,
                },
                None,
            )
            .await?;
        assert_eq!(completion, WaitCompletion::TimedOut);
        Ok(())
    }

    #[tokio::test]
    async fn expired_deadline_is_typed_and_does_not_dispatch_a_timer() -> Result<()> {
        let host = host(BTreeMap::new())?;
        let now = unix_millis()?;
        let completion = DurableCommunication::new(host)
            .wait(
                WaitRequest {
                    operation_id: operation(40),
                    waiter: task(1),
                    target: WaitTarget::Deadline {
                        deadline_epoch_ms: now.saturating_sub(1),
                    },
                    timeout_epoch_ms: None,
                    cancellation_id: None,
                },
                None,
            )
            .await?;
        assert_eq!(completion, WaitCompletion::Deadline);
        Ok(())
    }

    #[tokio::test]
    async fn wait_rechecks_clock_after_slow_durable_admission() -> Result<()> {
        let mut host = host(BTreeMap::new())?;
        Arc::get_mut(&mut host)
            .expect("recording host is uniquely owned")
            .observe_delay = Duration::from_millis(30);
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let stream = acyclic_stream::StreamClient::new(provider);
        let waits = Arc::new(StreamWaitStore::new(stream.clone()));
        let deadline = unix_millis()?.saturating_add(5);
        let completion = DurableCommunication::new(host)
            .with_wait_store(waits)
            .wait(
                WaitRequest {
                    operation_id: operation(48),
                    waiter: task(1),
                    target: WaitTarget::Deadline {
                        deadline_epoch_ms: deadline,
                    },
                    timeout_epoch_ms: None,
                    cancellation_id: None,
                },
                None,
            )
            .await?;
        assert_eq!(completion, WaitCompletion::Deadline);
        let journal = stream
            .stream(format!("harness/v2/waits/{}", task(1)))
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(journal.bounds().await?.tail, 2);
        Ok(())
    }

    #[tokio::test]
    async fn wait_rechecks_timeout_after_slow_durable_admission() -> Result<()> {
        let mut host = host(BTreeMap::new())?;
        Arc::get_mut(&mut host)
            .expect("recording host is uniquely owned")
            .observe_delay = Duration::from_millis(30);
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let stream = acyclic_stream::StreamClient::new(provider);
        let waits = Arc::new(StreamWaitStore::new(stream.clone()));
        let timeout = unix_millis()?.saturating_add(5);
        let completion = DurableCommunication::new(host)
            .with_wait_store(waits)
            .wait(
                WaitRequest {
                    operation_id: operation(49),
                    waiter: task(1),
                    target: WaitTarget::Messages {
                        task_id: task(1),
                        after: 0,
                        limit: 1,
                    },
                    timeout_epoch_ms: Some(timeout),
                    cancellation_id: None,
                },
                None,
            )
            .await?;
        assert_eq!(completion, WaitCompletion::TimedOut);
        let journal = stream
            .stream(format!("harness/v2/waits/{}", task(1)))
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(journal.bounds().await?.tail, 2);
        Ok(())
    }

    #[test]
    fn future_deadline_must_fit_the_wait_horizon() -> Result<()> {
        let now = unix_millis()?;
        let request = WaitRequest {
            operation_id: operation(41),
            waiter: task(1),
            target: WaitTarget::Deadline {
                deadline_epoch_ms: now + MAX_WAIT_DURATION_MS + 1,
            },
            timeout_epoch_ms: None,
            cancellation_id: None,
        };
        assert!(matches!(
            request.validate(Some(now)),
            Err(Error::Invalid(message)) if message.contains("deadline")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn wait_observes_already_cancelled_and_closed_channels_without_spinning() -> Result<()> {
        let host = host(BTreeMap::new())?;
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let waits = Arc::new(StreamWaitStore::new(acyclic_stream::StreamClient::new(
            provider,
        )));
        let communication = DurableCommunication::new(host.clone()).with_wait_store(waits);
        let (sender, receiver) = tokio::sync::watch::channel(true);
        let completion = communication
            .wait(
                WaitRequest {
                    operation_id: operation(36),
                    waiter: task(1),
                    target: WaitTarget::Messages {
                        task_id: task(1),
                        after: 0,
                        limit: 10,
                    },
                    timeout_epoch_ms: None,
                    cancellation_id: Some(operation(35)),
                },
                Some(receiver),
            )
            .await?;
        assert_eq!(completion, WaitCompletion::Cancelled);
        drop(sender);
        let now = unix_millis()?;
        let completion = tokio::time::timeout(
            Duration::from_millis(250),
            communication.wait(
                WaitRequest {
                    operation_id: operation(37),
                    waiter: task(1),
                    target: WaitTarget::Messages {
                        task_id: task(1),
                        after: 0,
                        limit: 10,
                    },
                    timeout_epoch_ms: Some(now + 40),
                    cancellation_id: Some(operation(34)),
                },
                Some(tokio::sync::watch::channel(false).1),
            ),
        )
        .await
        .map_err(|_| Error::Storage("closed cancellation wait exceeded bound".into()))??;
        assert_eq!(completion, WaitCompletion::TimedOut);
        Ok(())
    }

    #[tokio::test]
    async fn live_cancellation_requires_identity_before_receiver_use() -> Result<()> {
        let host = host(BTreeMap::new())?;
        let request = WaitRequest {
            operation_id: operation(47),
            waiter: task(1),
            target: WaitTarget::Messages {
                task_id: task(1),
                after: 0,
                limit: 1,
            },
            timeout_epoch_ms: None,
            cancellation_id: None,
        };
        let communication = DurableCommunication::new(host);
        let (_, receiver) = tokio::sync::watch::channel(true);
        assert!(matches!(
            communication.wait(request, Some(receiver)).await,
            Err(Error::Invalid(message)) if message.contains("declared cancellation identity")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn wait_store_replays_terminal_completion_before_observation() -> Result<()> {
        let host = host(BTreeMap::from([(
            task(2),
            Outcome::Succeeded(json!("first")),
        )]))?;
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let store = Arc::new(StreamWaitStore::new(acyclic_stream::StreamClient::new(
            provider.clone(),
        )));
        let request = WaitRequest {
            operation_id: operation(38),
            waiter: task(1),
            target: WaitTarget::Tasks {
                task_ids: vec![task(2)],
            },
            timeout_epoch_ms: None,
            cancellation_id: None,
        };
        let communication = DurableCommunication::new(host.clone()).with_wait_store(store);
        assert_eq!(
            communication.wait(request.clone(), None).await?,
            WaitCompletion::Tasks {
                outcomes: vec![(task(2), Outcome::Succeeded(json!("first")))],
            }
        );
        assert_eq!(
            host.observed_outcomes
                .lock()
                .expect("recording host lock")
                .as_slice(),
            &[task(2)]
        );
        // The second call returns the retained completion from the admission
        // store before asking the host for a new observation.
        let reopened = Arc::new(StreamWaitStore::new(acyclic_stream::StreamClient::new(
            provider,
        )));
        assert_eq!(
            DurableCommunication::new(host.clone())
                .with_wait_store(reopened)
                .wait(request, None)
                .await?,
            WaitCompletion::Tasks {
                outcomes: vec![(task(2), Outcome::Succeeded(json!("first")))],
            }
        );
        assert_eq!(
            host.observed_outcomes
                .lock()
                .expect("recording host lock")
                .as_slice(),
            &[task(2)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn stream_wait_store_replays_after_provider_reopen() -> Result<()> {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let request = WaitRequest {
            operation_id: operation(39),
            waiter: task(1),
            target: WaitTarget::Messages {
                task_id: task(1),
                after: 0,
                limit: 10,
            },
            timeout_epoch_ms: None,
            cancellation_id: Some(operation(40)),
        };
        let store = StreamWaitStore::new(acyclic_stream::StreamClient::new(provider.clone()));
        assert_eq!(store.open(request.clone()).await?, None);
        let completion = WaitCompletion::Cancelled;
        assert_eq!(
            store.complete(request.clone(), completion.clone()).await?,
            completion
        );
        // A newly constructed store reads the owner journal rather than a
        // process-local cache and returns the retained winner.
        let reopened = StreamWaitStore::new(acyclic_stream::StreamClient::new(provider));
        assert_eq!(
            reopened.open(request.clone()).await?,
            Some(WaitCompletion::Cancelled)
        );
        assert_eq!(
            reopened.complete(request, WaitCompletion::TimedOut).await?,
            WaitCompletion::Cancelled
        );
        Ok(())
    }

    #[tokio::test]
    async fn explicit_wait_cancellation_is_replayed_after_restart() -> Result<()> {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let request = WaitRequest {
            operation_id: operation(42),
            waiter: task(1),
            target: WaitTarget::Messages {
                task_id: task(1),
                after: 0,
                limit: 10,
            },
            timeout_epoch_ms: None,
            cancellation_id: Some(operation(43)),
        };
        let host = host(BTreeMap::new())?;
        let store = Arc::new(StreamWaitStore::new(acyclic_stream::StreamClient::new(
            provider.clone(),
        )));
        // Explicit cancellation is scoped to an existing durable wait
        // admission; it cannot mint a cancellation record for an unknown
        // operation identity.
        assert_eq!(store.open(request.clone()).await?, None);
        let communication = DurableCommunication::new(host).with_wait_store(store);
        assert_eq!(
            communication.cancel(request.clone()).await?,
            WaitCompletion::Cancelled
        );
        let reopened_store = StreamWaitStore::new(acyclic_stream::StreamClient::new(provider));
        let reopened = reopened_store.open(request.clone()).await?;
        assert_eq!(reopened, Some(WaitCompletion::Cancelled));
        let mut mismatched = request;
        mismatched.cancellation_id = Some(operation(45));
        assert!(matches!(
            reopened_store.open(mismatched).await,
            Err(Error::Conflict(message)) if message.contains("identity")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_cannot_cross_owner_wait_store() -> Result<()> {
        let host = host(BTreeMap::new())?;
        let owner_provider = Arc::new(acyclic_stream::MemoryStream::default());
        let foreign_provider = Arc::new(acyclic_stream::MemoryStream::default());
        let request = WaitRequest {
            operation_id: operation(47),
            waiter: task(1),
            target: WaitTarget::Messages {
                task_id: task(1),
                after: 0,
                limit: 1,
            },
            timeout_epoch_ms: None,
            cancellation_id: Some(operation(48)),
        };
        let owner_store = Arc::new(StreamWaitStore::new(acyclic_stream::StreamClient::new(
            owner_provider,
        )));
        owner_store.open(request.clone()).await?;
        let foreign_store = Arc::new(StreamWaitStore::new(acyclic_stream::StreamClient::new(
            foreign_provider,
        )));
        let error = DurableCommunication::new(host)
            .with_wait_store(foreign_store)
            .cancel(request)
            .await
            .expect_err("a foreign owner must not cancel another wait");
        assert!(matches!(error, Error::Conflict(message) if message.contains("retained admission")));
        Ok(())
    }

    #[tokio::test]
    async fn durable_wait_cancellation_requires_the_declared_identity() -> Result<()> {
        let host = host(BTreeMap::new())?;
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let store = Arc::new(StreamWaitStore::new(acyclic_stream::StreamClient::new(
            provider,
        )));
        let request = WaitRequest {
            operation_id: operation(44),
            waiter: task(1),
            target: WaitTarget::Messages {
                task_id: task(1),
                after: 0,
                limit: 1,
            },
            timeout_epoch_ms: None,
            cancellation_id: None,
        };
        assert!(matches!(
            store.cancel(request.clone()).await,
            Err(Error::Invalid(message)) if message.contains("cancellation identity")
        ));
        assert!(matches!(
            DurableCommunication::new(host)
                .with_wait_store(store)
                .cancel(request)
                .await,
            Err(Error::Invalid(message)) if message.contains("cancellation identity")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stream_replay_rejects_a_forged_early_timeout_after_clock_advance() -> Result<()> {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let store = StreamWaitStore::new(acyclic_stream::StreamClient::new(provider));
        // Make the declared timeout stale relative to the current process
        // clock, while keeping it after the forged durable completion time.
        // A replay implementation that checks only `now` would accept this
        // record; replay must use the timestamp persisted with the event.
        let now = unix_millis()?;
        let request = WaitRequest {
            operation_id: operation(46),
            waiter: task(1),
            target: WaitTarget::Messages {
                task_id: task(1),
                after: 0,
                limit: 1,
            },
            timeout_epoch_ms: Some(now.saturating_sub(1_000)),
            cancellation_id: None,
        };
        let stream = store.wait_stream(request.waiter)?;
        store
            .append(
                &stream,
                &request,
                PersistedWaitEvent::Admission {
                    contract: WAIT_EVENT_CONTRACT.into(),
                    request: request.clone(),
                },
                "admission",
            )
            .await?;
        store
            .append(
                &stream,
                &request,
                PersistedWaitEvent::Completion {
                    contract: WAIT_EVENT_CONTRACT.into(),
                    request: request.clone(),
                    completion: WaitCompletion::TimedOut,
                    completed_at_epoch_ms: 1,
                },
                "completion",
            )
            .await?;
        assert!(matches!(
            store.open(request).await,
            Err(Error::Conflict(message)) if message.contains("before")
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stream_replay_rejects_a_future_completion_timestamp() -> Result<()> {
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        let store = StreamWaitStore::new(acyclic_stream::StreamClient::new(provider));
        let request = WaitRequest {
            operation_id: operation(48),
            waiter: task(1),
            target: WaitTarget::Messages {
                task_id: task(1),
                after: 0,
                limit: 1,
            },
            timeout_epoch_ms: Some(unix_millis()?.saturating_sub(1)),
            cancellation_id: None,
        };
        let stream = store.wait_stream(request.waiter)?;
        store
            .append(
                &stream,
                &request,
                PersistedWaitEvent::Admission {
                    contract: WAIT_EVENT_CONTRACT.into(),
                    request: request.clone(),
                },
                "admission",
            )
            .await?;
        store
            .append(
                &stream,
                &request,
                PersistedWaitEvent::Completion {
                    contract: WAIT_EVENT_CONTRACT.into(),
                    request: request.clone(),
                    completion: WaitCompletion::TimedOut,
                    completed_at_epoch_ms: unix_millis()?.saturating_add(60_000),
                },
                "completion",
            )
            .await?;
        assert!(matches!(
            store.open(request).await,
            Err(Error::Conflict(message)) if message.contains("future")
        ));
        Ok(())
    }
}
