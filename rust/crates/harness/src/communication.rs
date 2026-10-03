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
use futures::future::join_all;
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
        let identity = OperationId::parse(&item.message_id)
            .map_err(|_| Error::Invalid("inbox message identity is not canonical".into()))?;
        nonzero_operation(identity, "inbox message")?;
        if !identities.insert(identity) {
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

/// Host-backed communication and wait adapter.
///
/// This adapter only composes the existing durable host boundary.  It does
/// not own a second journal or a process-local mailbox.  The host remains the
/// authority for admission, grants, idempotent publication, and recovery.
#[derive(Clone)]
pub struct DurableCommunication {
    host: Arc<dyn DurableTaskHost>,
}

impl DurableCommunication {
    /// Binds communication to one owner-retained durable host.
    #[must_use]
    pub fn new(host: Arc<dyn DurableTaskHost>) -> Self {
        Self { host }
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
        let now = unix_millis()?;
        request.validate(Some(now))?;
        self.authorize_wait(&request).await?;
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
                result = &mut wait => return result,
                () = &mut timeout_sleep => return Ok(WaitCompletion::TimedOut),
                changed = async {
                    match cancellation.as_mut() {
                        Some(receiver) => receiver.changed().await.map_err(|_| ()),
                        None => std::future::pending::<std::result::Result<(), ()>>().await,
                    }
                } => {
                    if changed.is_ok() && cancellation.as_ref().is_some_and(|receiver| *receiver.borrow()) {
                        return Ok(WaitCompletion::Cancelled);
                    }
                }
            }
        }
    }

    async fn authorize_wait(&self, request: &WaitRequest) -> Result<()> {
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
        let value =
            serde_json::to_value(self).map_err(|error| Error::Invalid(error.to_string()))?;
        crate::contract::canonical_json_bytes(&value)
    }

    /// Reconstructs a persisted wait record and rejects noncanonical bytes.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self> {
        let value: Value =
            serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        let record: Self = serde_json::from_value(value.clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        record.request.validate(None)?;
        if crate::contract::canonical_json_bytes(&value)? != bytes {
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
    }

    impl DurableTaskHost for RecordingHost {
        fn observe_admission<'a>(
            &'a self,
            task_id: TaskId,
        ) -> futures::future::BoxFuture<'a, Result<TaskAdmissionRecord>> {
            Box::pin(async move {
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
        }))
    }

    fn item(sequence: u64, id: u8) -> Result<InboxItem> {
        Ok(InboxItem {
            task_id: task(2),
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
        first.validate()?;
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
        let communication = DurableCommunication::new(host);
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
}
