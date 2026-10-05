//! Shared owner-retained mailbox records. Task lifecycle stays with its host.

use crate::{
    Error, OperationId, Result, TaskId,
    communication::message_endpoint_operation,
    conversation::{ContentResidencyVerifier, FileRef},
    runtime::{DurableTaskHost, read_granted},
    scheduler::InboxItem,
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, IdempotencyOutcome, StreamClient, StreamError,
    StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;

/// Version-one envelope; old unversioned records fail closed.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MailEvent {
    pub(crate) schema_version: u8,
    pub(crate) sender: TaskId,
    pub(crate) recipient: TaskId,
    pub(crate) message_id: OperationId,
    pub(crate) payload: FileRef,
}

impl MailEvent {
    pub(crate) fn validate_for(&self, recipient: TaskId) -> Result<()> {
        if self.schema_version != 1 {
            return Err(Error::Invalid(
                "unsupported mail event schema version".into(),
            ));
        }
        if self.sender.into_bytes() == [0; 16]
            || self.recipient.into_bytes() == [0; 16]
            || self.message_id.into_bytes() == [0; 16]
        {
            return Err(Error::Invalid("mail event identity is empty".into()));
        }
        if self.recipient != recipient {
            return Err(Error::Conflict(
                "mail event recipient differs from its mailbox".into(),
            ));
        }
        self.payload.validate()
    }
}

pub(crate) struct MailboxStore<P> {
    stream: StreamClient<P>,
    reader: Arc<dyn ContentResidencyVerifier>,
}

impl<P: StreamProvider> MailboxStore<P> {
    pub(crate) fn new(stream: StreamClient<P>, reader: Arc<dyn ContentResidencyVerifier>) -> Self {
        Self { stream, reader }
    }

    fn mailbox(&self, task: TaskId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/mail/{task}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    pub(crate) async fn send(
        &self,
        host: &dyn DurableTaskHost,
        sender_id: TaskId,
        recipient_id: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> Result<()> {
        payload.validate()?;
        let sender = host.communication_scope(sender_id).await?;
        if !sender.grants.contains("mail:send") {
            return Err(Error::Unauthorized("sender scope lacks mail:send".into()));
        }
        let recipient = host.communication_scope(recipient_id).await?;
        if sender.parent != Some(recipient_id) && recipient.parent != Some(sender_id) {
            return Err(Error::Unauthorized(
                "message endpoints are not direct parent and child".into(),
            ));
        }
        recipient.limits.validate_file(&payload)?;
        if !read_granted(&recipient.grants, &payload)? {
            return Err(Error::Unauthorized(
                "recipient cannot read the mailed file".into(),
            ));
        }
        self.reader.verify(&payload).await?;
        let event = MailEvent {
            schema_version: 1,
            sender: sender_id,
            recipient: recipient_id,
            message_id,
            payload,
        };
        event.validate_for(recipient_id)?;
        let bytes = crate::contract::canonical_json_bytes(&event)?;
        let mailbox = self.mailbox(recipient_id)?;
        publish_control_record(
            &self.stream,
            &mailbox,
            "mail",
            recipient_id,
            message_endpoint_operation(sender_id, recipient_id, message_id),
            &bytes,
        )
        .await
    }

    pub(crate) async fn inbox(
        &self,
        host: &dyn DurableTaskHost,
        task: TaskId,
        after: u64,
        limit: usize,
    ) -> Result<Vec<InboxItem>> {
        if limit == 0 || limit > 1_024 {
            return Err(Error::Invalid("inbox page bound is invalid".into()));
        }
        let recipient = host.communication_scope(task).await?;
        if !recipient.grants.contains("mail:read") {
            return Err(Error::Unauthorized(
                "recipient scope lacks mail:read".into(),
            ));
        }
        let mailbox = self.mailbox(task)?;
        let bounds = match mailbox.bounds().await {
            Ok(bounds) => bounds,
            Err(StreamError::NotFound) => return Ok(Vec::new()),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        if after > bounds.tail {
            return Err(Error::Invalid("inbox cursor is beyond the tail".into()));
        }
        let page_limit = u32::try_from(limit)
            .map_err(|_| Error::Invalid("inbox page bound is invalid".into()))?;
        let page = match mailbox.read(after, page_limit).await {
            Ok(records) => records.try_collect::<Vec<_>>().await?,
            Err(StreamError::NotFound) => return Ok(Vec::new()),
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        let mut items = Vec::with_capacity(page.len());
        for record in page {
            let value: Value = serde_json::from_slice(&record.value)
                .map_err(|error| Error::Storage(error.to_string()))?;
            if crate::contract::canonical_json_bytes(&value)? != record.value.as_ref() {
                return Err(Error::Storage("mail event is not canonical JSON".into()));
            }
            let event: MailEvent =
                serde_json::from_value(value).map_err(|error| Error::Storage(error.to_string()))?;
            event.validate_for(task)?;
            recipient.limits.validate_file(&event.payload)?;
            if !read_granted(&recipient.grants, &event.payload)? {
                return Err(Error::Unauthorized(
                    "mail history contains an unreadable payload".into(),
                ));
            }
            let sender = host.communication_scope(event.sender).await?;
            if !sender.grants.contains("mail:send")
                || (sender.parent != Some(task) && recipient.parent != Some(event.sender))
            {
                return Err(Error::Unauthorized(
                    "mail history contains an unauthorized endpoint".into(),
                ));
            }
            // Delivery time belongs to the immutable owner commit, not to the
            // envelope; retrying the same message must retain the same bytes.
            let delivered_at_epoch_ms = record.committed_at_micros / 1_000;
            if delivered_at_epoch_ms == 0 {
                return Err(Error::Storage(
                    "mail record is missing its committed delivery timestamp".into(),
                ));
            }
            items.push(InboxItem {
                task_id: task,
                sender: event.sender,
                delivered_at_epoch_ms,
                sequence: record
                    .sequence
                    .checked_add(1)
                    .ok_or_else(|| Error::Storage("inbox sequence exhausted".into()))?,
                message_id: event.message_id.to_string(),
                payload: event.payload,
            });
        }
        Ok(items)
    }
}

/// Retains the existing control identity namespace and validates a reconciled
/// append against its exact committed record before acknowledging success.
pub(crate) async fn publish_control_record<P: StreamProvider>(
    client: &StreamClient<P>,
    stream: &acyclic_stream::Stream<P>,
    kind: &str,
    task: TaskId,
    operation: OperationId,
    bytes: &[u8],
) -> Result<()> {
    let identity = format!("harness/v2/{kind}/{task}/{operation}");
    let key = StreamKey::new(Bytes::copy_from_slice(
        blake3::hash(identity.as_bytes()).as_bytes(),
    ))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let outcome = match stream
        .append_batch(vec![Bytes::copy_from_slice(bytes)], None, Some(key.clone()))
        .await
    {
        Ok(outcome) => outcome,
        Err(StreamError::Unavailable) => match client.inspect_idempotency(key).await {
            Ok(Some(observation)) => match observation.outcome {
                IdempotencyOutcome::Append(outcome) => outcome,
                _ => {
                    return Err(Error::Conflict(
                        "control identity has another operation kind".into(),
                    ));
                }
            },
            Ok(None) | Err(_) => return Err(Error::Indeterminate(operation)),
        },
        Err(StreamError::IdempotencyMismatch) => {
            return Err(Error::Conflict(
                "control identity reused with different content".into(),
            ));
        }
        Err(error) => return Err(Error::Storage(error.to_string())),
    };
    match outcome {
        AppendOutcome::Committed(receipt) if receipt.end == receipt.start + 1 => {
            let records = stream
                .read(receipt.start, 1)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let [record] = records.as_slice() else {
                return Err(Error::Conflict(
                    "control publication differs from its committed record".into(),
                ));
            };
            if record.sequence != receipt.start
                || record.commit_id != receipt.commit_id
                || record.value.as_ref() != bytes
            {
                return Err(Error::Conflict(
                    "control publication differs from its committed record".into(),
                ));
            }
            Ok(())
        }
        AppendOutcome::Committed(_) => Err(Error::Storage("invalid control append receipt".into())),
        AppendOutcome::TailConflict { .. } => Err(Error::Conflict(
            "unconditional control append conflicted".into(),
        )),
    }
}
