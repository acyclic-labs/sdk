//! Shared owner-retained mailbox records. Task lifecycle stays with its host.

use crate::{
    Error, OperationId, Result, TaskId,
    communication::{message_endpoint_operation, publish_control_record},
    conversation::{ContentResidencyVerifier, FileRef},
    runtime::{DurableTaskHost, read_granted},
    scheduler::InboxItem,
};
use acyclic_stream::{StreamClient, StreamError, StreamProvider};
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
        self.send_inner(host, sender_id, recipient_id, message_id, payload, false)
            .await
    }

    /// Completes a message whose owner-journal admission won before a
    /// lifecycle fence. The mailbox remains the only publication record;
    /// this path only changes the lifecycle check, never authorization or
    /// payload verification.
    pub(crate) async fn send_admitted(
        &self,
        host: &dyn DurableTaskHost,
        sender_id: TaskId,
        recipient_id: TaskId,
        message_id: OperationId,
        payload: FileRef,
        admitted: bool,
    ) -> Result<()> {
        self.send_inner(
            host,
            sender_id,
            recipient_id,
            message_id,
            payload,
            admitted,
        )
        .await
    }

    async fn send_inner(
        &self,
        host: &dyn DurableTaskHost,
        sender_id: TaskId,
        recipient_id: TaskId,
        message_id: OperationId,
        payload: FileRef,
        admitted: bool,
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
        if !admitted {
            sender.require_new_mutation()?;
            recipient.require_new_mutation()?;
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

    /// Finds one exact endpoint message without admitting a new delivery.
    /// This is the replay probe used when a task is lifecycle-fenced: callers
    /// may recover a committed operation, but a changed payload remains a
    /// conflict and no new mailbox record is published.
    pub(crate) async fn find_message(
        &self,
        host: &dyn DurableTaskHost,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
    ) -> Result<Option<FileRef>> {
        let mut after = 0_u64;
        let message_id = message_id.to_string();
        loop {
            let page = self.inbox(host, recipient, after, 1_024).await?;
            let page_len = page.len();
            let Some(item) = page
                .iter()
                .find(|item| item.sender == sender && item.message_id == message_id)
            else {
                if page_len < 1_024 {
                    return Ok(None);
                }
                after = page
                    .last()
                    .map(|item| item.sequence)
                    .ok_or_else(|| Error::Storage("mailbox page made no progress".into()))?;
                continue;
            };
            return Ok(Some(item.payload.clone()));
        }
    }
}
