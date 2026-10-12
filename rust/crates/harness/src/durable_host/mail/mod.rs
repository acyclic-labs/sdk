//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Publishes ref-only mail under the sender's exact uncancelled lease.
    #[cfg(feature = "filesystem")]
    pub async fn send_owned(
        self: &Arc<Self>,
        sender: TaskId,
        fence: crate::scheduler::LeaseFence,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> Result<()> {
        let owner = self.journal_owner(sender, fence.clone()).await?;
        let bytes = OwnedBytes::from(
            self.mail_bytes(sender, recipient, message_id, payload)
                .await?,
        );
        let path = acyclic_stream::StreamPath::new(format!("harness/v2/mail/{recipient}"))?;
        let key = Self::event_key("mail", recipient, message_id)?;
        loop {
            self.verify_owner(sender, &fence, false).await?;
            let (tail, found) = self.mail_state(recipient, message_id, &bytes).await?;
            if found {
                self.verify_owner(sender, &fence, false).await?;
                return Ok(());
            }
            if owner
                .append(
                    path.clone(),
                    tail,
                    &key,
                    bytes.clone(),
                    crate::distributed::JournalWrite::Fresh,
                )
                .await?
            {
                self.verify_owner(sender, &fence, false).await?;
                return Ok(());
            }
        }
    }

    pub(super) fn mailbox(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        self.stream
            .stream(format!("harness/v2/mail/{task_id}"))
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    pub(super) async fn mail_bytes(
        &self,
        sender: TaskId,
        recipient: TaskId,
        message_id: OperationId,
        payload: FileRef,
    ) -> Result<Vec<u8>> {
        payload.validate(&mut None)?;
        let sender_admission = self
            .admission(OperationId::from_bytes(sender.into_bytes()))
            .await?;
        if !sender_admission.grants.contains("mail:send") {
            return Err(Error::Unauthorized("sender scope lacks mail:send".into()));
        }
        let recipient_admission = self
            .admission(OperationId::from_bytes(recipient.into_bytes()))
            .await?;
        recipient_admission.limits.validate_file(&payload, &mut None)?;
        if !read_granted(&recipient_admission.grants, &payload)? {
            return Err(Error::Unauthorized(
                "recipient cannot read the mailed file".into(),
            ));
        }
        self.reader.verify(&payload, recipient_admission.limits.file_bytes, None)?.await?;
        let event = MailEvent {
            sender,
            message_id,
            payload,
        };
        crate::contract::canonical_json_bytes(&event)
    }

    pub(super) async fn mail_state(
        &self,
        recipient: TaskId,
        message_id: OperationId,
        bytes: &[u8],
    ) -> Result<(u64, bool)> {
        let mailbox = self.mailbox(recipient)?;
        let tail = match mailbox.bounds().await {
            Ok(bounds) => bounds.tail,
            Err(StreamError::NotFound) => 0,
            Err(error) => return Err(error.into()),
        };
        let mut after = 0;
        let mut found = false;
        while after < tail {
            let page = mailbox
                .read(after, (tail - after).min(64) as u32)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            if page.is_empty() || page.len() > 64 {
                return Err(Error::Storage("invalid mail replay page".into()));
            }
            for record in page {
                if record.sequence != after || after >= tail {
                    return Err(Error::Storage("mail replay sequence differs".into()));
                }
                let event: MailEvent = crate::contract::json_from_slice(&record.value)
                    .map_err(|error| Error::Storage(error.to_string()))?;
                event.payload.validate(&mut None)?;
                if crate::contract::canonical_json_bytes(&event)? != record.value.as_ref() {
                    return Err(Error::Storage("mail event is not canonical JSON".into()));
                }
                if event.message_id == message_id {
                    if found || record.value.as_ref() != bytes {
                        return Err(Error::Conflict("mail identity reused".into()));
                    }
                    found = true;
                }
                after += 1;
            }
        }
        Ok((tail, found))
    }
}
