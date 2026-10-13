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
        let bytes = self
            .mail_bytes(sender, recipient, message_id, payload)
            .await?;
        self.publish_mail(sender, recipient, message_id, &bytes, Some(&fence))
            .await
    }

    pub(super) fn mailbox(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(self.stream.clone(), format!("harness/v2/mail/{task_id}"))
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
            .admission(OperationId::from_bytes(sender.into_bytes()), None)
            .await?;
        if !sender_admission.grants.contains("mail:send") {
            return Err(Error::Unauthorized("sender scope lacks mail:send".into()));
        }
        let recipient_admission = self
            .admission(OperationId::from_bytes(recipient.into_bytes()), None)
            .await?;
        sender_admission.limits.validate_file(&payload, &mut None)?;
        if !read_granted(&sender_admission.grants, &payload)? {
            return Err(Error::Unauthorized(
                "sender cannot read the mailed file".into(),
            ));
        }
        recipient_admission
            .limits
            .validate_file(&payload, &mut None)?;
        if !read_granted(&recipient_admission.grants, &payload)? {
            return Err(Error::Unauthorized(
                "recipient cannot read the mailed file".into(),
            ));
        }
        self.reader
            .verify(&payload, payload.descriptor().byte_length(), None)?
            .await?;
        let event = MailEvent {
            sender,
            recipient,
            message_id,
            payload,
        };
        crate::contract::canonical_json_bytes(&event)
    }

    pub(super) async fn publish_mail(
        &self,
        sender: TaskId,
        recipient: TaskId,
        message: OperationId,
        bytes: &[u8],
        fence: Option<&crate::scheduler::LeaseFence>,
    ) -> Result<()> {
        use acyclic_stream::{
            CommitCondition, CommitMutation, CommitOutcome, CommittedMutation, Stream,
        };
        let mailbox = self.mailbox(recipient)?;
        let intent = Stream::new(
            self.stream.clone(),
            format!("harness/v2/mail-intents/{sender}/{message}"),
        )?;
        loop {
            if let Some(fence) = fence {
                self.verify_owner(sender, fence, false).await?;
            }
            match intent.bounds().await {
                Ok(bounds) => {
                    let records = intent.read(0, 1).await?.try_collect::<Vec<_>>().await?;
                    let [record] = records.as_slice() else {
                        return Err(Error::Storage("mail intent is missing".into()));
                    };
                    if bounds.tail != 1 || record.sequence != 0 || record.value.as_ref() != bytes {
                        return Err(Error::Conflict("mail identity reused".into()));
                    }
                    return Ok(());
                }
                Err(StreamError::NotFound) => (),
                Err(error) => return Err(error.into()),
            }
            let (tail, mailbox_guard) = match mailbox.bounds().await {
                Ok(bounds) => (
                    bounds.tail,
                    CommitCondition::Tail {
                        path: mailbox.path().clone(),
                        expected: bounds.tail,
                    },
                ),
                Err(StreamError::NotFound) => (
                    0,
                    CommitCondition::Absent {
                        path: mailbox.path().clone(),
                    },
                ),
                Err(error) => return Err(error.into()),
            };
            let mut conditions = vec![
                mailbox_guard,
                CommitCondition::Absent {
                    path: intent.path().clone(),
                },
            ];
            let mut hash = blake3::Hasher::new();
            hash.update(b"harness/v2/mail-intent\0");
            hash.update(&sender.into_bytes());
            hash.update(&message.into_bytes());
            hash.update(&tail.to_le_bytes());
            hash.update(blake3::hash(bytes).as_bytes());
            if let Some(fence) = fence {
                let guard = self
                    .coordinator
                    .lock()
                    .await
                    .journal_condition(
                        &self.owner,
                        &self.owner_scope,
                        &self.verifier,
                        OperationId::from_bytes(sender.into_bytes()),
                        fence,
                        crate::distributed::JournalWrite::Fresh,
                        &mut None,
                    )
                    .await?;
                let CommitCondition::Tail { expected, .. } = &guard else {
                    return Err(Error::Storage("mail owner guard is invalid".into()));
                };
                hash.update(&expected.to_le_bytes());
                conditions.push(guard);
            }
            let request = acyclic_stream::CommitRequest {
                conditions,
                mutations: vec![
                    CommitMutation::Append {
                        path: intent.path().clone(),
                        records: vec![OwnedBytes::copy_from_slice(bytes)],
                    },
                    CommitMutation::Append {
                        path: mailbox.path().clone(),
                        records: vec![OwnedBytes::copy_from_slice(bytes)],
                    },
                ],
                idempotency_key: StreamKey::new(OwnedBytes::copy_from_slice(
                    hash.finalize().as_bytes(),
                ))?,
            };
            let mut work = None;
            let request = (request, crate::wire_codec::CodecQuota::new(&mut work, 0)?);
            match crate::distributed::commit(request, &self.stream, Some(message), None)
                .await?
                .0
            {
                CommitOutcome::Conflict(_) => continue,
                CommitOutcome::Committed(envelope) => {
                    let [
                        CommittedMutation::Append(saved),
                        CommittedMutation::Append(delivered),
                    ] = envelope.mutations.as_slice()
                    else {
                        return Err(Error::Indeterminate(message));
                    };
                    for (append, path, start) in
                        [(saved, intent.path(), 0), (delivered, mailbox.path(), tail)]
                    {
                        if &append.path != path
                            || append.start != start
                            || append.end
                                != start.checked_add(1).ok_or_else(|| {
                                    Error::Storage("mail sequence exhausted".into())
                                })?
                            || append.tail != append.end
                            || append.records.len() != 1
                            || append.records[0].sequence != start
                            || append.records[0].value.as_ref() != bytes
                            || append.records[0].commit_id != envelope.commit_id
                        {
                            return Err(Error::Indeterminate(message));
                        }
                    }
                    return Ok(());
                }
            }
        }
    }
}
