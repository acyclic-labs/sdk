//! Received mail.
use super::*;

impl<P: StreamProvider + 'static> CoordinatorTaskHost<P> {
    /// Resolves one exact retained parcel under its receiver's original lease and sole receive command.
    #[cfg(feature = "filesystem")]
    #[allow(
        clippy::too_many_lines,
        reason = "The original receive fence and parcel custody remain in one authenticated replay future."
    )]
    pub async fn read_received_mail_owned(
        self: &Arc<Self>,
        owner: &TaskJournalOwner<P>,
        receive_operation: OperationId,
        item: &InboxItem,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = observer
            .as_mut()
            .map(|observer| observer as &mut dyn crate::store::HistoryIo);
        if !owner.original_host_binding_matches(self) {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel owner belongs to another host",
            ));
        }
        owner.verify_read_access(&mut work).await?;
        let (receiver, _) = owner.task_binding();
        if item.task_id != receiver || item.sequence == 0 {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel recipient or sequence differs",
            ));
        }
        let path = crate::executor::preparation_format(
            format_args!(
                "harness/v2/workflows/{}",
                owner.workflow_admission().operation_id
            ),
            &mut work,
        )?;
        let _path = io
            .map(|io| io.guard_owned(path.capacity() as u64))
            .transpose()?;
        let (workflow, _workflow_path) =
            crate::store::operations::stream(&owner.stream(), &path, &mut work)?;
        let tail =
            crate::store::operations::bounds_with_work(&owner.stream(), workflow.path(), &mut work)
                .await?
                .map_err(|error| crate::store::operations::stream_error_with_work(error, &work))?
                .tail;
        let index = tail.checked_sub(1).ok_or_else(|| {
            crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel receive checkpoint is absent",
            )
        })?;
        let latest = self
            .read_mail_workflow_record(owner, &workflow, index, io)
            .await?;
        let prior;
        let (receive, receive_transition) = if let [command] = latest.transition.commands.as_slice()
            && command.operation_id == receive_operation
            && command.kind == crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
        {
            (command, &latest.transition)
        } else {
            let results = latest
                .input
                .get("commands")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    crate::store::guarded_boundary(
                        &work,
                        crate::wire::ErrorCode::Unauthorized,
                        "parcel receive result is not committed",
                    )
                })?;
            let [result] = results.as_slice() else {
                return Err(crate::store::guarded_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parcel must consume one receive result",
                ));
            };
            let exact = match io {
                Some(io) => io.value_json(
                    &Outcome::Succeeded(item),
                    acyclic_stream::MAX_RECORD_BYTES as u64,
                )?,
                None => serde_json::to_value(Outcome::Succeeded(item))
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            };
            let operation = match io {
                Some(io) => {
                    io.value_json(&receive_operation, acyclic_stream::MAX_RECORD_BYTES as u64)?
                }
                None => serde_json::to_value(receive_operation)
                    .map_err(|error| Error::Invalid(error.to_string()))?,
            };
            if result["operation_id"] != operation || result["value"] != exact {
                return Err(crate::store::guarded_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parcel differs from the committed receive result",
                ));
            }
            let prior_index = latest.prior.revision.checked_sub(1).ok_or_else(|| {
                crate::store::guarded_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parcel receive outbox is absent",
                )
            })?;
            prior = self
                .read_mail_workflow_record(owner, &workflow, prior_index, io)
                .await?;
            let [command] = prior.transition.commands.as_slice() else {
                return Err(crate::store::guarded_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parcel requires a sole receive outbox",
                ));
            };
            if command.operation_id != receive_operation
                || command.kind != crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
                || prior.checkpoint()?
                    != (
                        &latest.prior.machine,
                        latest.prior.revision,
                        &latest.prior.state,
                    )
            {
                return Err(crate::store::guarded_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parcel receive operation differs",
                ));
            }
            (command, &prior.transition)
        };
        let receiver_admission = self
            .admission(owner.workflow_admission().operation_id, &mut work)
            .await?;
        let selection_bytes = self
            .read_mail_source(&receiver_admission, &receive.payload, &mut work)
            .await?;
        let selection: crate::filesystem::MailReceiveTaskCommand = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData,
                &selection_bytes,
                crate::wire::ErrorCode::Storage,
            )?)?,
            None => crate::contract::json_from_slice(&selection_bytes)
                .map_err(|error| Error::Storage(error.to_string()))?,
        };
        let after = if crate::filesystem::child_agent::is_child(&receiver_admission) {
            crate::filesystem::child_agent::mail_cursor(receive_transition, receive).map_err(
                |reason| {
                    crate::store::guarded_boundary(
                        &work,
                        crate::wire::ErrorCode::Unauthorized,
                        reason,
                    )
                },
            )?
        } else {
            selection.after
        };
        if after.checked_add(1) != Some(item.sequence) {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel cursor differs from the retained receive",
            ));
        }
        let mailbox = self.mailbox(receiver)?;
        let mut records = crate::store::operations::read_records(
            &self.stream,
            mailbox.path(),
            item.sequence - 1,
            1,
            &mut work,
        )
        .await?
        .map_err(|error| crate::store::operations::stream_error_with_work(error, &work))?;
        let record = records.next(&mut work).await?.ok_or_else(|| {
            crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel source is absent",
            )
        })?;
        if records.next(&mut work).await?.is_some() {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel source is absent",
            ));
        }
        let event: MailEvent = mail::decode(&record.value, io)?;
        let mut message = [0; 36];
        let message: &str = uuid::Uuid::from_bytes(event.message_id.into_bytes())
            .hyphenated()
            .encode_lower(&mut message);
        if record.sequence.checked_add(1) != Some(item.sequence)
            || message != item.message_id
            || event.payload != item.payload
            || mail::encode(&event, acyclic_stream::MAX_RECORD_BYTES as u64, io)?.as_ref()
                != record.value.as_ref()
        {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel differs from its retained mailbox item",
            ));
        }
        let sender_admission = self
            .admission(
                OperationId::from_bytes(event.sender.into_bytes()),
                &mut work,
            )
            .await?;
        let direct = mail::authorize_route(
            event.sender,
            &sender_admission,
            receiver,
            &receiver_admission,
            io,
        )?;
        receiver_admission
            .limits
            .validate_file(&event.payload, &mut work)?;
        if !direct
            && !crate::runtime::read_granted_with_work(
                &receiver_admission.grants,
                &event.payload,
                &mut work,
            )?
        {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "custom parcel requires recipient file authority",
            ));
        }
        if !self
            .observe_mail_location(
                event.sender,
                receiver,
                event.message_id,
                &record.value,
                Some(record.sequence),
                io,
            )
            .await?
        {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel has no original atomic receipt",
            ));
        }
        let bytes = self
            .read_mail_source(&sender_admission, &event.payload, &mut work)
            .await?;
        owner.verify_read_access(&mut work).await?;
        Ok(bytes)
    }

    #[cfg(feature = "filesystem")]
    pub(super) async fn read_mail_workflow_record(
        &self,
        owner: &TaskJournalOwner<P>,
        workflow: &acyclic_stream::Stream<P>,
        index: u64,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<crate::workflow::WorkflowRecord> {
        let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = observer
            .as_mut()
            .map(|observer| observer as &mut dyn crate::store::HistoryIo);
        let mut records = crate::store::operations::read_records(
            &self.stream,
            workflow.path(),
            index,
            1,
            &mut work,
        )
        .await?
        .map_err(|error| crate::store::operations::stream_error_with_work(error, &work))?;
        let record = records.next(&mut work).await?.ok_or_else(|| {
            crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel workflow record is absent",
            )
        })?;
        if records.next(&mut work).await?.is_some() {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel workflow record is absent",
            ));
        }
        let reference: FileRef = mail::decode(&record.value, io)?;
        owner.input_limits().validate_file(&reference, &mut work)?;
        let encoded = match io {
            Some(io) => io.encode(&reference, acyclic_stream::MAX_RECORD_BYTES as u64, false)?,
            None => serde_json::to_vec(&reference)
                .map_err(|error| Error::Storage(error.to_string()))?
                .into(),
        };
        if record.sequence != index
            || reference.descriptor().media_type() != "application/json"
            || encoded.as_ref() != record.value.as_ref()
        {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Storage,
                "parcel workflow reference differs",
            ));
        }
        drop(encoded);
        let bytes = self
            .reader
            .read(&reference, reference.descriptor().byte_length(), io)?
            .await?;
        let _bytes = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        reference.descriptor().verify(&bytes, &mut work)?;
        let envelope: crate::workflow::WorkflowRecord = match io {
            Some(io) => crate::filesystem::retain_decoded(io.decode(
                std::marker::PhantomData::<crate::workflow::WorkflowRecord>,
                &bytes,
                crate::wire::ErrorCode::Storage,
            )?)?,
            None => crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(error.to_string()))?,
        };
        envelope.validate(io)?;
        if envelope.prior.revision != index || envelope.checkpoint()?.1 != index + 1 {
            return Err(crate::store::guarded_boundary(
                &work,
                crate::wire::ErrorCode::Storage,
                "parcel workflow revision differs",
            ));
        }
        owner.validate_workflow_record(&envelope, io)?;
        Ok(envelope)
    }

    /// Resolves only the authenticated donor's exact immutable parcel bytes.
    #[cfg(feature = "filesystem")]
    pub(super) fn mailbox(&self, task: TaskId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(
            self.stream.clone(),
            format!("harness/v3/mail/by-recipient/{task}"),
        )
        .map_err(Error::from)
    }
    pub(super) async fn read_mail_source(
        &self,
        admission: &TaskAdmissionRecord,
        payload: &FileRef,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        payload.validate(work)?;
        admission.limits.validate_file(payload, work)?;
        if !crate::runtime::read_granted_with_work(&admission.grants, payload, work)? {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "mail donor cannot read its payload",
            ));
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        #[cfg(feature = "filesystem")]
        let (scope, retained) = self.authenticated_scope(admission, quota.work()).await?;
        #[cfg(feature = "filesystem")]
        quota.adopt(retained)?;
        #[cfg(feature = "filesystem")]
        let access = scope.created_access();
        #[cfg(feature = "filesystem")]
        let reader: &dyn ContentResidencyVerifier = 'reader: {
            if let Some(access) = access.as_ref() {
                for (file, reader) in &access.references {
                    if let Some(io) = quota.work().as_deref_mut() {
                        io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect {
                            items: 1,
                        })?;
                    }
                    if file == payload {
                        break 'reader reader.as_ref();
                    }
                }
                for binding in &access.content {
                    if let Some(io) = quota.work().as_deref_mut() {
                        io.codec_charge(crate::executor::json::JsonCodecCharge::Inspect {
                            items: 1,
                        })?;
                    }
                    if &binding.volume == payload.volume() {
                        break 'reader binding.binding.reader.as_ref();
                    }
                }
            }
            self.reader.as_ref()
        };
        #[cfg(not(feature = "filesystem"))]
        let reader = self.reader.as_ref();
        let bytes = match quota.work().as_deref_mut() {
            Some(io) => {
                crate::store::read_content_with_work(io, reader, payload)
                    .await?
                    .0
            }
            None => {
                reader
                    .read(payload, payload.descriptor().byte_length(), None)?
                    .await?
            }
        };
        payload.descriptor().verify(&bytes, quota.work())?;
        Ok(bytes)
    }
}
