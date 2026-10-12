//! Received mail.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Resolves one exact retained parcel under its receiver's original lease and sole receive command.
    #[cfg(feature = "filesystem")]
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
            .map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        if !owner.original_host_binding_matches(self) {
            return Err(admission_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel owner belongs to another host",
            ));
        }
        owner.verify_read_access(&mut work).await?;
        let (receiver, _) = owner.task_binding();
        if item.task_id != receiver || item.sequence == 0 {
            return Err(admission_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel recipient or sequence differs",
            ));
        }
        let path = crate::executor::with_history_io(io, |work| {
            crate::contract::preparation_format(
                format_args!(
                    "harness/v2/workflows/{}",
                    owner.workflow_admission().operation_id
                ),
                work,
            )
        })?;
        let _path = io
            .map(|io| io.guard_owned(path.capacity() as u64))
            .transpose()?;
        let (workflow, _workflow_path) =
            crate::store::operations::stream(&owner.stream(), &path, &mut work)?;
        let tail =
            crate::store::operations::bounds_with_work(&owner.stream(), workflow.path(), &mut work)
                .await?
                .map_err(|error| crate::distributed::sdk_error(error, &work))?
                .tail;
        let index = tail.checked_sub(1).ok_or_else(|| {
            admission_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel receive checkpoint is absent",
            )
        })?;
        let latest = self
            .read_mail_workflow_record(owner, &workflow, index, io)
            .await?;
        let prior;
        let receive = if let [command] = latest.transition.commands.as_slice()
            && command.operation_id == receive_operation
            && command.kind == crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
        {
            command
        } else {
            let results = latest.input["commands"].as_array().ok_or_else(|| {
                admission_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parcel receive result is not committed",
                )
            })?;
            let [result] = results.as_slice() else {
                return Err(admission_boundary(
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
                return Err(admission_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parcel differs from the committed receive result",
                ));
            }
            let prior_index = latest.prior.revision.checked_sub(1).ok_or_else(|| {
                admission_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parcel receive outbox is absent",
                )
            })?;
            prior = self
                .read_mail_workflow_record(owner, &workflow, prior_index, io)
                .await?;
            let [command] = prior.transition.commands.as_slice() else {
                return Err(admission_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parcel requires a sole receive outbox",
                ));
            };
            if command.operation_id != receive_operation
                || command.kind != crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
                || prior.next != latest.prior
            {
                return Err(admission_boundary(
                    &work,
                    crate::wire::ErrorCode::Unauthorized,
                    "parcel receive operation differs",
                ));
            }
            command
        };
        let receiver_admission = self
            .admission_with_work(owner.workflow_admission().operation_id, &mut work)
            .await?;
        let selection: crate::filesystem::MailReceiveTaskCommand = mail::decode(
            &self
                .read_mail_source(&receiver_admission, &receive.payload, &mut work)
                .await?,
            io,
        )?;
        if selection.after.checked_add(1) != Some(item.sequence) {
            return Err(admission_boundary(
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
        .map_err(|error| crate::distributed::sdk_error(error, &work))?;
        let record = records.next(&mut work).await?.ok_or_else(|| {
            admission_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel source is absent",
            )
        })?;
        if records.next(&mut work).await?.is_some() {
            return Err(admission_boundary(
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
            return Err(admission_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel differs from its retained mailbox item",
            ));
        }
        let sender_admission = self
            .admission_with_work(
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
            return Err(admission_boundary(
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
            return Err(admission_boundary(
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
            .map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        let mut records =
            crate::store::operations::read_records(&self.stream, workflow.path(), index, 1, &mut work)
                .await?
                .map_err(|error| crate::distributed::sdk_error(error, &work))?;
        let record = records.next(&mut work).await?.ok_or_else(|| {
            admission_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel workflow record is absent",
            )
        })?;
        if records.next(&mut work).await?.is_some() {
            return Err(admission_boundary(
                &work,
                crate::wire::ErrorCode::Unauthorized,
                "parcel workflow record is absent",
            ));
        }
        let reference: FileRef = mail::decode(&record.value, io)?;
        owner.input_limits().validate_file(&reference, &mut work)?;
        if record.sequence != index
            || mail::encode(&reference, acyclic_stream::MAX_RECORD_BYTES as u64, io)?.as_ref()
                != record.value.as_ref()
        {
            return Err(admission_boundary(
                &work,
                crate::wire::ErrorCode::Storage,
                "parcel workflow reference differs",
            ));
        }
        let (value, held) = self.read_json(&reference, &mut work).await?;
        let _value = io.map(|io| io.guard_owned(held)).transpose()?;
        let envelope: crate::workflow::WorkflowRecord = match io {
            Some(io) => crate::filesystem::retain_decoded(
                io.value_decode_preparation(&value, crate::wire::ErrorCode::Storage)?,
            ),
            None => {
                serde_json::from_value(value).map_err(|error| Error::Storage(error.to_string()))?
            }
        };
        envelope.validate(io)?;
        if envelope.prior.revision != index || envelope.next.revision != index + 1 {
            return Err(admission_boundary(
                &work,
                crate::wire::ErrorCode::Storage,
                "parcel workflow revision differs",
            ));
        }
        owner.validate_workflow_record(&envelope, io)?;
        Ok(envelope)
    }

    pub(super) fn mailbox(&self, task_id: TaskId) -> Result<acyclic_stream::Stream<P>> {
        acyclic_stream::Stream::new(
            std::sync::Arc::clone(&self.stream),
            format!("harness/v3/mail/by-recipient/{task_id}"),
        )
        .map_err(|error| Error::Invalid(error.to_string()))
    }

    pub(super) fn event_key(
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
    ) -> Result<StreamKey> {
        let identity = format!("harness/v2/{kind}/{task_id}/{operation_id}");
        StreamKey::new(OwnedBytes::copy_from_slice(
            blake3::hash(identity.as_bytes()).as_bytes(),
        ))
        .map_err(|error| Error::Invalid(error.to_string()))
    }

    /// Resolves only the authenticated donor's exact immutable parcel bytes.
    pub(super) async fn read_mail_source(
        &self,
        admission: &TaskAdmissionRecord,
        payload: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        payload.validate(work)?;
        admission.limits.validate_file(payload, work)?;
        if !crate::runtime::read_granted_with_work(&admission.grants, payload, work)? {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "mail donor cannot read its payload",
            ));
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let (scope, retained) = self
            .authenticated_scope_with_work(admission, quota.work())
            .await?;
        quota.adopt(retained)?;
        #[cfg(feature = "filesystem")]
        let access = scope.created_access();
        #[cfg(feature = "filesystem")]
        let mut reader: Option<&dyn ContentResidencyVerifier> = None;
        #[cfg(feature = "filesystem")]
        if let Some(access) = access.as_ref() {
            for binding in &access.references {
                if let Some(io) = quota.work().as_deref_mut() {
                    io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
                }
                if &binding.0 == payload {
                    reader = Some(binding.1.as_ref());
                    break;
                }
            }
            if reader.is_none() {
                for binding in &access.content {
                    if let Some(io) = quota.work().as_deref_mut() {
                        io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
                    }
                    if &binding.volume == payload.volume() {
                        reader = Some(binding.binding.reader.as_ref());
                        break;
                    }
                }
            }
        }
        #[cfg(not(feature = "filesystem"))]
        let reader = scope
            .created_reference(payload)
            .map(|reader| reader.as_ref())
            .or_else(|| {
                scope
                    .created_content(Some(payload.volume()))
                    .map(|binding| binding.1.as_ref())
            });
        let reader = reader.unwrap_or(self.reader.as_ref());
        let bytes = match quota.work().as_deref_mut() {
            Some(io) => {
                crate::store::history::read_content_with_work(io, reader, payload)
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

    pub(super) async fn publish_control(
        &self,
        stream: &acyclic_stream::Stream<P>,
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
        bytes: &[u8],
    ) -> Result<()> {
        if self
            .publish_control_at(stream, kind, task_id, operation_id, bytes, None)
            .await?
        {
            Ok(())
        } else {
            Err(Error::Conflict(
                "unconditional control append conflicted".into(),
            ))
        }
    }

    pub(super) async fn publish_control_at(
        &self,
        stream: &acyclic_stream::Stream<P>,
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
        bytes: &[u8],
        expected_tail: Option<u64>,
    ) -> Result<bool> {
        let key = Self::event_key(kind, task_id, operation_id)?;
        let key = if let Some(tail) = expected_tail {
            let mut digest = blake3::Hasher::new();
            digest.update(b"harness/v2/control-tail\0");
            digest.update(key.as_bytes());
            digest.update(&tail.to_le_bytes());
            StreamKey::new(OwnedBytes::copy_from_slice(digest.finalize().as_bytes()))?
        } else {
            key
        };
        let outcome = crate::distributed::append(
            (stream, &self.stream),
            [OwnedBytes::copy_from_slice(bytes)],
            0,
            expected_tail,
            key,
            operation_id,
            &mut None,
        )
        .await?;
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
                Ok(true)
            }
            AppendOutcome::Committed(_) => {
                Err(Error::Storage("invalid control append receipt".into()))
            }
            AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }
}
