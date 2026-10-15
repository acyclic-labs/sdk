//! The original v3 atomic mail engine with optional original SDK observation.

use super::*;

use crate::executor::PreparationIo;
#[cfg(feature = "filesystem")]
use crate::filesystem::UploadRecord as MailRecord;
use crate::store::{HistoryIo, operations};
use acyclic_stream::{CommitCondition, CommitOutcome};
#[cfg(not(feature = "filesystem"))]
use serde::de::DeserializeOwned as MailRecord;

mod inbox;

fn boundary(
    io: Option<&PreparationIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.is_some() {
        return Error::NativeBoundary { code, reason };
    }
    let _ = io;
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        crate::wire::ErrorCode::Unsupported => Error::Unsupported(reason.into()),
        _ => Error::Storage(reason.into()),
    }
}

fn require_original(io: Option<&PreparationIo>) -> Result<()> {
    let Some(io) = io else {
        return Ok(());
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.original_native_work().is_some() {
        return io.check_cancellation();
    }
    Err(boundary(
        Some(io),
        crate::wire::ErrorCode::Unsupported,
        "mail observer requires its original native work",
    ))
}

pub(super) fn encode<T: Serialize>(
    value: &T,
    maximum: u64,
    io: Option<&PreparationIo>,
) -> Result<OwnedBytes> {
    require_original(io)?;
    canonical_payload(value, maximum, io)
}

pub(super) fn decode<T: MailRecord>(bytes: &[u8], io: Option<&PreparationIo>) -> Result<T> {
    require_original(io)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        return io.decode_json(bytes);
    }
    crate::contract::json_from_slice(bytes).map_err(|error| Error::Storage(error.to_string()))
}

fn validate_file(
    file: &FileRef,
    limits: crate::conversation::Limits,
    io: Option<&PreparationIo>,
) -> Result<()> {
    require_original(io)?;
    #[cfg(feature = "filesystem")]
    let mut observer = io.map(PreparationIo::history_io);
    #[cfg(feature = "filesystem")]
    let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
    #[cfg(not(feature = "filesystem"))]
    let mut work: Option<&mut dyn HistoryIo> = None;
    limits.validate_file(file, &mut work)
}

pub(super) fn authorize_route(
    sender: TaskId,
    sender_admission: &TaskAdmissionRecord,
    recipient: TaskId,
    recipient_admission: &TaskAdmissionRecord,
    io: Option<&PreparationIo>,
) -> Result<bool> {
    let direct =
        sender_admission.parent == Some(recipient) || recipient_admission.parent == Some(sender);
    if !direct
        && (!sender_admission.grants.contains(capability::MAIL_SEND)
            || !recipient_admission.grants.contains(capability::MAIL_READ))
    {
        return Err(boundary(
            io,
            crate::wire::ErrorCode::Unauthorized,
            "custom mail route requires original sender and recipient grants",
        ));
    }
    Ok(direct)
}

fn reserve<T>(items: &mut Vec<T>, maximum: usize, io: Option<&PreparationIo>) -> Result<()> {
    if items.len() < items.capacity() {
        return Ok(());
    }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        return crate::store::history::ensure_slot(items, maximum, &mut io.history_io());
    }
    let target = items.capacity().saturating_mul(2).max(4).min(maximum);
    if target <= items.capacity() {
        return Err(boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "mail page exceeds requested limit",
        ));
    }
    items
        .try_reserve_exact(target - items.len())
        .map_err(Error::Allocation)
}

fn recipient_path(task: TaskId, text: &mut [u8; 80]) -> Result<&str> {
    let prefix = b"harness/v3/mail/by-recipient/";
    let (path, _) = text.split_at_mut(prefix.len() + 36);
    let (head, id) = path.split_at_mut(prefix.len());
    head.copy_from_slice(prefix);
    uuid::Uuid::from_bytes(task.into_bytes())
        .hyphenated()
        .encode_lower(id);
    std::str::from_utf8(path).map_err(|error| Error::Invalid(error.to_string()))
}

fn intent_path(sender: TaskId, message: OperationId, text: &mut [u8; 112]) -> Result<&str> {
    let prefix = b"harness/v3/mail/by-intent/";
    let (path, _) = text.split_at_mut(prefix.len() + 73);
    let (head, ids) = path.split_at_mut(prefix.len());
    head.copy_from_slice(prefix);
    let (sender_text, message_text) = ids.split_at_mut(36);
    uuid::Uuid::from_bytes(sender.into_bytes())
        .hyphenated()
        .encode_lower(sender_text);
    let (separator, message_text) = message_text.split_at_mut(1);
    separator.copy_from_slice(b"/");
    uuid::Uuid::from_bytes(message.into_bytes())
        .hyphenated()
        .encode_lower(message_text);
    std::str::from_utf8(path).map_err(|error| Error::Invalid(error.to_string()))
}

impl<P: StreamProvider + 'static> CoordinatorTaskHost<P> {
    async fn mail_admission(
        &self,
        task: TaskId,
        io: Option<&PreparationIo>,
    ) -> Result<TaskAdmissionRecord> {
        require_original(io)?;
        #[cfg(feature = "filesystem")]
        let mut history = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = history.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Option<&mut dyn HistoryIo> = None;
        self.admission(OperationId::from_bytes(task.into_bytes()), &mut work)
            .await
    }

    pub(super) async fn prepare_mail(
        &self,
        (sender, sender_admission): (TaskId, &TaskAdmissionRecord),
        recipient: TaskId,
        message: OperationId,
        payload: FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<(OwnedBytes, FileRef)> {
        require_original(io)?;
        let recipient_admission = self.mail_admission(recipient, io).await?;
        let direct = authorize_route(
            sender,
            sender_admission,
            recipient,
            &recipient_admission,
            io,
        )?;
        validate_file(&payload, sender_admission.limits, io)?;
        validate_file(&payload, recipient_admission.limits, io)?;
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Option<&mut dyn HistoryIo> = None;
        if !crate::runtime::read_granted_with_work(&sender_admission.grants, &payload, &mut work)?
            || (!direct
                && !crate::runtime::read_granted_with_work(
                    &recipient_admission.grants,
                    &payload,
                    &mut work,
                )?)
        {
            return Err(boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "mail source or custom recipient lacks original file READ",
            ));
        }
        let event = MailEvent {
            sender,
            recipient,
            message_id: message,
            schema_revision: MAIL_SCHEMA_REVISION,
            route_revision: MAIL_ROUTE_REVISION,
            payload,
        };
        let bytes = encode(&event, acyclic_stream::MAX_RECORD_BYTES as u64, io)?;
        Ok((bytes, event.payload))
    }

    /// Publishes mail under the sender's exact uncancelled lease.
    #[cfg(feature = "filesystem")]
    pub async fn send_mail(
        self: &Arc<Self>,
        sender: TaskId,
        fence: &crate::scheduler::LeaseFence,
        recipient: TaskId,
        message: OperationId,
        payload: FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        require_original(io)?;
        let owner = self.journal_owner(sender, fence, io).await?;
        self.send_from_owner(&owner, recipient, message, payload, io)
            .await
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn send_from_owner(
        &self,
        owner: &TaskJournalOwner<P>,
        recipient: TaskId,
        message: OperationId,
        payload: FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        require_original(io)?;
        if !owner.original_host_binding_matches(self) {
            return Err(boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "mail sender belongs to another original host",
            ));
        }
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|io| io as &mut dyn HistoryIo);
        owner.verify(false, &mut work).await?;
        let (bytes, payload) = self
            .prepare_mail(
                (owner.task_id(), owner.admission()),
                recipient,
                message,
                payload,
                io,
            )
            .await?;
        self.publish_mail(
            (owner.task_id(), owner.admission()),
            recipient,
            message,
            bytes,
            Some(owner),
            io,
        )
        .await?;
        Ok(payload)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn reconcile_sent_mail(
        self: &Arc<Self>,
        sender: TaskId,
        fence: &crate::scheduler::LeaseFence,
        recipient: TaskId,
        message: OperationId,
        payload: FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        require_original(io)?;
        let owner = self.journal_owner(sender, fence, io).await?;
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Option<&mut dyn HistoryIo> = None;
        owner.verify(true, &mut work).await?;
        let bytes = self
            .prepare_mail((sender, owner.admission()), recipient, message, payload, io)
            .await?
            .0;
        let retained = self
            .observe_mail_location(sender, recipient, message, &bytes, None, io)
            .await?;
        owner.verify(true, &mut work).await?;
        Ok(retained)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Conditional append and same-key recovery share one admitted parcel and observer."
    )]
    pub(super) async fn publish_mail(
        &self,
        (sender, sender_admission): (TaskId, &TaskAdmissionRecord),
        recipient: TaskId,
        message: OperationId,
        bytes: OwnedBytes,
        #[cfg(feature = "filesystem")] owner: Option<&TaskJournalOwner<P>>,
        #[cfg(not(feature = "filesystem"))] _owner: Option<&std::convert::Infallible>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        require_original(io)?;
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Option<&mut dyn HistoryIo> = None;
        let mut recipient_text = [0; 80];
        let mailbox = operations::stream(
            &self.stream,
            recipient_path(recipient, &mut recipient_text)?,
            &mut work,
        )?;
        let mut intent_text = [0; 112];
        let intent = operations::stream(
            &self.stream,
            intent_path(sender, message, &mut intent_text)?,
            &mut work,
        )?;
        let mut source_verified = false;
        loop {
            #[cfg(feature = "filesystem")]
            if let Some(owner) = owner {
                owner.verify(false, &mut work).await?;
            }
            if self
                .observe_mail_location(sender, recipient, message, &bytes, None, io)
                .await?
            {
                #[cfg(feature = "filesystem")]
                if let Some(owner) = owner {
                    owner.verify(false, &mut work).await?;
                }
                return Ok(());
            }
            if !source_verified {
                let event: MailEvent = decode(&bytes, io)?;
                if event.sender != sender
                    || event.message_id != message
                    || (io.is_some()
                        && (event.recipient != recipient
                            || event.schema_revision != MAIL_SCHEMA_REVISION
                            || event.route_revision != MAIL_ROUTE_REVISION))
                {
                    return Err(boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "mail publication v3 identity differs",
                    ));
                }
                let source = self
                    .read_mail_source(sender_admission, &event.payload, &mut work)
                    .await?;
                drop(source);
                source_verified = true;
            }
            let (tail, target) =
                match operations::bounds_with_work(&self.stream, mailbox.0.path(), &mut work)
                    .await?
                {
                    Ok(bounds) => (
                        bounds.tail,
                        CommitCondition::Tail {
                            path: mailbox.0.path().clone(),
                            expected: bounds.tail,
                        },
                    ),
                    Err(StreamError::NotFound) => (
                        0,
                        CommitCondition::Absent {
                            path: mailbox.0.path().clone(),
                        },
                    ),
                    Err(error) => return Err(operations::stream_error_with_work(error, &work)),
                };
            if let Some(observer) = work.as_deref_mut() {
                observer.hashed_bytes(bytes.len() as u64)?;
            }
            let location = MailLocation {
                sender,
                recipient,
                message_id: message,
                schema_revision: MAIL_SCHEMA_REVISION,
                route_revision: MAIL_ROUTE_REVISION,
                sequence: tail,
                value_digest: *blake3::hash(&bytes).as_bytes(),
            };
            let location_bytes = encode(&location, MAX_MAIL_LOCATION_BYTES as u64, io)?;
            let absent = CommitCondition::Absent {
                path: intent.0.path().clone(),
            };
            #[cfg(feature = "filesystem")]
            let owner_condition = match owner {
                Some(owner) => Some(
                    owner
                        .condition(crate::distributed::JournalWrite::Fresh, &mut work)
                        .await?,
                ),
                None => None,
            };
            #[cfg(feature = "filesystem")]
            let fence = owner.map(|owner| &owner.fence);
            #[cfg(not(feature = "filesystem"))]
            let owner_condition: Option<CommitCondition> = None;
            #[cfg(not(feature = "filesystem"))]
            let fence: Option<&crate::scheduler::LeaseFence> = None;
            let mut conditions = [("", None); 3];
            let mut count = 0;
            for (slot, condition) in conditions.iter_mut().zip(
                [Some(&target), Some(&absent), owner_condition.as_ref()]
                    .into_iter()
                    .flatten(),
            ) {
                *slot = match condition {
                    CommitCondition::Tail { path, expected } => (path.as_str(), Some(*expected)),
                    CommitCondition::Head { path, expected, .. } => {
                        (path.as_str(), Some(*expected))
                    }
                    CommitCondition::Absent { path } => (path.as_str(), None),
                };
                count += 1;
            }
            let physical = encode(
                &(&location, fence, conditions.split_at(count).0),
                acyclic_stream::MAX_RECORD_BYTES as u64,
                io,
            )?;
            if let Some(observer) = work.as_deref_mut() {
                observer.hashed_bytes(
                    physical.len() as u64 + b"acyclic.harness.mail-publication.v2\0".len() as u64,
                )?;
            }
            let mut hash = blake3::Hasher::new();
            hash.update(b"acyclic.harness.mail-publication.v2\0");
            hash.update(&physical);
            let digest = *hash.finalize().as_bytes();
            drop(physical);
            let (key, held) = crate::distributed::digest_key(&digest, &mut work)?;
            let mut request = crate::distributed::commit_request(key, held, &mut work);
            crate::distributed::add_condition(&mut request, target)?;
            crate::distributed::add_condition(&mut request, absent)?;
            if let Some(condition) = owner_condition {
                crate::distributed::add_condition(&mut request, condition)?;
            }
            crate::distributed::add_record(&mut request, mailbox.0.path().clone(), bytes.clone())?;
            crate::distributed::add_record(&mut request, intent.0.path().clone(), location_bytes)?;
            let (outcome, retained) =
                crate::distributed::commit(request, &self.stream, Some(message), None).await?;
            let committed = matches!(&outcome, CommitOutcome::Committed(_));
            drop(outcome);
            if let Some(observer) = work.as_deref_mut() {
                observer.release_owned(retained)?;
            }
            if !committed {
                continue;
            }
            if !self
                .observe_mail_location(sender, recipient, message, &bytes, None, io)
                .await?
            {
                return Err(boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "committed mail receipt is absent",
                ));
            }
            #[cfg(feature = "filesystem")]
            if let Some(owner) = owner {
                owner.verify(false, &mut work).await?;
            }
            return Ok(());
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "One bounded replay authenticates both the retained parcel and its journal location."
    )]
    pub(super) async fn observe_mail_location(
        &self,
        sender: TaskId,
        recipient: TaskId,
        message: OperationId,
        bytes: &[u8],
        expected_sequence: Option<u64>,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        require_original(io)?;
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Option<&mut dyn HistoryIo> = None;
        let mut text = [0; 112];
        let location = operations::stream(
            &self.stream,
            intent_path(sender, message, &mut text)?,
            &mut work,
        )?;
        let mut records =
            match operations::read_records(&self.stream, location.0.path(), 0, 2, &mut work).await?
            {
                Ok(records) => records,
                Err(StreamError::NotFound) => return Ok(false),
                Err(error) => return Err(operations::stream_error_with_work(error, &work)),
            };
        let record = records.next(&mut work).await?.ok_or_else(|| {
            boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "mail location must contain one record",
            )
        })?;
        if records.next(&mut work).await?.is_some()
            || record.sequence != 0
            || record.value.len() > MAX_MAIL_LOCATION_BYTES
        {
            return Err(boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "mail location exceeds its bounded format",
            ));
        }
        let retained: MailLocation = decode(&record.value, io)?;
        if retained.sender != sender
            || retained.message_id != message
            || retained.schema_revision != MAIL_SCHEMA_REVISION
            || retained.route_revision != MAIL_ROUTE_REVISION
            || encode(&retained, MAX_MAIL_LOCATION_BYTES as u64, io)?.as_slice()
                != record.value.as_ref()
        {
            return Err(boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "mail location identity differs",
            ));
        }
        if expected_sequence.is_some_and(|sequence| sequence != retained.sequence) {
            return Err(boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "inbox record differs from its atomic location",
            ));
        }
        let mut text = [0; 80];
        let mailbox = operations::stream(
            &self.stream,
            recipient_path(retained.recipient, &mut text)?,
            &mut work,
        )?;
        let mut originals = match operations::read_records(
            &self.stream,
            mailbox.0.path(),
            retained.sequence,
            1,
            &mut work,
        )
        .await?
        {
            Ok(records) => records,
            Err(error) => return Err(operations::stream_error_with_work(error, &work)),
        };
        let original = originals.next(&mut work).await?.ok_or_else(|| {
            boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "mail location source is absent",
            )
        })?;
        if let Some(observer) = work.as_deref_mut() {
            observer.hashed_bytes(original.value.len() as u64)?;
        }
        if originals.next(&mut work).await?.is_some()
            || original.sequence != retained.sequence
            || original.commit_id != record.commit_id
            || blake3::hash(&original.value).as_bytes() != &retained.value_digest
        {
            return Err(boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "mail location differs from its atomic source",
            ));
        }
        if retained.recipient != recipient || original.value.as_ref() != bytes {
            return Err(boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "mail identity reused",
            ));
        }
        Ok(true)
    }
}

#[cfg(feature = "filesystem")]
impl MailRecord for MailEvent {
    fn logical_owned_bytes(&self) -> Result<u64> {
        self.payload.logical_owned_bytes()
    }
}
#[cfg(feature = "filesystem")]
impl MailRecord for MailLocation {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(0)
    }
}
