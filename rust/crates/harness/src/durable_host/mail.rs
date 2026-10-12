//! The original v3 atomic mail engine with optional original SDK observation.

use super::*;
use crate::distributed::{sdk_error, sdk_source};
use crate::executor::PreparationIo;
#[cfg(feature = "filesystem")]
use crate::filesystem::PreparationRecord as MailRecord;
#[cfg(not(feature = "filesystem"))]
use serde::de::DeserializeOwned as MailRecord;
use crate::store::{history::{HistoryIo, canonical_bytes_with_work}, operations};
use acyclic_stream::{CommitCondition, CommitOutcome};
use std::{fmt::Write as _, mem::size_of};

type Observer<'a> = Option<&'a mut dyn HistoryIo>;

fn boundary(io: Option<&PreparationIo>, code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.is_some() { return Error::NativeBoundary { code, reason }; }
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
    let Some(io) = io else { return Ok(()); };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.original_native_work().is_some() { return io.check_cancellation(); }
    Err(boundary(Some(io), crate::wire::ErrorCode::Unsupported,
        "mail observer requires its original native work"))
}

pub(crate) fn received_copy_operation(task: TaskId, receive: OperationId,
    io: Option<&PreparationIo>) -> Result<OperationId> {
    require_original(io)?;
    let domain = b"acyclic.harness.received-copy.v1\0";
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { io.history_io().hashed_bytes(domain.len() as u64 + 32)?; }
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(&task.into_bytes());
    hash.update(&receive.into_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok(OperationId::from_bytes(id))
}

fn encode<T: Serialize + ?Sized>(value: &T, maximum: u64, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
    require_original(io)?;
    if io.is_none() { crate::contract::validate_json_byte_bound(value, maximum)?; }
    #[cfg(feature = "filesystem")]
    let mut observer = io.map(PreparationIo::history_io);
    #[cfg(feature = "filesystem")]
    let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
    #[cfg(not(feature = "filesystem"))]
    let mut work: Observer<'_> = None;
    let bytes = canonical_bytes_with_work(value, maximum, &mut work)?;
    Ok(operations::owned_bytes_from_retained_vec(bytes, &mut work)?.0)
}

fn decode<T: MailRecord>(bytes: &[u8], io: Option<&PreparationIo>) -> Result<T> {
    require_original(io)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io { return Ok(crate::filesystem::retain_decoded(io.decode_preparation::<T>(bytes, crate::wire::ErrorCode::Storage)?)); }
    crate::contract::json_from_slice(bytes).map_err(|error| Error::Storage(error.to_string()))
}

fn validate_file(file: &FileRef, limits: crate::conversation::Limits, io: Option<&PreparationIo>) -> Result<()> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = io {
        let work = io.original_native_work().ok_or_else(|| boundary(Some(io),
            crate::wire::ErrorCode::Unsupported, "mail file validation requires original native work"))?;
        file.validate_with_native_work(work)?;
        if file.path().len() > limits.path_bytes || file.descriptor().byte_length() > limits.file_bytes {
            return Err(boundary(Some(io), crate::wire::ErrorCode::Invalid,
                "mail file exceeds original admission limits"));
        }
        return Ok(());
    }
    file.validate()?;
    limits.validate_file(file)
}

fn reserve<T>(items: &mut Vec<T>, maximum: usize, io: Option<&PreparationIo>) -> Result<()> {
    if items.len() < items.capacity() { return Ok(()); }
    let previous = items.capacity();
    let target = previous.saturating_mul(2).max(4).min(maximum);
    let requested = target.checked_sub(previous).and_then(|slots| slots.checked_mul(size_of::<T>()))
        .ok_or_else(|| boundary(io, crate::wire::ErrorCode::Invalid, "mail page capacity overflows"))? as u64;
    if target <= previous { return Err(boundary(io, crate::wire::ErrorCode::Storage, "mail page exceeds requested limit")); }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.retain_owned(requested)?;
        io.allocation_attempt((target * size_of::<T>()) as u64)?;
    }
    items.try_reserve_exact(target - items.len()).map_err(Error::Allocation)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let actual = ((items.capacity() - previous) * size_of::<T>()) as u64;
        if actual >= requested { io.retain_owned(actual - requested)?; }
        else { io.release(requested - actual)?; }
    }
    Ok(())
}

fn recipient_path(task: TaskId, text: &mut [u8; 80]) -> &str {
    let prefix = b"harness/v3/mail/by-recipient/";
    text[..prefix.len()].copy_from_slice(prefix);
    uuid::Uuid::from_bytes(task.into_bytes()).hyphenated().encode_lower(&mut text[prefix.len()..prefix.len() + 36]);
    std::str::from_utf8(&text[..prefix.len() + 36]).expect("canonical ASCII mail path")
}

fn intent_path(sender: TaskId, message: OperationId, text: &mut [u8; 112]) -> &str {
    let prefix = b"harness/v3/mail/by-intent/";
    text[..prefix.len()].copy_from_slice(prefix);
    uuid::Uuid::from_bytes(sender.into_bytes()).hyphenated().encode_lower(&mut text[prefix.len()..prefix.len() + 36]);
    text[prefix.len() + 36] = b'/';
    uuid::Uuid::from_bytes(message.into_bytes()).hyphenated().encode_lower(&mut text[prefix.len() + 37..prefix.len() + 73]);
    std::str::from_utf8(&text[..prefix.len() + 73]).expect("canonical ASCII mail intent path")
}

impl<P: StreamProvider + 'static> CoordinatorTaskHost<P> {
    async fn mail_admission(&self, task: TaskId, io: Option<&PreparationIo>) -> Result<TaskAdmissionRecord> {
        require_original(io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = io {
            let (admission, custody) = self.observe_admission_with_native_work(task,
                io.original_native_work().expect("checked original mail observer")).await?;
            io.absorb_native_custody(custody)?;
            return Ok(admission);
        }
        self.admission_with_work(OperationId::from_bytes(task.into_bytes()), &mut None).await
    }

    async fn prepare_mail(&self, sender: TaskId, recipient: TaskId, message: OperationId,
        payload: FileRef, io: Option<&PreparationIo>) -> Result<(OwnedBytes, FileRef)> {
        require_original(io)?;
        let sender_admission = self.mail_admission(sender, io).await?;
        let recipient_admission = self.mail_admission(recipient, io).await?;
        let direct = sender_admission.parent == Some(recipient) || recipient_admission.parent == Some(sender);
        if !direct && (!sender_admission.grants.contains(capability::MAIL_SEND)
            || !recipient_admission.grants.contains(capability::MAIL_READ)) {
            return Err(boundary(io, crate::wire::ErrorCode::Unauthorized,
                "custom mail route requires original sender and recipient grants"));
        }
        validate_file(&payload, sender_admission.limits, io)?;
        validate_file(&payload, recipient_admission.limits, io)?;
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Observer<'_> = None;
        if !crate::runtime::read_granted_with_work(&sender_admission.grants, &payload, &mut work)?
            || (!direct && !crate::runtime::read_granted_with_work(&recipient_admission.grants, &payload, &mut work)?) {
            return Err(boundary(io, crate::wire::ErrorCode::Unauthorized,
                "mail source or custom recipient lacks original file READ"));
        }
        let event = MailEvent { sender, recipient, message_id: message,
            schema_revision: MAIL_SCHEMA_REVISION, route_revision: MAIL_ROUTE_REVISION, payload };
        let bytes = encode(&event, acyclic_stream::MAX_RECORD_BYTES as u64, io)?;
        Ok((bytes, event.payload))
    }

    pub(super) async fn mail_bytes(&self, sender: TaskId, recipient: TaskId,
        message: OperationId, payload: FileRef, io: Option<&PreparationIo>) -> Result<OwnedBytes> {
        self.prepare_mail(sender, recipient, message, payload, io).await.map(|(bytes, _)| bytes)
    }

    pub(super) async fn inbox_page(&self, task: TaskId, after: u64, limit: usize,
        io: Option<&PreparationIo>) -> Result<Vec<InboxItem>> {
        require_original(io)?;
        if limit == 0 { return Err(boundary(io, crate::wire::ErrorCode::Invalid, "inbox page bound is invalid")); }
        let recipient = self.mail_admission(task, io).await?;
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Observer<'_> = None;
        let mut text = [0; 80];
        let mailbox = operations::stream_text_with_work(&self.stream, recipient_path(task, &mut text), &mut work)?;
        let bounds = match operations::bounds_with_work(&self.stream, mailbox.stream.path(), &mut work).await? {
            Ok(bounds) => bounds,
            Err(error) if *sdk_source(&error) == StreamError::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(sdk_error(error, &work)),
        };
        if after > bounds.tail { return Err(boundary(io, crate::wire::ErrorCode::Invalid, "inbox cursor is beyond the tail")); }
        let maximum = u32::try_from(limit.min(acyclic_stream::MAX_ITEMS))
            .map_err(|_| boundary(io, crate::wire::ErrorCode::Invalid, "inbox page bound is invalid"))?;
        let mut records = match operations::read_records_with_work(&self.stream, &mailbox.stream,
            after, maximum, &mut work).await? {
            Ok(records) => records,
            Err(error) if *sdk_source(&error) == StreamError::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(sdk_error(error, &work)),
        };
        let mut expected = after;
        let mut items = Vec::new();
        let mut output_bytes = 0_u64;
        while let Some(record) = records.next(&mut work).await? {
            if items.len() >= maximum as usize || record.sequence != expected {
                return Err(boundary(io, crate::wire::ErrorCode::Storage, "inbox page extent or replay sequence differs"));
            }
            expected = expected.checked_add(1).ok_or_else(|| boundary(io,
                crate::wire::ErrorCode::Storage, "inbox sequence exhausted"))?;
            let event: MailEvent = decode(&record.value, io)?;
            let canonical = encode(&event, acyclic_stream::MAX_RECORD_BYTES as u64, io)?;
            if canonical.as_slice() != record.value.as_ref() || event.recipient != task
                || event.schema_revision != MAIL_SCHEMA_REVISION || event.route_revision != MAIL_ROUTE_REVISION {
                return Err(boundary(io, crate::wire::ErrorCode::Storage, "inbox v3 recipient or version differs"));
            }
            drop(canonical);
            validate_file(&event.payload, recipient.limits, io)?;
            let sender = self.mail_admission(event.sender, io).await?;
            let direct = sender.parent == Some(task) || recipient.parent == Some(event.sender);
            if !direct && (!sender.grants.contains(capability::MAIL_SEND) || !recipient.grants.contains(capability::MAIL_READ)) {
                return Err(boundary(io, crate::wire::ErrorCode::Unauthorized, "custom inbox route lacks original send/read grants"));
            }
            validate_file(&event.payload, sender.limits, io)?;
            if !crate::runtime::read_granted_with_work(&sender.grants, &event.payload, &mut work)?
                || (!direct && !crate::runtime::read_granted_with_work(&recipient.grants, &event.payload, &mut work)?) {
                return Err(boundary(io, crate::wire::ErrorCode::Unauthorized, "inbox source or custom recipient lacks original file READ"));
            }
            if !self.observe_mail_location(event.sender, task, event.message_id,
                &record.value, Some(record.sequence), io).await? {
                return Err(boundary(io, crate::wire::ErrorCode::Unsupported, "unindexed mail format is not supported"));
            }
            reserve(&mut items, maximum as usize, io)?;
            let mut message_id = String::new();
            #[cfg(feature = "filesystem")]
            if let Some(io) = io { io.retain_owned(36)?; io.allocation_attempt(36)?; }
            message_id.try_reserve_exact(36).map_err(Error::Allocation)?;
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                if message_id.capacity() >= 36 { io.retain_owned(message_id.capacity() as u64 - 36)?; }
                else { io.release(36 - message_id.capacity() as u64)?; }
            }
            if let Some(observer) = work.as_deref_mut() { observer.encoded_bytes(36)?; }
            write!(&mut message_id, "{}", event.message_id).expect("reserved canonical UUID text");
            if let Some(observer) = work.as_deref_mut() {
                observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
            }
            items.push(InboxItem { task_id: task, sequence: expected, message_id, payload: event.payload });
            output_bytes = output_bytes.checked_add(record.value.len() as u64).ok_or_else(||
                boundary(io, crate::wire::ErrorCode::Storage, "inbox output byte extent exhausted"))?;
        }
        if let Some(observer) = work.as_deref_mut() { observer.return_output(output_bytes, items.len() as u64)?; }
        Ok(items)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Reads one bounded page using the original native ledger.
    pub async fn inbox_with_native_work(&self, task: TaskId, after: u64, limit: usize,
        work: &acyclic_fs::NativeMaterializationWork) -> Result<(Vec<InboxItem>, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let items = self.inbox_page(task, after, limit, Some(&io)).await?;
        let owned = items.iter().try_fold((items.capacity() * size_of::<InboxItem>()) as u64, |owned, item| {
            let payload = item.payload.logical_owned_bytes()?;
            owned.checked_add(item.message_id.capacity() as u64)
                .and_then(|owned| owned.checked_add(payload))
                .ok_or_else(|| boundary(Some(&io), crate::wire::ErrorCode::Invalid, "inbox retained capacity overflows"))
        })?;
        let custody = io.into_native_custody(owned)?;
        Ok((items, custody))
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn send_owned_with_io(self: &Arc<Self>, sender: TaskId,
        fence: &crate::scheduler::LeaseFence, recipient: TaskId, message: OperationId,
        payload: FileRef, io: Option<&PreparationIo>) -> Result<FileRef> {
        require_original(io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let owner = match io {
            Some(io) => self.journal_owner_with_native_work(sender, fence,
                io.original_native_work().expect("checked original mail observer")).await?,
            None => self.journal_owner(sender, fence.clone()).await?,
        };
        #[cfg(target_arch = "wasm32")]
        let owner = self.journal_owner(sender, fence.clone()).await?;
        self.send_from_owner(&owner, recipient, message, payload, io).await
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn send_from_owner(&self, owner: &TaskJournalOwner<P>,
        recipient: TaskId, message: OperationId, payload: FileRef, io: Option<&PreparationIo>) -> Result<FileRef> {
        require_original(io)?;
        if !std::ptr::eq(self, owner.host.as_ref()) {
            return Err(boundary(io, crate::wire::ErrorCode::Unauthorized, "mail sender belongs to another original host"));
        }
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Observer<'_> = None;
        self.verify_mail_owner(owner.task_id, Some(&owner.fence), Some(owner), false, &mut work).await?;
        let (bytes, payload) = self.prepare_mail(owner.task_id, recipient, message, payload, io).await?;
        self.publish_mail(owner.task_id, recipient, message, bytes, Some(&owner.fence), Some(owner), io).await?;
        Ok(payload)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn reconcile_sent_mail(self: &Arc<Self>, sender: TaskId,
        fence: &crate::scheduler::LeaseFence, recipient: TaskId, message: OperationId,
        payload: FileRef, io: Option<&PreparationIo>) -> Result<bool> {
        require_original(io)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let owner = match io {
            Some(io) => Some(self.journal_owner_with_native_work(sender, fence,
                io.original_native_work().expect("checked original mail observer")).await?),
            None => None,
        };
        #[cfg(target_arch = "wasm32")]
        let owner: Option<TaskJournalOwner<P>> = None;
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Observer<'_> = None;
        self.verify_mail_owner(sender, Some(fence), owner.as_ref(), true, &mut work).await?;
        let bytes = self.mail_bytes(sender, recipient, message, payload, io).await?;
        let retained = self.observe_mail_location(sender, recipient, message, &bytes, None, io).await?;
        self.verify_mail_owner(sender, Some(fence), owner.as_ref(), true, &mut work).await?;
        Ok(retained)
    }

    pub(super) async fn publish_mail(&self, sender: TaskId, recipient: TaskId,
        message: OperationId, bytes: OwnedBytes, fence: Option<&crate::scheduler::LeaseFence>,
        #[cfg(feature = "filesystem")] owner: Option<&TaskJournalOwner<P>>,
        #[cfg(not(feature = "filesystem"))] owner: Option<&std::convert::Infallible>, io: Option<&PreparationIo>) -> Result<()> {
        require_original(io)?;
        if io.is_some() && fence.is_some() && owner.is_none() {
            return Err(boundary(io, crate::wire::ErrorCode::Unsupported, "native sender requires its original retained owner"));
        }
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Observer<'_> = None;
        let mut recipient_text = [0; 80];
        let mailbox = operations::stream_text_with_work(&self.stream,
            recipient_path(recipient, &mut recipient_text), &mut work)?;
        let mut intent_text = [0; 112];
        let intent = operations::stream_text_with_work(&self.stream,
            intent_path(sender, message, &mut intent_text), &mut work)?;
        let mut source_verified = false;
        loop {
            self.verify_mail_owner(sender, fence, owner, false, &mut work).await?;
            if self.observe_mail_location(sender, recipient, message, &bytes, None, io).await? {
                self.verify_mail_owner(sender, fence, owner, false, &mut work).await?;
                return Ok(());
            }
            if !source_verified {
                let event: MailEvent = decode(&bytes, io)?;
                if event.sender != sender || event.message_id != message
                    || (io.is_some() && (event.recipient != recipient
                        || event.schema_revision != MAIL_SCHEMA_REVISION || event.route_revision != MAIL_ROUTE_REVISION)) {
                    return Err(boundary(io, crate::wire::ErrorCode::Storage, "mail publication v3 identity differs"));
                }
                let admission = self.mail_admission(sender, io).await?;
                let source = self.read_mail_source(&admission, &event.payload, &mut work).await?;
                drop(source);
                source_verified = true;
            }
            let (tail, target) = match operations::bounds_with_work(&self.stream, mailbox.stream.path(), &mut work).await? {
                Ok(bounds) => (bounds.tail, CommitCondition::Tail { path: mailbox.stream.path().clone(), expected: bounds.tail }),
                Err(error) if *sdk_source(&error) == StreamError::NotFound => (0, CommitCondition::Absent { path: mailbox.stream.path().clone() }),
                Err(error) => return Err(sdk_error(error, &work)),
            };
            if let Some(observer) = work.as_deref_mut() { observer.hashed_bytes(bytes.len() as u64)?; }
            let location = MailLocation { sender, recipient, message_id: message,
                schema_revision: MAIL_SCHEMA_REVISION, route_revision: MAIL_ROUTE_REVISION,
                sequence: tail, value_digest: *blake3::hash(&bytes).as_bytes() };
            let location_bytes = encode(&location, MAX_MAIL_LOCATION_BYTES as u64, io)?;
            let absent = CommitCondition::Absent { path: intent.stream.path().clone() };
            #[cfg(feature = "filesystem")]
            let owner_condition = match (owner, fence) {
                (Some(owner), _) => Some(owner.condition(crate::distributed::JournalWrite::Fresh, &mut work).await?),
                (None, Some(fence)) => Some(self.coordinator.lock().await.journal_condition(
                    &self.owner, &self.owner_scope, &self.verifier, OperationId::from_bytes(sender.into_bytes()),
                    fence, crate::distributed::JournalWrite::Fresh, &mut work).await?),
                (None, None) => None,
            };
            #[cfg(not(feature = "filesystem"))]
            let owner_condition: Option<CommitCondition> = if fence.is_some() {
                return Err(boundary(io, crate::wire::ErrorCode::Unsupported, "owned mail publication is not bound"));
            } else { None };
            let mut conditions = [("", None); 3];
            let mut count = 0;
            for condition in [Some(&target), Some(&absent), owner_condition.as_ref()].into_iter().flatten() {
                conditions[count] = match condition {
                    CommitCondition::Tail { path, expected } => (path.as_str(), Some(*expected)),
                    CommitCondition::Absent { path } => (path.as_str(), None),
                };
                count += 1;
            }
            let physical = encode(&(&location, fence, &conditions[..count]), acyclic_stream::MAX_RECORD_BYTES as u64, io)?;
            if let Some(observer) = work.as_deref_mut() {
                observer.hashed_bytes(physical.len() as u64 + b"acyclic.harness.mail-publication.v2\0".len() as u64)?;
            }
            let mut hash = blake3::Hasher::new();
            hash.update(b"acyclic.harness.mail-publication.v2\0");
            hash.update(&physical);
            let digest = *hash.finalize().as_bytes();
            drop(physical);
            let (key, held) = crate::distributed::digest_key(&digest, &mut work)?;
            let mut request = crate::distributed::PreparedCommitRequest::new(key, held, &mut work);
            request.push_condition(target)?;
            request.push_condition(absent)?;
            if let Some(condition) = owner_condition { request.push_condition(condition)?; }
            request.append(mailbox.stream.path().clone(), bytes.clone())?;
            request.append(intent.stream.path().clone(), location_bytes)?;
            let (outcome, retained) = request.commit(&self.stream, Some(message), None, None, None).await?;
            let committed = matches!(&outcome, CommitOutcome::Committed(_));
            drop(outcome);
            if let Some(observer) = work.as_deref_mut() { observer.release_owned(retained)?; }
            if !committed { continue; }
            if !self.observe_mail_location(sender, recipient, message, &bytes, None, io).await? {
                return Err(boundary(io, crate::wire::ErrorCode::Storage, "committed mail receipt is absent"));
            }
            self.verify_mail_owner(sender, fence, owner, false, &mut work).await?;
            return Ok(());
        }
    }

    async fn verify_mail_owner(&self, sender: TaskId, fence: Option<&crate::scheduler::LeaseFence>,
        #[cfg(feature = "filesystem")] owner: Option<&TaskJournalOwner<P>>,
        #[cfg(not(feature = "filesystem"))] owner: Option<&std::convert::Infallible>, settlement: bool, work: &mut Observer<'_>) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(owner) = owner {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(original) = work.as_ref().and_then(|io| io.original_native_work()) {
                if !owner._native_custody.as_ref().is_some_and(|guard| guard.matches_work(original)) {
                    return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                        reason: "mail owner differs from its original native metadata ledger" });
                }
            }
            return owner.verify(settlement, work).await;
        }
        if let Some(fence) = fence { return self.verify_owner(sender, fence, settlement, &mut None).await; }
        Ok(())
    }

    pub(super) async fn observe_mail_location(&self, sender: TaskId, recipient: TaskId,
        message: OperationId, bytes: &[u8], expected_sequence: Option<u64>,
        io: Option<&PreparationIo>) -> Result<bool> {
        require_original(io)?;
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Observer<'_> = None;
        let mut text = [0; 112];
        let location = operations::stream_text_with_work(&self.stream, intent_path(sender, message, &mut text), &mut work)?;
        let mut records = match operations::read_records_with_work(&self.stream, &location.stream, 0, 2, &mut work).await? {
            Ok(records) => records,
            Err(error) if *sdk_source(&error) == StreamError::NotFound => return Ok(false),
            Err(error) => return Err(sdk_error(error, &work)),
        };
        let record = records.next(&mut work).await?.ok_or_else(|| boundary(io,
            crate::wire::ErrorCode::Storage, "mail location must contain one record"))?;
        if records.next(&mut work).await?.is_some() || record.sequence != 0
            || record.value.len() > MAX_MAIL_LOCATION_BYTES {
            return Err(boundary(io, crate::wire::ErrorCode::Storage, "mail location exceeds its bounded format"));
        }
        let retained: MailLocation = decode(&record.value, io)?;
        if retained.sender != sender || retained.message_id != message
            || retained.schema_revision != MAIL_SCHEMA_REVISION || retained.route_revision != MAIL_ROUTE_REVISION
            || encode(&retained, MAX_MAIL_LOCATION_BYTES as u64, io)?.as_slice() != record.value.as_ref() {
            return Err(boundary(io, crate::wire::ErrorCode::Storage, "mail location identity differs"));
        }
        if expected_sequence.is_some_and(|sequence| sequence != retained.sequence) {
            return Err(boundary(io, crate::wire::ErrorCode::Storage, "inbox record differs from its atomic location"));
        }
        let mut text = [0; 80];
        let mailbox = operations::stream_text_with_work(&self.stream, recipient_path(retained.recipient, &mut text), &mut work)?;
        let mut originals = match operations::read_records_with_work(&self.stream, &mailbox.stream,
            retained.sequence, 1, &mut work).await? {
            Ok(records) => records,
            Err(error) => return Err(sdk_error(error, &work)),
        };
        let original = originals.next(&mut work).await?.ok_or_else(|| boundary(io,
            crate::wire::ErrorCode::Storage, "mail location source is absent"))?;
        if let Some(observer) = work.as_deref_mut() { observer.hashed_bytes(original.value.len() as u64)?; }
        if originals.next(&mut work).await?.is_some() || original.sequence != retained.sequence
            || original.commit_id != record.commit_id || blake3::hash(&original.value).as_bytes() != &retained.value_digest {
            return Err(boundary(io, crate::wire::ErrorCode::Storage, "mail location differs from its atomic source"));
        }
        if retained.recipient != recipient || original.value.as_ref() != bytes {
            return Err(boundary(io, crate::wire::ErrorCode::Conflict, "mail identity reused"));
        }
        Ok(true)
    }
}

#[cfg(feature = "filesystem")]
macro_rules! mail_records {
    ($($record:ty),*) => {$(
        impl MailRecord for $record {
            fn maximum_slot_bytes() -> u64 { crate::filesystem::preparation_slot_bytes::<Self>() }
            fn diagnostic_label_bytes() -> u64 {
                crate::contract::preparation_serde_diagnostic_label_bytes()
                    + concat!("sender recipient message_id schema_revision route_revision payload sequence value_digest ",
                        "unknown field duplicate field missing field invalid type invalid value invalid length ").len() as u64
            }
        }
    )*};
}
#[cfg(feature = "filesystem")]
mail_records!(MailEvent, MailLocation);
