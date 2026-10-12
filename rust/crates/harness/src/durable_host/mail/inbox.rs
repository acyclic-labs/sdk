//! Bounded inbox replay.
use super::*;

impl<P: StreamProvider + 'static> CoordinatorTaskHost<P> {
    pub(in super::super) async fn inbox_page(
        &self,
        task: TaskId,
        after: u64,
        limit: usize,
        io: Option<&PreparationIo>,
    ) -> Result<Vec<InboxItem>> {
        require_original(io)?;
        if limit == 0 {
            return Err(boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "inbox page bound is invalid",
            ));
        }
        let recipient = self.mail_admission(task, io).await?;
        #[cfg(feature = "filesystem")]
        let mut observer = io.map(PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|io| io as &mut dyn HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work: Observer<'_> = None;
        let mut text = [0; 80];
        let mailbox = operations::stream(&self.stream, recipient_path(task, &mut text), &mut work)?;
        let bounds =
            match operations::bounds_with_work(&self.stream, mailbox.0.path(), &mut work).await? {
                Ok(bounds) => bounds,
                Err(error) if *sdk_source(&error) == StreamError::NotFound => return Ok(Vec::new()),
                Err(error) => return Err(sdk_error(error, &work)),
            };
        if after > bounds.tail {
            return Err(boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "inbox cursor is beyond the tail",
            ));
        }
        let maximum = u32::try_from(limit.min(acyclic_stream::MAX_ITEMS)).map_err(|_| {
            boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "inbox page bound is invalid",
            )
        })?;
        let mut records =
            match operations::read_records(&self.stream, &mailbox.0, after, maximum, &mut work)
                .await?
            {
                Ok(records) => records,
                Err(error) if *sdk_source(&error) == StreamError::NotFound => return Ok(Vec::new()),
                Err(error) => return Err(sdk_error(error, &work)),
            };
        let mut expected = after;
        let mut items = Vec::new();
        let mut output_bytes = 0_u64;
        while let Some(record) = records.next(&mut work).await? {
            if items.len() >= maximum as usize || record.sequence != expected {
                return Err(boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "inbox page extent or replay sequence differs",
                ));
            }
            expected = expected.checked_add(1).ok_or_else(|| {
                boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "inbox sequence exhausted",
                )
            })?;
            let event: MailEvent = decode(&record.value, io)?;
            let canonical = encode(&event, acyclic_stream::MAX_RECORD_BYTES as u64, io)?;
            if canonical.as_slice() != record.value.as_ref()
                || event.recipient != task
                || event.schema_revision != MAIL_SCHEMA_REVISION
                || event.route_revision != MAIL_ROUTE_REVISION
            {
                return Err(boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "inbox v3 recipient or version differs",
                ));
            }
            drop(canonical);
            validate_file(&event.payload, recipient.limits, io)?;
            let sender = self.mail_admission(event.sender, io).await?;
            let direct = authorize_route(event.sender, &sender, task, &recipient, io)?;
            validate_file(&event.payload, sender.limits, io)?;
            if !crate::runtime::read_granted_with_work(&sender.grants, &event.payload, &mut work)?
                || (!direct
                    && !crate::runtime::read_granted_with_work(
                        &recipient.grants,
                        &event.payload,
                        &mut work,
                    )?)
            {
                return Err(boundary(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "inbox source or custom recipient lacks original file READ",
                ));
            }
            if !self
                .observe_mail_location(
                    event.sender,
                    task,
                    event.message_id,
                    &record.value,
                    Some(record.sequence),
                    io,
                )
                .await?
            {
                return Err(boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "unindexed mail format is not supported",
                ));
            }
            reserve(&mut items, maximum as usize, io)?;
            let mut message_id = String::new();
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.retain_owned(36)?;
                io.allocation_attempt(36)?;
            }
            message_id
                .try_reserve_exact(36)
                .map_err(Error::Allocation)?;
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                if message_id.capacity() >= 36 {
                    io.retain_owned(message_id.capacity() as u64 - 36)?;
                } else {
                    io.release(36 - message_id.capacity() as u64)?;
                }
            }
            if let Some(observer) = work.as_deref_mut() {
                observer.encoded_bytes(36)?;
            }
            write!(&mut message_id, "{}", event.message_id).expect("reserved canonical UUID text");
            if let Some(observer) = work.as_deref_mut() {
                observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
            }
            items.push(InboxItem {
                task_id: task,
                sequence: expected,
                message_id,
                payload: event.payload,
            });
            output_bytes = output_bytes
                .checked_add(record.value.len() as u64)
                .ok_or_else(|| {
                    boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "inbox output byte extent exhausted",
                    )
                })?;
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.return_output(output_bytes, items.len() as u64)?;
        }
        Ok(items)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Reads one bounded page using the original native ledger.
    pub async fn inbox_with_native_work(
        &self,
        task: TaskId,
        after: u64,
        limit: usize,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Vec<InboxItem>, acyclic_fs::NativeAllocation)> {
        let io = PreparationIo::from_native_work(work)?;
        let items = self.inbox_page(task, after, limit, Some(&io)).await?;
        let owned = items.iter().try_fold(
            (items.capacity() * size_of::<InboxItem>()) as u64,
            |owned, item| {
                let payload = item.payload.logical_owned_bytes()?;
                owned
                    .checked_add(item.message_id.capacity() as u64)
                    .and_then(|owned| owned.checked_add(payload))
                    .ok_or_else(|| {
                        boundary(
                            Some(&io),
                            crate::wire::ErrorCode::Invalid,
                            "inbox retained capacity overflows",
                        )
                    })
            },
        )?;
        let custody = io.into_native_custody(owned)?;
        Ok((items, custody))
    }
}
