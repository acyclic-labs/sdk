use super::*;

/// One canonical event and its atomic derived indexes.
pub(crate) struct IndexedPublication {
    pub(crate) operation_id: OperationId,
    pub(crate) path: StreamPath,
    pub(crate) expected_tail: u64,
    pub(crate) bytes: OwnedBytes,
    pub(super) indexes: Vec<(StreamPath, u64, OwnedBytes)>,
    retained_payload_bytes: u64,
}

impl IndexedPublication {
    pub(crate) fn new(
        authority: &Authority,
        event: &Event,
        bytes: Vec<u8>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Self> {
        let (bytes, retained) = owned_bytes_from_retained_vec(bytes, work)?;
        let prepared = (|| {
            if let Some(io) = work.as_deref() {
                io.check_cancellation()?;
            }
            let expected_tail = event
                .revision
                .checked_sub(1)
                .ok_or_else(|| Error::Invalid("indexed event revision must be positive".into()))?;
            let path = publication_path(authority, "harness/v2/", None, work)?;
            Ok((expected_tail, path))
        })();
        let (expected_tail, (path, path_owned)) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                drop(bytes);
                return release_owned_after_drop(Err(error), retained, work);
            }
        };
        let mut publication = Self {
            operation_id: event.operation_id,
            path,
            expected_tail,
            bytes,
            indexes: Vec::new(),
            retained_payload_bytes: retained + path_owned,
        };
        let result = (|| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let location = OperationLocation {
                authority: match quota.work().as_deref_mut() {
                    Some(io) => io.clone_authority(authority)?,
                    None => authority.clone(),
                },
                operation_id: event.operation_id,
                revision: event.revision,
                intent_digest: event.intent_digest,
                effect_position: None,
            };
            quota.adopt(location.authority.id.capacity() as u64)?;
            let operation = publication_path(
                authority,
                "harness/v2/aggregate-operations/",
                Some((&event.operation_id, 36)),
                quota.work(),
            )?;
            let encoded = crate::store::history::canonical_bytes_with_work(
                &location,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                quota.work(),
            );
            publication.add_derived_index_with_work(operation, 0, encoded, quota.work())?;
            let message = match &event.payload {
                crate::core::EventPayload::ConversationBound { .. } => Some((uuid::Uuid::nil(), 0)),
                crate::core::EventPayload::ConversationMessageAppended { message } => {
                    Some((message.id, message.sequence))
                }
                crate::core::EventPayload::ProjectMergePublished { receipt } => {
                    Some((receipt.notice.id, receipt.notice.sequence))
                }
                _ => None,
            };
            if let Some((message_id, sequence)) = message {
                let binding = matches!(
                    event.payload,
                    crate::core::EventPayload::ConversationBound { .. }
                );
                if !binding && sequence == 0 {
                    return Err(crate::store::guarded_boundary(
                        quota.work(),
                        crate::wire::ErrorCode::Invalid,
                        "message sequence index requires a positive conversation sequence",
                    ));
                }
                let location = MessageLocation {
                    location,
                    message_id,
                    sequence,
                };
                let path = if binding {
                    publication_path(
                        authority,
                        "harness/v2/conversation-heads/",
                        None,
                        quota.work(),
                    )?
                } else {
                    publication_path(
                        authority,
                        "harness/v2/conversation-messages/",
                        Some((&message_id, 36)),
                        quota.work(),
                    )?
                };
                let encoded = crate::store::history::canonical_bytes_with_work(
                    &location,
                    acyclic_stream::MAX_RECORD_BYTES as u64,
                    quota.work(),
                );
                drop(location);
                publication.add_derived_index_with_work(path, 0, encoded, quota.work())?;
                if !binding {
                    let bytes = publication.indexes.last().expect("message index").2.clone();
                    let path = publication_path(
                        authority,
                        "harness/v2/conversation-sequences/",
                        Some((&sequence, 20)),
                        quota.work(),
                    )?;
                    publication.push_index(path, 0, bytes.clone(), 0, quota.work())?;
                    let path = publication_path(
                        authority,
                        "harness/v2/conversation-heads/",
                        None,
                        quota.work(),
                    )?;
                    publication.push_index(path, sequence, bytes, 0, quota.work())?;
                }
            } else {
                drop(location);
            }
            Ok(())
        })();
        match result {
            Ok(()) => Ok(publication),
            Err(error) => {
                let owned = publication.retained_owned();
                drop(publication);
                release_owned_after_drop(Err(error), owned, work)
            }
        }
    }

    pub(crate) fn retained_owned(&self) -> u64 {
        self.retained_payload_bytes
            + (self.indexes.capacity() * std::mem::size_of::<(StreamPath, u64, OwnedBytes)>())
                as u64
    }

    pub(in crate::store) fn set_effect_position_with_work(
        &mut self,
        position: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref() {
            io.check_cancellation()?;
        }
        let bytes = &mut self
            .indexes
            .first_mut()
            .ok_or_else(|| Error::Storage("publication operation index is absent".into()))?
            .2;
        let mut location = decode_location_with_work(bytes, work)?;
        location.effect_position = Some(position);
        let owned = location.authority.id.capacity() as u64;
        let encoded = crate::store::history::canonical_bytes_with_work(
            &location,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        );
        drop(location);
        let encoded = release_owned_after_drop(encoded, owned, work)?;
        let (encoded, capacity) = owned_bytes_from_retained_vec(encoded, work)?;
        let old_capacity = if work
            .as_deref()
            .is_some_and(|io| !original_native_decode_mode(io))
        {
            bytes.retained_capacity().unwrap_or(0) as u64
        } else {
            0
        };
        drop(std::mem::replace(bytes, encoded));
        self.retained_payload_bytes = self.retained_payload_bytes - old_capacity + capacity;
        release_owned_after_drop(Ok(()), old_capacity, work)
    }

    pub(in crate::store) fn add_derived_index_with_work(
        &mut self,
        path: (StreamPath, u64),
        tail: u64,
        bytes: Result<Vec<u8>>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let encoded = bytes.and_then(|bytes| owned_bytes_from_retained_vec(bytes, work));
        match encoded {
            Ok((bytes, retained)) => self.push_index(path, tail, bytes, retained, work),
            Err(error) => {
                let (path, retained) = path;
                drop(path);
                release_owned_after_drop(Err(error), retained, work)
            }
        }
    }

    fn push_index(
        &mut self,
        path: (StreamPath, u64),
        tail: u64,
        bytes: OwnedBytes,
        retained: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let (path, path_owned) = path;
        if let Some(io) = work.as_deref_mut() {
            let capacity = self.indexes.len() + 1;
            if let Err(error) = crate::store::history::reserve(&mut self.indexes, capacity, io) {
                drop(path);
                drop(bytes);
                return release_owned_after_drop(Err(error), retained + path_owned, work);
            }
        }
        self.retained_payload_bytes += retained + path_owned;
        self.indexes.push((path, tail, bytes));
        Ok(())
    }

    pub(crate) fn add_index(
        &self,
        request: &mut (
            acyclic_stream::CommitRequest,
            crate::wire_codec::CodecQuota<'_, '_>,
        ),
    ) -> Result<()> {
        for (path, tail, bytes) in &self.indexes {
            crate::distributed::add_condition(
                request,
                if *tail == 0 {
                    CommitCondition::Absent { path: path.clone() }
                } else {
                    CommitCondition::Tail {
                        path: path.clone(),
                        expected: *tail,
                    }
                },
            )?;
            crate::distributed::add_record(request, path.clone(), bytes.clone())?;
        }
        Ok(())
    }

    pub(crate) async fn request<'a, 'b, P: StreamProvider>(
        &self,
        client: &std::sync::Arc<P>,
        key: IdempotencyKey,
        work: &'a mut Option<&'b mut dyn crate::store::HistoryIo>,
    ) -> Result<(
        acyclic_stream::CommitRequest,
        crate::wire_codec::CodecQuota<'a, 'b>,
    )> {
        let key_owned = if work
            .as_deref()
            .is_some_and(|io| !original_native_decode_mode(io))
        {
            32
        } else {
            0
        };
        let mut request = crate::distributed::commit_request(key, key_owned, work);
        if let Some(io) = request.1.work().as_deref() {
            io.check_cancellation()?;
        }
        let target = match bounds_with_work(client, &self.path, request.1.work()).await? {
            Ok(_) => CommitCondition::Tail {
                path: self.path.clone(),
                expected: self.expected_tail,
            },
            Err(error)
                if *sdk_source(&error) == acyclic_stream::StreamError::NotFound
                    && self.expected_tail == 0 =>
            {
                drop(error);
                CommitCondition::Absent {
                    path: self.path.clone(),
                }
            }
            Err(error) => return Err(sdk_error(error, request.1.work())),
        };
        crate::distributed::add_condition(&mut request, target)?;
        crate::distributed::add_record(&mut request, self.path.clone(), self.bytes.clone())?;
        self.add_index(&mut request)?;
        Ok(request)
    }
    pub(crate) fn verify(
        &self,
        envelope: &CommittedEnvelope,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref() {
            io.check_cancellation()?;
        }
        if envelope.mutations.len() != 1 + self.indexes.len() {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "indexed publication has invalid mutation count",
            ));
        }
        for (path, tail, bytes) in std::iter::once((&self.path, self.expected_tail, &self.bytes))
            .chain(
                self.indexes
                    .iter()
                    .map(|(path, tail, bytes)| (path, *tail, bytes)),
            )
        {
            if let Some(io) = work.as_deref() {
                io.check_cancellation()?;
            }
            let append = envelope
                .mutations
                .iter()
                .find_map(|mutation| match mutation {
                    CommittedMutation::Append(append) if &append.path == path => Some(append),
                    _ => None,
                })
                .ok_or_else(|| {
                    crate::store::guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Storage,
                        "indexed publication is missing an append",
                    )
                })?;
            let end = tail.checked_add(1).ok_or_else(|| {
                crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Storage,
                    "indexed publication tail overflows",
                )
            })?;
            if append.start != tail
                || append.end != end
                || append.tail != end
                || append.records.len() != 1
                || append.records.first().is_none_or(|record| {
                    record.sequence != tail
                        || record.value != *bytes
                        || record.commit_id != envelope.commit_id
                })
            {
                return Err(crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Storage,
                    "indexed publication append is invalid",
                ));
            }
        }
        Ok(())
    }
}
