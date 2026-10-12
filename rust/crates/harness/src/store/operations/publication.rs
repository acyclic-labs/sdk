use super::*;

/// The existing event and a ref-only location, without another admission lifecycle.
pub(crate) struct IndexedPublication {
    pub(crate) operation_id: OperationId,
    pub(crate) path: StreamPath,
    pub(crate) expected_tail: u64,
    pub(crate) bytes: OwnedBytes,
    pub(super) indexes: Vec<(StreamPath, u64, OwnedBytes)>,
    retained_payload_bytes: u64,
    operation_index_capacity: u64,
}

pub(super) fn reserve_indexes(
    indexes: &mut Vec<(StreamPath, u64, OwnedBytes)>,
    additional: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    let Some(observer) = work.as_deref_mut() else {
        return Ok(());
    };
    let needed = indexes
        .len()
        .checked_add(additional)
        .ok_or_else(|| Error::Invalid("publication index count overflows".into()))?;
    if needed <= indexes.capacity() {
        return Ok(());
    }
    let element_bytes = std::mem::size_of::<(StreamPath, u64, OwnedBytes)>() as u64;
    let old_capacity = indexes.capacity();
    observer.admit_allocation((needed - old_capacity) as u64 * element_bytes)?;
    indexes
        .try_reserve_exact(additional)
        .map_err(|cause| observer.allocation_failure(cause))?;
    observer.retain_owned((indexes.capacity() - old_capacity) as u64 * element_bytes)?;
    Ok(())
}

impl IndexedPublication {
    pub(crate) fn new(
        authority: &Authority,
        event: &Event,
        bytes: Vec<u8>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let byte_capacity = bytes.capacity() as u64;
        let prepared = (|| {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
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
                return release_owned_after_drop(Err(error), byte_capacity, work);
            }
        };
        let (bytes, byte_owned) = match owned_bytes_from_retained_vec(bytes, work) {
            Ok(bytes) => bytes,
            Err(error) => {
                drop(path);
                return release_owned_after_drop(Err(error), path_owned, work);
            }
        };
        let mut publication = Self {
            operation_id: event.operation_id,
            retained_payload_bytes: byte_owned + path_owned,
            operation_index_capacity: 0,
            path,
            expected_tail,
            bytes,
            indexes: Vec::new(),
        };
        let result = (|| {
            let location = OperationLocation {
                authority: match work.as_deref_mut() {
                    Some(observer) => observer.clone_authority(authority)?,
                    None => authority.clone(),
                },
                operation_id: event.operation_id,
                revision: event.revision,
                intent_digest: event.intent_digest,
                effect_position: None,
            };
            let location_capacity = location.authority.id.capacity() as u64;
            let result = (|| {
                let message = match &event.payload {
                    crate::core::EventPayload::ConversationBound { .. } => {
                        Some((uuid::Uuid::nil(), 0))
                    }
                    crate::core::EventPayload::ConversationMessageAppended { message } => {
                        Some((message.id, message.sequence))
                    }
                    crate::core::EventPayload::ProjectMergePublished { receipt } => {
                        Some((receipt.notice.id, receipt.notice.sequence))
                    }
                    _ => None,
                };
                let binding = matches!(
                    event.payload,
                    crate::core::EventPayload::ConversationBound { .. }
                );
                reserve_indexes(
                    &mut publication.indexes,
                    if binding {
                        2
                    } else if message.is_some() {
                        4
                    } else {
                        1
                    },
                    work,
                )?;
                let path = publication_path(
                    authority,
                    "harness/v2/aggregate-operations/",
                    Some((&event.operation_id, 36)),
                    work,
                )?;
                let encoded = crate::store::history::canonical_bytes_with_work(
                    &location,
                    acyclic_stream::MAX_RECORD_BYTES as u64,
                    work,
                );
                publication.add_derived_index_with_work(path, 0, encoded, work)?;
                if let Some((message_id, sequence)) = message {
                    if !binding && sequence == 0 {
                        return Err(crate::store::guarded_boundary(
                            work,
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
                        publication_path(authority, "harness/v2/conversation-heads/", None, work)?
                    } else {
                        publication_path(
                            authority,
                            "harness/v2/conversation-messages/",
                            Some((&message_id, 36)),
                            work,
                        )?
                    };
                    let encoded = crate::store::history::canonical_bytes_with_work(
                        &location,
                        acyclic_stream::MAX_RECORD_BYTES as u64,
                        work,
                    );
                    drop(location);
                    publication.add_derived_index_with_work(path, 0, encoded, work)?;
                    if !binding {
                        let index = publication.indexes.len() - 1;
                        let path = publication_path(
                            authority,
                            "harness/v2/conversation-sequences/",
                            Some((&sequence, 20)),
                            work,
                        )?;
                        publication.push_index_with_work(
                            path,
                            0,
                            publication.indexes[index].2.clone(),
                            0,
                            work,
                        )?;
                        let path = publication_path(
                            authority,
                            "harness/v2/conversation-heads/",
                            None,
                            work,
                        )?;
                        publication.push_index_with_work(
                            path,
                            sequence,
                            publication.indexes[index].2.clone(),
                            0,
                            work,
                        )?;
                    }
                } else {
                    drop(location);
                }
                Ok(())
            })();
            release_owned_after_drop(result, location_capacity, work)
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

    /// Exact source capacities, not encoded lengths or opaque SDK/provider heap.
    pub(crate) fn retained_owned(&self) -> u64 {
        self.retained_payload_bytes
            + self.indexes.capacity() as u64
                * std::mem::size_of::<(StreamPath, u64, OwnedBytes)>() as u64
    }

    pub(in crate::store) fn set_effect_position_with_work(
        &mut self,
        position: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        let (_, _, bytes) = self
            .indexes
            .first_mut()
            .ok_or_else(|| Error::Storage("publication operation index is absent".into()))?;
        let mut location = decode_location_with_work(bytes, work)?;
        location.effect_position = Some(position);
        let location_capacity = location.authority.id.capacity() as u64;
        let encoded = crate::store::history::canonical_bytes_with_work(
            &location,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        );
        drop(location);
        let encoded = release_owned_after_drop(encoded, location_capacity, work)?;
        let (encoded, capacity) = owned_bytes_from_retained_vec(encoded, work)?;
        let old = std::mem::replace(bytes, encoded);
        drop(old);
        let old_capacity = std::mem::replace(&mut self.operation_index_capacity, capacity);
        self.retained_payload_bytes = self.retained_payload_bytes - old_capacity + capacity;
        release_owned_after_drop(Ok(()), old_capacity, work)?;
        Ok(())
    }

    pub(in crate::store) fn add_derived_index_with_work(
        &mut self,
        path: (StreamPath, u64),
        tail: u64,
        bytes: Result<Vec<u8>>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let bytes = match bytes {
            Ok(bytes) => bytes,
            Err(error) => {
                let (path, retained) = path;
                drop(path);
                return release_owned_after_drop(Err(error), retained, work);
            }
        };
        let (bytes, capacity) = match owned_bytes_from_retained_vec(bytes, work) {
            Ok(bytes) => bytes,
            Err(error) => {
                let (path, retained) = path;
                drop(path);
                return release_owned_after_drop(Err(error), retained, work);
            }
        };
        if self.indexes.is_empty() {
            self.operation_index_capacity = capacity;
        }
        self.push_index_with_work(path, tail, bytes, capacity, work)
    }

    fn push_index_with_work(
        &mut self,
        path: (StreamPath, u64),
        tail: u64,
        bytes: OwnedBytes,
        capacity: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let (path, path_bytes) = path;
        let admitted = reserve_indexes(&mut self.indexes, 1, work);
        if let Err(error) = admitted {
            drop(path);
            drop(bytes);
            return release_owned_after_drop(Err(error), capacity + path_bytes, work);
        }
        self.retained_payload_bytes += capacity + path_bytes;
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
        work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(
        acyclic_stream::CommitRequest,
        crate::wire_codec::CodecQuota<'a, 'b>,
    )> {
        let key_owned = if work
            .as_deref()
            .is_some_and(|observer| !original_native_decode_mode(observer))
        {
            32
        } else {
            0
        };
        let mut request = crate::distributed::commit_request(key, key_owned, work);
        let target = self
            .target_condition_with_work(client, request.1.work())
            .await?;
        crate::distributed::add_condition(&mut request, target)?;
        crate::distributed::add_record(&mut request, self.path.clone(), self.bytes.clone())?;
        self.add_index(&mut request)?;
        Ok(request)
    }

    async fn target_condition_with_work<P: StreamProvider>(
        &self,
        client: &std::sync::Arc<P>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<CommitCondition> {
        if let Some(observer) = work.as_deref() {
            observer.check_cancellation()?;
        }
        let bounds = bounds_with_work(client, &self.path, work).await?;
        match bounds {
            Ok(_) => Ok(CommitCondition::Tail {
                path: self.path.clone(),
                expected: self.expected_tail,
            }),
            Err(error)
                if error == acyclic_stream::StreamError::NotFound
                    && self.expected_tail == 0 =>
            {
                drop(error);
                Ok(CommitCondition::Absent {
                    path: self.path.clone(),
                })
            }
            Err(error) => Err(crate::store::operations::stream_error_with_work(error, work)),
        }
    }

    pub(crate) fn verify(
        &self,
        envelope: &CommittedEnvelope,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref() {
            observer.check_cancellation()?;
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
            if let Some(observer) = work.as_deref() {
                observer.check_cancellation()?;
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
