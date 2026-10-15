use super::*;

/// One canonical event and its atomic derived indexes.
pub(crate) struct IndexedPublication {
    #[cfg(feature = "filesystem")]
    pub(crate) operation_id: OperationId,
    pub(crate) path: StreamPath,
    pub(crate) expected_tail: u64,
    pub(crate) bytes: OwnedBytes,
    pub(in crate::store) indexes: Vec<(StreamPath, u64, OwnedBytes)>,
    retained_payload_bytes: u64,
}

impl IndexedPublication {
    pub(crate) fn new(
        authority: &Authority,
        event: &Event,
        bytes: Vec<u8>,
        effect_position: Option<u64>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Self> {
        let mut quota = CodecQuota::from_retained(work, 0);
        let (bytes, retained) = owned_bytes_from_retained_vec(bytes, quota.work())?;
        quota.adopt(retained)?;
        if let Some(io) = quota.work().as_deref() {
            io.check_cancellation()?;
        }
        let expected_tail = event
            .revision
            .checked_sub(1)
            .ok_or_else(|| Error::Invalid("indexed event revision must be positive".into()))?;
        let (path, path_owned) = publication_path(authority, "harness/v2/", None, quota.work())?;
        quota.adopt(path_owned)?;
        let mut publication = Self {
            #[cfg(feature = "filesystem")]
            operation_id: event.operation_id,
            path,
            expected_tail,
            bytes,
            indexes: Vec::new(),
            retained_payload_bytes: quota.retained(),
        };
        quota.transfer();
        drop(quota);
        let result = (|| {
            let location = OperationLocation {
                authority,
                operation_id: event.operation_id,
                revision: event.revision,
                intent_digest: event.intent_digest,
                effect_position,
            };
            let operation = publication_path(
                authority,
                "harness/v2/aggregate-operations/",
                Some(&event.operation_id),
                work,
            )?;
            let encoded = crate::store::history::canonical_bytes_with_work(
                &location,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
            .and_then(|bytes| owned_bytes_from_retained_vec(bytes, work));
            publication.add_index(operation, 0, encoded, work)?;
            publication.add_message_indexes(authority, event, location, work)?;
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

    pub(in crate::store) fn add_index(
        &mut self,
        path: (StreamPath, u64),
        tail: u64,
        bytes: Result<(OwnedBytes, u64)>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let mut quota = CodecQuota::from_retained(work, path.1);
        let (bytes, retained) = bytes?;
        quota.adopt(retained)?;
        if let Some(io) = quota.work().as_deref_mut() {
            let capacity = self.indexes.len() + 1;
            crate::store::history::reserve(&mut self.indexes, capacity, io)?;
        }
        self.retained_payload_bytes += quota.retained();
        self.indexes.push((path.0, tail, bytes));
        quota.transfer();
        Ok(())
    }

    pub(crate) async fn request<'a, 'b, P: StreamProvider>(
        &self,
        client: &Arc<P>,
        key: IdempotencyKey,
        work: &'a mut Option<&'b mut dyn HistoryIo>,
    ) -> Result<(CommitRequest, CodecQuota<'a, 'b>)> {
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
        let absent = match bounds_with_work(client, &self.path, request.1.work()).await? {
            Ok(_) => false,
            Err(StreamError::NotFound) if self.expected_tail == 0 => true,
            Err(error) => return Err(stream_error_with_work(error, request.1.work())),
        };
        let appends = std::iter::once((&self.path, self.expected_tail, &self.bytes, absent)).chain(
            self.indexes
                .iter()
                .map(|(path, tail, bytes)| (path, *tail, bytes, *tail == 0)),
        );
        for (path, tail, bytes, absent) in appends {
            crate::distributed::add_condition(
                &mut request,
                if absent {
                    CommitCondition::Absent { path: path.clone() }
                } else {
                    CommitCondition::Tail {
                        path: path.clone(),
                        expected: tail,
                    }
                },
            )?;
            crate::distributed::add_record(&mut request, path.clone(), bytes.clone())?;
        }
        Ok(request)
    }
    pub(crate) fn verify(
        &self,
        envelope: &CommittedEnvelope,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let appends = std::iter::once((&self.path, self.expected_tail, &self.bytes)).chain(
            self.indexes
                .iter()
                .map(|(path, tail, bytes)| (path, *tail, bytes)),
        );
        let mut valid = envelope.mutations.len() == 1 + self.indexes.len();
        for (path, tail, bytes) in appends {
            if let Some(io) = work.as_deref() {
                io.check_cancellation()?;
            }
            valid &= envelope
                .mutations
                .iter()
                .find_map(|mutation| match mutation {
                    CommittedMutation::Append(append) if &append.path == path => Some(append),
                    _ => None,
                })
                .is_some_and(|append| {
                    tail.checked_add(1).is_some_and(|end| {
                        (append.start, append.end, append.tail, append.records.len())
                            == (tail, end, end, 1)
                            && append.records.first().is_some_and(|record| {
                                record.sequence == tail
                                    && record.value == *bytes
                                    && record.commit_id == envelope.commit_id
                            })
                    })
                });
        }
        if !valid {
            return Err(boundary(
                work,
                Storage,
                "indexed publication append is invalid",
            ));
        }
        Ok(())
    }

    fn add_message_indexes(
        &mut self,
        authority: &Authority,
        event: &Event,
        location: OperationLocation<&Authority>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        let message = match &event.payload {
            EventPayload::ConversationBound { .. } => Some((uuid::Uuid::nil(), 0)),
            EventPayload::ConversationMessageAppended { message } => {
                Some((message.id, message.sequence))
            }
            EventPayload::ProjectMergePublished { receipt } => {
                Some((receipt.notice.id, receipt.notice.sequence))
            }
            _ => None,
        };
        if let Some((message_id, sequence)) = message {
            if sequence == 0 && !matches!(event.payload, EventPayload::ConversationBound { .. }) {
                return Err(boundary(
                    work,
                    Invalid,
                    "message sequence index requires a positive conversation sequence",
                ));
            }
            let location = MessageLocation {
                location,
                message_id,
                sequence,
            };
            let head = publication_path(authority, "harness/v2/conversation-heads/", None, work)?;
            let encoded = crate::store::history::canonical_bytes_with_work(
                &location,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
            .and_then(|bytes| owned_bytes_from_retained_vec(bytes, work));
            self.add_index(head, sequence, encoded, work)?;
            if sequence > 0 {
                let bytes = self
                    .indexes
                    .last()
                    .ok_or_else(|| boundary(work, Storage, "message head index is missing"))?
                    .2
                    .clone();
                for (prefix, key) in [
                    (
                        "harness/v2/conversation-messages/",
                        &message_id as &dyn std::fmt::Display,
                    ),
                    (
                        "harness/v2/conversation-sequences/",
                        &sequence as &dyn std::fmt::Display,
                    ),
                ] {
                    let path = publication_path(authority, prefix, Some(key), work)?;
                    self.add_index(path, 0, Ok((bytes.clone(), 0)), work)?;
                }
            }
        }
        Ok(())
    }
}
