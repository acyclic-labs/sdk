//! Original owner checks and one atomic journal append.
use super::*;

impl<P: StreamProvider> TaskJournalOwner<P> {
    pub(crate) async fn append(
        &self,
        path: acyclic_stream::StreamPath,
        expected_tail: u64,
        key: Option<&StreamKey>,
        bytes: OwnedBytes,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<bool> {
        use acyclic_stream::{CommitCondition, CommitOutcome, CommittedMutation};
        if let Some(io) = work.as_deref_mut() {
            io.hashed_bytes(bytes.len() as u64)?;
        }
        let payload = blake3::hash(&bytes);
        let path = &path;
        let bytes = &bytes;
        let payload = &payload;
        Box::pin(super::publication::retry::<true, _, _, _>(work, move |work| async move {
            let owner = self.condition(write, work).await?;
            let target =
                match crate::store::operations::bounds_with_work(&self.host.stream, &path, work).await?
                {
                    Ok(_) => CommitCondition::Tail {
                        path: path.clone(),
                        expected: expected_tail,
                    },
                    Err(error) if error == StreamError::NotFound && expected_tail == 0 => {
                        CommitCondition::Absent { path: path.clone() }
                    }
                    Err(error) => {
                        return Err(crate::store::operations::stream_error_with_work(
                            error, work,
                        ));
                    }
                };
            let CommitCondition::Tail { path: owner_path, expected } = &owner else {
                return Err(Error::Invalid("coordinator tail condition required".into()));
            };
            let owner_path = owner_path.clone();
            let owner_tail = *expected;
            // Native journals use the payload digest itself as the caller key.
            // Reuse this hash rather than hashing the same immutable buffer twice.
            let key = key.map_or(payload.as_bytes().as_slice(), StreamKey::as_bytes);
            let digest = crate::executor::json::canonical_json_digest(
                &(
                    "harness/v2/owned-journal",
                    key,
                    self.task_id(),
                    &self.fence,
                    expected,
                    path.as_str(),
                    expected_tail,
                    matches!(target, CommitCondition::Absent { .. }),
                    payload.as_bytes(),
                ),
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )?;
            let (key, held) = crate::distributed::digest_key(&digest, work)?;
            let request = crate::distributed::journal_request::<true>(
                key,
                held,
                owner,
                target,
                path.clone(),
                bytes.clone(),
                work,
            )?;
            let (outcome, retained) =
                crate::distributed::commit(request, &self.host.stream, Some(self.operation_id()), None)
                    .await?;
            let received = crate::wire_codec::CodecQuota::from_retained(work, retained);
            let result = match &outcome {
                CommitOutcome::Conflict(conflicts) if super::publication::advanced(conflicts, &owner_path, owner_tail) => Ok(None),
                CommitOutcome::Conflict(_) => Ok(Some(false)),
                CommitOutcome::Committed(envelope) => {
                    let valid = match envelope.mutations.as_slice() {
                        [CommittedMutation::Append(append)] => {
                            append.path == *path
                                && append.start == expected_tail
                                && expected_tail.checked_add(1) == Some(append.end)
                                && append.tail == append.end
                                && matches!(append.records.as_slice(), [record] if record.sequence == expected_tail && record.value == *bytes && record.commit_id == envelope.commit_id)
                        }
                        _ => false,
                    };
                    if valid {
                        Ok(Some(true))
                    } else {
                        Err(Error::Indeterminate(self.operation_id()))
                    }
                }
            };
            drop(outcome);
            drop(received);
            Ok((result?, work))
        }))
        .await
    }
}
