use super::*;

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// The locator and its authority record are one conditional Stream commit.
    /// Its absent condition admits exactly one successful semantic operation,
    /// independently of any rejected compare-and-append attempt keys.
    pub(super) fn locate_operation_in(
        &self,
        authority: AuthorityId,
        record: &crate::streams_record::BorrowedStreamsDurableRecord<'_>,
        conditions: &mut Vec<CommitCondition>,
        mutations: &mut Vec<CommitMutation>,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let path = work.operation_path(authority, record.operation_id)?;
        let value = work.stream_bytes(
            OPERATION_LOCATOR_DOMAIN,
            &[
                &record.sequence.get().to_le_bytes(),
                record.digest.as_bytes(),
            ],
        )?;
        conditions.push(CommitCondition::Absent { path: path.clone() });
        work.append_record(mutations, path, [value])
    }

    pub(super) async fn located_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<(DurableCommit, acyclic_stream::CommitId)>, AuthorityStoreError> {
        work.check()?;
        self.authority_require_live(authority, work).await?;
        let path = work.operation_path(authority, operation)?;
        match self.tail(path.clone(), work).await {
            Err(error) if authority_missing(&error) => return Ok(None),
            Err(error) => return Err(error),
            Ok(1) => {}
            Ok(_) => {
                return Err(authority_corrupt(
                    "operation locator has an invalid record count",
                    work,
                ));
            }
        }
        let locator = self.record(path, 0, work).await?;
        let location = work.decode_fixed::<40>(
            &locator.0.value,
            OPERATION_LOCATOR_DOMAIN,
            "invalid operation locator",
        )?;
        let sequence = u64::from_le_bytes(
            location[..8]
                .try_into()
                .map_err(|_| crate::WorkError::Overflow)?,
        );
        if sequence == 0 {
            return Err(authority_corrupt("operation locator selects genesis", work));
        }
        let path = work.path(authority, "/records", None)?;
        let tail = match self.tail(path.clone(), work).await {
            Err(error) if authority_missing(&error) => 0,
            result => result?,
        };
        if sequence >= tail {
            return Err(authority_corrupt(
                "operation locator selects a missing record",
                work,
            ));
        }
        let record = match self.record(path, sequence, work).await {
            Err(error) if authority_missing(&error) => {
                return Err(authority_corrupt(
                    "operation locator selects a missing record",
                    work,
                ));
            }
            result => result?,
        };
        let durable = authority_decode_borrowed(authority, &record.0.value, work)?;
        if durable.operation_id != operation
            || durable.sequence.get() != sequence
            || record.0.sequence != sequence
            || locator.0.sequence != 0
            || durable.digest.as_bytes() != &location[8..]
            || record.0.commit_id != locator.0.commit_id
            || record.0.committed_at_micros != locator.0.committed_at_micros
        {
            return Err(authority_corrupt(
                "operation locator does not bind its authority record",
                work,
            ));
        }
        let payload = work.bytes(durable.payload)?;
        let commit = durable_commit(durable, record.0.committed_at_micros, payload);
        work.check()?;
        Ok(Some((commit, record.0.commit_id)))
    }
}
