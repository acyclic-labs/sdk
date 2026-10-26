use super::*;

/// Evidence retained from the authenticated authority record. A locator is
/// authoritative only when Streams published both records in the same commit.
pub(super) struct OperationRecordProof {
    sequence: u64,
    digest: Digest,
    commit_id: acyclic_stream::CommitId,
    committed_at_micros: u64,
}

impl OperationRecordProof {
    pub(super) fn new(
        durable: crate::streams_record::BorrowedStreamsDurableRecord<'_>,
        record: &acyclic_stream::Record,
        work: &AuthorityWork<'_>,
    ) -> Result<Self, AuthorityStoreError> {
        if durable.sequence.get() != record.sequence || record.sequence == 0 {
            return Err(authority_corrupt(
                "operation record has an invalid sequence",
                work,
            ));
        }
        Ok(Self {
            sequence: record.sequence,
            digest: durable.digest,
            commit_id: record.commit_id,
            committed_at_micros: record.committed_at_micros,
        })
    }

    fn verify(
        &self,
        locator: &acyclic_stream::Record,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let location = work.decode_fixed::<40>(
            &locator.value,
            OPERATION_LOCATOR_DOMAIN,
            "invalid operation locator",
        )?;
        if location[..8] != self.sequence.to_le_bytes()
            || &location[8..] != self.digest.as_bytes()
            || locator.sequence != 0
            || locator.commit_id != self.commit_id
            || locator.committed_at_micros != self.committed_at_micros
        {
            return Err(authority_corrupt(
                "operation locator does not bind its authority record",
                work,
            ));
        }
        Ok(())
    }
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) async fn verify_recovered_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        proof: &OperationRecordProof,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        let path = work.operation_path(&self.placement, authority, operation)?;
        if !work.is_native() {
            work.admit(WorkCounters {
                authority_records_read: 2,
                authority_bytes_read: STREAM_RECORD_LIMIT.saturating_mul(2),
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
            work.charge(WorkCounters {
                backend_read_operations: 1,
                ..WorkCounters::UNCHARGED
            })?;
        }
        let stream = self
            .authority_read(
                acyclic_stream::ReadRequest {
                    path,
                    from: 0,
                    limit: 2,
                },
                work,
            )
            .await;
        let mut stream = match stream {
            Err(error) if authority_missing(&error) && proof.sequence == 1 => return Ok(()),
            Err(error) if authority_missing(&error) => {
                return Err(authority_corrupt(
                    "recovered operation locator is missing",
                    work,
                ));
            }
            result => result?,
        };
        let locator = stream.next(work).await?;
        let Some(locator) = locator else {
            return if proof.sequence == 1 {
                Ok(())
            } else {
                Err(authority_corrupt(
                    "recovered operation locator is missing",
                    work,
                ))
            };
        };
        proof.verify(&locator.0, work)?;
        if stream.next(work).await?.is_some() {
            return Err(authority_corrupt(
                "operation locator has an invalid record count",
                work,
            ));
        }
        work.check()
    }

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
        let path = work.operation_path(&self.placement, authority, record.operation_id)?;
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

    // The first semantic record has a fixed address even when later records
    // require an operation locator. Read its authenticated identity directly.
    async fn initial_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<(DurableCommit, acyclic_stream::CommitId)>, AuthorityStoreError> {
        let path = work.path(&self.placement, authority, "/records", None)?;
        let tail = match self.tail(path.clone(), work).await {
            Err(error) if authority_missing(&error) => return Ok(None),
            result => result?,
        };
        if tail <= 1 {
            return Ok(None);
        }
        let record = self.record(path, 1, work).await?;
        let durable = authority_decode_borrowed(authority, &record.0.value, work)?;
        if durable.sequence != Sequence::new(1) || record.0.sequence != 1 {
            return Err(authority_corrupt(
                "initial operation has an invalid sequence",
                work,
            ));
        }
        if durable.operation_id != operation {
            return Ok(None);
        }
        let payload = work.bytes(durable.payload)?;
        let commit = durable_commit(durable, record.0.committed_at_micros, payload);
        work.check()?;
        Ok(Some((commit, record.0.commit_id)))
    }

    pub(super) async fn located_operation(
        &self,
        authority: AuthorityId,
        operation: OperationId,
        work: &mut AuthorityWork<'_>,
    ) -> Result<Option<(DurableCommit, acyclic_stream::CommitId)>, AuthorityStoreError> {
        work.check()?;
        self.authority_require_live(authority, work).await?;
        let path = work.operation_path(&self.placement, authority, operation)?;
        match self.tail(path.clone(), work).await {
            Err(error) if authority_missing(&error) => {
                return self.initial_operation(authority, operation, work).await;
            }
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
        let path = work.path(&self.placement, authority, "/records", None)?;
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
        if durable.operation_id != operation || durable.sequence.get() != sequence {
            return Err(authority_corrupt(
                "operation locator does not bind its authority record",
                work,
            ));
        }
        OperationRecordProof::new(durable, &record.0, work)?.verify(&locator.0, work)?;
        let payload = work.bytes(durable.payload)?;
        let commit = durable_commit(durable, record.0.committed_at_micros, payload);
        work.check()?;
        Ok(Some((commit, record.0.commit_id)))
    }
}
