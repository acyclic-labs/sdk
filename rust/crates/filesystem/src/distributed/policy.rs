use super::*;

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
    /// Every disclosure publication checks its source in the same Streams
    /// transaction. No earlier check, writer-local registry or lease can replace it.
    pub(super) fn fence_policy(
        &self,
        mut request: CommitRequest,
        work: &mut AuthorityWork<'_>,
    ) -> Result<CommitRequest, AuthorityStoreError> {
        let Some(cut) = crate::PolicyCut::current() else {
            return Ok(request);
        };
        if cut.owner != self.operation_claims().owner_identity() {
            return Err(AuthorityStoreError::RejectedStatic(
                "policy cut and publication use different Streams owners",
            ));
        }
        let mut conditions = work.vec(request.conditions.len() + 3)?;
        conditions.append(&mut request.conditions);
        conditions.push(CommitCondition::Tail {
            path: work.path(cut.authority, "/records", None)?,
            expected: cut
                .head
                .sequence
                .get()
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?,
        });
        conditions.push(CommitCondition::Tail {
            path: work.path(cut.authority, "/epochs", None)?,
            expected: cut.epoch_tail,
        });
        conditions.push(CommitCondition::Absent {
            path: work.path(cut.authority, "/retirement", None)?,
        });
        request.conditions = conditions;
        // A rejected stale cut cannot consume a later freshly authorized attempt.
        request.idempotency_key = work.key(
            b"policy-cut-attempt\0",
            &[
                request.idempotency_key.as_bytes(),
                &cut.authority.into_bytes(),
                cut.head.digest.as_bytes(),
                &cut.head.sequence.get().to_le_bytes(),
                &cut.epoch_tail.to_le_bytes(),
            ],
        )?;
        Ok(request)
    }
}
