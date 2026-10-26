//! Conditional writes: the condition is read under the transaction's own
//! observation fence, so a concurrent change to the binding makes the commit
//! conflict instead of silently replacing it.
use super::*;

/// What a conditional write requires of the file currently at its path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileCondition {
    /// Nothing is bound at the path.
    Absent,
    /// The path binds exactly this file identity.
    FileId(FileId),
    /// The path binds a regular file whose complete content has this SHA-256.
    Digest([u8; 32]),
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Transaction<A, O> {
    /// Creates or replaces one complete file only while `condition` holds.
    /// A failed condition returns [`WorkspaceError::StaleIdentity`] and stages
    /// nothing.
    pub async fn write_if<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        bytes: impl Into<acyclic_stream::OwnedBytes>,
        condition: FileCondition,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            self.require(path, condition, &mut accounting, cancellation)
                .await?;
            self.write_with_accounting(path, bytes, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Removes one binding only while `condition` holds. `Absent` never holds
    /// for a binding that can be removed.
    pub async fn remove_when<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        condition: FileCondition,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let file_id = self
                .require(path, condition, &mut accounting, cancellation)
                .await?
                .ok_or(WorkspaceError::NotFound)?;
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::push_mutation(
                &mut operations,
                crate::kernel::Owned::new(
                    AuthoredMutation::Remove {
                        path: path.value,
                        expected_file_id: Some(file_id),
                    },
                    path.allocation,
                    path.bytes,
                ),
                &mut accounting,
            )?;
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Checks `condition` against the fenced binding at `path`, returning the
    /// bound file identity.
    async fn require(
        &mut self,
        path: &str,
        condition: FileCondition,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<FileId>, WorkspaceError> {
        let mut path = self.mutation_path(path, accounting, cancellation)?;
        let looked_up = self
            .checkout
            .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true)
            .await;
        accounting.release_owned(&mut path.allocation, path.bytes)?;
        let record = looked_up?.0.record;
        let holds = match (condition, &record) {
            (FileCondition::Absent, existing) => existing.is_none(),
            (FileCondition::FileId(expected), Some(record)) => record.file_id == expected,
            (FileCondition::Digest(expected), Some(record)) => {
                self.content_digest(*record, accounting, cancellation)
                    .await?
                    == expected
            }
            (FileCondition::FileId(_) | FileCondition::Digest(_), None) => false,
        };
        if !holds {
            return Err(WorkspaceError::StaleIdentity);
        }
        Ok(record.map(|record| record.file_id))
    }

    async fn content_digest(
        &mut self,
        record: crate::kernel::FileRecord,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<[u8; 32], WorkspaceError> {
        let length = match &record.payload {
            FilePayload::InlineRegular(data) => {
                crate::foundation::usize_to_u64(data.as_bytes().len())
            }
            FilePayload::Regular { logical_bytes, .. } => *logical_bytes,
            _ => return Err(WorkspaceError::NotRegularFile),
        };
        let step = self
            .checkout
            .volume_config()
            .limits
            .maximum_read_bytes
            .max(1);
        let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
        let mut offset = 0;
        while offset < length {
            let range = crate::ByteRange {
                offset,
                length: step.min(length - offset),
            };
            let read = self
                .checkout
                .read_record_range_with_accounting(record, range, range, accounting, cancellation)
                .await?;
            accounting.charge(crate::WorkCounters {
                bytes_hashed: range.length,
                ..crate::WorkCounters::UNCHARGED
            })?;
            sha2::Digest::update(&mut hasher, read.bytes.as_ref());
            offset += range.length;
        }
        Ok(sha2::Digest::finalize(hasher).into())
    }
}
