use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    /// Replaces one destination with a complete copy-on-write clone.
    pub async fn copy<W: crate::facade::FsWork>(
        &mut self,
        source: &str,
        destination: &str,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let mut source = self.mutation_path(source, &mut accounting, cancellation)?;
            let destination = self.mutation_path(destination, &mut accounting, cancellation)?;
            let record = self
                .checkout
                .lookup_no_follow_with_accounting(
                    &source.value,
                    &mut accounting,
                    cancellation,
                    true,
                )
                .await?
                .0
                .record
                .ok_or(WorkspaceError::NotFound)?;
            let length = regular_file_bytes(record)?;
            let existing = self
                .checkout
                .lookup_no_follow_with_accounting(
                    &destination.value,
                    &mut accounting,
                    cancellation,
                    true,
                )
                .await?
                .0
                .record;
            if existing.is_some_and(|record| record.kind != FileKind::Regular) {
                return Err(WorkspaceError::NotRegularFile);
            }
            let mut operations =
                crate::kernel::Owned::allocate(if length == 0 { 1 } else { 2 }, &mut accounting)?;
            Self::reset_file(
                &destination.value,
                existing,
                &mut operations,
                &mut accounting,
                cancellation,
            )?;
            if length == 0 {
                source.retire(&mut accounting)?;
                destination.retire(&mut accounting)?;
            } else {
                let mut allocation = source.allocation;
                let mut destination = destination;
                allocation.absorb(&mut destination.allocation)?;
                let bytes = source
                    .bytes
                    .checked_add(destination.bytes)
                    .ok_or(crate::WorkError::Overflow)?;
                Self::push_mutation(
                    &mut operations,
                    crate::kernel::Owned::new(
                        AuthoredMutation::CloneRange(crate::FileCloneRequest {
                            source: source.value,
                            source_offset: 0,
                            destination: destination.value,
                            destination_offset: 0,
                            length,
                        }),
                        allocation,
                        bytes,
                    ),
                    &mut accounting,
                )?;
            }
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Replaces one file by concatenating already staged immutable content.
    pub async fn write_staged<W: crate::facade::FsWork>(
        &mut self,
        path: &str,
        parts: &[crate::StagedContent],
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            if parts.is_empty() {
                return Err(WorkspaceError::EmptyContentSet);
            }
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let existing = self
                .checkout
                .lookup_no_follow_with_accounting(&path.value, &mut accounting, cancellation, true)
                .await?
                .0
                .record;
            if existing.is_some_and(|record| record.kind != FileKind::Regular) {
                return Err(WorkspaceError::NotRegularFile);
            }
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::reset_file(
                &path.value,
                existing,
                &mut operations,
                &mut accounting,
                cancellation,
            )?;
            let maximum = parts
                .len()
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?;
            let mut offset = 0_u64;
            for part in parts {
                accounting.charge(item_work(1))?;
                if part.logical_bytes() != 0 {
                    operations.ensure_for_push(maximum, &mut accounting)?;
                    let (copy, allocation) = path
                        .value
                        .clone_with_accounting(&mut accounting, cancellation)?;
                    let bytes = copy.retained_bytes();
                    Self::push_mutation(
                        &mut operations,
                        crate::kernel::Owned::new(
                            AuthoredMutation::WriteFromContent {
                                path: copy,
                                offset,
                                content: *part,
                            },
                            allocation,
                            bytes,
                        ),
                        &mut accounting,
                    )?;
                }
                offset = offset
                    .checked_add(part.logical_bytes())
                    .ok_or(WorkspaceError::ContentLengthOverflow)?;
            }
            path.retire(&mut accounting)?;
            self.apply_authored(operations, &mut accounting, cancellation)
                .await
        }
        .await;
        work.finish(result, &accounting)
    }
    pub(super) fn reset_file(
        path: &NamespacePath,
        existing: Option<crate::kernel::FileRecord>,
        operations: &mut crate::kernel::Owned<Vec<AuthoredMutation>>,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<(), WorkspaceError> {
        let (path, allocation) = path.clone_with_accounting(accounting, cancellation)?;
        let bytes = path.retained_bytes();
        let operation = if existing.is_some() {
            AuthoredMutation::Resize {
                path,
                logical_bytes: 0,
            }
        } else {
            AuthoredMutation::CreateFile {
                path,
                bytes: acyclic_stream::OwnedBytes::new(),
                metadata: FileMetadata::default(),
            }
        };
        Self::push_mutation(
            operations,
            crate::kernel::Owned::new(operation, allocation, bytes),
            accounting,
        )
    }
}
