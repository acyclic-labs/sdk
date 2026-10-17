use super::*;

pub(super) async fn write<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut Transaction<A, O>,
    path: &str,
    bytes: acyclic_stream::OwnedBytes,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<(), WorkspaceError> {
    let path = transaction.mutation_path(path, accounting, cancellation)?;
    let existing = transaction
        .checkout
        .lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true)
        .await?
        .0
        .record;
    let mut operations =
        crate::kernel::Owned::allocate(if existing.is_some() { 2 } else { 1 }, accounting)?;
    if let Some(record) = existing {
        if record.kind != FileKind::Regular {
            return Err(WorkspaceError::NotRegularFile);
        }
        Transaction::<A, O>::reset_file(
            &path.value,
            Some(record),
            &mut operations,
            accounting,
            cancellation,
        )?;
    }
    let operation = if existing.is_some() {
        AuthoredMutation::Write {
            path: path.value,
            offset: 0,
            bytes: bytes.into(),
        }
    } else {
        AuthoredMutation::CreateFile {
            path: path.value,
            bytes: bytes.into(),
            metadata: FileMetadata::default(),
        }
    };
    Transaction::<A, O>::push_mutation(
        &mut operations,
        crate::kernel::Owned::new(operation, path.allocation, path.bytes),
        accounting,
    )?;
    transaction
        .apply_authored(operations, accounting, cancellation)
        .await
}
