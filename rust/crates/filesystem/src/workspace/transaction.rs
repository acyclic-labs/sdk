use super::*;

/// One unpublished sparse atomic workspace transaction.
pub struct Transaction<A, O> {
    pub(super) workspace: Workspace<A, O>,
    pub(crate) checkout: Checkout<A, O>,
    pub(super) idempotency_key: IdempotencyKey,
    pub(super) requires_rebase: bool,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Transaction<A, O> {
    pub(super) fn from_checkout(workspace: &Workspace<A, O>, mut checkout: Checkout<A, O>, idempotency_key: IdempotencyKey, requires_rebase: bool) -> Self {
        checkout.bind_authored_operation(idempotency_key.operation_id());
        Self { workspace: workspace.clone(), checkout, idempotency_key, requires_rebase }
    }

    pub(crate) fn for_checkout_candidate<W: crate::facade::FsWork>(
        workspace: &Workspace<A, O>, checkout: Checkout<A, O>, work: W, cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let accounting = work.accounting(cancellation);
        let result = (|| -> Result<Self, WorkspaceError> {
            accounting.check()?;
            let idempotency_key = match checkout.bound_authored_operation_id() {
                #[cfg(not(target_arch = "wasm32"))]
                Some(id) if accounting.native_work().is_some() => IdempotencyKey::from_bytes(id.into_bytes()),
                #[cfg(not(target_arch = "wasm32"))]
                None if accounting.native_work().is_some() => return Err(FsError::MutationNotAllowed.into()),
                _ => IdempotencyKey::new(),
            };
            Ok(Self { workspace: workspace.clone(), checkout, idempotency_key, requires_rebase: false })
        })();
        work.finish(result, &accounting)
    }

    pub(super) fn mutation_path(&self, path: &str, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken) -> Result<crate::kernel::Owned<NamespacePath>, WorkspaceError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() { self.checkout.validate_native_work(original)?; }
        let (path, allocation) = customer_path_with_accounting(path, self.checkout.volume_config(), accounting, cancellation)?;
        let bytes = path.retained_bytes();
        Ok(crate::kernel::Owned::new(path, allocation, bytes))
    }

    pub(super) fn push_mutation<T>(operations: &mut crate::kernel::Owned<Vec<T>>, mut operation: crate::kernel::Owned<T>, accounting: &mut KernelAccounting<'_>) -> Result<(), WorkspaceError> {
        let copied = crate::WorkCounters { bytes_copied: std::mem::size_of::<T>() as u64, ..crate::WorkCounters::UNCHARGED };
        accounting.admit(copied)?;
        operations.allocation.absorb(&mut operation.allocation)?;
        operations.bytes = operations.bytes.checked_add(operation.bytes).ok_or(crate::WorkError::Overflow)?;
        operations.value.push(operation.value);
        accounting.record_completed(copied)?;
        Ok(())
    }

    pub(super) async fn apply_authored(&mut self, operations: crate::kernel::Owned<Vec<AuthoredMutation>>, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken) -> Result<(), WorkspaceError> {
        self.checkout.apply_authored_transaction_with_accounting(operations, accounting, cancellation).await?
            .retire(accounting).map_err(WorkspaceError::from)
    }

    pub(super) async fn mutate<W: crate::facade::FsWork>(
        &mut self, path: &str, build: impl FnOnce(NamespacePath) -> Result<AuthoredMutation, WorkspaceError>, work: W, cancellation: &CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let operation = match build(path.value) {
                Ok(value) => crate::kernel::Owned::new(value, path.allocation, path.bytes),
                Err(error) => {
                    let mut allocation = path.allocation;
                    accounting.release_owned(&mut allocation, path.bytes)?;
                    return Err(error);
                }
            };
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::push_mutation(&mut operations, operation, &mut accounting)?;
            self.apply_authored(operations, &mut accounting, cancellation).await
        }.await;
        work.finish(result, &accounting)
    }

    /// Creates one directory with canonical metadata.
    pub async fn create_directory<W: crate::facade::FsWork>(&mut self, path: &str, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        self.mutate(path, |path| Ok(AuthoredMutation::CreateDirectory { path, metadata: FileMetadata::default() }), work, cancellation).await
    }

    /// Creates one new file with canonical metadata.
    pub async fn create_file<W: crate::facade::FsWork>(&mut self, path: &str, bytes: impl Into<acyclic_stream::OwnedBytes>, metadata: FileMetadata, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        self.mutate(path, |path| Ok(AuthoredMutation::CreateFile { path, bytes: bytes.into(), metadata }), work, cancellation).await
    }

    /// Replaces one exact file range.
    pub async fn write_range<W: crate::facade::FsWork>(&mut self, path: &str, offset: u64, bytes: impl Into<acyclic_stream::OwnedBytes>, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        self.mutate(path, |path| Ok(AuthoredMutation::Write { path, offset, bytes: bytes.into() }), work, cancellation).await
    }

    /// Creates a symbolic link with an opaque target.
    pub async fn create_symbolic_link<W: crate::facade::FsWork>(&mut self, path: &str, target: impl Into<acyclic_stream::OwnedBytes>, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        self.mutate(path, |path| Ok(AuthoredMutation::CreateSymbolicLink { path, target: target.into(), metadata: FileMetadata::default() }), work, cancellation).await
    }

    /// Creates one FIFO, socket, or device node.
    pub async fn create_special<W: crate::facade::FsWork>(&mut self, path: &str, kind: FileKind, device: Option<(u32, u32)>, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        self.mutate(path, |path| match (kind, device) {
            (FileKind::Fifo | FileKind::Socket, None) => Ok(AuthoredMutation::CreateEmptySpecial { path, kind, metadata: FileMetadata::default() }),
            (FileKind::CharacterDevice | FileKind::BlockDevice, Some((major, minor))) => Ok(AuthoredMutation::CreateDevice { path, kind, major, minor, metadata: FileMetadata::default() }),
            _ => Err(WorkspaceError::IncompatibleWorkspace),
        }, work, cancellation).await
    }

    /// Replaces canonical metadata.
    pub async fn set_metadata<W: crate::facade::FsWork>(&mut self, path: &str, metadata: FileMetadata, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        self.mutate(path, |path| Ok(AuthoredMutation::SetMetadata { path, metadata }), work, cancellation).await
    }

    pub(crate) async fn preserve_file_identity<W: crate::facade::FsWork>(&mut self, path: &str, file_id: FileId, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        self.mutate(path, |path| Ok(AuthoredMutation::Reidentify { path, file_id }), work, cancellation).await
    }

    pub(crate) async fn restore_record<W: crate::facade::FsWork>(&mut self, path: &str, record: crate::kernel::FileRecord, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let path = self.mutation_path(path, &mut accounting, cancellation)?;
            let mut operations = crate::kernel::Owned::allocate(1, &mut accounting)?;
            Self::push_mutation(&mut operations, crate::kernel::Owned::new(crate::kernel::Mutation::Restore { path: path.value, record }, path.allocation, path.bytes), &mut accounting)?;
            self.checkout.mutate_owned_with_accounting(operations, &mut accounting, cancellation).await.map_err(WorkspaceError::from)
        }.await;
        work.finish(result, &accounting)
    }

    /// Streams immutable content under supplied work limits.
    pub async fn stage_content<R: crate::kernel::AsyncBlobSource, W: crate::facade::FsWork>(&self, source: &mut R, maximum_source_bytes: u64, work: W, cancellation: &CancellationToken) -> W::Output<crate::StagedContent, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.checkout.stage_content_with_accounting(source, maximum_source_bytes, &mut accounting, cancellation).await.map_err(WorkspaceError::from);
        work.finish(result, &accounting)
    }

    /// Creates missing directory prefixes without replacing existing objects.
    pub async fn create_dir_all<W: crate::facade::FsWork>(&mut self, path: &str, work: W, cancellation: &CancellationToken) -> W::Output<(), WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = self.create_dirs(path, &mut accounting, cancellation).await;
        work.finish(result, &accounting)
    }

    pub(super) async fn create_dirs(&mut self, path: &str, accounting: &mut KernelAccounting<'_>, cancellation: &CancellationToken) -> Result<(), WorkspaceError> {
        accounting.check()?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() { self.checkout.validate_native_work(original)?; }
        if PortablePath::validate(path, self.checkout.volume_config().limits)
            .map_err(|error| WorkspaceError::from(crate::kernel::NamespacePathError::Path(error)))? == 0 { return Ok(()); }
        for end in path.match_indices('/').skip(1).map(|(index, _)| index).chain(std::iter::once(path.len())) {
            let path = self.mutation_path(&path[..end], accounting, cancellation)?;
            match self.checkout.lookup_no_follow_with_accounting(&path.value, accounting, cancellation, true).await?.0.record {
                Some(record) if record.kind == FileKind::Directory => path.retire(accounting)?,
                Some(_) => { path.retire(accounting)?; return Err(WorkspaceError::NotDirectory); }
                None => {
                    let mut operations = crate::kernel::Owned::allocate(1, accounting)?;
                    Self::push_mutation(&mut operations, crate::kernel::Owned::new(AuthoredMutation::CreateDirectory { path: path.value, metadata: FileMetadata::default() }, path.allocation, path.bytes), accounting)?;
                    self.apply_authored(operations, accounting, cancellation).await?;
                }
            }
        }
        Ok(())
    }
}
