use super::*;

/// One sparse atomic workspace transaction.
#[napi]
pub struct NativeWorkspaceTransaction {
    pub(in crate::bindings) inner:
        tokio::sync::Mutex<Transaction<LocalAuthorityBackend, LocalObjectBackend>>,
}

#[napi]
impl NativeWorkspaceTransaction {
    /// Creates every absent directory on one canonical path.
    #[napi(js_name = createDirAll)]
    pub async fn create_dir_all(&self, path: String) -> Result<()> {
        sdk_value!(unbounded_future!(
            self.inner.lock().await,
            create_dir_all(&path)
        ))
    }

    /// Creates exactly one empty directory.
    #[napi(js_name = createDirectory)]
    pub async fn create_directory(&self, path: String) -> Result<()> {
        sdk_value!(unbounded_future!(
            self.inner.lock().await,
            create_directory(&path)
        ))
    }

    /// Creates one symbolic link with an opaque target.
    #[napi(js_name = createSymbolicLink)]
    pub async fn create_symbolic_link(&self, path: String, target: Buffer) -> Result<()> {
        sdk_value!(unbounded_future!(
            self.inner.lock().await,
            create_symbolic_link(&path, bytes::Bytes::from(target.to_vec()))
        ))
    }

    /// Creates or replaces one complete file inside this transaction.
    #[napi]
    pub async fn write(&self, path: String, bytes: Buffer) -> Result<()> {
        sdk_value!(unbounded_future!(
            self.inner.lock().await,
            write(&path, bytes::Bytes::from(bytes.to_vec()))
        ))
    }

    /// Removes one existing namespace binding inside this transaction.
    #[napi]
    pub async fn remove(&self, path: String) -> Result<()> {
        sdk_value!(unbounded_future!(self.inner.lock().await, remove(&path)))
    }

    /// Clones one complete regular file without copying its body.
    #[napi]
    pub async fn copy(&self, source: String, destination: String) -> Result<()> {
        self.inner
            .lock()
            .await
            .copy(&source, &destination)
            .await
            .map_err(napi_error)
    }

    /// Atomically renames one namespace binding inside this transaction.
    #[napi]
    pub async fn rename(&self, source: String, destination: String) -> Result<()> {
        sdk_value!(unbounded_future!(
            self.inner.lock().await,
            rename(&source, &destination, true)
        ))
    }

    /// Creates one same-workspace hard link.
    #[napi(js_name = hardLink)]
    pub async fn hard_link(&self, source: String, destination: String) -> Result<()> {
        sdk_value!(unbounded_future!(
            self.inner.lock().await,
            hard_link(&source, &destination)
        ))
    }

    /// Replaces one sparse regular-file range.
    #[napi(js_name = writeRange)]
    pub async fn write_range(&self, path: String, offset: BigInt, bytes: Buffer) -> Result<()> {
        sdk_value!(unbounded_future!(
            self.inner.lock().await,
            write_range(
                &path,
                bigint_u64(&offset, "offset")?,
                bytes::Bytes::from(bytes.to_vec())
            )
        ))
    }

    /// Changes one regular file's logical length.
    #[napi]
    pub async fn resize(&self, path: String, logical_bytes: BigInt) -> Result<()> {
        self.inner
            .lock()
            .await
            .resize(&path, bigint_u64(&logical_bytes, "logicalBytes")?)
            .await
            .map_err(napi_error)
    }

    /// Punches a hole or installs allocated zeros over one exact range.
    #[napi(js_name = zeroRange)]
    pub async fn zero_range(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        allocated: bool,
        extend: bool,
    ) -> Result<()> {
        self.inner
            .lock()
            .await
            .zero_range(
                &path,
                ByteRange {
                    offset: bigint_u64(&offset, "offset")?,
                    length: bigint_u64(&length, "length")?,
                },
                allocated,
                extend,
            )
            .await
            .map_err(napi_error)
    }

    /// Preallocates one sparse range without replacing content.
    #[napi]
    pub async fn preallocate(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        keep_size: bool,
    ) -> Result<()> {
        self.inner
            .lock()
            .await
            .preallocate(
                &path,
                ByteRange {
                    offset: bigint_u64(&offset, "offset")?,
                    length: bigint_u64(&length, "length")?,
                },
                keep_size,
            )
            .await
            .map_err(napi_error)
    }

    /// Clones one immutable range without reading file bytes.
    #[napi(js_name = cloneRange)]
    pub async fn clone_range(
        &self,
        source: String,
        source_offset: BigInt,
        destination: String,
        destination_offset: BigInt,
        length: BigInt,
    ) -> Result<()> {
        self.inner
            .lock()
            .await
            .clone_range(
                &source,
                bigint_u64(&source_offset, "sourceOffset")?,
                &destination,
                bigint_u64(&destination_offset, "destinationOffset")?,
                bigint_u64(&length, "length")?,
            )
            .await
            .map_err(napi_error)
    }

    /// Publishes the complete candidate through one idempotent head CAS.
    #[napi]
    pub async fn commit(
        &self,
        lease: Option<NativeOperationWindowLease>,
    ) -> Result<NativeWorkspaceCommit> {
        let permit = publication_permit(lease.as_ref())?;
        self.inner
            .lock()
            .await
            .commit(
                None,
                permit,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                publication_ready::<acyclic_fs::WorkspaceError>,
            )
            .await
            .map(|receipt| workspace_commit(receipt.value))
            .map_err(|failure| napi_error(failure.error))
    }

    /// Safely advances this retained candidate and sparsely replays its work.
    #[napi]
    pub async fn rebase(
        &self,
        maximum_conflicts: NapiU32,
    ) -> Result<NativeTransactionRebaseResult> {
        let maximum_conflicts = maximum_conflicts.into_inner();

        self.inner
            .lock()
            .await
            .rebase(maximum_conflicts)
            .await
            .map(native_transaction_rebase)
            .map_err(napi_error)
    }
}

pub(in crate::bindings) fn native_transaction_rebase(
    outcome: TransactionRebase<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeTransactionRebaseResult {
    match outcome {
        TransactionRebase::Rebased(generation) => NativeTransactionRebaseResult {
            status: "rebased".to_owned(),
            generation_id: Some(Buffer::from(generation.id().digest().into_bytes().to_vec())),
            conflicts: Vec::new(),
            truncated: false,
        },
        TransactionRebase::Conflicted {
            conflicts,
            truncated,
        } => NativeTransactionRebaseResult {
            status: "conflicted".to_owned(),
            generation_id: None,
            conflicts: conflicts
                .into_iter()
                .map(native_transaction_conflict)
                .collect(),
            truncated,
        },
    }
}

pub(in crate::bindings) fn native_transaction_conflict(
    value: RebaseConflict,
) -> NativeTransactionConflict {
    let mut result = NativeTransactionConflict {
        region: String::new(),
        file_id: None,
        directory_id: None,
        offset: None,
        length: None,
        sparse_target: None,
        name: None,
        maximum_entries: None,
        usage: match value.usage {
            DependencyUse::Observation => "observation",
            DependencyUse::Mutation => "mutation",
            DependencyUse::ObservationAndMutation => "observation-and-mutation",
        }
        .to_owned(),
        expected: match value.expected {
            DependencyState::Absent => None,
            DependencyState::Present(digest) => Some(Buffer::from(digest.into_bytes().to_vec())),
        },
        actual: match value.actual {
            DependencyState::Absent => None,
            DependencyState::Present(digest) => Some(Buffer::from(digest.into_bytes().to_vec())),
        },
    };
    match value.region {
        DependencyRegion::FileRecord(file_id) => {
            "file-record".clone_into(&mut result.region);
            result.file_id = Some(Buffer::from(file_id.into_bytes().to_vec()));
        }
        DependencyRegion::Metadata(file_id) => {
            "metadata".clone_into(&mut result.region);
            result.file_id = Some(Buffer::from(file_id.into_bytes().to_vec()));
        }
        DependencyRegion::FileLength(file_id) => {
            "file-length".clone_into(&mut result.region);
            result.file_id = Some(Buffer::from(file_id.into_bytes().to_vec()));
        }
        DependencyRegion::ContentRange {
            file_id,
            offset,
            length,
        } => {
            "content-range".clone_into(&mut result.region);
            result.file_id = Some(Buffer::from(file_id.into_bytes().to_vec()));
            result.offset = Some(bigint(offset));
            result.length = Some(bigint(length));
        }
        DependencyRegion::SparseSeek {
            file_id,
            offset,
            target,
        } => {
            "sparse-seek".clone_into(&mut result.region);
            result.file_id = Some(Buffer::from(file_id.into_bytes().to_vec()));
            result.offset = Some(bigint(offset));
            result.sparse_target = Some(
                match target {
                    ExtentSeekTarget::Data => "data",
                    ExtentSeekTarget::Hole => "hole",
                }
                .to_owned(),
            );
        }
        DependencyRegion::DirectoryName { directory_id, name } => {
            "directory-name".clone_into(&mut result.region);
            result.directory_id = Some(Buffer::from(directory_id.into_bytes().to_vec()));
            result.name = Some(native_workspace_name_value(name));
        }
        DependencyRegion::DirectoryRange {
            directory_id,
            after,
            maximum_entries,
        } => {
            "directory-range".clone_into(&mut result.region);
            result.directory_id = Some(Buffer::from(directory_id.into_bytes().to_vec()));
            result.name = after.map(native_workspace_name_value);
            result.maximum_entries = Some(maximum_entries);
        }
    }
    result
}

pub(in crate::bindings) fn workspace_commit(
    outcome: TransactionCommit<LocalAuthorityBackend, LocalObjectBackend>,
) -> NativeWorkspaceCommit {
    let (status, generation) = match outcome {
        TransactionCommit::Committed(value) => ("committed", Some(value)),
        TransactionCommit::AlreadyCommitted(value) => ("already-committed", Some(value)),
        TransactionCommit::Conflict { actual } => ("conflict", Some(actual)),
        TransactionCommit::Fenced => ("fenced", None),
        TransactionCommit::IdempotencyConflict => ("idempotency-conflict", None),
    };
    NativeWorkspaceCommit {
        status: status.to_owned(),
        generation_id: generation.map(|value| native_generation_buffer(value.id())),
    }
}
