use super::*;

/// One sparse atomic browser workspace transaction.
#[wasm_bindgen]
pub struct BrowserTransaction {
    pub(in crate::bindings) engine:
        Transaction<BrowserAuthority, CachedObjectStore<BrowserObjects>>,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserWorkspaceCommit {
    pub(in crate::bindings) status: BrowserWorkspaceCommitStatus,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) generation_id: Option<serde_bytes::ByteBuf>,
}

/// Exact result of advancing a retained transaction to the current head.
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserTransactionRebase {
    pub(in crate::bindings) status: BrowserTransactionRebaseStatus,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) generation_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) conflicts: Vec<BrowserTransactionConflict>,
    pub(in crate::bindings) truncated: bool,
}

/// One observation or mutation dependency that prevents a safe rebase.
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserTransactionConflict {
    pub(in crate::bindings) region: BrowserTransactionConflictRegion,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) file_id: Option<serde_bytes::ByteBuf>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) directory_id: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) offset: Option<u64>,
    pub(in crate::bindings) length: Option<u64>,
    pub(in crate::bindings) sparse_target: Option<BrowserTransactionSparseSeek>,
    pub(in crate::bindings) name: Option<BrowserWorkspaceName>,
    pub(in crate::bindings) maximum_entries: Option<u32>,
    pub(in crate::bindings) usage: BrowserTransactionDependencyUse,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) expected: Option<serde_bytes::ByteBuf>,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) actual: Option<serde_bytes::ByteBuf>,
}

#[wasm_bindgen]
impl BrowserTransaction {
    /// Creates every absent directory on one canonical path.
    #[wasm_bindgen(js_name = createDirAll)]
    pub async fn create_dir_all(&mut self, path: String) -> Result<(), JsValue> {
        Box::pin(self.engine.create_dir_all(
            &path,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .map_err(js_error)
    }

    /// Creates exactly one empty directory.
    #[wasm_bindgen(js_name = createDirectory)]
    pub async fn create_directory(&mut self, path: String) -> Result<(), JsValue> {
        self.engine
            .create_directory(
                &path,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(js_error)
    }

    /// Creates one symbolic link with an opaque target.
    #[wasm_bindgen(js_name = createSymbolicLink)]
    pub async fn create_symbolic_link(
        &mut self,
        path: String,
        target: Vec<u8>,
    ) -> Result<(), JsValue> {
        let target = bytes::Bytes::from(target);
        self.engine
            .create_symbolic_link(
                &path,
                target,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(js_error)
    }

    /// Creates or replaces one complete file inside this transaction.
    #[wasm_bindgen]
    pub async fn write(&mut self, path: String, bytes: Vec<u8>) -> Result<(), JsValue> {
        let bytes = bytes::Bytes::from(bytes);
        Box::pin(self.engine.write(
            &path,
            bytes,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .map_err(js_error)
    }

    /// Removes one existing namespace binding inside this transaction.
    #[wasm_bindgen]
    pub async fn remove(&mut self, path: String) -> Result<(), JsValue> {
        self.engine
            .remove(
                &path,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(js_error)
    }

    /// Clones one complete regular file without copying its body.
    #[wasm_bindgen]
    pub async fn copy(&mut self, source: String, destination: String) -> Result<(), JsValue> {
        Box::pin(self.engine.copy(&source, &destination))
            .await
            .map_err(js_error)
    }

    /// Atomically renames one namespace binding inside this transaction.
    #[wasm_bindgen]
    pub async fn rename(&mut self, source: String, destination: String) -> Result<(), JsValue> {
        Box::pin(self.engine.rename(
            &source,
            &destination,
            true,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        ))
        .await
        .map(|receipt| receipt.value)
        .map_err(|failure| failure.error)
        .map_err(js_error)
    }

    /// Creates one same-workspace hard link.
    #[wasm_bindgen(js_name = hardLink)]
    pub async fn hard_link(&mut self, source: String, destination: String) -> Result<(), JsValue> {
        self.engine
            .hard_link(
                &source,
                &destination,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(js_error)
    }

    /// Replaces one sparse regular-file range.
    #[wasm_bindgen(js_name = writeRange)]
    pub async fn write_range(
        &mut self,
        path: String,
        offset: u64,
        bytes: Vec<u8>,
    ) -> Result<(), JsValue> {
        let bytes = bytes::Bytes::from(bytes);
        self.engine
            .write_range(
                &path,
                offset,
                bytes,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
            .map_err(js_error)
    }

    /// Changes one regular file's logical length.
    pub async fn resize(&mut self, path: String, logical_bytes: u64) -> Result<(), JsValue> {
        self.engine
            .resize(&path, logical_bytes)
            .await
            .map_err(js_error)
    }

    /// Punches a hole or installs allocated zeros over one exact range.
    #[wasm_bindgen(js_name = zeroRange)]
    pub async fn zero_range(
        &mut self,
        path: String,
        offset: u64,
        length: u64,
        allocated: bool,
        extend: bool,
    ) -> Result<(), JsValue> {
        let range = ByteRange { offset, length };
        self.engine
            .zero_range(&path, range, allocated, extend)
            .await
            .map_err(js_error)
    }

    /// Preallocates one sparse range without replacing content.
    pub async fn preallocate(
        &mut self,
        path: String,
        offset: u64,
        length: u64,
        keep_size: bool,
    ) -> Result<(), JsValue> {
        let range = ByteRange { offset, length };
        self.engine
            .preallocate(&path, range, keep_size)
            .await
            .map_err(js_error)
    }

    /// Clones one immutable range without reading file bytes.
    #[wasm_bindgen(js_name = cloneRange)]
    pub async fn clone_range(
        &mut self,
        source: String,
        source_offset: u64,
        destination: String,
        destination_offset: u64,
        length: u64,
    ) -> Result<(), JsValue> {
        self.engine
            .clone_range(
                &source,
                source_offset,
                &destination,
                destination_offset,
                length,
            )
            .await
            .map_err(js_error)
    }

    /// Publishes the complete candidate through one idempotent head CAS.
    #[wasm_bindgen]
    pub async fn commit(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "BrowserOperationWindowLease | undefined")]
        lease: Option<JsValue>,
    ) -> Result<Ts<BrowserWorkspaceCommit>, JsValue> {
        let permit = browser_publication_permit(lease)?;
        let outcome = {
            let value = &mut self.engine;
            Box::pin(async {
                value
                    .commit(
                        None,
                        permit,
                        acyclic_fs::WorkBudget::UNBOUNDED,
                        &acyclic_fs::CancellationToken::new(),
                        |_| async {
                            Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::WorkspaceError>>(
                                acyclic_fs::OperationReceipt {
                                    value: (),
                                    work: acyclic_fs::WorkCounters::default(),
                                },
                            )
                        },
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
            })
            .await
            .map(browser_workspace_commit)
        }
        .map_err(js_error)?;
        ts(outcome)
    }

    /// Safely advances this retained candidate and sparsely replays its work.
    #[wasm_bindgen]
    pub async fn rebase(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_conflicts: JsValue,
    ) -> Result<Ts<BrowserTransactionRebase>, JsValue> {
        let maximum_conflicts = wasm_u32(&maximum_conflicts)?;
        let outcome = self
            .engine
            .rebase(maximum_conflicts)
            .await
            .map(browser_transaction_rebase)
            .map_err(js_error)?;
        ts(outcome)
    }
}

pub(in crate::bindings) fn browser_workspace_commit<A, O>(
    outcome: TransactionCommit<A, O>,
) -> BrowserWorkspaceCommit {
    match outcome {
        TransactionCommit::Committed(value) => BrowserWorkspaceCommit {
            status: BrowserWorkspaceCommitStatus::Committed,
            generation_id: Some(value.id().digest().into_bytes().to_vec().into()),
        },
        TransactionCommit::AlreadyCommitted(value) => BrowserWorkspaceCommit {
            status: BrowserWorkspaceCommitStatus::AlreadyCommitted,
            generation_id: Some(value.id().digest().into_bytes().to_vec().into()),
        },
        TransactionCommit::Conflict { actual } => BrowserWorkspaceCommit {
            status: BrowserWorkspaceCommitStatus::Conflict,
            generation_id: Some(actual.id().digest().into_bytes().to_vec().into()),
        },
        TransactionCommit::Fenced => BrowserWorkspaceCommit {
            status: BrowserWorkspaceCommitStatus::Fenced,
            generation_id: None,
        },
        TransactionCommit::IdempotencyConflict => BrowserWorkspaceCommit {
            status: BrowserWorkspaceCommitStatus::IdempotencyConflict,
            generation_id: None,
        },
    }
}

pub(in crate::bindings) fn browser_transaction_rebase<A, O>(
    outcome: TransactionRebase<A, O>,
) -> BrowserTransactionRebase {
    match outcome {
        TransactionRebase::Rebased(generation) => BrowserTransactionRebase {
            status: BrowserTransactionRebaseStatus::Rebased,
            generation_id: Some(generation.id().digest().into_bytes().to_vec().into()),
            conflicts: Vec::new(),
            truncated: false,
        },
        TransactionRebase::Conflicted {
            conflicts,
            truncated,
        } => BrowserTransactionRebase {
            status: BrowserTransactionRebaseStatus::Conflicted,
            generation_id: None,
            conflicts: conflicts
                .into_iter()
                .map(browser_transaction_conflict)
                .collect(),
            truncated,
        },
    }
}

pub(in crate::bindings) fn browser_transaction_conflict(
    value: RebaseConflict,
) -> BrowserTransactionConflict {
    let mut result = BrowserTransactionConflict {
        region: BrowserTransactionConflictRegion::FileRecord,
        file_id: None,
        directory_id: None,
        offset: None,
        length: None,
        sparse_target: None,
        name: None,
        maximum_entries: None,
        usage: match value.usage {
            DependencyUse::Observation => BrowserTransactionDependencyUse::Observation,
            DependencyUse::Mutation => BrowserTransactionDependencyUse::Mutation,
            DependencyUse::ObservationAndMutation => {
                BrowserTransactionDependencyUse::ObservationAndMutation
            }
        },
        expected: match value.expected {
            DependencyState::Absent => None,
            DependencyState::Present(digest) => Some(digest.into_bytes().to_vec().into()),
        },
        actual: match value.actual {
            DependencyState::Absent => None,
            DependencyState::Present(digest) => Some(digest.into_bytes().to_vec().into()),
        },
    };
    match value.region {
        DependencyRegion::FileRecord(file_id) => {
            result.region = BrowserTransactionConflictRegion::FileRecord;
            result.file_id = Some(file_id.into_bytes().to_vec().into());
        }
        DependencyRegion::Metadata(file_id) => {
            result.region = BrowserTransactionConflictRegion::Metadata;
            result.file_id = Some(file_id.into_bytes().to_vec().into());
        }
        DependencyRegion::FileLength(file_id) => {
            result.region = BrowserTransactionConflictRegion::FileLength;
            result.file_id = Some(file_id.into_bytes().to_vec().into());
        }
        DependencyRegion::ContentRange {
            file_id,
            offset,
            length,
        } => {
            result.region = BrowserTransactionConflictRegion::ContentRange;
            result.file_id = Some(file_id.into_bytes().to_vec().into());
            result.offset = Some(offset);
            result.length = Some(length);
        }
        DependencyRegion::SparseSeek {
            file_id,
            offset,
            target,
        } => {
            result.region = BrowserTransactionConflictRegion::SparseSeek;
            result.file_id = Some(file_id.into_bytes().to_vec().into());
            result.offset = Some(offset);
            result.sparse_target = Some(match target {
                ExtentSeekTarget::Data => BrowserTransactionSparseSeek::Data,
                ExtentSeekTarget::Hole => BrowserTransactionSparseSeek::Hole,
            });
        }
        DependencyRegion::DirectoryName { directory_id, name } => {
            result.region = BrowserTransactionConflictRegion::DirectoryName;
            result.directory_id = Some(directory_id.into_bytes().to_vec().into());
            result.name = Some(browser_workspace_name_value(name));
        }
        DependencyRegion::DirectoryRange {
            directory_id,
            after,
            maximum_entries,
        } => {
            result.region = BrowserTransactionConflictRegion::DirectoryRange;
            result.directory_id = Some(directory_id.into_bytes().to_vec().into());
            result.name = after.map(browser_workspace_name_value);
            result.maximum_entries = Some(maximum_entries);
        }
    }
    result
}
