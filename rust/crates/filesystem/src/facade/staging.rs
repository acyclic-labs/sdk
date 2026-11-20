use super::*;

/// Authenticated content staged from a bounded stream without materializing the
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedContent {
    /// Root of the immutable chunk/index closure.
    pub(super) root: ObjectId,
    /// Exact logical source length.
    pub(super) logical_bytes: u64,
}

impl StagedContent {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) const fn from_canonical_parts(root: ObjectId, logical_bytes: u64) -> Self {
        Self {
            root,
            logical_bytes,
        }
    }

    /// Authenticated immutable blob-root identity created by this SDK.
    #[must_use]
    pub const fn root(&self) -> ObjectId {
        self.root
    }

    /// Exact staged logical byte length authenticated by the blob build.
    #[must_use]
    pub const fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }
}

/// The objects a record itself names: the roots of its closure. A retention
/// batch holds these, and each root's envelope chain holds everything the
/// record reaches, so a publication's reference set is bounded by the records
/// it makes durable, never by the closure's size or the volume's history.
struct RecordRoots(Vec<ObjectId>);

impl RecordRoots {
    fn of(records: &[FileRecord]) -> Self {
        let mut roots = Vec::with_capacity(records.len() * 3);
        for record in records {
            roots.push(record.metadata);
            roots.extend(record.bindings);
            roots.extend(payload_root(&record.payload));
        }
        Self(roots)
    }
}

/// The object a payload's own closure hangs from, when it has one.
fn payload_root(payload: &FilePayload) -> Option<ObjectId> {
    match payload {
        FilePayload::Regular { extents, .. } => Some(*extents),
        FilePayload::Directory { entries } => Some(*entries),
        FilePayload::SymbolicLink { target, .. } => Some(*target),
        FilePayload::ReparsePoint { payload, .. } => Some(*payload),
        FilePayload::InlineRegular(_) | FilePayload::Empty | FilePayload::Device { .. } => None,
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(super) async fn flush_publication(
        &self,
        scope: crate::PublicationScope<'_>,
        accounting: &mut KernelAccounting<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<crate::PublicationHold, FsError> {
        accounting.check()?;
        let mut work = crate::async_storage::AuthorityFrame::borrow(accounting)?;
        let result = self
            .inner
            .objects
            .flush_before_publish(scope, &mut work)
            .await;
        finish_authority_work(work)?;
        result.map_err(FsError::from)
    }
    pub(crate) async fn make_records_durable<W: FsWork>(
        &self,
        config: VolumeConfig,
        records: &[FileRecord],
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::PublicationHold, FsError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            accounting.check()?;
            let proven_at = self.inner.objects.collection_sweeps();
            let (closure, allocation) = {
                let mut frame = crate::async_storage::AuthorityFrame::borrow(&mut accounting)?;
                let meter =
                    crate::kernel::closure::ProofMeter::from_accounting(frame.accounting())?;
                crate::kernel::closure::ProofContext::new(
                    &self.inner.objects,
                    closure_limits(config),
                    meter,
                )
                .prove_record_closure(records)
                .await?
            };
            // The proof authenticates every object the records reach. The
            // batch holds only the roots the records themselves name, whose
            // envelope chains hold the rest, so the retain request is bounded
            // by the records and never by the closure's size.
            drop(closure);
            let roots = RecordRoots::of(records);
            let result = self
                .flush_publication(
                    crate::PublicationScope {
                        root: None,
                        related_root: None,
                        related_objects: &[],
                        objects: &roots.0,
                        proven_at,
                    },
                    &mut accounting,
                    cancellation,
                )
                .await;
            drop(roots);
            drop(allocation);
            result
        }
        .await;
        budget.finish(result, &accounting)
    }
    /// Holds the authenticated closure across an external durable publication.
    pub(crate) async fn make_generation_durable<W: FsWork>(
        &self,
        volume: &Volume<A, O>,
        generation: GenerationId,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::PublicationHold, WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            accounting.check()?;
            if !self.same_deployment(&volume.fs) {
                return Err(WorkspaceError::ForeignGeneration);
            }
            let publication = self
                .retention_publication_with_accounting(
                    volume,
                    generation.object_id(),
                    &mut accounting,
                )
                .await?;
            let result = self
                .flush_publication(
                    crate::PublicationScope {
                        root: Some(publication.generation_root),
                        related_root: None,
                        related_objects: &[],
                        objects: &publication.objects,
                        proven_at: publication.proven_at,
                    },
                    &mut accounting,
                    cancellation,
                )
                .await;
            drop(publication);
            result.map_err(Into::into)
        }
        .await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn restore_workspace_generation(
        &self,
        workspace: &Volume<A, O>,
        generation: GenerationId,
        expected: GenerationId,
        operation_id: OperationId,
        permit: PublicationPermit,
    ) -> Result<crate::WorkspaceRestore<GenerationId>, WorkspaceError> {
        let cancellation = CancellationToken::new();
        if let Some(recorded) = self
            .workspace_operation_generation(
                workspace,
                operation_id,
                None,
                None,
                &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
                &cancellation,
            )
            .await?
            .0
            .map(|(generation, record, _evidence)| {
                drop(record);
                generation
            })
        {
            return Ok(if recorded == generation {
                crate::WorkspaceRestore::AlreadyRestored(recorded)
            } else {
                crate::WorkspaceRestore::IdempotencyConflict
            });
        }
        let (current, authority_head) = workspace
            .resolve_head_generation(
                &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
                &cancellation,
            )
            .await
            .map_err(WorkspaceError::engine)?;
        let current = GenerationId::from_root(current);
        if current != expected {
            return Ok(crate::WorkspaceRestore::Stale(current));
        }
        if current == generation {
            return Ok(crate::WorkspaceRestore::Current(current));
        }
        let generation_root = generation.object_id();
        let (root, _) = read_generation_root(
            &self.inner.objects,
            generation_root,
            workspace.config,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        if root.volume_id != workspace.id {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let publication = publish_generation(
            self.clone(),
            PublishGenerationRequest {
                authority_id: volume_authority_id(workspace.id),
                volume_id: workspace.id,
                epoch: authority_head.epoch,
                expected: authority_head,
                operation_id,
                generation_root,
            },
            PublicationIntent::guarded(None, permit),
            closure_limits(workspace.config),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(WorkspaceError::engine)?;
        Ok(match publication.outcome {
            AppendOutcome::Committed(_) => {
                crate::WorkspaceRestore::Restored(publication.proof.generation_id)
            }
            AppendOutcome::AlreadyCommitted(_) => {
                crate::WorkspaceRestore::AlreadyRestored(publication.proof.generation_id)
            }
            AppendOutcome::Conflict { .. } => {
                let (actual, _) = workspace
                    .resolve_head_generation(
                        &mut KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
                        &cancellation,
                    )
                    .await
                    .map_err(WorkspaceError::engine)?;
                crate::WorkspaceRestore::Stale(GenerationId::from_root(actual))
            }
            AppendOutcome::Fenced { .. } => crate::WorkspaceRestore::Fenced,
            AppendOutcome::IdempotencyConflict { .. } => {
                crate::WorkspaceRestore::IdempotencyConflict
            }
        })
    }

    /// Executes one active local-residency prediction through this filesystem's
    pub async fn execute_residency(
        &self,
        speculator: &crate::ResidencySpeculator,
        permit: crate::ResidencyPermit,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> crate::storage::ObjectResult<u64> {
        crate::execute_residency(
            speculator,
            &self.inner.objects,
            permit,
            budget,
            cancellation,
        )
        .await
    }

    pub(super) async fn put_encoded_with_accounting(
        &self,
        kind: ObjectKind,
        encoded: (Vec<u8>, KernelOwnedAllocation),
        accounting: &mut KernelAccounting<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(original) = accounting.native_work() {
            let mut encoded = encoded;
            let allocation = match &mut encoded.1 {
                KernelOwnedAllocation::NativeOriginal(allocation) => allocation,
                KernelOwnedAllocation::Standalone => return Err(FsError::HandleAllocationFailed),
            };
            let hashed = HashedObject::from_native_vec(
                kind,
                &mut encoded.0,
                allocation,
                original,
                _cancellation,
            )?;
            let hashed =
                crate::async_storage::prepare_object(&self.inner.objects, hashed, accounting)
                    .await?;
            let object = hashed.object_id();
            let mut work = crate::async_storage::AuthorityFrame::borrow(accounting)?;
            self.inner.objects.put_hashed(hashed, &mut work).await?;
            return Ok(object);
        }
        let capacity = encoded.0.capacity() as u64;
        let hashing = WorkCounters {
            bytes_hashed: (encoded.0.len() as u64),
            ..WorkCounters::UNCHARGED
        };
        let owner = WorkCounters {
            allocation_operations: u64::from(encoded.0.capacity() != encoded.0.len()),
            ..WorkCounters::UNCHARGED
        };
        accounting.admit(owner)?;
        accounting.admit(hashing)?;
        let bytes = {
            let (payload, _allocation) = encoded;
            let bytes = acyclic_stream::OwnedBytes::from(payload);
            if let Err(error) = accounting.record_completed(owner) {
                drop(bytes);
                return Err(error);
            }
            bytes
        };
        let hashed = HashedObject::new(kind, bytes);
        accounting.record_completed(hashing)?;
        let hashed =
            crate::async_storage::prepare_object(&self.inner.objects, hashed, accounting).await?;
        let object = hashed.object_id();
        let result = {
            let mut object_work = crate::async_storage::AuthorityFrame::borrow(accounting)?;
            let result = self
                .inner
                .objects
                .put_hashed(hashed, &mut object_work)
                .await;
            object_work.spent()?.verify(object_work.budget)?;
            result
        };
        let (allocations, ..) = accounting
            .standalone_parts()
            .ok_or(FsError::HandleAllocationFailed)?;
        allocations
            .release(capacity)
            .map_err(|error| error.into_work_or(FsError::PendingMutationAllocationFailed))?;
        result.map_err(FsError::from)?;
        Ok(object)
    }
    pub(super) async fn put_generation_root_with_accounting(
        &self,
        root: &GenerationRoot,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<ObjectId, FsError> {
        let mut accounting = accounting.allocation_scope()?;
        let encoded = crate::kernel::codec::encode_owned(&mut accounting, |observe, error| {
            crate::kernel::encode_generation_root(root, observe, error)
        })?;
        self.put_encoded_with_accounting(
            ObjectKind::GenerationRoot,
            encoded,
            &mut accounting,
            cancellation,
        )
        .await
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Checkout<A, O> {
    pub(super) async fn stage_authored_blob_with_accounting(
        &self,
        content: acyclic_stream::OwnedBytes,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<StagedContent, FsError> {
        let mut source = std::io::Cursor::new(content.as_ref());
        let result = self
            .stage_content_with_accounting(
                &mut source,
                (content.len() as u64).max(1),
                accounting,
                cancellation,
            )
            .await;
        drop(content);
        result
    }

    /// Streams content into authenticated chunks, with an optional source byte policy.
    pub async fn stage_content<R: AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_source_bytes: impl Into<Option<u64>> + Send,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> FsResult<StagedContent> {
        let maximum_source_bytes = maximum_source_bytes.into();
        in_heap(move || async move {
            let mut accounting = KernelAccounting::standalone(budget, cancellation);
            let result = self
                .stage_content_with_accounting(
                    source,
                    maximum_source_bytes,
                    &mut accounting,
                    cancellation,
                )
                .await;
            measured(result, &accounting)
        })
        .await
    }

    pub(crate) async fn stage_content_with_accounting<R: AsyncBlobSource>(
        &self,
        source: &mut R,
        maximum_source_bytes: impl Into<Option<u64>> + Send,
        accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<StagedContent, FsError> {
        stage_content_for_volume_with_accounting(
            &self.volume,
            source,
            maximum_source_bytes,
            accounting,
            cancellation,
        )
        .await
    }
}

pub(crate) async fn stage_content_for_volume_with_accounting<
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    R: AsyncBlobSource,
>(
    volume: &Volume<A, O>,
    source: &mut R,
    maximum_source_bytes: impl Into<Option<u64>> + Send,
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<StagedContent, FsError> {
    let maximum_source_bytes = maximum_source_bytes.into();
    if maximum_source_bytes == Some(0) {
        return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
    }
    accounting.check()?;
    cancellation.check()?;
    let blob = crate::kernel::build_blob_with_accounting(
        &volume.fs.inner.objects,
        source,
        BlobBuildOptions::for_limits(
            DecodeLimits::for_volume(volume.config),
            maximum_source_bytes,
        ),
        accounting,
        cancellation,
    )
    .await?;
    Ok(StagedContent {
        root: blob.root,
        logical_bytes: blob.logical_bytes,
    })
}

#[cfg(not(target_arch = "wasm32"))]
impl AsRef<[u8]> for crate::kernel::Owned<Vec<u8>> {
    fn as_ref(&self) -> &[u8] {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{Child, TreeEntry};
    use crate::test_support::{put_file_metadata, put_tree_page};
    use std::sync::{Arc, Mutex};

    /// Records the reference count of every retain request the store issues.
    struct Counting<S> {
        inner: S,
        requests: Arc<Mutex<Vec<usize>>>,
    }

    impl<S: AsyncObjectStore> AsyncObjectStore for Counting<S> {
        crate::async_storage::forward_object_store!(self => self.inner;
            put,
            read,
            read_many,
            contains,
        );

        fn flush_before_publish(
            &self,
            scope: crate::PublicationScope<'_>,
            context: &mut crate::async_storage::AuthorityWork<'_>,
        ) -> impl std::future::Future<Output = Result<crate::PublicationHold, ObjectStoreError>>
        + acyclic_stream::ProviderTask {
            // Exactly the reference set `retention` builds for this scope.
            let count = usize::from(scope.root.is_some())
                + usize::from(scope.related_root.is_some())
                + if scope.root.is_none() {
                    scope.objects.len()
                } else {
                    0
                }
                + if scope.related_root.is_none() {
                    scope.related_objects.len()
                } else {
                    0
                };
            self.requests.lock().unwrap().push(count);
            self.inner.flush_before_publish(scope, context)
        }
    }

    fn config() -> VolumeConfig {
        VolumeConfig::portable(Lifecycle::Ephemeral)
    }

    /// A directory record whose tree holds `entries` names, one per leaf page,
    /// so its authenticated closure grows with the number of names.
    async fn directory_record<S: AsyncObjectStore>(
        store: &S,
        entries: usize,
    ) -> Result<FileRecord, Box<dyn std::error::Error>> {
        let metadata = put_file_metadata(store, FileMetadata::default()).await?;
        let mut children = Vec::with_capacity(entries);
        for index in 0..entries {
            let name =
                LogicalName::new(NameEncoding::Utf8, format!("file-{index:04}").into_bytes())?;
            let leaf = put_tree_page(
                store,
                &TreePage::Leaf(vec![TreeEntry {
                    name: name.clone(),
                    file_id: FileId::from_bytes([index as u8; 16]),
                    kind: FileKind::Regular,
                }]),
                1,
            )
            .await?;
            children.push(Child {
                first: name,
                page: leaf,
            });
        }
        let tree = put_tree_page(
            store,
            &TreePage::Internal(children),
            u32::try_from(entries)?,
        )
        .await?;
        Ok(FileRecord {
            bindings: None,
            file_id: FileId::from_bytes([0; 16]),
            kind: FileKind::Directory,
            link_count: 1,
            metadata,
            payload: FilePayload::Directory { entries: tree },
        })
    }

    /// The number of objects the record's authenticated closure reaches.
    async fn closure_size<S: AsyncObjectStore>(store: &S, record: &FileRecord) -> usize {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut frame = crate::async_storage::AuthorityFrame::borrow(&mut accounting).unwrap();
        let meter =
            crate::kernel::closure::ProofMeter::from_accounting(frame.accounting()).unwrap();
        let (closure, guard) =
            crate::kernel::closure::ProofContext::new(store, closure_limits(config()), meter)
                .prove_record_closure(std::slice::from_ref(record))
                .await
                .unwrap();
        let size = closure.len();
        drop(closure);
        drop(guard);
        size
    }

    /// A publication's retain request is bounded by the records it makes
    /// durable, not by the closure they reach, so it cannot grow with the
    /// volume's history.
    #[tokio::test]
    async fn retain_request_is_bounded_by_the_records_not_the_closure() {
        let mut requests = Vec::new();
        let mut closures = Vec::new();
        for entries in [1, 10, 100] {
            let recorded = Arc::new(Mutex::new(Vec::new()));
            let store = Counting {
                inner: crate::LogicalObjectStore::new(Arc::new(
                    acyclic_objects::MemoryObjects::memory(),
                )),
                requests: Arc::clone(&recorded),
            };
            let fs = Fs::new(
                crate::distributed::StreamAuthorityStore::new(acyclic_stream::ProviderOwner::new(
                    acyclic_stream::MemoryStream::default(),
                )),
                store,
                crate::EmbeddedCapabilities::MEMORY,
            );
            let record = directory_record(fs.objects(), entries).await.unwrap();
            let cancellation = CancellationToken::new();
            let hold = fs
                .make_records_durable(
                    config(),
                    std::slice::from_ref(&record),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map(|receipt| receipt.value)
                .unwrap();
            drop(hold);
            let issued = recorded.lock().unwrap().clone();
            assert_eq!(issued.len(), 1, "one retain per publication");
            requests.push(issued[0]);
            closures.push(closure_size(fs.objects(), &record).await);
        }
        assert!(
            closures[0] < closures[1] && closures[1] < closures[2],
            "the closure grows with the volume's history: {closures:?}"
        );
        assert_eq!(
            requests,
            [2, 2, 2],
            "the retain request is bounded by the record: {requests:?}"
        );
    }
}
