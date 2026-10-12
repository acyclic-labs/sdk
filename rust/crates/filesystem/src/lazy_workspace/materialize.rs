use super::*;


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{


    /// Promotes exactly one demanded source node into authored copy-on-write state.
    /// Directories remain shallow and regular files are fetched in bounded ranges.
    pub async fn promote(
        &self,
        path: &str,
        maximum_bytes: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<TransactionCommit<A, O>, LazyWorkspaceError> {
        self.promote_with_permit(
            path,
            maximum_bytes,
            idempotency_key,
            crate::PublicationPermit::Unrestricted,
        )
        .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "populates every source node kind in one transaction so the pinned source fact is checked once"
    )]
    async fn populate_source_node_measured(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        pinned: &PinnedSourceNode,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, LazyWorkspaceError> {
        in_heap(move || async move {
            let node = &pinned.node;
            let source = pinned.source;
            let mut work = WorkCounters::default();
            match node.kind {
                SourceNodeKind::Directory => {
                    let applied = transaction
                        .create_directory_measured(
                            requested,
                            remaining_work(work, budget)?,
                            cancellation,
                        )
                        .await
                        .map_err(workspace_error)?;
                    work = account_work(work, applied, budget)?;
                }
                SourceNodeKind::RegularFile => {
                    work = self
                        .populate_regular_source_node(
                            transaction,
                            requested,
                            pinned,
                            maximum_bytes,
                            budget,
                            cancellation,
                        )
                        .await?;
                }
                SourceNodeKind::SymbolicLink => {
                    let receipt = self
                        .source
                        .read_link(
                            source,
                            &self.namespace_path(requested)?,
                            node.version,
                            cancellation,
                        )
                        .await
                        .map_err(|failure| LazyWorkspaceError::from(failure.error))?;
                    work = account_work(work, receipt.work, budget)?;
                    let applied = transaction
                        .create_symbolic_link_measured(
                            requested,
                            receipt.value,
                            remaining_work(work, budget)?,
                            cancellation,
                        )
                        .await
                        .map_err(workspace_error)?;
                    work = account_work(work, applied, budget)?;
                }
                SourceNodeKind::Fifo
                | SourceNodeKind::Socket
                | SourceNodeKind::CharacterDevice
                | SourceNodeKind::BlockDevice => {
                    let kind = match node.kind {
                        SourceNodeKind::Fifo => FileKind::Fifo,
                        SourceNodeKind::Socket => FileKind::Socket,
                        SourceNodeKind::CharacterDevice => FileKind::CharacterDevice,
                        SourceNodeKind::BlockDevice => FileKind::BlockDevice,
                        _ => unreachable!(),
                    };
                    let applied = transaction
                        .create_special_measured(
                            requested,
                            kind,
                            node.device,
                            remaining_work(work, budget)?,
                            cancellation,
                        )
                        .await
                        .map_err(workspace_error)?;
                    work = account_work(work, applied, budget)?;
                }
                SourceNodeKind::Unsupported => return Err(LazyWorkspaceError::UnsupportedNode),
            }
            let applied = transaction
                .set_metadata_measured(
                    requested,
                    source_file_metadata(node.metadata),
                    remaining_work(work, budget)?,
                    cancellation,
                )
                .await
                .map_err(workspace_error)?;
            work = account_work(work, applied, budget)?;
            if node.kind != SourceNodeKind::Directory {
                let applied = transaction
                    .preserve_file_identity_measured(
                        requested,
                        self.source_file_id(node),
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(workspace_error)?;
                work = account_work(work, applied, budget)?;
            }
            Ok(work)
        })
        .await
    }

    async fn populate_regular_source_node(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        pinned: &PinnedSourceNode,
        maximum_bytes: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<WorkCounters, LazyWorkspaceError> {
        in_heap(move || async move {
            let node = &pinned.node;
            let source = pinned.source;
            let length = node
                .logical_bytes
                .ok_or(LazyWorkspaceError::NotRegularFile)?;
            if length > maximum_bytes {
                return Err(LazyWorkspaceError::TooLarge);
            }
            let applied = transaction
                .create_file_measured(requested, Bytes::new(), budget, cancellation)
                .await
                .map_err(workspace_error)?;
            let mut work = account_work(WorkCounters::default(), applied, budget)?;
            let mut offset = 0_u64;
            while offset < length {
                let requested_bytes = (length - offset).min(1024 * 1024);
                let receipt = self
                    .source
                    .read_range(
                        source,
                        &self.namespace_path(requested)?,
                        node.version,
                        offset,
                        requested_bytes,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| LazyWorkspaceError::from(failure.error))?;
                work = account_work(work, receipt.work, budget)?;
                let chunk = receipt.value;
                if chunk.is_empty() {
                    return Err(LazyWorkspaceError::Demand(DemandError::StaleVersion));
                }
                let chunk_length =
                    u64::try_from(chunk.len()).map_err(|_| LazyWorkspaceError::TooLarge)?;
                let applied = transaction
                    .write_range_measured(
                        requested,
                        offset,
                        chunk,
                        remaining_work(work, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(workspace_error)?;
                work = account_work(work, applied, budget)?;
                offset = offset
                    .checked_add(chunk_length)
                    .ok_or(LazyWorkspaceError::TooLarge)?;
            }
            Ok(work)
        })
        .await
    }

    async fn stage_regular_source_content(
        &self,
        transaction: &crate::Transaction<A, O>,
        requested: &str,
        pinned: &PinnedSourceNode,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<crate::StagedContent>, LazyWorkspaceError> {
        let length = pinned
            .node
            .logical_bytes
            .ok_or(LazyWorkspaceError::NotRegularFile)?;
        let mut source = DemandBlobSource {
            provider: self.source.as_ref(),
            source: pinned.source,
            path: self.namespace_path(requested)?,
            version: pinned.node.version,
            offset: 0,
            length,
            work: None,
            failure: None,
        };
        let staged = transaction
            .stage_content_measured(&mut source, length.max(1), budget, cancellation)
            .await
            .map_err(|error| {
                source
                    .failure
                    .take()
                    .map_or_else(|| workspace_error(error), LazyWorkspaceError::Demand)
            })?;
        if staged.value.logical_bytes() != length {
            return Err(LazyWorkspaceError::Demand(DemandError::StaleVersion));
        }
        Ok(staged)
    }
}
