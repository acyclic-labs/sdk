use super::*;

/// Streams one version-pinned demanded file into the SDK blob builder while
/// preserving the demand provider's exact work receipt.
pub(super) struct DemandBlobSource<'a, D> {
    pub(super) provider: &'a D,
    pub(super) source: SourceReference,
    pub(super) path: NamespacePath,
    pub(super) version: SourceVersion,
    pub(super) offset: u64,
    pub(super) length: u64,
    pub(super) work: Option<WorkCounters>,
    pub(super) failure: Option<DemandError>,
}

impl<D: DemandSource> crate::kernel::AsyncBlobSource for DemandBlobSource<'_, D> {
    async fn read<'a>(
        &'a mut self,
        destination: &'a mut [u8],
        cancellation: &'a CancellationToken,
    ) -> std::io::Result<usize> {
        let remaining = self.length.saturating_sub(self.offset);
        if remaining == 0 {
            self.work = Some(WorkCounters::default());
            return Ok(0);
        }
        let requested = remaining.min(u64::try_from(destination.len()).unwrap_or(u64::MAX));
        let receipt = self
            .provider
            .read_range(
                self.source,
                &self.path,
                self.version,
                self.offset,
                requested,
                cancellation,
            )
            .await;
        let receipt = match receipt {
            Ok(receipt) => receipt,
            Err(failure) => {
                self.work = Some(*failure.work);
                self.failure = Some(failure.error);
                return Err(std::io::Error::other("version-pinned demand read failed"));
            }
        };
        self.work = Some(receipt.work);
        let chunk = receipt.value;
        if chunk.is_empty() || u64::try_from(chunk.len()).unwrap_or(u64::MAX) > requested {
            self.failure = Some(DemandError::StaleVersion);
            return Err(std::io::Error::other(
                "version-pinned demand read was incomplete",
            ));
        }
        self.offset = self
            .offset
            .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
            .ok_or_else(|| std::io::Error::other("demand source offset overflow"))?;
        destination
            .get_mut(..chunk.len())
            .ok_or_else(|| std::io::Error::other("demand source exceeded read bound"))?
            .copy_from_slice(&chunk);
        Ok(chunk.len())
    }

    fn take_work(&mut self) -> Option<WorkCounters> {
        self.work.take()
    }
}


impl<A, O, D, S> LazyWorkspace<A, O, D, S>
where
    A: AsyncAuthorityStore,
    O: AsyncObjectStore,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{


    /// Reads one exact range, demanding no unrelated content.
    pub async fn read_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.read_range_measured(path, offset, length, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
    }

    /// Opens the inspected regular source file for repeated reads, each of
    /// which proves `node` is still the file's current version.
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) async fn open_source_file(
        &self,
        path: &str,
        source: SourceReference,
        node: SourceNode,
    ) -> Result<Box<dyn crate::demand::DemandFile>, LazyWorkspaceError> {
        if node.kind != SourceNodeKind::RegularFile {
            return Err(LazyWorkspaceError::NotRegularFile);
        }
        self.source
            .open_file(
                source,
                &self.namespace_path(path)?,
                node.version,
                &CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| LazyWorkspaceError::from(failure.error))
    }

    async fn read_range_measured(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Bytes>, LazyWorkspaceError> {
        let lookup = self.lookup_measured(path, budget, cancellation).await?;
        let prior = lookup.work;
        let ResolvedLazyLookup { lookup, source } = lookup.value;
        match lookup {
            LazyLookup::Authored {
                path: authored_path,
                ..
            } => {
                let value = self
                    .workspace
                    .read_range(&authored_path, offset, length)
                    .await
                    .map_err(workspace_error)?;
                Ok(OperationReceipt { value, work: prior })
            }
            LazyLookup::Shadow { record, .. } => {
                if record.kind != FileKind::Regular {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let receipt = self
                    .workspace
                    .detached_record(record)
                    .read_range(
                        crate::ByteRange { offset, length },
                        remaining_work(prior, budget)?,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| LazyWorkspaceError::Workspace(failure.error.to_string()));
                let receipt = receipt?;
                Ok(OperationReceipt {
                    value: receipt.value.bytes,
                    work: account_work(prior, receipt.work, budget)?,
                })
            }
            LazyLookup::Source(node) => {
                if node.kind != SourceNodeKind::RegularFile {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let source = source.ok_or(LazyWorkspaceError::Concurrent)?;
                let receipt = self
                    .source
                    .read_range(
                        source,
                        &self.namespace_path(path)?,
                        node.version,
                        offset,
                        length,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| LazyWorkspaceError::from(failure.error))?;
                Ok(OperationReceipt {
                    value: receipt.value,
                    work: account_work(prior, receipt.work, budget)?,
                })
            }
        }
    }

    /// Reads a complete regular file under an explicit byte bound.
    pub async fn read(&self, path: &str, maximum_bytes: u64) -> Result<Bytes, LazyWorkspaceError> {
        let stat = self.stat(path).await?;
        let length = stat
            .logical_bytes
            .ok_or(LazyWorkspaceError::NotRegularFile)?;
        if length > maximum_bytes {
            return Err(LazyWorkspaceError::TooLarge);
        }
        self.read_range(path, 0, length).await
    }

    /// Reads one exact symbolic-link target without following it.
    pub async fn read_link(&self, path: &str) -> Result<Bytes, LazyWorkspaceError> {
        let cancellation = CancellationToken::new();
        self.read_link_measured(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map(|receipt| receipt.value)
    }

    async fn read_link_measured(
        &self,
        path: &str,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> Result<OperationReceipt<Bytes>, LazyWorkspaceError> {
        let lookup = self.lookup_measured(path, budget, cancellation).await?;
        let prior = lookup.work;
        let ResolvedLazyLookup { lookup, source } = lookup.value;
        match lookup {
            LazyLookup::Authored {
                path: authored_path,
                stat,
            } => {
                if stat.kind != FileKind::SymbolicLink {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let value = self
                    .workspace
                    .read_symbolic_link(&authored_path)
                    .await
                    .map_err(workspace_error)?;
                Ok(OperationReceipt { value, work: prior })
            }
            LazyLookup::Shadow { record, .. } => {
                let receipt = self
                    .workspace
                    .detached_record(record)
                    .read_symbolic_link(remaining_work(prior, budget)?, cancellation)
                    .await
                    .map_err(|failure| LazyWorkspaceError::Workspace(failure.error.to_string()))?;
                Ok(OperationReceipt {
                    value: receipt.value,
                    work: account_work(prior, receipt.work, budget)?,
                })
            }
            LazyLookup::Source(node) => {
                if node.kind != SourceNodeKind::SymbolicLink {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let source = source.ok_or(LazyWorkspaceError::Concurrent)?;
                let receipt = self
                    .source
                    .read_link(
                        source,
                        &self.namespace_path(path)?,
                        node.version,
                        cancellation,
                    )
                    .await
                    .map_err(|failure| LazyWorkspaceError::from(failure.error))?;
                Ok(OperationReceipt {
                    value: receipt.value,
                    work: account_work(prior, receipt.work, budget)?,
                })
            }
        }
    }

    /// Finds a sparse boundary without demanding file contents.
    ///
    /// Authored files use their authenticated extent tree. Unresolved native
    /// sources currently expose only dense length/version facts, so their
    /// exact portable representation is data through EOF followed by a hole.
    pub async fn seek(
        &self,
        path: &str,
        offset: u64,
        target: LazySeekTarget,
    ) -> Result<Option<u64>, LazyWorkspaceError> {
        match self.lookup(path).await? {
            LazyLookup::Authored {
                path: authored_path,
                stat,
            } => {
                if stat.kind != FileKind::Regular {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let length = stat
                    .logical_bytes
                    .ok_or(LazyWorkspaceError::NotRegularFile)?;
                if offset >= length {
                    return Ok(None);
                }
                let plan = self
                    .workspace
                    .plan_extents(&authored_path, offset, length - offset, 65_536)
                    .await
                    .map_err(workspace_error)?;
                let found = plan.spans.into_iter().find(|span| match target {
                    LazySeekTarget::Data => span.kind != WorkspaceExtentKind::Hole,
                    LazySeekTarget::Hole => span.kind == WorkspaceExtentKind::Hole,
                });
                Ok(found
                    .map(|span| span.offset)
                    .or_else(|| (target == LazySeekTarget::Hole).then_some(length)))
            }
            LazyLookup::Shadow { record, .. } => {
                if record.kind != FileKind::Regular {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                self.workspace
                    .detached_record(record)
                    .seek(
                        offset,
                        match target {
                            LazySeekTarget::Data => crate::kernel::ExtentSeekTarget::Data,
                            LazySeekTarget::Hole => crate::kernel::ExtentSeekTarget::Hole,
                        },
                        crate::WorkBudget::UNBOUNDED,
                        &crate::CancellationToken::new(),
                    )
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| LazyWorkspaceError::Workspace(failure.error.to_string()))
            }
            LazyLookup::Source(node) => {
                if node.kind != SourceNodeKind::RegularFile {
                    return Err(LazyWorkspaceError::NotRegularFile);
                }
                let length = node
                    .logical_bytes
                    .ok_or(LazyWorkspaceError::NotRegularFile)?;
                if offset >= length {
                    return Ok(None);
                }
                Ok(Some(match target {
                    LazySeekTarget::Data => offset,
                    LazySeekTarget::Hole => length,
                }))
            }
        }
    }
}
