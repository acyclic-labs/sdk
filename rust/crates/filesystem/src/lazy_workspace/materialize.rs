use super::*;
impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> LazyWorkspace<A, O, D, S>
{
    pub(super) async fn populate_source_node(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        path: &str,
        source: SourceReference,
        node: SourceNode,
        maximum: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let mut mutations = None;
        let mut namespace = None;
        // Fund the actual body, rather than an outer future that still builds
        // this large state machine on its poller's stack. Keep the cleanup
        // outside admission so it also runs if frame retirement fails.
        let result = async {
            crate::heap_future::in_borrowed_frame!(ctx, |ctx| async {
                namespace = Some(self.namespace_path(path, ctx)?);
                if node.kind == SourceNodeKind::Directory {
                    return self
                        .populate_source_directory(
                            transaction,
                            path,
                            source,
                            node,
                            &mut namespace,
                            ctx,
                        )
                        .await;
                }
                if self
                    .restore_source_regular_file(transaction, node, maximum, &mut namespace, ctx)
                    .await?
                {
                    return Ok(());
                }
                mutations = Some(Owned::allocate(1, ctx)?);
                let mutation = self
                    .prepare_source_mutation(
                        transaction,
                        path,
                        (source, node),
                        maximum,
                        &mut namespace,
                        ctx,
                    )
                    .await?;
                crate::Transaction::<A, O>::push_mutation(
                    mutations.as_mut().ok_or(crate::FsError::InvalidDiff)?,
                    mutation,
                    ctx,
                )?;
                transaction
                    .apply_authored(
                        mutations.take().ok_or(crate::FsError::InvalidDiff)?,
                        ctx,
                        cancellation(ctx),
                    )
                    .await?;
                Ok(())
            })
        }
        .await;
        if let Some(namespace) = namespace {
            namespace.retire(ctx)?;
        }
        if let Some(pending) = mutations {
            pending.retire(ctx)?;
        }
        result
    }

    pub(super) async fn populate_source_directory(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        path: &str,
        source: SourceReference,
        node: SourceNode,
        namespace: &mut Option<Owned<NamespacePath>>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        crate::heap_future::in_borrowed_frame!(ctx, |ctx| async move {
            let staged = async {
                let entries = transaction
                    .checkout
                    .stage_empty_tree_with_accounting(ctx, cancellation(ctx))
                    .await?;
                let metadata = transaction
                    .checkout
                    .stage_metadata_with_accounting(
                        source_file_metadata(node.metadata),
                        ctx,
                        cancellation(ctx),
                    )
                    .await?;
                Ok::<_, LazyWorkspaceError>(FileRecord {
                    bindings: None,
                    file_id: self.source_file_id(&node.file_identity, ctx)?,
                    kind: FileKind::Directory,
                    link_count: 1,
                    metadata,
                    payload: FilePayload::Directory { entries },
                })
            }
            .await;
            let record = staged?;
            let mut directories = Owned::allocate(1, ctx)?;
            // Keep the input with the caller through admission and staging. Only
            // the actual mutation batch takes ownership of its namespace.
            let mut namespace = namespace.take().ok_or(crate::FsError::InvalidDiff)?;
            // The batch owns the path even if the copy admission fails.
            let retained = (|| {
                let bytes = directories
                    .bytes
                    .checked_add(namespace.bytes)
                    .ok_or(crate::WorkError::Overflow)?;
                directories.allocation.absorb(&mut namespace.allocation)?;
                directories.bytes = bytes;
                Ok::<_, crate::FsError>(())
            })();
            if let Err(error) = retained {
                namespace.retire(ctx)?;
                directories.retire(ctx)?;
                return Err(error.into());
            }
            let copied = crate::Transaction::<A, O>::push_mutation(
                &mut directories,
                Owned::new(
                    crate::kernel::Mutation::Create {
                        path: namespace.value,
                        record,
                    },
                    namespace.allocation,
                    0,
                ),
                ctx,
            );
            if let Err(error) = copied {
                directories.retire(ctx)?;
                return Err(error.into());
            }
            match transaction
                .checkout
                .apply_mutations(directories, ctx, cancellation(ctx))
                .await
            {
                Err(crate::FsError::Mutation(
                    crate::kernel::GenerationMutationError::FileIdentityAlreadyExists,
                )) => {
                    self.reconcile_source_directory(transaction, path, source, record.file_id, ctx)
                        .await?;
                }
                result => result?,
            }
            Ok(())
        })
    }

    pub(super) async fn restore_source_regular_file(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        node: SourceNode,
        maximum: u64,
        namespace: &mut Option<Owned<NamespacePath>>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<bool, LazyWorkspaceError> {
        crate::heap_future::in_borrowed_frame!(ctx, |ctx| async move {
            if node.kind != SourceNodeKind::RegularFile {
                return Ok(false);
            }
            if node
                .logical_bytes
                .ok_or(LazyWorkspaceError::NotRegularFile)?
                > maximum
            {
                return Err(LazyWorkspaceError::TooLarge);
            }
            let id = self.source_file_id(&node.file_identity, ctx)?;
            let Some(record) = self.authored_source_record(transaction, id, ctx).await? else {
                return Ok(false);
            };
            Self::restore_source_record(transaction, record, namespace, ctx).await?;
            Ok(true)
        })
    }

    pub(super) async fn restore_source_record(
        transaction: &mut crate::Transaction<A, O>,
        record: FileRecord,
        namespace: &mut Option<Owned<NamespacePath>>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let mut links = Owned::allocate(1, ctx)?;
        let namespace = namespace.take().ok_or(crate::FsError::InvalidDiff)?;
        let insertion = crate::Transaction::<A, O>::push_mutation(
            &mut links,
            Owned::new(
                crate::kernel::Mutation::Restore {
                    path: namespace.value,
                    record,
                },
                namespace.allocation,
                namespace.bytes,
            ),
            ctx,
        );
        if let Err(error) = insertion {
            links.retire(ctx)?;
            return Err(error.into());
        }
        transaction
            .checkout
            .apply_mutations(links, ctx, cancellation(ctx))
            .await?;
        Ok(())
    }

    pub(super) async fn authored_source_record(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        id: FileId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Option<FileRecord>, LazyWorkspaceError> {
        match accounted!(ctx, |work, cancel| transaction
            .checkout
            .read_file_record_by_id(id, work, cancel))
        {
            Ok(record) => Ok(Some(record)),
            Err(LazyWorkspaceError::Filesystem(crate::FsError::NotFound)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(super) async fn prepare_source_mutation(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        path: &str,
        source_node: (SourceReference, SourceNode),
        maximum: u64,
        namespace: &mut Option<Owned<NamespacePath>>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Owned<AuthoredMutation>, LazyWorkspaceError> {
        crate::heap_future::in_borrowed_frame!(ctx, |ctx| async move {
            let namespace = namespace.take().ok_or(crate::FsError::InvalidDiff)?;
            let (source, node) = source_node;
            let metadata = source_file_metadata(node.metadata);
            let mutation = async {
                Ok::<_, LazyWorkspaceError>(match node.kind {
                    SourceNodeKind::RegularFile => {
                        let length = node
                            .logical_bytes
                            .ok_or(LazyWorkspaceError::NotRegularFile)?;
                        if length > maximum {
                            return Err(LazyWorkspaceError::TooLarge);
                        }
                        let mut content = DemandBlobSource {
                            provider: self.source.as_ref(),
                            source,
                            path: &namespace.value,
                            version: node.version,
                            offset: 0,
                            length,
                            work: None,
                            failure: None,
                            observation_error: None,
                        };
                        let file_id = Some(self.source_file_id(&node.file_identity, ctx)?);
                        if length <= crate::kernel::MAXIMUM_INLINE_FILE_BYTES as u64 {
                            let bytes = content.read_inline(ctx).await?;
                            AuthoredMutation::CreateFile {
                                path: namespace.value,
                                bytes,
                                metadata,
                                file_id,
                            }
                        } else {
                            let content =
                                Self::stage_source_content(transaction, &mut content, ctx).await?;
                            AuthoredMutation::CreateFileFromContent {
                                path: namespace.value,
                                content,
                                metadata,
                                file_id,
                            }
                        }
                    }
                    SourceNodeKind::Directory => unreachable!("source directory created above"),
                    SourceNodeKind::SymbolicLink => {
                        let target = self
                            .read_resolved(
                                &LazyLookup::Source { source, node },
                                path,
                                None,
                                maximum,
                                ctx,
                            )
                            .await?;
                        AuthoredMutation::CreateSymbolicLink {
                            file_id: Some(self.source_file_id(&node.file_identity, ctx)?),
                            path: namespace.value,
                            target: target.bytes,
                            metadata,
                        }
                    }
                    SourceNodeKind::Fifo | SourceNodeKind::Socket => {
                        AuthoredMutation::CreateEmptySpecial {
                            file_id: Some(self.source_file_id(&node.file_identity, ctx)?),
                            path: namespace.value,
                            kind: if node.kind == SourceNodeKind::Fifo {
                                FileKind::Fifo
                            } else {
                                FileKind::Socket
                            },
                            metadata,
                        }
                    }
                    SourceNodeKind::CharacterDevice | SourceNodeKind::BlockDevice => {
                        let (major, minor) =
                            node.device.ok_or(LazyWorkspaceError::UnsupportedNode)?;
                        AuthoredMutation::CreateDevice {
                            file_id: Some(self.source_file_id(&node.file_identity, ctx)?),
                            path: namespace.value,
                            kind: if node.kind == SourceNodeKind::CharacterDevice {
                                FileKind::CharacterDevice
                            } else {
                                FileKind::BlockDevice
                            },
                            major,
                            minor,
                            metadata,
                        }
                    }
                    SourceNodeKind::Unsupported => return Err(LazyWorkspaceError::UnsupportedNode),
                })
            }
            .await;
            match mutation {
                Ok(mutation) => Ok(Owned::new(mutation, namespace.allocation, namespace.bytes)),
                Err(error) => {
                    crate::kernel::retire_allocation(namespace.allocation, namespace.bytes, ctx)?;
                    Err(error)
                }
            }
        })
    }

    async fn stage_source_content(
        transaction: &mut crate::Transaction<A, O>,
        source: &mut DemandBlobSource<'_, D>,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::StagedContent, LazyWorkspaceError> {
        crate::heap_future::in_borrowed_frame!(ctx, |ctx| async move {
            let length = source.length;
            let staged = transaction
                .checkout
                .stage_content_with_accounting(source, length, ctx, cancellation(ctx))
                .await;
            if let Some(observation) = source.observation_error {
                ctx.invalidate_receipt(observation);
            }
            let content = staged.map_err(|error| {
                source.failure.take().map_or_else(
                    || LazyWorkspaceError::from(error),
                    LazyWorkspaceError::Demand,
                )
            })?;
            if content.logical_bytes() != length {
                return Err(DemandError::StaleVersion.into());
            }
            Ok(content)
        })
    }

    async fn reconcile_source_directory(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        destination: &str,
        source: SourceReference,
        id: FileId,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        let old = transaction
            .checkout
            .directory_binding_with_accounting(id, ctx, cancellation(ctx))
            .await?;
        let result = async {
            // A second live source binding is not a rename. Never admit directory hard links.
            if let Some(node) = self.source_lookup(&source, &old.value, ctx).await?
                && self.source_file_id(&node.file_identity, ctx)? == id
            {
                return Err(crate::FsError::FileIdentityCollision.into());
            }
            accounted!(ctx, |work, cancel| transaction.rename(
                &old.value,
                destination,
                false,
                work,
                cancel
            ))?;
            Ok(())
        }
        .await;
        old.retire(ctx)?;
        result
    }
}
