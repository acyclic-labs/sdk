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
                mutations = Some(Owned::allocate(2, ctx)?);
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
                    mutations.as_mut().expect("pending mutations"),
                    mutation,
                    ctx,
                )?;
                if !matches!(
                    node.kind,
                    SourceNodeKind::Directory | SourceNodeKind::RegularFile
                ) {
                    let namespace = self.namespace_path(path, ctx)?;
                    let id = match self.source_file_id(&node.file_identity, ctx) {
                        Ok(id) => id,
                        Err(error) => {
                            namespace.retire(ctx)?;
                            return Err(error);
                        }
                    };
                    crate::Transaction::<A, O>::push_mutation(
                        mutations.as_mut().expect("pending mutations"),
                        Owned::new(
                            AuthoredMutation::Reidentify {
                                path: namespace.value,
                                file_id: id,
                            },
                            namespace.allocation,
                            namespace.bytes,
                        ),
                        ctx,
                    )?;
                }
                transaction
                    .apply_authored(
                        mutations.take().expect("complete mutations"),
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
            match accounted!(ctx, |work, cancel| transaction
                .checkout
                .read_file_record_by_id(id, work, cancel))
            {
                Ok(record) => {
                    let mut links = Owned::allocate(1, ctx)?;
                    let namespace = namespace.take().ok_or(crate::FsError::InvalidDiff)?;
                    crate::Transaction::<A, O>::push_mutation(
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
                    )?;
                    transaction
                        .checkout
                        .apply_mutations(links, ctx, cancellation(ctx))
                        .await?;
                    return Ok(true);
                }
                Err(LazyWorkspaceError::Filesystem(crate::FsError::NotFound)) => {}
                Err(error) => return Err(error),
            }
            Ok(false)
        })
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
                        let content = self
                            .stage_source_content(
                                transaction,
                                source,
                                node,
                                &namespace.value,
                                maximum,
                                ctx,
                            )
                            .await?;
                        let file_id = Some(self.source_file_id(&node.file_identity, ctx)?);
                        AuthoredMutation::CreateFileFromContent {
                            path: namespace.value,
                            content,
                            metadata,
                            file_id,
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
                            path: namespace.value,
                            target: target.bytes,
                            metadata,
                        }
                    }
                    SourceNodeKind::Fifo | SourceNodeKind::Socket => {
                        AuthoredMutation::CreateEmptySpecial {
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
        &self,
        transaction: &mut crate::Transaction<A, O>,
        source: SourceReference,
        node: SourceNode,
        namespace: &NamespacePath,
        maximum: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::StagedContent, LazyWorkspaceError> {
        crate::heap_future::in_borrowed_frame!(ctx, |ctx| async move {
            let length = node
                .logical_bytes
                .ok_or(LazyWorkspaceError::NotRegularFile)?;
            if length > maximum {
                return Err(LazyWorkspaceError::TooLarge);
            }
            let mut source = DemandBlobSource {
                provider: self.source.as_ref(),
                source,
                path: namespace,
                version: node.version,
                offset: 0,
                length,
                work: None,
                failure: None,
            };
            let content = transaction
                .checkout
                .stage_content_with_accounting(&mut source, length.max(1), ctx, cancellation(ctx))
                .await
                .map_err(|error| {
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
            if let Some(node) = self.source_lookup(source, &old.value, ctx).await? {
                if self.source_file_id(&node.file_identity, ctx)? == id {
                    return Err(crate::FsError::FileIdentityCollision.into());
                }
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
