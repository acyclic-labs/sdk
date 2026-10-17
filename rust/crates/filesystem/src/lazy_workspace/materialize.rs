use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
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
        let result = async {
            let mut namespace = self.namespace_path(path, ctx)?;
            if node.kind == SourceNodeKind::Directory {
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
                let record = match staged {
                    Ok(record) => record,
                    Err(error) => {
                        namespace.retire(ctx)?;
                        return Err(error);
                    }
                };
                let mut directories = match Owned::allocate(1, ctx) {
                    Ok(directories) => directories,
                    Err(error) => {
                        namespace.retire(ctx)?;
                        return Err(error.into());
                    }
                };
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
                        self.reconcile_source_directory(
                            transaction,
                            path,
                            source,
                            record.file_id,
                            ctx,
                        )
                        .await?;
                    }
                    result => result?,
                }
                return Ok(());
            }
            if node.kind == SourceNodeKind::RegularFile {
                if node
                    .logical_bytes
                    .ok_or(LazyWorkspaceError::NotRegularFile)?
                    > maximum
                {
                    namespace.retire(ctx)?;
                    return Err(LazyWorkspaceError::TooLarge);
                }
                let id = match self.source_file_id(&node.file_identity, ctx) {
                    Ok(id) => id,
                    Err(error) => {
                        namespace.retire(ctx)?;
                        return Err(error);
                    }
                };
                match accounted!(ctx, |work, cancel| transaction
                    .checkout
                    .read_file_record_by_id(id, work, cancel))
                {
                    Ok(record) => {
                        let mut links = Owned::allocate(1, ctx)?;
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
                        return Ok(());
                    }
                    Err(LazyWorkspaceError::Filesystem(crate::FsError::NotFound)) => {}
                    Err(error) => {
                        namespace.retire(ctx)?;
                        return Err(error);
                    }
                }
            }
            mutations = Some(match Owned::allocate(2, ctx) {
                Ok(mutations) => mutations,
                Err(error) => {
                    namespace.retire(ctx)?;
                    return Err(error.into());
                }
            });
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
                        let mut source = DemandBlobSource {
                            provider: self.source.as_ref(),
                            source,
                            path: &namespace.value,
                            version: node.version,
                            offset: 0,
                            length,
                            work: None,
                            failure: None,
                        };
                        let content = transaction
                            .checkout
                            .stage_content_with_accounting(
                                &mut source,
                                length.max(1),
                                ctx,
                                cancellation(ctx),
                            )
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
            let mutation = match mutation {
                Ok(mutation) => Owned::new(mutation, namespace.allocation, namespace.bytes),
                Err(error) => {
                    crate::kernel::retire_allocation(namespace.allocation, namespace.bytes, ctx)?;
                    return Err(error);
                }
            };
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
        }
        .await;
        if let Some(pending) = mutations {
            pending.retire(ctx)?;
        }
        result
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
