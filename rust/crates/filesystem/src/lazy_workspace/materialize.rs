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
        let mut mutations = Some(Owned::allocate(2, ctx)?);
        let result = async {
            let namespace = self.namespace_path(path, ctx)?;
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
                        let content = self
                            .stage_regular_source_content(
                                &transaction.checkout,
                                &namespace.value,
                                source,
                                &node,
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
                    SourceNodeKind::Directory => AuthoredMutation::CreateDirectory {
                        path: namespace.value,
                        metadata,
                    },
                    SourceNodeKind::SymbolicLink => {
                        let target = self
                            .read_resolved(
                                &LazyLookup::Source { source, node },
                                path,
                                None,
                                maximum,
                                crate::model::GenerationSelector::Head,
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
    pub(super) async fn stage_regular_source_content(
        &self,
        checkout: &crate::Checkout<A, O>,
        path: &NamespacePath,
        source: SourceReference,
        node: &SourceNode,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<crate::StagedContent, LazyWorkspaceError> {
        let length = node
            .logical_bytes
            .ok_or(LazyWorkspaceError::NotRegularFile)?;
        let mut source = DemandBlobSource {
            provider: self.source.as_ref(),
            source,
            path,
            version: node.version,
            offset: 0,
            length,
            work: None,
            failure: None,
        };
        let staged = checkout
            .stage_content_with_accounting(&mut source, length.max(1), ctx, cancellation(ctx))
            .await
            .map_err(|error| {
                source.failure.take().map_or_else(
                    || LazyWorkspaceError::from(error),
                    LazyWorkspaceError::Demand,
                )
            })?;
        if staged.logical_bytes() != length {
            return Err(DemandError::StaleVersion.into());
        }
        Ok(staged)
    }
}
