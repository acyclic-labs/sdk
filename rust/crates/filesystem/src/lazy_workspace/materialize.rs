use super::*;
use crate::kernel::{KernelAccounting, Owned};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    pub(super) async fn populate_source_node(
        &self,
        transaction: &mut crate::Transaction<A, O>,
        requested: &str,
        source: SourceReference,
        node: SourceNode,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(), LazyWorkspaceError> {
        accounting.check()?;
        let cancellation = cancellation(accounting);
        let metadata = source_file_metadata(node.metadata);
        let namespace = self.namespace_path(requested, accounting)?;
        let mut mutations = Owned::allocate(2, accounting)?;
        let mutation = match node.kind {
            SourceNodeKind::RegularFile => {
                let length = node
                    .logical_bytes
                    .ok_or(LazyWorkspaceError::NotRegularFile)?;
                if length > maximum_bytes {
                    namespace.retire(accounting)?;
                    mutations.retire(accounting)?;
                    return Err(LazyWorkspaceError::TooLarge);
                }
                let content = self
                    .stage_regular_source_content(
                        &transaction.checkout,
                        &namespace.value,
                        source,
                        &node,
                        accounting,
                    )
                    .await?;
                AuthoredMutation::CreateFileFromContent {
                    path: namespace.value,
                    content,
                    metadata,
                    file_id: Some(self.source_file_id(&node.file_identity, accounting)?),
                }
            }
            SourceNodeKind::Directory => AuthoredMutation::CreateDirectory {
                path: namespace.value,
                metadata,
            },
            SourceNodeKind::SymbolicLink => {
                let lookup = LazyLookup::Source { source, node };
                let target = self
                    .read_unauthored(&lookup, requested, None, maximum_bytes, accounting)
                    .await?;
                AuthoredMutation::CreateSymbolicLink {
                    path: namespace.value,
                    target: target.bytes,
                    metadata,
                }
            }
            SourceNodeKind::Fifo | SourceNodeKind::Socket => AuthoredMutation::CreateEmptySpecial {
                path: namespace.value,
                kind: if node.kind == SourceNodeKind::Fifo {
                    FileKind::Fifo
                } else {
                    FileKind::Socket
                },
                metadata,
            },
            SourceNodeKind::CharacterDevice | SourceNodeKind::BlockDevice => {
                let (major, minor) = node.device.ok_or(LazyWorkspaceError::UnsupportedNode)?;
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
            SourceNodeKind::Unsupported => {
                namespace.retire(accounting)?;
                mutations.retire(accounting)?;
                return Err(LazyWorkspaceError::UnsupportedNode);
            }
        };
        crate::Transaction::<A, O>::push_mutation(
            &mut mutations,
            Owned::new(mutation, namespace.allocation, namespace.bytes),
            accounting,
        )?;
        if !matches!(
            node.kind,
            SourceNodeKind::Directory | SourceNodeKind::RegularFile
        ) {
            let namespace = self.namespace_path(requested, accounting)?;
            let mutation = AuthoredMutation::Reidentify {
                path: namespace.value,
                file_id: self.source_file_id(&node.file_identity, accounting)?,
            };
            crate::Transaction::<A, O>::push_mutation(
                &mut mutations,
                Owned::new(mutation, namespace.allocation, namespace.bytes),
                accounting,
            )?;
        }
        let applied = transaction
            .checkout
            .apply_authored_transaction_with_accounting(mutations, accounting, cancellation)
            .await?;
        applied.retire(accounting)?;
        Ok(())
    }

    pub(super) async fn stage_regular_source_content(
        &self,
        checkout: &crate::Checkout<A, O>,
        path: &NamespacePath,
        reference: SourceReference,
        node: &SourceNode,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<crate::StagedContent, LazyWorkspaceError> {
        let length = node
            .logical_bytes
            .ok_or(LazyWorkspaceError::NotRegularFile)?;
        let mut source = DemandBlobSource {
            provider: self.source.as_ref(),
            source: reference,
            path,
            version: node.version,
            offset: 0,
            length,
            work: None,
            failure: None,
        };
        let result = checkout
            .stage_content_with_accounting(
                &mut source,
                length.max(1),
                accounting,
                cancellation(accounting),
            )
            .await;
        let staged = result.map_err(|error| {
            source
                .failure
                .take()
                .map_or_else(|| error.into(), LazyWorkspaceError::Demand)
        })?;
        if staged.logical_bytes() != length {
            return Err(LazyWorkspaceError::Demand(DemandError::StaleVersion));
        }
        Ok(staged)
    }
}
