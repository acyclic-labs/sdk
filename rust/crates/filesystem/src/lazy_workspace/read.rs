use super::*;
use crate::kernel::OwnedSourceChunk;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Reads a complete regular file under an explicit byte bound.
    pub async fn read(
        &self,
        path: &str,
        maximum: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        self.read_current(path, None, maximum, SourceNodeKind::RegularFile, ctx)
            .await
    }
    /// Reads one exact regular-file range.
    pub async fn read_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        self.read_current(
            path,
            Some(crate::ByteRange { offset, length }),
            length,
            SourceNodeKind::RegularFile,
            ctx,
        )
        .await
    }
    /// Reads opaque symbolic-link bytes without following the link.
    pub async fn read_link(
        &self,
        path: &str,
        maximum: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        self.read_current(path, None, maximum, SourceNodeKind::SymbolicLink, ctx)
            .await
    }
    async fn read_current(
        &self,
        path: &str,
        range: Option<crate::ByteRange>,
        maximum: u64,
        kind: SourceNodeKind,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        let lookup = self.lookup(path, ctx).await?;
        let result = if lookup.0.stat().kind != kind {
            Err(LazyWorkspaceError::NotRegularFile)
        } else {
            self.read_resolved(
                &lookup.0,
                path,
                range,
                maximum,
                crate::model::GenerationSelector::Head,
                ctx,
            )
            .await
        };
        self.retire_lookup(lookup, ctx)?;
        result
    }
    /// Reads an immutable cut and reports actual source participation.
    pub async fn read_at_snapshot(
        &self,
        cut: LazySnapshotRef,
        path: &str,
        range: Option<crate::ByteRange>,
        maximum: u64,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<(OwnedSourceChunk, bool), LazyWorkspaceError> {
        let (lookup, used, guard) = self.lookup_at_snapshot(cut, path, ctx).await?;
        let lookup = lookup.ok_or(LazyWorkspaceError::NotFound)?;
        let source = matches!(lookup, LazyLookup::Source { .. });
        let result = if source && cut.source != self.source.reference() {
            Err(LazyWorkspaceError::StaleSource)
        } else {
            self.read_resolved(
                &lookup,
                path,
                range,
                maximum,
                crate::model::GenerationSelector::Exact(cut.authored_generation),
                ctx,
            )
            .await
            .map(|bytes| (bytes, used || source))
        };
        self.retire_lookup((lookup, guard), ctx)?;
        result
    }
    pub(crate) async fn read_resolved(
        &self,
        lookup: &LazyLookup,
        path: &str,
        range: Option<crate::ByteRange>,
        maximum: u64,
        generation: crate::model::GenerationSelector,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        let stat = lookup.stat();
        let range = match stat.kind {
            SourceNodeKind::RegularFile => {
                let range = range.unwrap_or(crate::ByteRange {
                    offset: 0,
                    length: stat
                        .logical_bytes
                        .ok_or(LazyWorkspaceError::NotRegularFile)?,
                });
                if range.length > maximum {
                    return Err(LazyWorkspaceError::TooLarge);
                }
                Some(range)
            }
            SourceNodeKind::SymbolicLink => None,
            _ => return Err(LazyWorkspaceError::NotRegularFile),
        };
        let bytes = match lookup {
            LazyLookup::Authored { path, .. } => {
                let mut checkout = self.checkout(generation, ctx).await?;
                let result = async {
                    let namespace = self.namespace_path(path, ctx)?;
                    let result = async {
                        if let Some(range) = range {
                            let read = checkout
                                .value
                                .read_file_range_with_accounting(
                                    &namespace.value,
                                    range,
                                    ctx,
                                    cancellation(ctx),
                                )
                                .await?;
                            Ok(read_chunk(read.bytes, read.logical_owned_bytes, ctx))
                        } else {
                            checkout
                                .value
                                .read_symbolic_link_with_accounting(
                                    &namespace.value,
                                    ctx,
                                    cancellation(ctx),
                                )
                                .await
                                .map_err(LazyWorkspaceError::from)
                        }
                    }
                    .await;
                    namespace.retire(ctx)?;
                    result
                }
                .await;
                checkout.retire(ctx)?;
                result?
            }
            LazyLookup::Shadow { record, .. } => {
                let detached = accounted!(ctx, |work, cancel| async {
                    crate::facade::DetachedFile::from_record(
                        self.workspace.volume.clone(),
                        *record,
                        work,
                        cancel,
                    )
                })?;
                if let Some(range) = range {
                    let read =
                        accounted!(ctx, |work, cancel| detached.read_range(range, work, cancel))?;
                    read_chunk(read.bytes, read.logical_owned_bytes, ctx)
                } else {
                    accounted!(ctx, |work, cancel| detached
                        .read_symbolic_link(work, cancel))?
                }
            }
            LazyLookup::Source { source, node } => {
                let namespace = self.namespace_path(path, ctx)?;
                let result = accounted!(ctx, |work, cancel| async {
                    if let Some(range) = range {
                        self.source
                            .read_range(
                                *source,
                                &namespace.value,
                                node.version,
                                range.offset,
                                range.length,
                                work,
                                cancel,
                            )
                            .await
                    } else {
                        self.source
                            .read_link(
                                *source,
                                &namespace.value,
                                node.version,
                                maximum,
                                work,
                                cancel,
                            )
                            .await
                    }
                });
                namespace.retire(ctx)?;
                result?
            }
        };
        if bytes.bytes.len() as u64 > maximum {
            drop(bytes);
            return Err(LazyWorkspaceError::TooLarge);
        }
        Ok(bytes)
    }
    pub(crate) async fn open_source_file(
        &self,
        path: &str,
        source: SourceReference,
        node: SourceNode,
        ctx: &mut KernelAccounting<'_>,
    ) -> Result<Box<dyn crate::demand::DemandFile>, LazyWorkspaceError> {
        if node.kind != SourceNodeKind::RegularFile {
            return Err(LazyWorkspaceError::NotRegularFile);
        }
        let namespace = self.namespace_path(path, ctx)?;
        let result = accounted!(ctx, |work, cancel| self.source.open_file(
            source,
            &namespace.value,
            node.version,
            work,
            cancel
        ));
        namespace.retire(ctx)?;
        result
    }
}

fn read_chunk(
    bytes: acyclic_stream::OwnedBytes,
    owned: u64,
    ctx: &KernelAccounting<'_>,
) -> OwnedSourceChunk {
    #[cfg(not(target_arch = "wasm32"))]
    if ctx.native_work().is_some() {
        return OwnedSourceChunk::intrinsic(bytes, owned);
    }
    OwnedSourceChunk::claimed(bytes, owned)
}
