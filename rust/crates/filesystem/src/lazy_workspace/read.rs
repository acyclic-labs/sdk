use super::*;
use crate::kernel::{KernelAccounting, OwnedSourceChunk};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore, D: DemandSource + 'static, S: LazyWorkspaceStore>
    LazyWorkspace<A, O, D, S>
{
    /// Reads one complete regular file under a byte limit.
    pub async fn read(
        &self,
        path: &str,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        self.read_current(path, None, maximum_bytes, accounting)
            .await
    }

    /// Reads one bounded range from the resolved current object.
    pub async fn read_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        self.read_current(
            path,
            Some(crate::ByteRange { offset, length }),
            length,
            accounting,
        )
        .await
    }

    /// Reads an opaque no-follow link target under a byte limit.
    pub async fn read_link(
        &self,
        path: &str,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        let lookup = self.lookup(path, accounting).await?;
        let result = self
            .read_unauthored(&lookup.0, path, None, maximum_bytes, accounting)
            .await;
        self.retire_lookup(lookup, accounting)?;
        result
    }

    async fn read_current(
        &self,
        path: &str,
        range: Option<crate::ByteRange>,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        let lookup = self.lookup(path, accounting).await?;
        if lookup.0.stat().kind != SourceNodeKind::RegularFile {
            self.retire_lookup(lookup, accounting)?;
            return Err(LazyWorkspaceError::NotRegularFile);
        }
        let result = self
            .read_unauthored(&lookup.0, path, range, maximum_bytes, accounting)
            .await;
        self.retire_lookup(lookup, accounting)?;
        result
    }

    /// Reads an exact immutable authored/source cut.
    pub async fn read_at_snapshot(
        &self,
        snapshot: LazySnapshotRef,
        path: &str,
        range: Option<crate::ByteRange>,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<(OwnedSourceChunk, bool), LazyWorkspaceError> {
        let (lookup, used, allocation) =
            self.lookup_at_snapshot(snapshot, path, accounting).await?;
        let lookup = lookup.ok_or(LazyWorkspaceError::NotFound)?;
        let result = match &lookup {
            LazyLookup::Authored { path, .. } => {
                let mut checkout = self
                    .checkout(
                        crate::model::GenerationSelector::Exact(snapshot.authored_generation),
                        accounting,
                    )
                    .await?;
                let namespace = self.namespace_path(path, accounting)?;
                let result = self
                    .read_checkout(
                        &mut checkout.value,
                        &namespace.value,
                        range,
                        maximum_bytes,
                        accounting,
                    )
                    .await;
                namespace.retire(accounting)?;
                checkout.retire(accounting)?;
                result.map(|bytes| (bytes, used))
            }
            _ => {
                let source = matches!(lookup, LazyLookup::Source { .. });
                if source && snapshot.source != self.source.reference() {
                    Err(LazyWorkspaceError::StaleSource)
                } else {
                    self.read_unauthored(&lookup, path, range, maximum_bytes, accounting)
                        .await
                        .map(|bytes| (bytes, used || source))
                }
            }
        };
        self.retire_lookup((lookup, allocation), accounting)?;
        result
    }

    pub(crate) async fn read_unauthored(
        &self,
        lookup: &LazyLookup,
        path: &str,
        range: Option<crate::ByteRange>,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        let kind = lookup.stat().kind;
        if kind != SourceNodeKind::RegularFile && kind != SourceNodeKind::SymbolicLink {
            return Err(LazyWorkspaceError::NotRegularFile);
        }
        let range = if kind == SourceNodeKind::RegularFile {
            let length = lookup
                .stat()
                .logical_bytes
                .ok_or(LazyWorkspaceError::NotRegularFile)?;
            let range = range.unwrap_or(crate::ByteRange { offset: 0, length });
            if range.length > maximum_bytes {
                return Err(LazyWorkspaceError::TooLarge);
            }
            Some(range)
        } else {
            None
        };
        match lookup {
            LazyLookup::Source { source, node } => {
                let namespace = self.namespace_path(path, accounting)?;
                let result = {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(original) = accounting.native_work() {
                        Some(
                            if let Some(range) = range {
                                self.source
                                    .read_range(
                                        *source,
                                        &namespace.value,
                                        node.version,
                                        range.offset,
                                        range.length,
                                        original,
                                        cancellation,
                                    )
                                    .await
                            } else {
                                self.source
                                    .read_link(
                                        *source,
                                        &namespace.value,
                                        node.version,
                                        maximum_bytes,
                                        original,
                                        cancellation,
                                    )
                                    .await
                            }
                            .map_err(LazyWorkspaceError::from),
                        )
                    } else {
                        None
                    }
                    #[cfg(target_arch = "wasm32")]
                    {
                        None::<Result<OwnedSourceChunk, LazyWorkspaceError>>
                    }
                };
                let result = if let Some(result) = result {
                    result
                } else {
                    let budget = accounting.remaining()?;
                    let result = if let Some(range) = range {
                        self.source
                            .read_range(
                                *source,
                                &namespace.value,
                                node.version,
                                range.offset,
                                range.length,
                                budget,
                                cancellation,
                            )
                            .await
                    } else {
                        self.source
                            .read_link(
                                *source,
                                &namespace.value,
                                node.version,
                                maximum_bytes,
                                budget,
                                cancellation,
                            )
                            .await
                    };
                    accept_receipt(result, accounting)
                };
                namespace.retire(accounting)?;
                let bytes = result?;
                if bytes.bytes.len() as u64 > maximum_bytes {
                    return Err(LazyWorkspaceError::TooLarge);
                }
                Ok(bytes)
            }
            LazyLookup::Shadow { record, .. } => {
                let detached = {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(original) = accounting.native_work() {
                        Some(
                            crate::facade::DetachedFile::from_record(
                                self.workspace.volume.clone(),
                                *record,
                                original,
                                cancellation,
                            )
                            .map_err(LazyWorkspaceError::from),
                        )
                    } else {
                        None
                    }
                    #[cfg(target_arch = "wasm32")]
                    {
                        None::<Result<crate::facade::DetachedFile<A, O>, LazyWorkspaceError>>
                    }
                };
                let detached = if let Some(detached) = detached {
                    detached?
                } else {
                    accept_receipt(
                        crate::facade::DetachedFile::from_record(
                            self.workspace.volume.clone(),
                            *record,
                            accounting.remaining()?,
                            cancellation,
                        ),
                        accounting,
                    )?
                };
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(original) = accounting.native_work() {
                    return if let Some(range) = range {
                        let read = detached.read_range(range, original, cancellation).await?;
                        Ok(OwnedSourceChunk::intrinsic(
                            read.bytes,
                            read.logical_owned_bytes,
                        ))
                    } else {
                        let read = detached.read_symbolic_link(original, cancellation).await?;
                        if read.bytes.len() as u64 > maximum_bytes {
                            return Err(LazyWorkspaceError::TooLarge);
                        }
                        Ok(read)
                    };
                }
                if let Some(range) = range {
                    let read = accept_receipt(
                        detached
                            .read_range(range, accounting.remaining()?, cancellation)
                            .await,
                        accounting,
                    )?;
                    Ok(OwnedSourceChunk::claimed(
                        read.bytes,
                        read.logical_owned_bytes,
                    ))
                } else {
                    let read = accept_receipt(
                        detached
                            .read_symbolic_link(accounting.remaining()?, cancellation)
                            .await,
                        accounting,
                    )?;
                    if read.bytes.len() as u64 > maximum_bytes {
                        return Err(LazyWorkspaceError::TooLarge);
                    }
                    Ok(read)
                }
            }
            LazyLookup::Authored { path, .. } => {
                let mut checkout = self
                    .checkout(crate::model::GenerationSelector::Head, accounting)
                    .await?;
                let namespace = self.namespace_path(path, accounting)?;
                let result = self
                    .read_checkout(
                        &mut checkout.value,
                        &namespace.value,
                        range,
                        maximum_bytes,
                        accounting,
                    )
                    .await;
                namespace.retire(accounting)?;
                checkout.retire(accounting)?;
                result
            }
        }
    }

    async fn read_checkout(
        &self,
        checkout: &mut crate::Checkout<A, O>,
        path: &NamespacePath,
        range: Option<crate::ByteRange>,
        maximum_bytes: u64,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<OwnedSourceChunk, LazyWorkspaceError> {
        let cancellation = cancellation(accounting);
        let lookup = checkout
            .lookup_no_follow_with_metadata_with_accounting(path, accounting, cancellation)
            .await?
            .ok_or(LazyWorkspaceError::NotFound)?;
        let bytes = if lookup.record.kind == FileKind::Regular {
            let range = range.unwrap_or(crate::ByteRange {
                offset: 0,
                length: record_logical_bytes(lookup.record)
                    .ok_or(LazyWorkspaceError::NotRegularFile)?,
            });
            if range.length > maximum_bytes {
                return Err(LazyWorkspaceError::TooLarge);
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                let read = checkout
                    .read_file_range_by_id(lookup.record.file_id, range, original, cancellation)
                    .await?;
                return Ok(OwnedSourceChunk::intrinsic(
                    read.bytes,
                    read.logical_owned_bytes,
                ));
            }
            let read = accept_receipt(
                checkout
                    .read_file_range_by_id(
                        lookup.record.file_id,
                        range,
                        accounting.remaining()?,
                        cancellation,
                    )
                    .await,
                accounting,
            )?;
            OwnedSourceChunk::claimed(read.bytes, read.logical_owned_bytes)
        } else if lookup.record.kind == FileKind::SymbolicLink {
            checkout
                .read_symbolic_link_with_accounting(path, accounting, cancellation)
                .await?
        } else {
            return Err(LazyWorkspaceError::NotRegularFile);
        };
        if bytes.bytes.len() as u64 > maximum_bytes {
            return Err(LazyWorkspaceError::TooLarge);
        }
        Ok(bytes)
    }

    /// Opens one source fact at its exact original version.
    pub(crate) async fn open_source_file(
        &self,
        path: &str,
        source: SourceReference,
        node: SourceNode,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Box<dyn crate::demand::DemandFile>, LazyWorkspaceError> {
        let namespace = self.namespace_path(path, accounting)?;
        let cancellation = cancellation(accounting);
        let result = {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                Some(
                    self.source
                        .open_file(
                            source,
                            &namespace.value,
                            node.version,
                            original,
                            cancellation,
                        )
                        .await
                        .map_err(LazyWorkspaceError::from),
                )
            } else {
                None
            }
            #[cfg(target_arch = "wasm32")]
            {
                None::<Result<Box<dyn crate::demand::DemandFile>, LazyWorkspaceError>>
            }
        };
        let result = if let Some(result) = result {
            result
        } else {
            accept_receipt(
                self.source
                    .open_file(
                        source,
                        &namespace.value,
                        node.version,
                        accounting.remaining()?,
                        cancellation,
                    )
                    .await,
                accounting,
            )
        };
        namespace.retire(accounting)?;
        result
    }
}
