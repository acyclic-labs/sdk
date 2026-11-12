use super::*;
mod unindexed;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Select the original publication, even when capture has rebound the
    /// returned file to a later identical generation. Point comparisons use
    /// the shared content comparison, skipping matching immutable chunks and
    /// comparing partial chunks under the original byte budget. No history is read.
    pub(in crate::filesystem) async fn original_file_progress(
        &self,
        file: &FileRef,
        io: &PreparationIo,
    ) -> Result<Option<GenerationRef>> {
        let reference = bounded_io::content_workspace(&self.provider, file.volume(), Some(io))?;
        let _reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        let workspace = self.open(&reference, Some(io)).await?;
        let generation = self.file_generation(file, Some(io))?;
        let _generation_hold = io.guard_owned(generation.logical_owned_bytes()?)?;
        let selected = self.generation(&workspace, &generation, Some(io)).await?;
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", file.path()), work)
        })?;
        let _path_hold = io.guard_owned(path.capacity() as u64)?;
        let original =
            Self::authenticate_selected_file(&workspace, &selected, file, &path, io).await?;
        let base = Self::publication_base(&workspace, &original, io).await?;
        if io.sdk(
            original
                .matches_optional_content(&base, &path, io, io.cancellation())
                .await,
        )? {
            return Ok(None);
        }
        bounded_io::retain(&original, Some(io)).await?;
        self.generation_ref_id(original.id(), Some(io)).map(Some)
    }

    async fn publication_base(
        workspace: &Workspace<A, O>,
        original: &Generation<A, O>,
        io: &PreparationIo,
    ) -> Result<Generation<A, O>> {
        let (parents, _allocation) = io.sdk(original.parents(io, io.cancellation()).await)?;
        let [parent] = parents.as_slice() else {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Invalid,
                "original file publication has no unique base",
            ));
        };
        io.sdk(workspace.generation(*parent, io, io.cancellation()).await)
    }

    /// Compare only explicitly published paths, at the original immutable cut.
    /// Disabled queries never call this boundary; one changed path ends the
    /// comparison without inspecting the rest of the publication.
    pub(in crate::filesystem) async fn original_changes_progress(
        &self,
        volume: &VolumeRef,
        changes: &crate::conversation::ContentChanges,
        io: Option<&PreparationIo>,
    ) -> Result<bool> {
        let unmeasured;
        let io = if let Some(io) = io {
            io
        } else {
            unmeasured = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &unmeasured
        };
        let reference = bounded_io::content_workspace(&self.provider, volume, Some(io))?;
        let _reference_hold = io.guard_owned(reference.logical_owned_bytes()?)?;
        let workspace = self.open(&reference, Some(io)).await?;
        let original = self
            .generation(&workspace, &changes.generation, Some(io))
            .await?;
        let base = Self::publication_base(&workspace, &original, io).await?;
        for member in &changes.members {
            let path = crate::executor::with_history_io(Some(io), |work| {
                crate::executor::preparation_format(format_args!("/{}", member.path()), work)
            })?;
            let _path_hold = io.guard_owned(path.capacity() as u64)?;
            if !io.sdk(
                original
                    .matches_optional_content(&base, &path, io, io.cancellation())
                    .await,
            )? {
                bounded_io::retain(&original, Some(io)).await?;
                return Ok(true);
            }
        }
        Ok(false)
    }
    #[cfg(test)]
    pub(in crate::filesystem) async fn read_pinned_range(
        &self,
        reference: &FileRef,
        range: acyclic_fs::ByteRange,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        crate::executor::with_history_io(Some(io), |work| reference.validate(work))?;
        if reference.volume().provider() != &self.provider
            || range
                .offset
                .checked_add(range.length)
                .is_none_or(|end| end > reference.descriptor().byte_length())
        {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Invalid,
                "range or content provider mismatch",
            ));
        }
        let workspace =
            bounded_io::content_workspace(&self.provider, reference.volume(), Some(io))?;
        let opened = self.open(&workspace, Some(io)).await?;
        let generation = self.file_generation(reference, Some(io))?;
        let selected = self.generation(&opened, &generation, Some(io)).await?;
        Self::read_selected_range(&opened, &selected, reference, range, io).await
    }

    pub(in crate::filesystem) async fn read_selected_range(
        opened: &Workspace<A, O>,
        selected: &acyclic_fs::Generation<A, O>,
        reference: &FileRef,
        range: acyclic_fs::ByteRange,
        io: &PreparationIo,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", reference.path()), work)
        })?;
        let original =
            Self::authenticate_selected_file(opened, selected, reference, &path, io).await?;
        let chunk = io.sdk(
            selected
                .read_range_if_unchanged(&original, &path, range, io, io.cancellation())
                .await,
        )?;
        bounded_io::retain(selected, Some(io)).await?;
        if io.original_native_work().is_none() {
            io.retain_owned(chunk.bytes.retained_capacity().unwrap_or(0) as u64)?;
        }
        Ok(chunk.bytes)
    }

    /// Resolve one reference against its stored publication before either a
    /// read or a conditional write trusts its caller-supplied descriptor.
    pub(super) async fn authenticate_selected_file(
        opened: &Workspace<A, O>,
        selected: &acyclic_fs::Generation<A, O>,
        reference: &FileRef,
        path: &str,
        io: &PreparationIo,
    ) -> Result<acyclic_fs::Generation<A, O>> {
        let metadata_key = content_metadata_key_io(reference.volume(), reference.path(), Some(io))?;
        let metadata =
            read_file_annotation(selected, reference.path(), &metadata_key, None, Some(io)).await?;
        let original = if let Some(metadata) = metadata {
            let (staged, _staged_hold) = decode_content_metadata(&metadata, io)?;
            if staged.volume() != reference.volume() || staged.path() != reference.path() {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "range metadata differs from the requested file",
                ));
            }
            let original =
                matching_publication(opened, selected, &staged, &metadata_key, &metadata, io)
                    .await?;
            if let Some(original) = original {
                if staged.descriptor() != reference.descriptor()
                    || staged.display_name() != reference.display_name()
                {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Conflict,
                        "range metadata differs from the requested file",
                    ));
                }
                original
            } else {
                unindexed::authenticate(selected, reference, path, io).await?;
                selected.clone()
            }
        } else {
            unindexed::authenticate(selected, reference, path, io).await?;
            selected.clone()
        };
        Ok(original)
    }
}

// Resolve the original operation before trusting an index inherited by a join.
// Both path resolution and explicit ranges use this same canonical-record check.
pub(super) async fn matching_publication<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    opened: &Workspace<A, O>,
    selected: &acyclic_fs::Generation<A, O>,
    staged: &FileRef,
    metadata_key: &AttributeName,
    metadata: &[u8],
    io: &PreparationIo,
) -> Result<Option<acyclic_fs::Generation<A, O>>> {
    let mut key = [0; 16];
    hex::decode_to_slice(staged.version(), &mut key).map_err(|_| {
        stock_boundary(
            Some(io),
            crate::wire::ErrorCode::Storage,
            "range metadata has no publication operation",
        )
    })?;
    io.charge(WorkCounters {
        items_examined: staged.version().len() as u64,
        ..Default::default()
    })?;
    let original = io
        .sdk(
            opened
                .operation_generation(FilesystemKey::from_bytes(key), io, io.cancellation())
                .await,
        )?
        .ok_or_else(|| {
            stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "range publication is absent",
            )
        })?;
    if selected.id() != original.id() {
        let original_metadata =
            read_file_annotation(&original, staged.path(), metadata_key, None, Some(io)).await?;
        if original_metadata.as_deref() != Some(metadata) {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "range index differs from its publication",
            ));
        }
        let path = crate::executor::preparation_format(
            format_args!("/{}", staged.path()),
            &mut Some(&mut io.history_io()),
        )?;
        let _path_hold = io.guard_owned(path.capacity() as u64)?;
        if !io.sdk(
            selected
                .matches_file_payload(&original, &path, io, io.cancellation())
                .await,
        )? {
            return Ok(None);
        }
    }
    Ok(Some(original))
}

use crate::executor::stock_boundary;

#[cfg(test)]
mod tests;
