use super::metadata::read_file_annotation_at;
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
        let cut = self.select_cut_at(&workspace, &generation, io).await?;
        // A file read at a private cut has no authored publication; its
        // progress is the exact cut it was read at.
        if cut.is_private() {
            return self.cut_ref(&cut, Some(&generation), io).map(Some);
        }
        let selected = cut.into_generation();
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", file.path()), work)
        })?;
        let _path_hold = io.guard_owned(path.capacity() as u64)?;
        let key = content_metadata_key_io(file.volume(), file.path(), Some(io))?;
        let _key_hold = io.guard_owned(key.retained_bytes() as u64)?;
        let original =
            Self::authenticate_selected_file(&workspace, &selected, file, (&path, &key), io)
                .await?;
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
        let root = self.open(&reference, Some(io)).await?;
        let (workspace, _binding) = self
            .content_operation_workspace(&root, changes.operation, false, io)
            .await?
            .ok_or_else(|| io.invalid("original change workspace is absent"))?;
        let publication = io
            .sdk(
                workspace
                    .operation_publication(changes.operation, None, io, io.cancellation())
                    .await,
            )?
            .ok_or_else(|| io.invalid("original change publication is absent"))?;
        let original = publication.generation();
        if original.id().to_bytes().as_slice() != changes.generation.as_resource().key() {
            return Err(io.invalid("change receipt names a different publication generation"));
        }
        let base = io
            .sdk(publication.predecessor(io, io.cancellation()).await)?
            .ok_or_else(|| io.invalid("original change publication has no predecessor"))?;
        if original.id() == base.id() {
            return Ok(false);
        }
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
                bounded_io::retain(original, Some(io)).await?;
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
    ) -> Result<bounded_io::Charged<acyclic_stream::OwnedBytes>> {
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
        let cut = self.select_cut_at(&opened, &generation, io).await?;
        self.read_cut_range(&cut, reference, range, io).await
    }

    pub(in crate::filesystem) async fn read_selected_range(
        opened: &Workspace<A, O>,
        selected: &acyclic_fs::Generation<A, O>,
        reference: &FileRef,
        range: acyclic_fs::ByteRange,
        io: &PreparationIo,
    ) -> Result<bounded_io::Charged<acyclic_stream::OwnedBytes>> {
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", reference.path()), work)
        })?;
        let _path_hold = io.guard_owned(path.capacity() as u64)?;
        let key = content_metadata_key_io(reference.volume(), reference.path(), Some(io))?;
        let _key_hold = io.guard_owned(key.retained_bytes() as u64)?;
        let original =
            Self::authenticate_selected_file(opened, selected, reference, (&path, &key), io)
                .await?;
        let chunk = io.sdk(
            selected
                .read_range_if_unchanged(&original, &path, range, io, io.cancellation())
                .await,
        )?;
        bounded_io::retain(selected, Some(io)).await?;
        io.retain_read_bytes(chunk.bytes)
    }

    /// Resolve one reference against its stored publication before either a
    /// read or a conditional write trusts its caller-supplied descriptor.
    pub(super) async fn authenticate_selected_file(
        opened: &Workspace<A, O>,
        selected: &acyclic_fs::Generation<A, O>,
        reference: &FileRef,
        annotation: (&str, &AttributeName),
        io: &PreparationIo,
    ) -> Result<acyclic_fs::Generation<A, O>> {
        let (path, metadata_key) = annotation;
        let metadata = read_file_annotation_at(selected, path, metadata_key, None, io).await?;
        let original = if let Some(metadata) = metadata {
            let _metadata_hold = io.guard_bytes(&metadata)?;
            let (staged, _staged_hold) = decode_content_metadata(&metadata, io)?;
            if staged.volume() != reference.volume() || staged.path() != reference.path() {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "range metadata differs from the requested file",
                ));
            }
            let original =
                matching_publication(opened, selected, &staged, annotation, &metadata, io).await?;
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
    annotation: (&str, &AttributeName),
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
        let (path, metadata_key) = annotation;
        let original_metadata =
            read_file_annotation_at(&original, path, metadata_key, None, io).await?;
        let _metadata_hold = original_metadata
            .as_ref()
            .map(|metadata| io.guard_bytes(metadata))
            .transpose()?;
        if original_metadata.as_deref() != Some(metadata) {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "range index differs from its publication",
            ));
        }
        if !io.sdk(
            selected
                .matches_file_payload(&original, path, io, io.cancellation())
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
