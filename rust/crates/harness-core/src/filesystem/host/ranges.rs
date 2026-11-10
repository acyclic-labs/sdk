use super::*;
mod unindexed;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
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
        let metadata_path =
            content_metadata_path_io(reference.volume(), reference.path(), true, Some(io))?;
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", reference.path()), work)
        })?;
        let metadata =
            read_generation_if_present(selected, &metadata_path, MAXIMUM_METADATA, io).await?;
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
                matching_publication(opened, selected, &staged, &metadata_path, &metadata, io)
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
                unindexed::authenticate(selected, reference, &path, io).await?;
                selected.clone()
            }
        } else {
            unindexed::authenticate(selected, reference, &path, io).await?;
            selected.clone()
        };
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
}

const MAXIMUM_METADATA: u64 = 6
    * (crate::conversation::MAX_PATH_BYTES as u64
        + 32 * crate::conversation::MAX_LABEL_BYTES as u64
        + 1024);

// Resolve the original operation before trusting an index inherited by a join.
// Both path resolution and explicit ranges use this same canonical-record check.
pub(super) async fn matching_publication<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    opened: &Workspace<A, O>,
    selected: &acyclic_fs::Generation<A, O>,
    staged: &FileRef,
    metadata_path: &str,
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
            read_generation(&original, metadata_path, MAXIMUM_METADATA, Some(io)).await?;
        if original_metadata.as_ref() != metadata {
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
