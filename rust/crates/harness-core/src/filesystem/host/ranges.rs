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
            fork_preparer_io::content_workspace(&self.provider, reference.volume(), Some(io))?;
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
        let metadata_path = content_metadata_path_io(reference.path(), true, Some(io))?;
        const MAXIMUM_METADATA: u64 = 6
            * (crate::conversation::MAX_PATH_BYTES as u64
                + 32 * crate::conversation::MAX_LABEL_BYTES as u64
                + 1024);
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", reference.path()), work)
        })?;
        let metadata =
            match read_generation(selected, &metadata_path, MAXIMUM_METADATA, Some(io)).await {
                Ok(metadata) => Some(metadata),
                Err(error) if error.code() == crate::wire::ErrorCode::NotFound => None,
                #[cfg(not(target_arch = "wasm32"))]
                Err(Error::NativeWorkspace { source, .. })
                    if matches!(source.as_ref(), acyclic_fs::WorkspaceError::NotFound) =>
                {
                    None
                }
                Err(error) => return Err(error),
            };
        let original = if let Some(metadata) = metadata {
            let staged: FileRef = io.decode_json(&metadata)?;
            if staged.volume() != reference.volume()
                || staged.path() != reference.path()
                || staged.descriptor() != reference.descriptor()
                || staged.display_name() != reference.display_name()
            {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "range metadata differs from the requested file",
                ));
            }
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
            // The operation fixes the original atomic payload + descriptor publication.
            // Comparing the protected index rejects a stale or substituted descriptor;
            // comparing immutable payloads rejects writes that bypassed that index.
            if selected.id() != original.id() {
                let original_metadata =
                    read_generation(&original, &metadata_path, MAXIMUM_METADATA, Some(io)).await?;
                if original_metadata.as_ref() != metadata.as_ref() {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Conflict,
                        "range index differs from its publication",
                    ));
                }
            }
            original
        } else {
            unindexed::authenticate(selected, reference, &path, io).await?;
            selected.clone()
        };
        let chunk = io.sdk(
            selected
                .read_range_if_unchanged(&original, &path, range, io, io.cancellation())
                .await,
        )?;
        fork_preparer_io::retain(selected, Some(io)).await?;
        if io.original_native_work().is_none() {
            io.retain_owned(chunk.bytes.retained_capacity().unwrap_or(0) as u64)?;
        }
        Ok(chunk.bytes)
    }
}

use crate::executor::stock_boundary;

#[cfg(test)]
mod tests;
