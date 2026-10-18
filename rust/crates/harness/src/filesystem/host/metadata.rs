//! Publication descriptors belong to immutable payloads, never to a path forever.
use super::*;

#[cfg(all(test, not(target_arch = "wasm32")))]
thread_local! {
    pub(in crate::filesystem) static TRUST_STALE_METADATA: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) async fn publication_range(
        &self,
        workspace: &Workspace<A, O>,
        selected: &acyclic_fs::Generation<A, O>,
        volume: &VolumeRef,
        path: &str,
        range: acyclic_fs::ByteRange,
        io: &PreparationIo,
    ) -> Result<Option<(FileRef, acyclic_stream::OwnedBytes)>> {
        let metadata_path = content_metadata_path_io(path, true, Some(io))?;
        let _path_hold = io.guard_owned(metadata_path.capacity() as u64)?;
        const MAXIMUM: u64 = 6
            * (crate::conversation::MAX_PATH_BYTES as u64
                + 32 * crate::conversation::MAX_LABEL_BYTES as u64
                + 1024);
        let metadata = match read_generation(selected, &metadata_path, MAXIMUM, Some(io)).await {
            Ok(metadata) => metadata,
            Err(error) if crate::filesystem::fork_preparer::path_absent(&error) => return Ok(None),
            Err(error) => return Err(error),
        };
        let staged: FileRef = io.decode_json(&metadata)?;
        let _staged_hold = io.guard_owned(staged.logical_owned_bytes()?)?;
        if staged.volume() != volume || staged.path() != path {
            return Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "publication metadata belongs to another file",
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
                workspace
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
                read_generation(&original, &metadata_path, MAXIMUM, Some(io)).await?;
            if original_metadata.as_ref() != metadata.as_ref() {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "range index differs from its publication",
                ));
            }
        }
        let original = &original;
        #[cfg(all(test, not(target_arch = "wasm32")))]
        let original = if TRUST_STALE_METADATA.with(std::cell::Cell::get) {
            selected
        } else {
            original
        };
        let requested = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let _requested_hold = io.guard_owned(requested.capacity() as u64)?;
        let result = selected
            .read_range_if_unchanged(original, &requested, range, io, io.cancellation())
            .await;
        // Native capture intentionally excludes private metadata. An old
        // descriptor remains historical, but cannot describe a different payload.
        if matches!(
            &result.0,
            Err(acyclic_fs::WorkspaceError::StaleIdentity | acyclic_fs::WorkspaceError::NotFound)
        ) {
            result.1?;
            return Ok(None);
        }
        let chunk = io.sdk(result)?;
        if io.original_native_work().is_none() {
            io.retain_owned(chunk.bytes.retained_capacity().unwrap_or(0) as u64)?;
        }
        Ok(Some((staged, chunk.bytes)))
    }
}
