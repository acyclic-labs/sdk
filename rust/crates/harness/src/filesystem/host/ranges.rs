use super::*;
mod unindexed;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
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
        if let Some((staged, bytes)) = self
            .publication_range(
                &opened,
                &selected,
                reference.volume(),
                reference.path(),
                range,
                io,
            )
            .await?
        {
            if staged.descriptor() != reference.descriptor()
                || staged.display_name() != reference.display_name()
            {
                return Err(stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "range metadata differs from the requested file",
                ));
            }
            fork_preparer_io::retain(&selected, Some(io)).await?;
            return Ok(bytes);
        }
        let path = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{}", reference.path()), work)
        })?;
        let _path_hold = io.guard_owned(path.capacity() as u64)?;
        unindexed::authenticate(&selected, reference, &path, io).await?;
        let chunk = io.sdk(
            selected
                .read_range_if_unchanged(&selected, &path, range, io, io.cancellation())
                .await,
        )?;
        fork_preparer_io::retain(&selected, Some(io)).await?;
        if io.original_native_work().is_none() {
            io.retain_owned(chunk.bytes.retained_capacity().unwrap_or(0) as u64)?;
        }
        Ok(chunk.bytes)
    }
}

use crate::executor::stock_boundary;

#[cfg(test)]
mod tests;
