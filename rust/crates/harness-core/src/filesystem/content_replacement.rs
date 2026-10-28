use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(super) fn require_unchanged_content<'a>(
        &'a self,
        workspace: &'a Workspace<A, O>,
        original: &'a GenerationRef,
        authorized: &'a GenerationRef,
        paths: [&'a str; 2],
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        let [metadata_path, content_path] = paths;
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let owned;
            let io = if let Some(io) = io {
                io
            } else {
                owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                &owned
            };
            let original = self.generation(workspace, original, Some(io)).await?;
            let authorized = self.generation(workspace, authorized, Some(io)).await?;
            let metadata = crate::executor::with_history_io(Some(io), |work| {
                crate::executor::preparation_format(format_args!("/{metadata_path}"), work)
            })?;
            let metadata_hold = io.guard_owned(metadata.capacity() as u64)?;
            // The protected metadata distinguishes publications of equal bytes;
            // the file payload also catches writes that bypassed that metadata.
            for path in [metadata.as_str(), content_path] {
                if !io.sdk(
                    authorized
                        .matches_file_payload(&original, path, io, io.cancellation())
                        .await,
                )? {
                    return Err(stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Conflict,
                        "source file changed before content authorization",
                    ));
                }
            }
            drop(metadata);
            drop(metadata_hold);
            Ok(())
        })
    }
}
