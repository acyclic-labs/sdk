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
            // The protected metadata distinguishes publications of equal bytes;
            // the file payload also catches writes that bypassed that metadata.
            for path in paths {
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
            Ok(())
        })
    }
}
