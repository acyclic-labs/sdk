use super::*;
use crate::conversation::{BoundedContentFuture, ContentHold, ContentResidencyVerifier, VolumeRef};
use crate::{
    executor::stock_boundary, filesystem::PreparationIo, resources::GenerationRef, wire::ErrorCode,
};

// Dispatch fences the complete operation; each source read checks the task grants.
struct ManifestReader<'a>(&'a TaskContext);

impl ContentResidencyVerifier for ManifestReader<'_> {
    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
        maximum_bytes: u64,
        ceiling: Option<&'a crate::Capabilities>,
        io: Option<&'a PreparationIo>,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        BoundedContentFuture::new(io, || async move {
            if reference.descriptor().byte_length() > maximum_bytes {
                return Err(stock_boundary(
                    io,
                    ErrorCode::Invalid,
                    "manifest source exceeds byte bound",
                ));
            }
            self.0
                .read_bound_file(
                    self.0.content_binding(Some(reference.volume())),
                    reference,
                    self.0.scope(),
                    ceiling,
                    io,
                )
                .await
        })
    }

    fn read_private_path<'a, 'io>(
        &'a self,
        volume: &'a VolumeRef,
        prefix: &'a str,
        path: &'a str,
        generation: Option<&'a GenerationRef>,
        maximum_bytes: u64,
        access: crate::conversation::ContentAccess<'a, 'io>,
    ) -> Result<
        BoundedContentFuture<
            'a,
            (
                FileRef,
                acyclic_stream::OwnedBytes,
                Option<ContentHold<'io>>,
            ),
        >,
    >
    where
        'io: 'a,
    {
        let crate::conversation::ContentAccess { ceiling, io } = access;

        BoundedContentFuture::new(io, || async move {
            let grants = ceiling.unwrap_or(self.0.scope().grants());
            if !grants.is_subset_of(self.0.scope().grants()) {
                return Err(stock_boundary(
                    io,
                    ErrorCode::Unauthorized,
                    "content caller cannot widen the bound task scope",
                ));
            }
            crate::executor::with_history_io(io, |work| {
                if !crate::core::directory_read_granted_with_work(grants, volume, prefix, work)? {
                    return Err(stock_boundary(
                        io,
                        ErrorCode::Unauthorized,
                        "task cannot read manifest skill root",
                    ));
                }
                Ok::<(), Error>(())
            })?;
            let reader = &self
                .0
                .content_binding(Some(volume))
                .ok_or_else(|| {
                    stock_boundary(
                        io,
                        ErrorCode::Unsupported,
                        "manifest source reader is not bound",
                    )
                })?
                .reader;
            reader
                .read_private_path(
                    volume,
                    prefix,
                    path,
                    generation,
                    maximum_bytes,
                    crate::conversation::ContentAccess {
                        ceiling: Some(grants),
                        io,
                    },
                )?
                .await
        })
    }
}

impl<P, A, O> FilesystemTaskCommands<'_, P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn model_context(
        &self,
        reference: FileRef,
        context: &TaskContext,
        io: Option<&PreparationIo>,
    ) -> Result<(ContextPipeline, crate::tool::ToolRegistry)> {
        #[cfg(not(target_arch = "wasm32"))]
        let tools = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                stock_boundary(
                    Some(io),
                    ErrorCode::Unsupported,
                    "manifest requires original work",
                )
            })?;
            let (tools, custody) = self.runtime.tool_registry().clone_with_native_work(work)?;
            io.absorb_native_custody(custody)?;
            tools
        } else {
            self.runtime.tool_registry().clone()
        };
        #[cfg(target_arch = "wasm32")]
        let tools = self.runtime.tool_registry().clone();
        let reader = ManifestReader(context);
        let (tools, stage) = crate::context::ContextManifest::bind_observed(
            reference,
            &reader,
            tools,
            context.scope().limits(),
            io,
        )
        .await?;
        let mut pipeline = ContextPipeline::default();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| {
                stock_boundary(
                    Some(io),
                    ErrorCode::Unsupported,
                    "manifest requires original work",
                )
            })?;
            let custody = pipeline.append_with_native_work(stage, work)?;
            io.absorb_native_custody(custody)?;
            return Ok((pipeline, tools));
        }
        pipeline = pipeline.with(stage);
        Ok((pipeline, tools))
    }
}
