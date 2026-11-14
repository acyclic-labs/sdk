//! Source admission and destination publication are independent capabilities.
use super::*;
use crate::conversation::{
    ContentCapture, ContentChange, ContentChangeResult, ContentChanges, ContentPath,
};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    pub(in crate::filesystem) async fn capture_private_path<'io>(
        &self,
        grant: &ContentGrant,
        capture: ContentCapture<'_>,
        maximum_bytes: Option<u64>,
        access: crate::conversation::ContentAccess<'_, 'io>,
    ) -> Result<crate::conversation::CapturedContent<'io>> {
        let ContentPath {
            volume,
            path,
            generation,
            ..
        } = capture.source;
        crate::executor::with_history_io(access.io, |work| -> Result<_> {
            volume.validate(work)?;
            generation.validate(work)?;
            crate::conversation::validate_content_path(capture.path, work)?;
            Ok(())
        })?;
        if generation.as_resource().provider() != volume.provider() || maximum_bytes == Some(0) {
            return Err(stock_boundary(
                access.io,
                crate::wire::ErrorCode::Invalid,
                "invalid pinned source capture",
            ));
        }
        let temporary;
        let io = if let Some(io) = access.io {
            io
        } else {
            temporary = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &temporary
        };
        #[cfg(not(target_arch = "wasm32"))]
        let native = self
            .skill_sources(volume)?
            .filter(|sources| sources.matches(volume, generation, path));
        #[cfg(not(target_arch = "wasm32"))]
        let native_source = native.is_some();
        #[cfg(target_arch = "wasm32")]
        let native_source = false;
        let selected = self
            .select_private_read(
                volume,
                grant,
                path,
                (!native_source).then_some(generation),
                access.ceiling,
                io,
            )
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        let _materializing = match &native {
            Some(sources) => Some(sources.materializer.enter().await),
            None => None,
        };
        let binding = capture.source_binding(access.io)?;
        let binding_path = crate::executor::with_history_io(access.io, |work| -> Result<_> {
            crate::executor::preparation_format(format_args!("{}.capture-v1", capture.path), work)
        })?;
        let _binding_path_hold = io.guard_owned(binding_path.capacity() as u64)?;
        // The caller ceiling narrows the original source, never supplies a
        // destination grant. Publication uses its independently bound owner.
        let publication_access = crate::conversation::ContentAccess {
            ceiling: None,
            io: access.io,
        };
        let (prior, hold) = capture
            .publisher
            .observe_changes(
                capture.publisher.volume(),
                capture.operation,
                None,
                publication_access,
            )?
            .await?;
        if let Some(prior) = prior {
            selected.recheck(grant, io).await?;
            let file = capture_result(
                &capture,
                prior,
                &binding_path,
                &binding,
                maximum_bytes,
                access.io,
            )?;
            let bytes = capture
                .reader
                .read(&file, file.descriptor().byte_length(), None, access.io)?
                .await?;
            crate::executor::with_history_io(access.io, |work| -> Result<_> {
                file.descriptor().verify(&bytes, work)
            })?;
            selected.recheck(grant, io).await?;
            return Ok((file, bytes, hold));
        }
        let bytes;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(sources) = &native {
            bytes = sources
                .read(volume, generation, path, maximum_bytes, access.io)?
                .await?;
        } else {
            bytes = self
                .read_capture_source(&selected, path, maximum_bytes, io)
                .await?;
        }
        #[cfg(target_arch = "wasm32")]
        {
            bytes = self
                .read_capture_source(&selected, path, maximum_bytes, io)
                .await?;
        }
        let bytes = bytes.ok_or_else(|| {
            stock_boundary(
                access.io,
                crate::wire::ErrorCode::NotFound,
                crate::conversation::SELECTED_FILE_MISSING,
            )
            .tool_input()
        })?;
        selected.recheck(grant, io).await?;
        let changes = [
            ContentChange::CreateAbsent {
                path: capture.path,
                bytes: &bytes,
                media_type: capture.media_type,
                display_name: capture.display_name,
            },
            ContentChange::CreateAbsent {
                path: &binding_path,
                bytes: &binding,
                media_type: "application/octet-stream",
                display_name: "capture-v1",
            },
        ];
        let (captured, hold) = capture
            .publisher
            .stage_changes(
                capture.publisher.volume(),
                capture.operation,
                &changes,
                publication_access,
            )?
            .await?;
        let file = capture_result(
            &capture,
            captured,
            &binding_path,
            &binding,
            maximum_bytes,
            access.io,
        )?;
        // The cold path returns its already admitted source bytes. It does
        // not read the newly published body back just to assemble a prompt.
        crate::executor::with_history_io(access.io, |work| file.descriptor().verify(&bytes, work))?;
        selected.recheck(grant, io).await?;
        Ok((file, bytes, hold))
    }

    async fn read_capture_source(
        &self,
        selected: &PrivateReadSelection<'_, A, O>,
        path: &str,
        maximum_bytes: Option<u64>,
        io: &PreparationIo,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let requested = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(format_args!("/{path}"), work)
        })?;
        let _hold = io.guard_owned(requested.capacity() as u64)?;
        read_generation_optional(
            &selected.generation,
            &requested,
            maximum_bytes,
            io,
            path_read_refusal,
        )
        .await
    }
}

fn capture_result(
    request: &ContentCapture<'_>,
    receipt: ContentChanges,
    binding_path: &str,
    binding: &[u8],
    maximum_bytes: Option<u64>,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    let version = bounded_io::hex_key(receipt.generation.as_resource().key(), io)?;
    let _version_hold = io
        .map(|io| io.guard_owned(version.capacity() as u64))
        .transpose()?;
    let mut members = receipt.members.into_iter();
    if let (
        Some(ContentChangeResult::Created(body)),
        Some(ContentChangeResult::Created(proof)),
        None,
    ) = (members.next(), members.next(), members.next())
    {
        let expected_volume = request.publisher.volume();
        if body.volume() == expected_volume
            && proof.volume() == expected_volume
            && body.version() == version
            && proof.version() == version
            && body.path() == request.path
            && proof.path() == binding_path
            && body.descriptor().media_type() == request.media_type
            && body.display_name() == request.display_name
            && proof.descriptor().media_type() == "application/octet-stream"
            && crate::executor::with_history_io(io, |work| {
                proof.descriptor().matches_bytes(binding, work)
            })?
        {
            if maximum_bytes.is_some_and(|maximum| body.descriptor().byte_length() > maximum) {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "captured source exceeds byte bound",
                )
                .tool_input());
            }
            return Ok(body);
        }
    }
    Err(stock_boundary(
        io,
        crate::wire::ErrorCode::Conflict,
        "capture operation has another source or destination",
    ))
}
