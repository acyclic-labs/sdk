use super::*;

use crate::conversation::{ContentIo, content_error};
use crate::wire::ErrorCode;

/// A validated agent-owned private writer, constructed only by
/// [`TaskContext::summary_destination`].
#[derive(Clone, Copy)]
pub(crate) struct PrivateSummaries<'a>(&'a ContentBindings);

/// Missing authority is data; failure to check authority remains an error.
#[derive(Clone, Copy)]
pub(crate) enum SummaryDestination<'a> {
    Writer(PrivateSummaries<'a>),
    Unavailable(&'static str),
}

impl<'a> SummaryDestination<'a> {
    pub(crate) fn writer(self) -> Option<PrivateSummaries<'a>> {
        match self {
            Self::Writer(writer) => Some(writer),
            Self::Unavailable(_) => None,
        }
    }

    pub(crate) fn require(self, io: Option<&ContentIo>) -> Result<PrivateSummaries<'a>> {
        match self {
            Self::Writer(writer) => Ok(writer),
            Self::Unavailable(reason) => Err(content_error(io, ErrorCode::Unauthorized, reason)),
        }
    }
}

pub(super) enum ContentPublication<'a> {
    PathBound,
    OperationBound,
    Replacement(&'a FileRef),
}

impl TaskContext {
    /// The turn's validated summary writer: the
    /// runtime's private volume, or an output volume that is itself agent
    /// private. Compaction is armed only when this resolves, so a due summary
    /// never discovers a missing writer.
    pub(crate) fn private_summaries(&self, io: Option<&ContentIo>) -> Result<PrivateSummaries<'_>> {
        self.summary_destination(io)?.require(io)
    }

    pub(crate) fn summary_destination(
        &self,
        io: Option<&ContentIo>,
    ) -> Result<SummaryDestination<'_>> {
        use SummaryDestination::{Unavailable, Writer};
        let binding = self
            .harness
            .artifacts
            .as_ref()
            .or_else(|| self.content_binding(None));
        let Some(binding) = binding else {
            return Ok(Unavailable("summaries require the task's private volume"));
        };
        let Some(writer) = binding.writer.as_ref() else {
            return Ok(Unavailable("task has no owner-bound content writer"));
        };
        if !can_write_volume(writer.volume(), &self.scope, io)? {
            return Ok(Unavailable("task scope cannot write this volume"));
        }
        if writer.volume().class() != crate::conversation::VolumeClass::AgentPrivate
            || !matches!(
                writer.volume().owner(),
                crate::conversation::VolumeOwner::Agent(_)
            )
        {
            return Ok(Unavailable(
                "private content requires an agent-owned private volume",
            ));
        }
        Ok(Writer(PrivateSummaries(binding)))
    }

    pub(crate) async fn stage_private_file_once(
        &self,
        summaries: PrivateSummaries<'_>,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        io: Option<&ContentIo>,
    ) -> Result<FileRef> {
        self.stage_bound_file(
            Some(summaries.0),
            operation_id,
            path,
            bytes,
            "text/plain",
            "summary.txt",
            ContentPublication::OperationBound,
            &self.scope,
            io,
        )
        .await
        .map(|(file, _)| file)
    }

    /// Stages exact bytes through the authenticated writer.
    pub async fn stage_file(
        &self,
        volume: Option<&VolumeRef>,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
    ) -> Result<FileRef> {
        let binding = self.content_binding(volume);
        if let Some(volume) = volume
            && bound_content_writer(binding, &self.scope, None)?.volume() != volume
        {
            return Err(Error::Unauthorized(
                "selected volume is not the bound destination".into(),
            ));
        }
        self.stage_bound_file(
            binding,
            operation_id,
            path,
            bytes,
            media_type,
            display_name,
            ContentPublication::PathBound,
            &self.scope,
            None,
        )
        .await
        .map(|(file, _)| file)
    }

    /// Publishes one immutable file per operation under caller IO limits.
    pub fn stage_file_once<'a>(
        &'a self,
        operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
        io: Option<&'a ContentIo>,
    ) -> impl std::future::Future<Output = Result<(FileRef, u64)>> + acyclic_stream::ProviderTask + 'a
    {
        let binding = self.content_binding(None).cloned();
        async move {
            self.stage_bound_file(
                binding.as_ref(),
                operation_id,
                path,
                bytes,
                media_type,
                display_name,
                ContentPublication::OperationBound,
                &self.scope,
                io,
            )
            .await
        }
    }

    /// Checks the original content destination without reading or publishing bytes.
    pub(crate) fn authorize_content_publication(
        &self,
        path: &str,
        source: Option<&FileRef>,
        scope: &RuntimeScope,
        io: Option<&ContentIo>,
    ) -> Result<()> {
        if let Some(source) = source {
            self.authorize_file_read(source, scope, scope.grants(), io)?;
        }
        publication_destination(
            self.tool_content(source.map(FileRef::volume)),
            scope,
            source,
            path,
            io,
        )?;
        Ok(())
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "one owner-bound publication path retains its binding, identity, bytes, metadata and receipt mode"
    )]
    pub(super) async fn stage_bound_file(
        &self,
        binding: Option<&ContentBindings>,
        operation_id: OperationId,
        path: &str,
        bytes: &[u8],
        media_type: &str,
        display_name: &str,
        publication: ContentPublication<'_>,
        scope: &RuntimeScope,
        io: Option<&ContentIo>,
    ) -> Result<(FileRef, u64)> {
        if bytes.len() as u64 > scope.limits.file_bytes || path.len() > scope.limits.path_bytes {
            return Err(content_error(
                io,
                ErrorCode::Invalid,
                "file exceeds task content limits",
            ));
        }
        let content = binding.ok_or_else(|| {
            content_error(io, ErrorCode::Unsupported, "content publisher is not bound")
        })?;
        let source = match publication {
            ContentPublication::Replacement(source) => Some(source),
            _ => None,
        };
        let (writer, volume, relative) =
            publication_destination(Some(content), scope, source, path, io)?;
        if !content.reader.supports_read(volume, io)? {
            return Err(content_error(
                io,
                ErrorCode::Unsupported,
                "original content reader has no bounded engine",
            ));
        }
        writer.supports_stage(io)?;
        let output = match publication {
            ContentPublication::Replacement(source) => {
                self.authorize_file_read(source, scope, scope.grants(), io)?;
                if source.volume() != volume || source.path() != relative.as_ref() {
                    return Err(content_error(
                        io,
                        ErrorCode::Unauthorized,
                        "source differs from the owner-bound destination",
                    ));
                }
                content
                    .reader
                    .verify(source, source.descriptor().byte_length(), io)?
                    .await?;
                writer
                    .stage_at(operation_id, source, bytes, Some(scope.grants()), io)?
                    .await?
            }
            ContentPublication::OperationBound => {
                writer
                    .stage_once(
                        operation_id,
                        path,
                        bytes,
                        media_type,
                        display_name,
                        crate::conversation::ContentAccess {
                            ceiling: Some(scope.grants()),
                            io,
                        },
                    )?
                    .await?
            }
            ContentPublication::PathBound => {
                writer
                    .stage(
                        operation_id,
                        path,
                        bytes,
                        media_type,
                        display_name,
                        crate::conversation::ContentAccess {
                            ceiling: Some(scope.grants()),
                            io,
                        },
                    )?
                    .await?
            }
        };
        finish_publication(
            content,
            output,
            PublishedFile {
                volume,
                path: relative.as_ref(),
                bytes,
                media_type,
                display_name,
            },
            scope,
            io,
        )
        .await
    }
}

pub(super) fn bound_content_writer<'a>(
    binding: Option<&'a ContentBindings>,
    scope: &RuntimeScope,
    io: Option<&ContentIo>,
) -> Result<&'a Arc<dyn ContentPublisher>> {
    let writer = content_writer(binding, io)?;
    authorize_volume_write(writer.volume(), scope, io)?;
    Ok(writer)
}

pub(super) fn content_writer<'a>(
    binding: Option<&'a ContentBindings>,
    io: Option<&ContentIo>,
) -> Result<&'a Arc<dyn ContentPublisher>> {
    let content = binding.ok_or_else(|| {
        content_error(io, ErrorCode::Unsupported, "content publisher is not bound")
    })?;
    let writer = content.writer.as_ref().ok_or_else(|| {
        content_error(
            io,
            ErrorCode::Unauthorized,
            "task has no owner-bound content writer",
        )
    })?;
    Ok(writer)
}

pub(super) fn publication_destination<'a>(
    binding: Option<&'a ContentBindings>,
    scope: &RuntimeScope,
    source: Option<&FileRef>,
    path: &'a str,
    io: Option<&ContentIo>,
) -> Result<(
    &'a Arc<dyn ContentPublisher>,
    &'a VolumeRef,
    std::borrow::Cow<'a, str>,
)> {
    let writer = content_writer(binding, io)?;
    let (volume, relative) = writer.destination(source.map(FileRef::volume), path, io)?;
    if source.is_some_and(|source| source.volume() != volume) {
        return Err(content_error(
            io,
            ErrorCode::Unauthorized,
            "source differs from the owner-bound destination",
        ));
    }
    authorize_volume_write(volume, scope, io)?;
    Ok((writer, volume, relative))
}

fn authorize_volume_write(
    volume: &VolumeRef,
    scope: &RuntimeScope,
    io: Option<&ContentIo>,
) -> Result<()> {
    if !can_write_volume(volume, scope, io)? {
        return Err(content_error(
            io,
            ErrorCode::Unauthorized,
            "task scope cannot write this volume",
        ));
    }
    Ok(())
}

/// Whether `scope` admits writing `volume`: its exact grant, or a structured
/// filesystem grant whose owner decides the path. The one check every writer
/// makes, so a summary destination and a publication never disagree.
fn can_write_volume(
    volume: &VolumeRef,
    scope: &RuntimeScope,
    io: Option<&ContentIo>,
) -> Result<bool> {
    crate::executor::with_history_io(io, |work| {
        crate::core::volume_write_granted_with_work(&scope.grants, volume, work)
    })
}

struct PublishedFile<'a> {
    volume: &'a VolumeRef,
    path: &'a str,
    bytes: &'a [u8],
    media_type: &'a str,
    display_name: &'a str,
}

async fn finish_publication<'io>(
    content: &ContentBindings,
    output: (FileRef, Option<crate::conversation::ContentHold<'io>>),
    expected: PublishedFile<'_>,
    scope: &RuntimeScope,
    io: Option<&'io ContentIo>,
) -> Result<(FileRef, u64)> {
    let file = &output.0;
    crate::executor::with_history_io(io, |work| scope.limits.validate_file(file, work))?;
    if file.volume() != expected.volume
        || file.path() != expected.path
        || file.display_name() != expected.display_name
        || file.descriptor().media_type() != expected.media_type
        || file.descriptor().byte_length() != expected.bytes.len() as u64
    {
        return Err(Error::Storage(
            "content publisher returned another file identity".into(),
        ));
    }
    #[cfg(feature = "filesystem")]
    let retained = output.1.as_ref().map_or(0, |hold| hold.bytes());
    #[cfg(not(feature = "filesystem"))]
    let retained = 0;
    #[cfg(feature = "filesystem")]
    if output.1.is_none()
        && let Some(io) = io
    {
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.original_native_work().is_some();
        #[cfg(target_arch = "wasm32")]
        let native = false;
        if !native {
            io.retain_owned(crate::filesystem::UploadRecord::logical_owned_bytes(file)?)?;
        }
    }
    crate::executor::with_history_io(io, |work| file.descriptor().verify(expected.bytes, work))?;
    content
        .reader
        .verify_publication(file, file.descriptor().byte_length(), io)?
        .await?;
    let (file, hold) = output;
    #[cfg(feature = "filesystem")]
    if let Some(hold) = hold {
        hold.into_retained()?;
    }
    #[cfg(not(feature = "filesystem"))]
    let _ = hold;
    Ok((file, retained))
}

#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn unbound_native_writer_returns_a_static_failure() -> Result<()> {
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(
                16,
                acyclic_fs::NativeMaterializationWork::accounting_state_bytes(),
            ),
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(cause, _)| Error::NativeWork {
            cause,
            custody: None,
        })?;
        let io = crate::executor::PreparationIo::from_native_work(&work)?;
        assert!(matches!(
            bound_content_writer(None, &RuntimeScope::default(), Some(&io)),
            Err(Error::NativeBoundary {
                code: ErrorCode::Unsupported,
                ..
            })
        ));
        Ok(())
    }
}
