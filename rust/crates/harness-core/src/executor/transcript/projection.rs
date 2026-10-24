//! Recover the admitted request together with its projection metadata.
use super::*;

#[cfg(all(
    test,
    feature = "filesystem",
    not(target_arch = "wasm32"),
    not(acyclic_harness_loom)
))]
mod tests;

type PreparedProjection = (
    ResponseProjection,
    Option<crate::model::PreparedModelRequest>,
);
type AdmittedProjection = (ResponseProjection, crate::model::PreparedModelRequest);

impl StoredProjection {
    /// The running stock turn already owns this projection's admitted request.
    /// Read its small metadata without decoding any historical message segment.
    pub(in crate::executor) async fn continuation_metadata(
        journal: &dyn ExecutionJournal,
        reference: &FileRef,
        original: Option<&PreparationIo>,
    ) -> Result<ResponseProjection> {
        #[cfg(feature = "filesystem")]
        let scope = crate::filesystem::PreparationScope::new(original)?;
        #[cfg(feature = "filesystem")]
        let io = scope.io();
        #[cfg(not(feature = "filesystem"))]
        let io = original;
        let stored = retain_preparation(load_json::<Self>(journal, reference, io).await?)?;
        #[cfg(feature = "filesystem")]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let context = crate::context::copy_context(
            &crate::context::Context {
                messages: Vec::new(),
                metadata: stored.metadata,
                current_input_index: stored.current_input_index,
            },
            io,
        )?;
        let working = stored
            .working
            .as_ref()
            .map(|reference| crate::model::clone_file(reference, io))
            .transpose()?;
        drop((
            stored.request,
            stored.canonical,
            stored.checkpoint,
            stored.working,
        ));
        #[cfg(feature = "filesystem")]
        {
            let retained = io.map_or(Ok(0), |io| {
                io.owned_bytes()?
                    .checked_sub(before)
                    .ok_or_else(|| invalid(Some(io)))
            })?;
            scope.finish(retained)?;
        }
        Ok(ResponseProjection {
            context,
            canonical: None,
            checkpoint: None,
            working,
        })
    }
}

async fn load<'a>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    keep_request: bool,
    original: Option<&'a PreparationIo>,
) -> Result<(
    PreparedProjection,
    Option<crate::conversation::ContentHold<'a>>,
)> {
    #[cfg(feature = "filesystem")]
    let scope = crate::filesystem::PreparationScope::new(original)?;
    #[cfg(feature = "filesystem")]
    let io = scope.io();
    #[cfg(not(feature = "filesystem"))]
    let io = original;
    let stored = retain_preparation(load_json::<StoredProjection>(journal, reference, io).await?)?;
    let request = load_model_request(journal, &stored.request, limits, io).await?;
    let (request, messages) = if keep_request {
        (Some(request), Vec::new())
    } else {
        (None, request.into_messages())
    };
    let context = crate::context::Context {
        messages,
        metadata: stored.metadata,
        current_input_index: stored.current_input_index,
    };
    #[cfg(feature = "filesystem")]
    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
    let projection = ResponseProjection {
        context: crate::context::copy_context(&context, io)?,
        canonical: stored
            .canonical
            .as_ref()
            .map(|file| crate::model::clone_file(file, io))
            .transpose()?,
        checkpoint: stored
            .checkpoint
            .as_ref()
            .map(|file| crate::model::clone_file(file, io))
            .transpose()?,
        working: stored
            .working
            .as_ref()
            .map(|file| crate::model::clone_file(file, io))
            .transpose()?,
    };
    let messages = request
        .as_ref()
        .map_or(projection.context.messages.as_slice(), |request| {
            request.request().messages.as_slice()
        });
    crate::context::validate_projected_messages(&projection.context, messages, limits, io)?;
    #[cfg(feature = "filesystem")]
    let hold = {
        let retained = io.map_or(Ok(0), |io| {
            let projection = io
                .owned_bytes()?
                .checked_sub(before)
                .ok_or_else(|| invalid(Some(io)))?;
            let request = request
                .as_ref()
                .map_or(Ok(0), |request| request.preparation_owned_bytes(io))?;
            projection
                .checked_add(request)
                .ok_or_else(|| invalid(Some(io)))
        })?;
        scope.finish(retained)?;
        original.map(|io| io.guard_owned(retained)).transpose()?
    };
    #[cfg(not(feature = "filesystem"))]
    let hold = None;
    Ok(((projection, request), hold))
}

pub(crate) async fn load_projection<'a>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    io: Option<&'a PreparationIo>,
) -> Result<(
    ResponseProjection,
    Option<crate::conversation::ContentHold<'a>>,
)> {
    let ((projection, _), hold) = load(journal, reference, limits, false, io).await?;
    Ok((projection, hold))
}

pub(crate) async fn load_prepared_projection<'a>(
    journal: &dyn ExecutionJournal,
    reference: &FileRef,
    limits: Limits,
    io: Option<&'a PreparationIo>,
) -> Result<(
    AdmittedProjection,
    Option<crate::conversation::ContentHold<'a>>,
)> {
    let ((projection, request), hold) = load(journal, reference, limits, true, io).await?;
    let request = request.ok_or_else(|| invalid(io))?;
    Ok(((projection, request), hold))
}
