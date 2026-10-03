//! Verify the durable completed exchange before entering the existing fork engine.
use super::*;
use crate::{
    batch_publication::ModelBatchPublication,
    conversation::ModelContextSelection,
    executor::load_json,
    model::ModelRequest,
    model_input::{CompletedModelBoundary, FrozenModelPrefix, PreparedModelInput},
    tool::ModelToolContext,
};

/// Exact completed model boundary and its authoritative parent conversation.
/// Workspace preparation and publication remain owned by the existing typed
/// fork APIs. The caller must persist its original `ForkRequest` before dispatch.
pub struct VerifiedModelForkBoundary<P> {
    boundary: CompletedModelBoundary,
    parent: StreamAggregate<P>,
}

impl<P> VerifiedModelForkBoundary<P> {
    /// Immutable model input inherited by every child at this batch boundary.
    pub const fn boundary(&self) -> &CompletedModelBoundary {
        &self.boundary
    }

    /// Consume the verification result without regenerating the prefix.
    pub fn into_parts(self) -> (CompletedModelBoundary, StreamAggregate<P>) {
        (self.boundary, self.parent)
    }
}

impl<P, A, O> HarnessStorage<P, A, O>
where
    P: acyclic_stream::StreamProvider + Send + Sync + 'static,
    A: acyclic_fs::AsyncAuthorityStore + Send + Sync + 'static,
    O: acyclic_fs::AsyncObjectStore + Send + Sync + 'static,
{
    /// Verify the exact durable admission and publish its ordered conversation
    /// results before preparing any child workspace. Refuses substituted content,
    /// unadmitted publications, incomplete exchanges, and later parent history.
    pub async fn verified_model_fork_boundary(
        &self,
        publication: &ModelBatchPublication,
        limits: Limits,
    ) -> Result<VerifiedModelForkBoundary<P>> {
        self.verify_model_fork_composition(publication, limits, None)
            .await
    }

    /// Verify a recursive boundary against the exact inheritance declaration
    /// retained by the composition layer for this child. Extra middle messages
    /// are rejected even when the child's own conversation tail matches.
    pub async fn verified_inherited_model_fork_boundary(
        &self,
        publication: &ModelBatchPublication,
        limits: Limits,
        inherited: &crate::model_input::InheritedModelContext,
    ) -> Result<VerifiedModelForkBoundary<P>> {
        self.verify_model_fork_composition(publication, limits, Some(inherited))
            .await
    }

    async fn verify_model_fork_composition(
        &self,
        publication: &ModelBatchPublication,
        limits: Limits,
        inherited: Option<&crate::model_input::InheritedModelContext>,
    ) -> Result<VerifiedModelForkBoundary<P>> {
        let expected = ModelToolContext {
            parent_operation: publication.parent_operation,
            step: publication.step,
        }
        .publication_operation();
        if publication.operation_id != expected {
            return Err(Error::Conflict("fork publication identity changed".into()));
        }
        let records = self.journal.replay(publication.parent_operation).await?;
        let mut admitted = false;
        let mut original = false;
        let mut completed = false;
        for record in &records {
            match &record.event {
                ExecutionEvent::ModelInputPrepared { step, request, .. }
                    if *step == publication.step =>
                {
                    if request != &publication.request || original {
                        return Err(Error::Conflict("fork request admission changed".into()));
                    }
                    original = true;
                }
                ExecutionEvent::ToolBatchCompleted { step, boundary }
                    if *step == publication.step =>
                {
                    if boundary != &publication.boundary || completed {
                        return Err(Error::Conflict("fork completed boundary changed".into()));
                    }
                    completed = true;
                }
                ExecutionEvent::BatchPublicationStarted {
                    step,
                    publication: reference,
                } if *step == publication.step => {
                    let actual: ModelBatchPublication =
                        load_json(self.journal.as_ref(), reference).await?;
                    if &actual != publication || admitted {
                        return Err(Error::Conflict("fork publication admission changed".into()));
                    }
                    admitted = true;
                }
                _ => {}
            }
        }
        if !admitted || !original || !completed {
            return Err(Error::Conflict(
                "fork publication is not durably admitted".into(),
            ));
        }
        let request: ModelRequest = load_json(self.journal.as_ref(), &publication.request).await?;
        let boundary: CompletedModelBoundary =
            load_json(self.journal.as_ref(), &publication.boundary).await?;
        boundary.verify(limits)?;
        let original = PreparedModelInput::prepare(request, limits)?;
        let original_prefix =
            FrozenModelPrefix::capture(&original, original.request().messages.len())?;
        original_prefix.verify(&PreparedModelInput::prepare(
            boundary.request.clone(),
            limits,
        )?)?;
        let parent = self
            .completed_conversation(publication.parent_operation, publication.step, limits)
            .await?;
        let state = parent
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Storage("fork parent conversation is missing".into()))?;
        let selected = select_model_context(
            state,
            ModelContextSelection {
                conversation_revision: state.messages.len() as u64,
                message_ids: state.messages.iter().map(|message| message.id).collect(),
            },
            self.content_verifier.as_ref(),
            limits.context_messages,
            limits.attachments,
            limits.render_bytes,
        )
        .await?;
        match inherited {
            Some(declaration) => {
                declaration.verify_composition(&boundary.request, &selected.messages, limits)?
            }
            None if selected.messages == boundary.request.messages => {}
            None => {
                return Err(Error::Conflict(
                    "fork boundary differs from authoritative conversation".into(),
                ));
            }
        }
        Ok(VerifiedModelForkBoundary { boundary, parent })
    }
}
