//! Verify the durable completed exchange before entering the existing fork engine.
use super::*;
use crate::{
    batch_publication::ModelBatchPublication,
    conversation::ModelContextSelection,
    executor::load_json,
    model::ModelRequest,
    model_input::{
        CompletedModelBoundary, FrozenModelPrefix, ModelInputManifest, PreparedModelInput,
    },
    tool::ModelToolContext,
};

/// Exact completed model boundary and its authoritative parent conversation.
/// Workspace preparation and publication remain owned by the existing typed
/// fork APIs. The caller must persist its original `ForkRequest` before dispatch.
pub struct VerifiedModelForkBoundary<P> {
    boundary: CompletedModelBoundary,
    publication: ModelBatchPublication,
    parent: StreamAggregate<P>,
}

impl<P> VerifiedModelForkBoundary<P> {
    pub(crate) const fn parent(&self) -> &StreamAggregate<P> {
        &self.parent
    }

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
    /// Attach only exact references declared by a durably verified model
    /// boundary. The parent resolver checks authority and immutable content
    /// before signing; preparation independently verifies them before allocation.
    pub async fn attach_model_fork_references(
        &self,
        verified: &VerifiedModelForkBoundary<P>,
        request: &mut crate::fork::ForkRequest,
    ) -> Result<()> {
        request.validate()?;
        let parent = verified.parent.reducer();
        if request.parent != *parent.authority()
            || request.parent != *self.issuer.verifier().audience()
            || request.parent_revision != parent.revision()
        {
            return Err(Error::Conflict(
                "model fork request differs from verified parent boundary".into(),
            ));
        }
        let messages = parent
            .conversation()
            .ok_or_else(|| Error::Storage("verified fork parent conversation is missing".into()))?;
        if request.preparation.inherited_through_sequence != messages.messages.len() as u64 {
            return Err(Error::Conflict(
                "model fork selection differs from verified conversation".into(),
            ));
        }
        let mut unique = std::collections::BTreeSet::new();
        let mut files = Vec::new();
        for message in &verified.boundary.request.messages {
            for file in crate::model_input::message_file_refs(
                message,
                &verified.boundary.request.tools,
            )? {
                if unique.insert(file.read_capability()?) {
                    self.content_verifier.verify(&file).await?;
                    files.push(file);
                }
            }
        }
        let mut references = crate::fork::ModelBoundaryReferences {
            publication: verified.publication.operation_id,
            publication_digest: crate::contract::canonical_json_digest(&verified.publication)?,
            boundary_digest: crate::contract::canonical_json_digest(&verified.boundary)?,
            attestation: [0; 32],
            files,
        };
        references.attestation = self.issuer.attest_model_boundary(
            &request.parent,
            &request.child,
            request.child_agent,
            &request.attached_agents,
            &references,
        )?;
        let mut candidate = request.clone();
        candidate.model_boundary = Some(references);
        candidate.validate()?;
        *request = candidate;
        Ok(())
    }

    /// Verify the exact durable admission and publish its ordered conversation
    /// results before preparing any child workspace. Refuses substituted content,
    /// unadmitted publications, incomplete exchanges, and later parent history.
    pub fn verified_model_fork_boundary<'a>(
        &'a self,
        publication: &'a ModelBatchPublication,
        limits: Limits,
    ) -> futures::future::BoxFuture<'a, Result<VerifiedModelForkBoundary<P>>> {
        let verification = self.verify_model_fork_composition(publication, limits, None);
        crate::stack_diagnostics::future_size("fork-boundary-verification", &verification);
        Box::pin(verification)
    }

    /// Verify a recursive boundary against the exact inheritance declaration
    /// retained by the composition layer for this child. Extra middle messages
    /// are rejected even when the child's own conversation tail matches.
    pub fn verified_inherited_model_fork_boundary<'a>(
        &'a self,
        publication: &'a ModelBatchPublication,
        limits: Limits,
        inherited: &'a crate::model_input::InheritedModelContext,
    ) -> futures::future::BoxFuture<'a, Result<VerifiedModelForkBoundary<P>>> {
        let verification = self.verify_model_fork_composition(publication, limits, Some(inherited));
        crate::stack_diagnostics::future_size("fork-boundary-verification", &verification);
        Box::pin(verification)
    }

    async fn verify_model_fork_composition(
        &self,
        publication: &ModelBatchPublication,
        limits: Limits,
        inherited: Option<&crate::model_input::InheritedModelContext>,
    ) -> Result<VerifiedModelForkBoundary<P>> {
        crate::stack_diagnostics::marker("fork-boundary-verification-enter");
        let expected = ModelToolContext {
            parent_operation: publication.parent_operation,
            step: publication.step,
            task_id: None,
        }
        .publication_operation();
        if publication.operation_id != expected {
            return Err(Error::Conflict("fork publication identity changed".into()));
        }
        let records = self.journal.replay(publication.parent_operation).await?;
        let mut admitted = false;
        let mut original_manifest = None;
        let mut completed = false;
        for record in &records {
            match &record.event {
                ExecutionEvent::ModelInputPrepared {
                    step,
                    request,
                    manifest,
                } if *step == publication.step => {
                    if request != &publication.request || original_manifest.is_some() {
                        return Err(Error::Conflict("fork request admission changed".into()));
                    }
                    original_manifest = Some(manifest);
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
        if !admitted || original_manifest.is_none() || !completed {
            return Err(Error::Conflict(
                "fork publication is not durably admitted".into(),
            ));
        }
        let request: ModelRequest = load_json(self.journal.as_ref(), &publication.request).await?;
        let boundary: CompletedModelBoundary =
            load_json(self.journal.as_ref(), &publication.boundary).await?;
        boundary.verify(limits)?;
        let manifest: ModelInputManifest = load_json(
            self.journal.as_ref(),
            original_manifest
                .ok_or_else(|| Error::Conflict("fork input manifest is missing".into()))?,
        )
        .await?;
        let original = PreparedModelInput::restore(
            request,
            limits,
            boundary.option_policy.as_ref(),
            manifest,
        )?;
        let original_prefix =
            FrozenModelPrefix::capture(&original, original.request().messages.len())?;
        original_prefix.verify(&PreparedModelInput::prepare_with_policy(
            boundary.request.clone(),
            limits,
            boundary.option_policy.as_ref(),
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
        Ok(VerifiedModelForkBoundary {
            boundary,
            publication: publication.clone(),
            parent,
        })
    }
}
