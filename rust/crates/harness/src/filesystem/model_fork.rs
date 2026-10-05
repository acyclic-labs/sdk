//! Verify the durable completed exchange before entering the existing fork engine.
use super::*;
use crate::fork::InheritedConversationPrefix;
use crate::{
    batch_publication::ModelBatchPublication,
    conversation::ModelContextSelection,
    executor::load_json,
    model::ModelRequest,
    model_input::{CompletedModelBoundary, FrozenModelPrefix, PreparedModelInput},
    tool::ModelToolContext,
};

fn collect_projected_file_refs(
    value: &serde_json::Value,
    files: &mut Vec<crate::conversation::FileRef>,
) {
    if let Ok(file) = serde_json::from_value::<crate::conversation::FileRef>(value.clone())
        && file.validate().is_ok()
    {
        files.push(file);
        return;
    }
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_projected_file_refs(value, files);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values() {
                collect_projected_file_refs(value, files);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

fn collect_tool_content_file_refs(
    content: &crate::model::ModelContent,
    files: &mut Vec<crate::conversation::FileRef>,
) {
    let parts = match content {
        crate::model::ModelContent::Part(part) => std::slice::from_ref(part),
        crate::model::ModelContent::Parts(parts) => parts.as_slice(),
        crate::model::ModelContent::Text(_) => return,
    };
    for part in parts {
        match part {
            crate::model::ModelContentPart::ToolCall { arguments, .. }
            | crate::model::ModelContentPart::ToolResult {
                value: arguments, ..
            } => {
                collect_projected_file_refs(arguments, files);
            }
            crate::model::ModelContentPart::Text { .. }
            | crate::model::ModelContentPart::File { .. } => {}
        }
    }
}

/// Exact completed model boundary and its authoritative parent conversation.
/// Workspace preparation and publication remain owned by the existing typed
/// fork APIs. The caller must persist its original `ForkRequest` before dispatch.
pub struct VerifiedModelForkBoundary<P> {
    boundary: CompletedModelBoundary,
    publication: ModelBatchPublication,
    parent: StreamAggregate<P>,
}

impl<P> VerifiedModelForkBoundary<P> {
    /// Immutable model input inherited by every child at this batch boundary.
    pub const fn boundary(&self) -> &CompletedModelBoundary {
        &self.boundary
    }

    /// Borrow the already verified parent aggregate without reopening or
    /// reconstructing its authoritative history.
    pub(crate) fn parent(&self) -> &StreamAggregate<P> {
        &self.parent
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
        self.attach_model_fork_references_from_parts(
            &verified.boundary,
            &verified.publication,
            &verified.parent,
            request,
        )
        .await
    }

    /// Attach references after the verified boundary has been split into its
    /// existing typed parts. This keeps the immutable model evidence owned by
    /// Harness while allowing Filesystem fork preparation to retain its
    /// established aggregate flow.
    pub(crate) async fn attach_model_fork_references_from_parts(
        &self,
        boundary: &CompletedModelBoundary,
        publication: &ModelBatchPublication,
        parent: &StreamAggregate<P>,
        request: &mut crate::fork::ForkRequest,
    ) -> Result<()> {
        request.validate()?;
        let parent = parent.reducer();
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
        for message in &boundary.request.messages {
            for file in message.content.file_refs() {
                if unique.insert(file.read_capability()?) {
                    self.content_verifier.verify(file).await?;
                    files.push(file.clone());
                }
            }
            let mut projected = Vec::new();
            collect_tool_content_file_refs(&message.content, &mut projected);
            for file in projected {
                if unique.insert(file.read_capability()?) {
                    self.content_verifier.verify(&file).await?;
                    files.push(file);
                }
            }
        }
        let mut references = crate::fork::ModelBoundaryReferences {
            publication: publication.operation_id,
            publication_digest: crate::contract::canonical_json_digest(publication)?,
            boundary_digest: crate::contract::canonical_json_digest(boundary)?,
            inherited_parent_revision: request.parent_revision,
            inherited_through_sequence: request.preparation.inherited_through_sequence,
            inherited_prefix_digest: {
                let parent_agent = messages.agent.ok_or_else(|| {
                    Error::Storage("verified fork parent agent is missing".into())
                })?;
                let prefix = InheritedConversationPrefix::select(
                    request.parent.clone(),
                    request.parent_revision,
                    parent_agent,
                    request.preparation.inherited_through_sequence,
                    &request.attached_agents,
                    &messages.messages,
                )?;
                crate::contract::canonical_json_digest(&prefix)?
            },
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
            task_id: None,
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
        let original = PreparedModelInput::prepare_with_policy(
            request,
            limits,
            boundary.option_policy.as_ref(),
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
