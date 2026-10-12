use super::*;
use crate::wire_codec::CodecQuota;

impl<P: StreamProvider> StreamAggregate<P> {
    pub(super) async fn validate_task_admission_content_with_work(
        &self,
        action: &Action,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        let (reference, reason) = match action {
            Action::OpenInteraction { ticket } => {
                (&ticket.request, "interaction content verifier is not bound")
            }
            Action::PlanEffect { request, .. } => {
                (request, "effect request content verifier is not bound")
            }
            Action::ResolveEffect { observation } => {
                let EffectStatus::Succeeded { result } = &observation.status else {
                    return Ok(());
                };
                (result, "effect result content verifier is not bound")
            }
            _ => return Ok(()),
        };
        let verifier = self
            .content_verifier
            .as_ref()
            .ok_or_else(|| guarded_boundary(work, crate::wire::ErrorCode::Unsupported, reason))?;
        if let Action::PlanEffect { result_schema, .. } = action {
            match work.as_deref_mut() {
                Some(io) => {
                    history::verify_content_with_work(io, verifier.as_ref(), reference).await?
                }
                None => {
                    verified_content_bytes(
                        verifier.as_ref(),
                        reference,
                        reference.descriptor().byte_length(),
                        None,
                    )
                    .await?;
                }
            }
            let mut quota = CodecQuota::new(work, 0)?;
            let (bytes, held) = Self::read_admission_content_with_work(
                verifier.as_ref(),
                result_schema,
                quota.work(),
            )
            .await?;
            quota.adopt(held)?;
            return crate::effects::validate_schema_bytes(result_schema, &bytes, quota.work());
        }
        let mut quota = CodecQuota::new(work, 0)?;
        let (bytes, held) =
            Self::read_admission_content_with_work(verifier.as_ref(), reference, quota.work())
                .await?;
        quota.adopt(held)?;
        match action {
            Action::OpenInteraction { ticket } => {
                let request = ticket.validate_request_bytes(&bytes, quota.work())?;
                quota.adopt(request.1)?;
                Ok(())
            }
            Action::ResolveEffect { observation } => {
                let effect = self.reducer.effect(observation.effect_id).ok_or_else(|| {
                    guarded_boundary(
                        quota.work(),
                        crate::wire::ErrorCode::NotFound,
                        "effect is not found",
                    )
                })?;
                let (schema, held) = Self::read_admission_content_with_work(
                    verifier.as_ref(),
                    &effect.result_schema,
                    quota.work(),
                )
                .await?;
                quota.adopt(held)?;
                crate::effects::validate_result_bytes(
                    &effect.result_schema,
                    &schema,
                    reference,
                    &bytes,
                    quota.work(),
                )
            }
            _ => unreachable!("content action selected above"),
        }
    }

    pub(super) async fn read_admission_content_with_work(
        verifier: &dyn ContentResidencyVerifier,
        reference: &crate::conversation::FileRef,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<(acyclic_stream::OwnedBytes, u64)> {
        match work.as_deref_mut() {
            Some(io) => history::read_content_with_work(io, verifier, reference).await,
            None => verifier
                .read(reference, reference.descriptor().byte_length(), None)?
                .await
                .map(|bytes| (bytes, 0)),
        }
    }

    pub(super) async fn validate_interaction_part_with_work(
        &self,
        ticket: &crate::interaction::InteractionTicket,
        reference: &crate::conversation::FileRef,
        detail_outcome: Option<&InteractionOutcome>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        let verifier = self.content_verifier.as_ref().ok_or_else(|| {
            guarded_boundary(
                work,
                crate::wire::ErrorCode::Unsupported,
                "interaction content verifier is not bound",
            )
        })?;
        let mut quota = CodecQuota::new(work, 0)?;
        let (request_bytes, held) = Self::read_admission_content_with_work(
            verifier.as_ref(),
            &ticket.request,
            quota.work(),
        )
        .await?;
        quota.adopt(held)?;
        let request = ticket.validate_request_bytes(&request_bytes, quota.work())?;
        quota.adopt(request.1)?;
        let (bytes, held) =
            Self::read_admission_content_with_work(verifier.as_ref(), reference, quota.work())
                .await?;
        quota.adopt(held)?;
        match detail_outcome {
            Some(outcome) => {
                ticket.validate_decision_bytes(&request.0, outcome, reference, &bytes, quota.work())
            }
            None => ticket.validate_answer_bytes(&request.0, reference, &bytes, quota.work()),
        }
    }
    pub(super) async fn validate_admission_content(
        &self,
        action: &crate::core::Action,
    ) -> Result<()> {
        self.validate_interaction_admission(action).await?;
        self.validate_extension_admission_content(action).await?;
        let message = match action {
            crate::core::Action::AppendConversationMessage { message } => Some(message.as_ref()),
            crate::core::Action::PublishProjectMerge { receipt } => Some(&receipt.notice),
            _ => None,
        };
        if let Some(message) = message {
            self.limits.validate_message(message, &mut None)?;
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("conversation content verifier is not bound".into())
            })?;
            verified_content_bytes(
                verifier.as_ref(),
                &message.content,
                (&message.content).descriptor().byte_length(),
                None,
            )
            .await?;
            match &message.attachments {
                ReferencedAttachments::Inline { items } => {
                    for attachment in items {
                        verified_content_bytes(
                            verifier.as_ref(),
                            &attachment.file,
                            (&attachment.file).descriptor().byte_length(),
                            None,
                        )
                        .await?;
                    }
                }
                ReferencedAttachments::Manifest {
                    manifest,
                    item_count,
                } => {
                    verified_attachment_manifest(
                        verifier.as_ref(),
                        manifest,
                        *item_count,
                        &self.limits,
                        None,
                    )
                    .await?;
                }
            }
            for reference in message.extensions.values() {
                verified_content_bytes(
                    verifier.as_ref(),
                    reference,
                    (reference).descriptor().byte_length(),
                    None,
                )
                .await?;
            }
        }
        self.validate_task_admission_content_with_work(action, &mut None)
            .await?;
        if let crate::core::Action::PublishProjectMerge { receipt } = action {
            let verifier = self
                .merge_verifier
                .as_ref()
                .ok_or_else(|| Error::Unsupported("project merge verifier is not bound".into()))?;
            verifier.verify(receipt).await?;
        }
        Ok(())
    }

    pub(super) async fn validate_interaction_admission(&self, action: &Action) -> Result<()> {
        let Action::ResolveInteraction { resolution } = action else {
            return Ok(());
        };
        let (ticket, _) = self
            .reducer
            .interaction(&resolution.id)
            .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
        if matches!(resolution.outcome, InteractionOutcome::Expired) {
            let deadline = ticket
                .deadline_unix_ms
                .ok_or_else(|| Error::Invalid("interaction has no expiry deadline".into()))?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| Error::Storage(error.to_string()))?
                .as_millis();
            if now < u128::from(deadline) {
                return Err(Error::Conflict(
                    "interaction deadline has not elapsed".into(),
                ));
            }
        }
        if let InteractionOutcome::Answered { answer } = &resolution.outcome {
            self.validate_interaction_part_with_work(ticket, answer, None, &mut None)
                .await?;
        }
        if let Some(detail) = &resolution.detail {
            self.validate_interaction_part_with_work(
                ticket,
                detail,
                Some(&resolution.outcome),
                &mut None,
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn validate_causal_reference(
        &self,
        reference: Option<&EventReference>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        let Some(reference) = reference else {
            return Ok(());
        };
        if reference.authority == *self.reducer.authority() {
            return Ok(());
        }
        let mut quota = CodecQuota::new(work, 0)?;
        let (path, held) =
            operations::publication_path(&reference.authority, "harness/v2/", None, quota.work())?;
        quota.adopt(held)?;
        let record = operations::one_record(
            &self.client,
            &path,
            reference.revision.saturating_sub(1),
            quota.work(),
        )
        .await?
        .ok_or_else(|| {
            guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::NotFound,
                "causal event",
            )
        })?;
        let held = operations::sdk_record_retained_bytes(&record, quota.work());
        quota.adopt(held)?;
        let ((authority, event), held) =
            crate::wire_codec::decode_event(&record.value, quota.work())?;
        quota.adopt(held + authority.id.capacity() as u64)?;
        if authority != reference.authority || event.revision != reference.revision {
            return Err(Error::Invalid(
                "causal event reference does not resolve exactly".into(),
            ));
        }
        Ok(())
    }
}
