use super::*;

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
                Some(observer) => {
                    history::verify_content_with_work(observer, verifier.as_ref(), reference)
                        .await?
                }
                None => {
                    verified_content_bytes(
                        verifier.as_ref(),
                        reference,
                        (reference).descriptor().byte_length(),
                        None,
                    )
                    .await
                    .map(|(bytes, _)| bytes)?;
                }
            }
            let mut scratch = crate::wire_codec::CodecQuota::from_retained(work, 0);
            let (bytes, claim) = Self::read_admission_content_with_work(
                verifier.as_ref(),
                result_schema,
                scratch.work(),
            )
            .await?;
            scratch.adopt(claim)?;
            let validated =
                crate::effects::validate_schema_bytes(result_schema, &bytes, scratch.work());
            drop(bytes);
            let released = scratch.relinquish(claim);
            return match validated {
                Ok(()) => released,
                Err(error) => {
                    let _ = released;
                    Err(error)
                }
            };
        }
        if let Action::ResolveEffect { observation } = action {
            let effect = self.reducer.effect(observation.effect_id).ok_or_else(|| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if work
                    .as_ref()
                    .is_some_and(|work| work.original_native_work().is_some())
                {
                    return Error::NativeBoundary {
                        code: crate::wire::ErrorCode::NotFound,
                        reason: "effect is not found",
                    };
                }
                Error::NotFound(format!("effect {}", observation.effect_id))
            })?;
            let mut scratch = crate::wire_codec::CodecQuota::from_retained(work, 0);
            let (bytes, claim) = Self::read_admission_content_with_work(
                verifier.as_ref(),
                reference,
                scratch.work(),
            )
            .await?;
            scratch.adopt(claim)?;
            let (schema_bytes, schema_claim) = Self::read_admission_content_with_work(
                verifier.as_ref(),
                &effect.result_schema,
                scratch.work(),
            )
            .await?;
            scratch.adopt(schema_claim)?;
            let validated = crate::effects::validate_result_bytes(
                &effect.result_schema,
                &schema_bytes,
                reference,
                &bytes,
                scratch.work(),
            );
            drop(schema_bytes);
            drop(bytes);
            let retained = scratch.retained();
            let released = scratch.relinquish(retained);
            return match validated {
                Ok(()) => released,
                Err(error) => {
                    let _ = released;
                    Err(error)
                }
            };
        }
        if let Action::OpenInteraction { ticket } = action {
            let mut scratch = crate::wire_codec::CodecQuota::from_retained(work, 0);
            let (bytes, claim) = Self::read_admission_content_with_work(
                verifier.as_ref(),
                reference,
                scratch.work(),
            )
            .await?;
            scratch.adopt(claim)?;
            let validated = ticket
                .validate_request_bytes(&bytes, scratch.work())
                .and_then(|request| {
                    let measured = request.1;
                    drop(request);
                    scratch
                        .work()
                        .as_deref_mut()
                        .map_or(Ok(()), |io| io.release_owned(measured))
                });
            drop(bytes);
            let released = scratch.relinquish(claim);
            return match validated {
                Ok(()) => released,
                Err(error) => {
                    let _ = released;
                    Err(error)
                }
            };
        }
        Ok(())
    }

    pub(super) async fn read_admission_content_with_work(
        verifier: &dyn ContentResidencyVerifier,
        reference: &crate::conversation::FileRef,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<(acyclic_stream::OwnedBytes, u64)> {
        match work.as_deref_mut() {
            Some(observer) => history::read_content_with_work(observer, verifier, reference).await,
            None => verifier
                .read(reference, reference.descriptor().byte_length(), None)?
                .await
                .map(|bytes| (bytes, 0)),
        }
    }

    #[cfg(feature = "filesystem")]
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
        let mut scratch = crate::wire_codec::CodecQuota::from_retained(work, 0);
        let (request_bytes, request_claim) = Self::read_admission_content_with_work(
            verifier.as_ref(),
            &ticket.request,
            scratch.work(),
        )
        .await?;
        scratch.adopt(request_claim)?;
        let request = ticket.validate_request_bytes(&request_bytes, scratch.work())?;
        scratch.adopt(request.1)?;
        let (bytes, claim) =
            Self::read_admission_content_with_work(verifier.as_ref(), reference, scratch.work())
                .await?;
        scratch.adopt(claim)?;
        match detail_outcome {
            Some(outcome) => ticket.validate_decision_bytes(
                &request.0,
                outcome,
                reference,
                &bytes,
                scratch.work(),
            ),
            None => ticket.validate_answer_bytes(&request.0, reference, &bytes, scratch.work()),
        }
    }

    pub(super) async fn validate_admission_content(
        &self,
        action: &crate::core::Action,
    ) -> Result<()> {
        self.validate_interaction_admission(action).await?;
        self.validate_extension_admission_content(action).await?;
        if matches!(
            action,
            Action::PlanEffect { .. } | Action::ResolveEffect { .. }
        ) {
            self.validate_task_admission_content_with_work(action, &mut None)
                .await?;
        }
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
            .await?
            .0;
            match &message.attachments {
                ReferencedAttachments::Inline { items } => {
                    for attachment in items {
                        verified_content_bytes(
                            verifier.as_ref(),
                            &attachment.file,
                            (&attachment.file).descriptor().byte_length(),
                            None,
                        )
                        .await?
                        .0;
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
                .await?
                .0;
            }
        }

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
            if let Action::OpenInteraction { ticket } = action {
                let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                    Error::Unsupported("interaction content verifier is not bound".into())
                })?;
                let bytes = verifier
                    .read(
                        &ticket.request,
                        ticket.request.descriptor().byte_length(),
                        None,
                    )?
                    .await?;
                ticket.validate_request_bytes(&bytes, &mut None)?;
            }
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
        let answer = match &resolution.outcome {
            InteractionOutcome::Answered { answer } => Some(answer),
            _ => None,
        };
        if answer.is_none() && resolution.detail.is_none() {
            return Ok(());
        }
        let verifier = self.content_verifier.as_ref().ok_or_else(|| {
            Error::Unsupported("interaction content verifier is not bound".into())
        })?;
        let bytes = verifier
            .read(
                &ticket.request,
                ticket.request.descriptor().byte_length(),
                None,
            )?
            .await?;
        let request = ticket.validate_request_bytes(&bytes, &mut None)?.0;
        if let Some(answer) = answer {
            let bytes = verifier
                .read(answer, answer.descriptor().byte_length(), None)?
                .await?;
            ticket.validate_answer_bytes(&request, answer, &bytes, &mut None)?;
        }
        if let Some(detail) = &resolution.detail {
            let bytes = verifier
                .read(detail, detail.descriptor().byte_length(), None)?
                .await?;
            ticket.validate_decision_bytes(
                &request,
                &resolution.outcome,
                detail,
                &bytes,
                &mut None,
            )?;
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
        let path = reference.authority.stream_path(work)?;
        let path_bytes = if work.is_some() {
            path.capacity() as u64
        } else {
            0
        };
        let read = match operations::stream(&self.client, &path, work) {
            Ok(stream) => {
                operations::one_record(
                    &self.client,
                    stream.0.path(),
                    reference.revision.saturating_sub(1),
                    work,
                )
                .await
            }
            Err(error) => Err(error),
        };
        drop(path);
        let released_path = match work.as_deref_mut() {
            Some(observer) => observer.release_owned(path_bytes),
            None => Ok(()),
        };
        let record = match read {
            Ok(record) => {
                released_path?;
                record
            }
            Err(error) => {
                let _ = released_path;
                return Err(error);
            }
        }
        .ok_or_else(|| guarded_boundary(work, crate::wire::ErrorCode::NotFound, "causal event"))?;
        let raw = operations::sdk_record_retained_bytes(&record, work);
        let decoded = crate::wire_codec::decode_event(&record.value, work);
        drop(record);
        let released_record = match work.as_deref_mut() {
            Some(observer) => observer.release_owned(raw),
            None => Ok(()),
        };
        let ((authority, event), event_retained) = match decoded {
            Ok(decoded) => {
                released_record?;
                decoded
            }
            Err(error) => {
                let _ = released_record;
                return Err(error);
            }
        };
        let valid = authority == reference.authority && event.revision == reference.revision;
        let owned = if work.is_some() {
            event_retained
                .checked_add(authority.id.capacity() as u64)
                .ok_or_else(|| Error::Invalid("causal record custody overflows".into()))?
        } else {
            0
        };
        drop(event);
        drop(authority);
        if let Some(work) = work.as_deref_mut() {
            work.release_owned(owned)?;
        }
        if !valid {
            return Err(Error::Invalid(
                "causal event reference does not resolve exactly".into(),
            ));
        }
        Ok(())
    }
}
