use super::*;
use acyclic_harness::runtime::{DurableTaskHost as _, TaskStateProvider};
impl NativeSession {
    pub(super) async fn publication_binding<'a>(
        &'a self,
        request: &NativePublicationRequest,
    ) -> Result<&'a NativePublicationConfig> {
        let mut selected = self
            .config
            .native_publications
            .iter()
            .filter(|entry| entry.operation_id == request.operation_id);
        let binding = selected
            .next()
            .ok_or_else(|| Error::Unauthorized("publication operation is not configured".into()))?;
        if selected.next().is_some()
            || canonical_bytes(&binding.caller_lease)? != canonical_bytes(&request.caller_lease)?
        {
            return Err(Error::Unauthorized(
                "publication request differs from its original caller and lease".into(),
            ));
        }
        binding.validate()?;
        if binding.to_project.provider() != &self.config.filesystem_provider {
            return Err(Error::Unauthorized(
                "publication selects another installed provider".into(),
            ));
        }
        let context = self
            .caller_context(binding.caller, &request.caller_lease)
            .await?;
        for grant in [
            binding
                .from_project
                .capability(VolumeOperation::Read, &mut None)?,
            binding
                .to_project
                .capability(VolumeOperation::Read, &mut None)?,
            binding
                .to_project
                .capability(VolumeOperation::Write, &mut None)?,
            "interaction:route".to_owned(),
        ] {
            if !context.scope().grants().contains(&grant) {
                return Err(Error::Unauthorized(
                    "current original task scope does not authorize publication".into(),
                ));
            }
        }
        Ok(binding)
    }
    pub(super) async fn publication_journal(
        &self,
        binding: &NativePublicationConfig,
        request: &NativePublicationRequest,
    ) -> Result<(Journal, Interactions)> {
        let owner = self
            .runtime
            .task_host()
            .journal_owner(
                binding.caller,
                LeaseFence::from(&request.caller_lease.reservation), None)
            .await?;
        let admission = self
            .runtime
            .task_host()
            .observe_admission(binding.caller)
            .await?;
        let maximum = (MAX_FRAME_BYTES as u64).min(admission.limits.file_bytes);
        let verifier = binding.owner.verifier()?;
        let journal = Journal::for_task(
            owner,
            binding.operation_id,
            binding.operation_id,
            self.files.clone(),
            &self.config.private_volume,
            &self.config.owner.verifier()?,
            &self.config.owner.scope,
            maximum,
            None,
        )?
        .with_interaction_owner(
            &verifier,
            &SchemaRegistry::default(),
            &binding.owner.scope,
            &binding.private_volume,
            None,
        )?;
        let interactions = Interactions::new(
            self.stream.clone(),
            self.files.clone(),
            &binding.owner.authority,
            &verifier,
            &SchemaRegistry::default(),
            &binding.owner.scope,
            &binding.private_volume,
            maximum,
            None,
        )?;
        Ok((journal, interactions))
    }
    pub(super) async fn publication_open_approval(
        &self,
        binding: &NativePublicationConfig,
        journal: &Journal,
        original: &MaterializationJournal,
    ) -> Result<()> {
        let digest = publication_digest(binding, original)?;
        if original.phase == MaterializationPhase::Preparing {
            return Err(Error::Conflict(
                "approval requires the complete captured original plan".into(),
            ));
        }
        journal.open_interaction(interaction_id(binding), Interaction::Approval {
            prompt: "Approve the exact retained native root generation publication; display text grants no authority.".into(),
            operation_id: binding.operation_id, action_digest: digest,
        }).await
    }
    pub(super) async fn publication_result(
        &self,
        binding: &NativePublicationConfig,
        interactions: &Interactions,
        journal: Option<MaterializationJournal>,
        work: WorkCounters,
    ) -> Result<NativePublicationResult> {
        let mut result = publication_base(binding, work);
        if let Some(journal) = journal {
            if journal.phase != MaterializationPhase::Preparing {
                result.action_digest = Some(publication_digest(binding, &journal)?);
            }
            result.status = match journal.phase {
                MaterializationPhase::Applied => "applied",
                MaterializationPhase::Applying | MaterializationPhase::Preparing => "recovering",
                MaterializationPhase::RollingBack | MaterializationPhase::RolledBack => "rejected",
                MaterializationPhase::Prepared => "pending",
            };
            result.publication = Some(journal);
        }
        if let Some((ticket, resolution)) = interactions.read(interaction_id(binding)).await? {
            let digest = result.action_digest.ok_or_else(|| {
                Error::Conflict("approval lacks the original publication journal".into())
            })?;
            if ticket.id != uuid::Uuid::from_bytes(interaction_id(binding).into_bytes())
                || !ticket.approval.as_ref().is_some_and(|approval| {
                    approval.operation_id == binding.operation_id
                        && approval.action_digest == digest
                })
            {
                return Err(Error::Unauthorized(
                    "retained approval differs from the complete original publication plan".into(),
                ));
            }
            let original = interactions
                .read_request(interaction_id(binding))
                .await?
                .ok_or_else(|| Error::Storage("publication approval request is missing".into()))?;
            if !matches!(original, Interaction::Approval { operation_id, action_digest, .. }
                if operation_id == binding.operation_id && action_digest == digest)
            {
                return Err(Error::Unauthorized(
                    "approval request differs from original publication".into(),
                ));
            }
            if let Some(resolution) = &resolution {
                resolution.validate(&ticket)?;
                result.status = match &resolution.outcome {
                    InteractionOutcome::Approved
                        if matches!(result.status, "applied" | "recovering") =>
                    {
                        result.status
                    }
                    InteractionOutcome::Approved => "approved",
                    InteractionOutcome::Indeterminate { .. } => "pending",
                    _ => "rejected",
                };
            } else if binding
                .owner
                .scope
                .capabilities()
                .contains("interaction:resolve")
            {
                result.approved_frame_hex =
                    Some(self.publication_approval_frame(binding, &ticket).await?);
            }
            result.interaction = Some(ticket);
            result.resolution = resolution;
        }
        Ok(result)
    }
}
