use super::*;
use prost::Message as _;
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
        if selected.next().is_some() || binding.caller_lease != request.caller_lease {
            return Err(Error::Unauthorized(
                "publication request differs from its original caller and lease".into(),
            ));
        }
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
                &(LeaseFence::from(&request.caller_lease.reservation)),
                None,
            )
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
        journal.open_interaction(interaction_id(binding), &Interaction::Approval {
            prompt: "Approve the exact retained native root generation publication; display text grants no authority.".into(),
            operation_id: binding.operation_id, action_digest: digest,
        }, None)?.await
    }
    pub(super) async fn publication_approval_frame(
        &self,
        binding: &NativePublicationConfig,
        ticket: &InteractionTicket,
    ) -> Result<String> {
        self.runtime
            .task_host()
            .verify_dispatch_owner(
                binding.caller,
                &LeaseFence::from(&binding.caller_lease.reservation),
                None,
            )?
            .await?;
        let issuer = AuthorityIssuer::new(
            &binding.owner.issuer,
            binding.owner.key,
            binding.owner.authority.clone(),
        );
        let capabilities =
            Capabilities::new(["interaction:resolve".to_owned(), ticket.responder_grant()]);
        let scope = issuer.issue(
            &format!("native-publication-responder:{}", ticket.id),
            &capabilities,
            binding.owner.scope.agent(),
            None,
            &mut None,
        )?;
        let aggregate = StreamAggregate::open(
            &self.stream,
            binding.owner.authority.clone(),
            issuer.verifier(&mut None)?,
            SchemaRegistry::default(),
            None,
            acyclic_harness::store::default_projection_read_limits(),
            acyclic_harness::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut None,
        )
        .await?;
        let operation = approval_operation(binding.operation_id);
        let action = Action::ResolveInteraction {
            resolution: InteractionResolution {
                id: ticket.id,
                expected_version: 1,
                outcome: InteractionOutcome::Approved {},
                detail: None,
            },
        };
        let proof: [u8; 32] =
            serde_json::from_value(serde_json::to_value(&scope).map_err(invalid)?["proof"].take())
                .map_err(invalid)?;
        let envelope = wire::CommandEnvelope {
            protocol: Some(current_protocol()),
            authority: Some(encode_authority(&binding.owner.authority)),
            operation: Some(wire::OperationIdentity {
                operation_id: operation.to_string(),
                idempotency_key: format!("native-publication-decision:{operation}"),
            }),
            expected_revision: aggregate.reducer().revision(),
            scope: Some(wire::Scope {
                id: scope.id().into(),
                capabilities: scope.capabilities().iter().map(ToOwned::to_owned).collect(),
                issuer: issuer.id().into(),
                agent_id: scope
                    .agent()
                    .map_or_else(String::new, |agent| agent.to_string()),
                parent_proof: Vec::new(),
                proof: proof.to_vec(),
            }),
            action_type: "resolve_interaction".into(),
            canonical_action_json: canonical_bytes(&action)?,
            ..Default::default()
        };
        Ok(hex::encode(
            wire::ClientFrame {
                frame: Some(wire::client_frame::Frame::Command(envelope)),
            }
            .encode_to_vec(),
        ))
    }

    pub(super) async fn publication_approval(
        &self,
        binding: &NativePublicationConfig,
        interactions: &Interactions,
        original: Option<&MaterializationJournal>,
    ) -> Result<Option<(InteractionTicket, Option<InteractionResolution>)>> {
        let approval = interactions.read(interaction_id(binding), None).await?;
        if let Some((ticket, resolution)) = &approval {
            let original = original
                .filter(|original| original.phase != MaterializationPhase::Preparing)
                .ok_or_else(|| {
                    Error::Conflict("approval lacks the original publication journal".into())
                })?;
            let digest = publication_digest(binding, original)?;
            if !ticket.approval.as_ref().is_some_and(|approval| {
                approval.operation_id == binding.operation_id && approval.action_digest == digest
            }) {
                return Err(Error::Unauthorized(
                    "retained approval differs from the complete original publication plan".into(),
                ));
            }
            // SDK read_request validates the pinned bytes against this ticket's approval binding.
            interactions
                .read_request(interaction_id(binding), None)
                .await?
                .ok_or_else(|| Error::Storage("publication approval request is missing".into()))?;
            if let Some(resolution) = resolution {
                resolution.validate(ticket)?;
            }
        }
        Ok(approval)
    }

    pub(super) async fn publication_report(
        &self,
        binding: &NativePublicationConfig,
        receipt: acyclic_fs::OperationReceipt<Option<MaterializationJournal>>,
        approval: Option<(InteractionTicket, Option<InteractionResolution>)>,
    ) -> Result<serde_json::Value> {
        let status = publication_status(receipt.value.as_ref(), approval.as_ref());
        let action_digest = receipt
            .value
            .as_ref()
            .filter(|journal| journal.phase != MaterializationPhase::Preparing)
            .map(|journal| publication_digest(binding, journal))
            .transpose()?;
        let approved_frame_hex = match &approval {
            Some((ticket, None))
                if binding
                    .owner
                    .scope
                    .capabilities()
                    .contains("interaction:resolve") =>
            {
                Some(self.publication_approval_frame(binding, ticket).await?)
            }
            _ => None,
        };
        let (interaction, resolution) = approval.map_or((None, None), |(ticket, resolution)| {
            (Some(ticket), resolution)
        });
        Ok(serde_json::json!({
            "operation_id": binding.operation_id, "caller": binding.caller, "status": status,
            "interaction": interaction, "resolution": resolution, "action_digest": action_digest,
            "publication": receipt.value, "work": receipt.work, "failure": null,
            "approved_frame_hex": approved_frame_hex,
        }))
    }
}
