use super::*;
use acyclic_harness::runtime::TaskStateProvider;
use prost::Message as _;
impl NativeSession {
    pub(super) async fn publication_approval_frame(
        &self,
        binding: &NativePublicationConfig,
        ticket: &InteractionTicket,
    ) -> Result<String> {
        self.runtime
            .task_host()
            .verify_dispatch_owner(
                binding.caller,
                LeaseFence::from(&binding.caller_lease.reservation),
            )
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
                outcome: InteractionOutcome::Approved,
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
}
