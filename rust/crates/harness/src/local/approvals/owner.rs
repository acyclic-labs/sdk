use super::*;
use crate::core::{Action, Command, SchemaRegistry};

pub(in crate::local) async fn bind_owner(
    stream: &Arc<LocalStream>,
    issuer: &AuthorityRoot,
    agent: AgentId,
) -> Result<()> {
    let verifier = issuer.verifier(&mut None)?;
    let mut aggregate = crate::store::StreamAggregate::open(
        stream,
        (verifier.audience().clone(), verifier, SchemaRegistry::new()),
        None,
        crate::store::default_projection_read_limits(),
        crate::store::DEFAULT_TERMINAL_EFFECTS,
        None,
        &mut None,
    )
    .await?;
    match aggregate
        .reducer()
        .conversation()
        .and_then(|conversation| conversation.agent)
    {
        Some(bound) if bound == agent => return Ok(()),
        Some(_) => {
            return Err(Error::Unauthorized(
                "approval conversation belongs to another agent".into(),
            ));
        }
        None => {}
    }
    aggregate
        .execute(
            Command {
                operation_id: OperationId::from_bytes(agent.into_bytes()),
                idempotency_key: crate::IdempotencyKey::new("approval-owner")?,
                expected_revision: aggregate.reducer().revision(),
                scope: issuer.bootstrap_scope(
                    "binder",
                    &Capabilities::new(["conversation:bind"]),
                    None,
                    &mut None,
                )?,
                causal_parent: None,
                action: Action::BindConversation { agent },
            },
            &mut None,
        )
        .await?;
    Ok(())
}
