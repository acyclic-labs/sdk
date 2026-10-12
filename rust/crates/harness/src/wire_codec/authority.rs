use super::*;

/// Classifies malformed input: [`Error::Storage`] for persisted events, [`Error::Invalid`] for client requests.
pub(crate) type Fault = fn(String) -> Error;

/// Converts a semantic aggregate identity without conflating its typed kind.
#[must_use]
pub fn encode_authority(authority: &Authority) -> wire::Authority {
    wire::Authority {
        kind: match authority.kind {
            AggregateKind::Agent => wire::AggregateKind::Agent as i32,
            AggregateKind::Conversation => wire::AggregateKind::Conversation as i32,
            AggregateKind::Session => wire::AggregateKind::Session as i32,
            AggregateKind::Turn => wire::AggregateKind::Turn as i32,
            AggregateKind::Task => wire::AggregateKind::Task as i32,
        },
        id: authority.id.clone(),
    }
}

pub(crate) fn decode_aggregate_kind(kind: i32, fault: Fault) -> Result<AggregateKind> {
    match wire::AggregateKind::try_from(kind)
        .map_err(|_| fault("aggregate kind is invalid".into()))?
    {
        wire::AggregateKind::Agent => Ok(AggregateKind::Agent),
        wire::AggregateKind::Conversation => Ok(AggregateKind::Conversation),
        wire::AggregateKind::Session => Ok(AggregateKind::Session),
        wire::AggregateKind::Turn => Ok(AggregateKind::Turn),
        wire::AggregateKind::Task => Ok(AggregateKind::Task),
        wire::AggregateKind::Unspecified => Err(fault("aggregate kind is unspecified".into())),
    }
}

pub(crate) fn decode_authority(authority: wire::Authority, fault: Fault) -> Result<Authority> {
    let kind = decode_aggregate_kind(authority.kind, fault)?;
    if authority.id.is_empty() {
        return Err(fault("authority identity is empty".into()));
    }
    Ok(Authority {
        kind,
        id: authority.id,
    })
}

pub(super) fn encode_reference(reference: &EventReference) -> wire::EventReference {
    wire::EventReference {
        authority: Some(encode_authority(&reference.authority)),
        revision: reference.revision,
    }
}

pub(super) fn decode_reference(
    reference: wire::EventReference,
    fault: Fault,
) -> Result<EventReference> {
    Ok(EventReference {
        authority: decode_authority(
            reference
                .authority
                .ok_or_else(|| fault("causal authority is missing".into()))?,
            fault,
        )?,
        revision: reference.revision,
    })
}

pub(crate) fn decode_scope(scope: wire::Scope) -> Result<Scope> {
    let parent_proof = if scope.parent_proof.is_empty() {
        None
    } else {
        Some(
            scope
                .parent_proof
                .try_into()
                .map_err(|_| Error::Invalid("scope parent proof must be 32 bytes".into()))?,
        )
    };
    let proof = scope
        .proof
        .try_into()
        .map_err(|_| Error::Invalid("scope proof must be 32 bytes".into()))?;
    Ok(Scope::from_wire(
        scope.id,
        Capabilities::new(scope.capabilities),
        scope.issuer,
        parse_scope_agent(&scope.agent_id)?,
        parent_proof,
        proof,
    ))
}

pub(super) fn parse_scope_agent(value: &str) -> Result<Option<AgentId>> {
    if value.is_empty() {
        Ok(None)
    } else {
        AgentId::parse(value).map(Some)
    }
}
