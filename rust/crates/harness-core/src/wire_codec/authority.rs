use super::*;
/// Converts an aggregate identity to its wire representation.
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
#[doc(hidden)]
pub fn decode_aggregate_kind(
    kind: i32,
    code: wire::ErrorCode,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<AggregateKind> {
    match wire::AggregateKind::try_from(kind)
        .map_err(|_| boundary(work, code, "invalid aggregate kind"))?
    {
        wire::AggregateKind::Agent => Ok(AggregateKind::Agent),
        wire::AggregateKind::Conversation => Ok(AggregateKind::Conversation),
        wire::AggregateKind::Session => Ok(AggregateKind::Session),
        wire::AggregateKind::Turn => Ok(AggregateKind::Turn),
        wire::AggregateKind::Task => Ok(AggregateKind::Task),
        wire::AggregateKind::Unspecified => Err(boundary(work, code, "unspecified aggregate kind")),
    }
}
pub(crate) fn decode_authority(
    authority: wire::Authority,
    code: wire::ErrorCode,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<Authority> {
    let kind = decode_aggregate_kind(authority.kind, code, work)?;
    if authority.id.is_empty() {
        return Err(boundary(work, code, "empty authority identity"));
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
    code: wire::ErrorCode,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<EventReference> {
    Ok(EventReference {
        authority: decode_authority(
            reference
                .authority
                .ok_or_else(|| boundary(work, code, "missing causal authority"))?,
            code,
            work,
        )?,
        revision: reference.revision,
    })
}
pub(crate) fn decode_scope(scope: wire::Scope) -> Result<Scope> {
    let parent = if scope.parent_proof.is_empty() {
        None
    } else {
        Some(
            scope
                .parent_proof
                .try_into()
                .map_err(|_| Error::Invalid("invalid scope parent proof".into()))?,
        )
    };
    Ok(Scope::from_wire(
        scope.id,
        Capabilities::new(scope.capabilities),
        scope.issuer,
        parse_scope_agent(&scope.agent_id, &None)?,
        parent,
        scope
            .proof
            .try_into()
            .map_err(|_| Error::Invalid("invalid scope proof".into()))?,
        scope.workload,
    ))
}
pub(super) fn parse_scope_agent(
    value: &str,
    work: &Option<&mut dyn HistoryIo>,
) -> Result<Option<AgentId>> {
    if value.is_empty() {
        return Ok(None);
    }
    Ok(Some(AgentId::from_bytes(
        *uuid::Uuid::parse_str(value)
            .map_err(|_| boundary(work, Invalid, "invalid scope agent UUID"))?
            .as_bytes(),
    )))
}
