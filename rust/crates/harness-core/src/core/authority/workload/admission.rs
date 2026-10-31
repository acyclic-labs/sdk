//! A workload chain's typed scope facts, read from every block.
use super::*;

/// The facts of the tip block: each block carries its scope identity and
/// grants, may name a new acting agent, and never widens its parent.
pub(super) fn scope_facts(capability: &VerifiedCapability) -> Result<Claims> {
    let mut last: Option<Claims> = None;
    for (index, block) in capability.blocks().iter().enumerate() {
        let mut id = None;
        let mut grants = None;
        let mut agent = last.as_ref().and_then(|parent| parent.agent);
        let mut named = false;
        for caveat in &block.caveats {
            match &caveat.kind {
                CaveatKind::Label(label) => {
                    if let Some(value) = label.strip_prefix(SCOPE)
                        && id.replace(value).is_some()
                    {
                        return Err(unauthorized("a block carries two scopes"));
                    }
                }
                CaveatKind::Grants(set) => {
                    if grants.replace(set).is_some() {
                        return Err(unauthorized("a block carries two scope grant sets"));
                    }
                }
                CaveatKind::LineageAppend(lineage) => {
                    for acting in &lineage.ids {
                        let Some(value) = acting.strip_prefix(AGENT) else {
                            continue;
                        };
                        if std::mem::replace(&mut named, true) {
                            return Err(unauthorized("a block names two agents"));
                        }
                        agent = Some(AgentId::parse(value).map_err(unauthorized)?);
                    }
                }
                CaveatKind::Pin(_) => return Err(unauthorized("untyped scope claim")),
                _ => {}
            }
        }
        let (Some(id), Some(grants)) = (id, grants) else {
            return Err(unauthorized("admission lacks a complete scope chain"));
        };
        let capabilities = capabilities(grants)?;
        if last
            .as_ref()
            .is_some_and(|parent| !capabilities.is_subset_of(&parent.capabilities))
        {
            return Err(unauthorized("admission widens its ancestor"));
        }
        let parent = match index {
            // A root's parent is the tip its certificate is revoked with.
            0 => capability
                .revocation_ids()
                .nth(capability.block_ids().len())
                .map(<[u8; 32]>::try_from)
                .transpose()
                .map_err(unauthorized)?,
            _ => capability.block_ids().get(index - 1).copied(),
        };
        last = Some(Claims {
            id: id.to_owned(),
            capabilities,
            agent,
            parent,
        });
    }
    last.ok_or_else(|| unauthorized("admission has no scope"))
}

/// The capabilities a scope grant set holds.
fn capabilities(grants: &GrantSet) -> Result<Capabilities> {
    let mut held = Vec::new();
    for grant in &grants.grants {
        let GrantKind::Resource(grant) = &grant.kind else {
            continue;
        };
        let Some(name) = grant.resource.name.strip_prefix(CAPABILITIES) else {
            continue;
        };
        if grant.resource.prefix || grant.actions.iter().any(|action| action != HOLD) {
            return Err(unauthorized("malformed capability grant"));
        }
        held.push(
            String::from_utf8(acyclic_auth::decode_component(name).map_err(unauthorized)?)
                .map_err(unauthorized)?,
        );
    }
    Ok(Capabilities::new(held))
}
