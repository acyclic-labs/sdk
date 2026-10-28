//! Sealed workload descendants plus independently authenticated runtime time.
use super::*;

pub(super) fn scope_facts(
    capability: &VerifiedCapability,
    historical: bool,
) -> Result<(RecordAttributes, Claims)> {
    let mut attributes = Vec::new();
    let mut previous: Option<Claims> = None;
    for scoped in capability.effective().pins() {
        let pin = scoped.pin();
        if historical && pin.name.starts_with("acyclic.admission.") {
            if scoped.declaring_block() + 1 != capability.block_ids().len() {
                return Err(unauthorized("admission facts precede the final block"));
            }
            continue;
        }
        let index = attributes.len();
        if scoped.declaring_block() != index || pin.name != format!("{CLAIM}{index}") {
            return Err(unauthorized("invalid admission scope ancestry"));
        }
        let PinValue::Literal(value) = &pin.value else {
            return Err(unauthorized("nonliteral admission scope"));
        };
        let current: Claims = serde_json::from_slice(value).map_err(unauthorized)?;
        if previous.as_ref().is_some_and(|parent| {
            current.issuer != parent.issuer
                || current.agent != parent.agent
                || !current.capabilities.is_subset_of(&parent.capabilities)
                || current.parent != capability.block_ids().get(index - 1).copied()
        }) {
            return Err(unauthorized("admission widens its ancestor"));
        }
        previous = Some(current);
        attributes.push(Attribute {
            name: pin.name.clone(),
            value: value.clone(),
        });
    }
    if attributes.len() + usize::from(historical) != capability.block_ids().len() {
        return Err(unauthorized("admission lacks a complete scope chain"));
    }
    Ok((
        RecordAttributes::try_new(attributes).map_err(unauthorized)?,
        previous.ok_or_else(|| unauthorized("admission has no scope"))?,
    ))
}
