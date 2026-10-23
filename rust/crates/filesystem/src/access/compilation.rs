use super::InvalidPolicy;

pub(super) const MAXIMUM_NFA_STATES: usize = 32_768;
pub(super) const MAXIMUM_STATES: usize = 4_096;
pub(super) const MAXIMUM_TRANSITIONS: usize = 262_144;
pub(super) const MAXIMUM_STATE_MEMBERS: usize = 262_144;
pub(super) const MAXIMUM_TEXT_BYTES: usize = 1_048_576;
pub(super) const MAXIMUM_WORK: usize = 4_194_304;

pub(super) fn spend(remaining: &mut usize, amount: usize) -> Result<(), InvalidPolicy> {
    *remaining = remaining
        .checked_sub(amount)
        .ok_or(InvalidPolicy("glob compilation exceeds work limit"))?;
    Ok(())
}

fn sum(values: impl IntoIterator<Item = usize>) -> Result<usize, InvalidPolicy> {
    values
        .into_iter()
        .try_fold(0_usize, |total, value| total.checked_add(value))
        .ok_or(InvalidPolicy("glob compilation storage overflow"))
}

fn product(left: usize, right: usize) -> Result<usize, InvalidPolicy> {
    left.checked_mul(right)
        .ok_or(InvalidPolicy("glob compilation storage overflow"))
}

pub(super) fn preparation(
    policy: &super::Policy,
    capability: &super::Capability,
    folded: bool,
) -> Result<(usize, usize), InvalidPolicy> {
    let rules = sum(policy.layers.iter().map(Vec::len))?;
    let grants = sum(capability.layers.iter().map(Vec::len))?;
    let layers = sum([policy.layers.len(), capability.layers.len()])?;
    let patterns = sum(policy
        .layers
        .iter()
        .flatten()
        .map(|rule| rule.resource.path.len())
        .chain(
            capability
                .layers
                .iter()
                .flatten()
                .map(|grant| grant.resource.path.len()),
        ))?;
    let identifiers = sum(policy.layers.iter().flatten().map(|rule| rule.id.len()))?;
    let records = sum([rules, grants])?;
    let questions = if folded {
        sum(policy
            .layers
            .iter()
            .flatten()
            .map(|rule| {
                rule.resource
                    .path
                    .bytes()
                    .filter(|byte| *byte == b'?')
                    .count()
            })
            .chain(capability.layers.iter().flatten().map(|grant| {
                grant
                    .resource
                    .path
                    .bytes()
                    .filter(|byte| *byte == b'?')
                    .count()
            })))?
    } else {
        0
    };
    let nodes = sum([
        product(patterns, 2)?,
        product(records, 4)?,
        product(questions, super::glob::folded_question_states())?,
    ])?
    .min(MAXIMUM_NFA_STATES);
    // Per-byte NFA nodes/edges, folding scratch and component vectors; fixed
    // per-record credit covers B-tree nodes, starts, rule/grant maps and ids.
    let bytes = sum([
        product(nodes, 512)?,
        product(patterns, 32)?,
        product(records, 4096)?,
        product(identifiers, 2)?,
        product(layers, 256)?,
        4096,
    ])?;
    let work = sum([
        product(sum([patterns, identifiers])?, 16)?,
        product(sum([records, layers])?, 128)?,
        product(nodes, 16)?,
    ])?;
    Ok((bytes, work))
}

pub(super) fn determinization(
    nodes: usize,
    symbols: usize,
    state_bytes: usize,
    maximum_states: usize,
) -> Result<usize, InvalidPolicy> {
    let states = 1_usize
        .checked_shl(u32::try_from(nodes).unwrap_or(u32::MAX))
        .unwrap_or(MAXIMUM_STATES)
        .min(maximum_states);
    let members = product(states, nodes)?.min(MAXIMUM_STATE_MEMBERS);
    let transitions = product(states, sum([symbols, 1])?)?.min(MAXIMUM_TRANSITIONS);
    // Subset keys and vectors, their growth and current NFA closure scratch;
    // state/transition vectors; map nodes; the direct Unicode alphabet table.
    sum([
        product(members, 4 * size_of::<usize>())?,
        product(nodes, product(8, size_of::<usize>())?)?,
        product(states, sum([product(state_bytes, 2)?, 256])?)?,
        product(transitions, 2 * size_of::<usize>())?,
        product(sum([symbols.min(4352), 1])?, 1024)?,
        4352 * size_of::<u16>(),
        4096,
    ])
}
