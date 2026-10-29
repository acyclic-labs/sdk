use super::InvalidPolicy;

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
    ])?;
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

/// Admit closure scratch and the direct Unicode table once. The scalar domain
/// has 4,352 blocks; this is a representation size, independent of policy size.
pub(super) fn determinization_scratch(
    nodes: usize,
    symbols: usize,
) -> Result<usize, InvalidPolicy> {
    sum([
        product(nodes, product(8, size_of::<usize>())?)?,
        product(sum([symbols.min(4352), 1])?, 2048)?,
        4352 * size_of::<u16>(),
        4096,
    ])
}

/// Each discovered subset pays before its vectors/map entries are allocated.
/// This includes vector growth, the lookup key, terminal state and match list.
pub(super) fn state_storage(
    members: usize,
    symbols: usize,
    state_bytes: usize,
) -> Result<usize, InvalidPolicy> {
    sum([
        product(members, 4 * size_of::<usize>())?,
        product(state_bytes, 2)?,
        product(sum([symbols, 1])?, 2 * size_of::<usize>())?,
        256,
    ])
}
