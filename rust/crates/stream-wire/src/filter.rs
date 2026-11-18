//! Canonical wire grammar for a flat Session filter.

use prost::Message as _;

use crate::wire;

/// A filter cannot be represented by the wire contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FilterError {
    /// A clause, range or attribute is malformed.
    Invalid,
    /// The serialized filter exceeds the command budget.
    Limit,
}

/// Validates and normalizes a flat filter before matching.
///
/// # Errors
/// Rejects malformed filters and filters over the command budget.
pub fn canonical(mut filter: wire::SessionFilter) -> Result<wire::SessionFilter, FilterError> {
    if !filter.all_of.is_empty() {
        return Err(FilterError::Invalid);
    }
    if filter.encoded_len() > wire::StreamLimit::MaxCommandBytes as usize {
        return Err(FilterError::Limit);
    }
    for range in &filter.keys {
        if !range.end.is_empty() && range.start >= range.end {
            return Err(FilterError::Invalid);
        }
    }
    filter.keys.sort_unstable_by(|left, right| {
        left.start
            .cmp(&right.start)
            .then_with(|| left.end.cmp(&right.end))
    });
    let mut ranges: Vec<wire::KeyRange> = Vec::new();
    for range in filter.keys {
        if let Some(last) = ranges.last_mut()
            && (last.end.is_empty() || range.start <= last.end)
        {
            if !last.end.is_empty() && (range.end.is_empty() || range.end > last.end) {
                last.end = range.end;
            }
        } else {
            ranges.push(range);
        }
    }
    filter.keys = ranges;
    filter
        .attributes
        .sort_unstable_by(|left, right| left.name.cmp(&right.name));
    if filter
        .attributes
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left.name == right.name))
    {
        return Err(FilterError::Invalid);
    }
    for attribute in &mut filter.attributes {
        if attribute.name.is_empty()
            || attribute.name.chars().any(char::is_control)
            || attribute.any_of.is_empty()
        {
            return Err(FilterError::Invalid);
        }
        attribute.any_of.sort_unstable();
        attribute.any_of.dedup();
    }
    Ok(filter)
}
