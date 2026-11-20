//! Exact encoded lengths of the canonical request messages.
//!
//! These are the one length calculation shared by request admission and the
//! wire encoders, so a command's admitted size and its encoded size cannot
//! disagree.

use crate::{
    AppendRecord, AppendRequest, CommitCondition, CommitEffect, CommitMutation, CommitRequest,
    ForkRequest, RecordAttribute,
};

/// A length-delimited block's encoded size.
pub fn block_len(bytes: usize) -> usize {
    bytes.saturating_add(1 + prost::length_delimiter_len(bytes))
}
/// A varint field's encoded size, absent when `None`.
pub fn uint_len(value: Option<u64>) -> usize {
    value.map_or(0, |value| 1 + prost::encoding::encoded_len_varint(value))
}
/// One record attribute's encoded size.
pub fn attribute_len(attribute: RecordAttribute<'_>) -> usize {
    block_len(attribute.name.len()).saturating_add(if attribute.value.is_empty() {
        0
    } else {
        block_len(attribute.value.len())
    })
}
/// A record body's encoded size, without its origin.
pub fn record_body_len(value: &[u8], headers: &crate::RecordHeaders) -> usize {
    let value = if value.is_empty() {
        0
    } else {
        block_len(value.len())
    };
    crate::changes::wire_keys(headers)
        .iter()
        .map(|key| block_len(key.len()))
        .chain(
            headers
                .attributes()
                .iter()
                .map(|attribute| block_len(attribute_len(attribute))),
        )
        .fold(value, usize::saturating_add)
}
/// One appended record's encoded size.
pub fn append_record_len(record: &AppendRecord) -> usize {
    record_body_len(&record.value, &record.headers)
}
/// A record list's encoded size.
pub fn records_len(records: &[AppendRecord]) -> usize {
    records_len_with(records, &append_record_len)
}
/// A record list's encoded size, with a caller-supplied per-record length.
pub fn records_len_with(
    records: &[AppendRecord],
    length: &impl Fn(&AppendRecord) -> usize,
) -> usize {
    records
        .iter()
        .map(|record| block_len(length(record)))
        .fold(0_usize, usize::saturating_add)
}
/// One commit condition's encoded size.
pub fn condition_len(condition: &CommitCondition) -> usize {
    block_len(match condition {
        CommitCondition::Head { path, expected, .. } => {
            block_len(path.as_str().len())
                + uint_len((*expected != 0).then_some(*expected))
                + block_len(32)
        }
        CommitCondition::Tail { path, expected } => {
            block_len(path.as_str().len()) + uint_len((*expected != 0).then_some(*expected))
        }
        CommitCondition::Absent { path } | CommitCondition::Present { path } => {
            block_len(path.as_str().len())
        }
    })
}
/// One commit mutation's encoded size.
pub fn mutation_len(mutation: &CommitMutation) -> usize {
    mutation_len_with(mutation, &append_record_len)
}
/// One commit mutation's encoded size, with a caller-supplied per-record length.
pub fn mutation_len_with(
    mutation: &CommitMutation,
    length: &impl Fn(&AppendRecord) -> usize,
) -> usize {
    block_len(match mutation {
        CommitMutation::Append { path, records } => {
            block_len(path.as_str().len()).saturating_add(records_len_with(records, length))
        }
        CommitMutation::Fork {
            source,
            destination,
            at_tail,
            records,
            exclude,
        } => {
            exclude
                .iter()
                .map(|path| block_len(path.as_str().len()))
                .sum::<usize>()
                + block_len(source.as_str().len())
                    .saturating_add(block_len(destination.as_str().len()))
                    .saturating_add(uint_len((*at_tail != 0).then_some(*at_tail)))
                    .saturating_add(records_len_with(records, length))
        }
    })
}
/// The encoded `CommitEffect` message, without its enclosing field.
pub fn effect_len_with(effect: &CommitEffect, length: &impl Fn(&AppendRecord) -> usize) -> usize {
    effect
        .conditions
        .iter()
        .map(|condition| block_len(condition_len(condition)))
        .fold(0_usize, usize::saturating_add)
        .saturating_add(
            effect
                .mutations
                .iter()
                .map(|mutation| block_len(mutation_len_with(mutation, length)))
                .fold(0_usize, usize::saturating_add),
        )
        .saturating_add(uint_len(effect.soft.then_some(1)))
}
/// A commit request's encoded size, with a caller-supplied per-record length.
pub fn commit_len_with(request: &CommitRequest, length: &impl Fn(&AppendRecord) -> usize) -> usize {
    request
        .effects
        .iter()
        .map(|effect| block_len(effect_len_with(effect, length)))
        .fold(0_usize, usize::saturating_add)
        .saturating_add(block_len(request.idempotency_key.as_bytes().len()))
        .saturating_add(uint_len(request.deadline_unix_millis))
}

/// The encoded size of an append request.
pub fn append_len(request: &AppendRequest) -> usize {
    block_len(request.path.as_str().len())
        + records_len(&request.records)
        + uint_len(request.if_tail)
        + block_len(request.idempotency_key.as_bytes().len())
}
/// The encoded size of a fork request.
pub fn fork_len(request: &ForkRequest) -> usize {
    request
        .exclude
        .iter()
        .map(|path| block_len(path.as_str().len()))
        .sum::<usize>()
        + block_len(request.source.as_str().len())
        + block_len(request.destination.as_str().len())
        + uint_len(request.at_tail)
        + block_len(request.idempotency_key.as_bytes().len())
}
/// The encoded size of a commit request.
pub fn commit_len(request: &CommitRequest) -> usize {
    commit_len_with(request, &append_record_len)
}
