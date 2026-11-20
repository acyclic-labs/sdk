//! The canonical request encoders. They write exactly the bytes whose sizes
//! [`super::len`] computes, so an admitted size and the encoded size agree.
use super::len::{
    append_record_len, attribute_len, block_len, condition_len, effect_len_with, mutation_len,
    records_len, uint_len,
};
use crate::{
    AppendRecord, AppendRequest, CommitCondition, CommitEffect, CommitMutation, CommitRequest,
    ForkRequest,
};

/// Writes a length-delimited field header.
pub fn header(out: &mut Vec<u8>, tag: u32, length: usize) {
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(length as u64, out);
}
fn bytes(out: &mut Vec<u8>, tag: u32, value: &[u8]) {
    header(out, tag, value.len());
    out.extend_from_slice(value);
}
fn uint(out: &mut Vec<u8>, tag: u32, value: Option<u64>) {
    if let Some(value) = value {
        prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, out);
        prost::encoding::encode_varint(value, out);
    }
}
fn records(out: &mut Vec<u8>, tag: u32, records: &[AppendRecord]) {
    for record in records {
        header(out, tag, append_record_len(record));
        if !record.value.is_empty() {
            bytes(out, 1, &record.value);
        }
        for key in crate::changes::wire_keys(&record.headers) {
            bytes(out, 2, key);
        }
        for attribute in record.headers.attributes() {
            header(out, 3, attribute_len(attribute));
            bytes(out, 1, attribute.name.as_bytes());
            if !attribute.value.is_empty() {
                bytes(out, 2, attribute.value);
            }
        }
    }
}

/// Writes an append request's fields.
pub fn append(out: &mut Vec<u8>, request: &AppendRequest) {
    bytes(out, 1, request.path.as_str().as_bytes());
    records(out, 2, &request.records);
    uint(out, 3, request.if_tail);
    bytes(out, 4, request.idempotency_key.as_bytes());
}
/// Writes a fork request's fields.
pub fn fork(out: &mut Vec<u8>, request: &ForkRequest) {
    for path in &request.exclude {
        bytes(out, 5, path.as_str().as_bytes());
    }
    bytes(out, 1, request.source.as_str().as_bytes());
    bytes(out, 2, request.destination.as_str().as_bytes());
    uint(out, 3, request.at_tail);
    bytes(out, 4, request.idempotency_key.as_bytes());
}
/// Writes a commit request's fields in tag order.
pub fn commit(out: &mut Vec<u8>, request: &CommitRequest) {
    // Generated protobuf writes fields in tag order: `effects` (6) follows
    // the key (3) and deadline (4).
    bytes(out, 3, request.idempotency_key.as_bytes());
    uint(out, 4, request.deadline_unix_millis);
    for effect in &request.effects {
        header(out, 6, effect_len_with(effect, &append_record_len));
        commit_effect(out, effect);
    }
}

fn commit_effect(out: &mut Vec<u8>, effect: &CommitEffect) {
    for condition in &effect.conditions {
        header(out, 1, condition_len(condition));
        match condition {
            CommitCondition::Present { path } => {
                header(out, 4, block_len(path.as_str().len()));
                bytes(out, 1, path.as_str().as_bytes());
            }
            CommitCondition::Head {
                path,
                expected,
                commit_id,
            } => {
                header(
                    out,
                    3,
                    block_len(path.as_str().len())
                        + uint_len((*expected != 0).then_some(*expected))
                        + block_len(32),
                );
                bytes(out, 1, path.as_str().as_bytes());
                uint(out, 2, (*expected != 0).then_some(*expected));
                bytes(out, 3, commit_id.as_bytes());
            }
            CommitCondition::Tail { path, expected } => {
                header(
                    out,
                    1,
                    block_len(path.as_str().len())
                        + uint_len((*expected != 0).then_some(*expected)),
                );
                bytes(out, 1, path.as_str().as_bytes());
                uint(out, 2, (*expected != 0).then_some(*expected));
            }
            CommitCondition::Absent { path } => {
                header(out, 2, block_len(path.as_str().len()));
                bytes(out, 1, path.as_str().as_bytes());
            }
        }
    }
    for mutation in &effect.mutations {
        header(out, 2, mutation_len(mutation));
        match mutation {
            CommitMutation::Append {
                path,
                records: values,
            } => {
                header(out, 1, block_len(path.as_str().len()) + records_len(values));
                bytes(out, 1, path.as_str().as_bytes());
                records(out, 2, values);
            }
            CommitMutation::Fork {
                source,
                destination,
                at_tail,
                records: values,
                exclude,
            } => {
                header(
                    out,
                    2,
                    block_len(source.as_str().len())
                        + block_len(destination.as_str().len())
                        + uint_len((*at_tail != 0).then_some(*at_tail))
                        + records_len(values)
                        + exclude
                            .iter()
                            .map(|path| block_len(path.as_str().len()))
                            .sum::<usize>(),
                );
                bytes(out, 1, source.as_str().as_bytes());
                bytes(out, 2, destination.as_str().as_bytes());
                uint(out, 3, (*at_tail != 0).then_some(*at_tail));
                records(out, 4, values);
                for path in exclude {
                    bytes(out, 5, path.as_str().as_bytes());
                }
            }
        }
    }
    uint(out, 3, effect.soft.then_some(1));
}
