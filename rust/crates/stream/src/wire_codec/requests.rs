use crate::{
    AppendRequest, CommitCondition, CommitMutation, CommitRequest, ForkRequest, OwnedBytes,
};

pub(crate) fn block_len(bytes: usize) -> usize {
    1 + prost::length_delimiter_len(bytes) + bytes
}
fn uint_len(value: Option<u64>) -> usize {
    value.map_or(0, |value| 1 + prost::encoding::encoded_len_varint(value))
}
fn records_len(records: &[OwnedBytes]) -> usize {
    records.iter().map(|record| block_len(record.len())).sum()
}

pub(crate) fn append_len(request: &AppendRequest) -> usize {
    block_len(request.path.as_str().len())
        + records_len(&request.records)
        + uint_len(request.if_tail)
        + request
            .idempotency_key
            .as_ref()
            .map_or(0, |key| block_len(key.as_bytes().len()))
}
pub(crate) fn fork_len(request: &ForkRequest) -> usize {
    block_len(request.source.as_str().len())
        + block_len(request.destination.as_str().len())
        + uint_len(request.at_tail)
        + request
            .idempotency_key
            .as_ref()
            .map_or(0, |key| block_len(key.as_bytes().len()))
}
fn condition_len(condition: &CommitCondition) -> usize {
    block_len(match condition {
        CommitCondition::Tail { path, expected } => {
            block_len(path.as_str().len()) + uint_len((*expected != 0).then_some(*expected))
        }
        CommitCondition::Absent { path } => block_len(path.as_str().len()),
    })
}
fn mutation_len(mutation: &CommitMutation) -> usize {
    block_len(match mutation {
        CommitMutation::Append { path, records } => {
            block_len(path.as_str().len()) + records_len(records)
        }
        CommitMutation::Fork {
            source,
            destination,
            at_tail,
            records,
        } => {
            block_len(source.as_str().len())
                + block_len(destination.as_str().len())
                + uint_len((*at_tail != 0).then_some(*at_tail))
                + records_len(records)
        }
    })
}
pub(crate) fn commit_len(request: &CommitRequest) -> usize {
    request
        .conditions
        .iter()
        .map(|condition| block_len(condition_len(condition)))
        .sum::<usize>()
        + request
            .mutations
            .iter()
            .map(|mutation| block_len(mutation_len(mutation)))
            .sum::<usize>()
        + block_len(request.idempotency_key.as_bytes().len())
}

fn header(out: &mut Vec<u8>, tag: u32, length: usize) {
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
fn records(out: &mut Vec<u8>, tag: u32, values: &[OwnedBytes]) {
    for value in values {
        bytes(out, tag, value);
    }
}

pub(crate) fn append(out: &mut Vec<u8>, request: &AppendRequest) {
    bytes(out, 1, request.path.as_str().as_bytes());
    records(out, 2, &request.records);
    uint(out, 3, request.if_tail);
    if let Some(key) = &request.idempotency_key {
        bytes(out, 4, key.as_bytes());
    }
}
pub(crate) fn fork(out: &mut Vec<u8>, request: &ForkRequest) {
    bytes(out, 1, request.source.as_str().as_bytes());
    bytes(out, 2, request.destination.as_str().as_bytes());
    uint(out, 3, request.at_tail);
    if let Some(key) = &request.idempotency_key {
        bytes(out, 4, key.as_bytes());
    }
}
pub(crate) fn commit(out: &mut Vec<u8>, request: &CommitRequest) {
    for condition in &request.conditions {
        header(out, 1, condition_len(condition));
        match condition {
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
    for mutation in &request.mutations {
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
            } => {
                header(
                    out,
                    2,
                    block_len(source.as_str().len())
                        + block_len(destination.as_str().len())
                        + uint_len((*at_tail != 0).then_some(*at_tail))
                        + records_len(values),
                );
                bytes(out, 1, source.as_str().as_bytes());
                bytes(out, 2, destination.as_str().as_bytes());
                uint(out, 3, (*at_tail != 0).then_some(*at_tail));
                records(out, 4, values);
            }
        }
    }
    bytes(out, 3, request.idempotency_key.as_bytes());
}

pub(crate) fn message_header(out: &mut Vec<u8>, tag: u32, length: usize) {
    header(out, tag, length);
}
