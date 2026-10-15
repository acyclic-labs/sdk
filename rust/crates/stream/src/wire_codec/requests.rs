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
        CommitCondition::Head { path, expected, .. } => {
            block_len(path.as_str().len()) + uint_len(Some(*expected)) + block_len(32)
        }
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

#[cfg(any(
    feature = "local",
    all(test, feature = "http"),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) fn header(out: &mut Vec<u8>, tag: u32, length: usize) {
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(length as u64, out);
}
#[cfg(any(
    feature = "local",
    all(test, feature = "http"),
    all(feature = "wasm", target_arch = "wasm32")
))]
fn bytes(out: &mut Vec<u8>, tag: u32, value: &[u8]) {
    header(out, tag, value.len());
    out.extend_from_slice(value);
}
#[cfg(any(
    feature = "local",
    all(test, feature = "http"),
    all(feature = "wasm", target_arch = "wasm32")
))]
fn uint(out: &mut Vec<u8>, tag: u32, value: Option<u64>) {
    if let Some(value) = value {
        prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, out);
        prost::encoding::encode_varint(value, out);
    }
}
#[cfg(any(
    feature = "local",
    all(test, feature = "http"),
    all(feature = "wasm", target_arch = "wasm32")
))]
fn records(out: &mut Vec<u8>, tag: u32, values: &[OwnedBytes]) {
    for value in values {
        bytes(out, tag, value);
    }
}

#[cfg(any(
    feature = "local",
    all(test, feature = "http"),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) fn append(out: &mut Vec<u8>, request: &AppendRequest) {
    bytes(out, 1, request.path.as_str().as_bytes());
    records(out, 2, &request.records);
    uint(out, 3, request.if_tail);
    if let Some(key) = &request.idempotency_key {
        bytes(out, 4, key.as_bytes());
    }
}
#[cfg(any(feature = "local", all(feature = "wasm", target_arch = "wasm32")))]
pub(crate) fn fork(out: &mut Vec<u8>, request: &ForkRequest) {
    bytes(out, 1, request.source.as_str().as_bytes());
    bytes(out, 2, request.destination.as_str().as_bytes());
    uint(out, 3, request.at_tail);
    if let Some(key) = &request.idempotency_key {
        bytes(out, 4, key.as_bytes());
    }
}
#[cfg(any(feature = "local", all(feature = "wasm", target_arch = "wasm32")))]
pub(crate) fn commit(out: &mut Vec<u8>, request: &CommitRequest) {
    for condition in &request.conditions {
        header(out, 1, condition_len(condition));
        match condition {
            CommitCondition::Head {
                path,
                expected,
                commit_id,
            } => {
                header(
                    out,
                    3,
                    block_len(path.as_str().len()) + uint_len(Some(*expected)) + block_len(32),
                );
                bytes(out, 1, path.as_str().as_bytes());
                uint(out, 2, Some(*expected));
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

#[cfg(all(test, feature = "http", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn request_path_append_encoding_matches_generated_protobuf()
    -> Result<(), Box<dyn std::error::Error>> {
        for tail in [None, Some(0), Some(u64::MAX)] {
            for key in [
                None,
                Some(crate::IdempotencyKey::new(bytes::Bytes::from_static(
                    b"request-key",
                ))?),
            ] {
                let request = AppendRequest {
                    path: crate::StreamPath::new("request/events")?,
                    records: vec![
                        OwnedBytes::from_static(b""),
                        OwnedBytes::from(vec![7; 65_536]),
                    ],
                    if_tail: tail,
                    idempotency_key: key,
                };
                let expected = crate::wire::AppendRequest {
                    path: request.path.to_string(),
                    records: request
                        .records
                        .iter()
                        .map(|record| bytes::Bytes::copy_from_slice(record))
                        .collect(),
                    if_tail: tail,
                    idempotency_key: request
                        .idempotency_key
                        .as_ref()
                        .map(|key| bytes::Bytes::copy_from_slice(key.as_bytes())),
                }
                .encode_to_vec();
                let mut actual = Vec::with_capacity(append_len(&request));
                let capacity = actual.capacity();
                append(&mut actual, &request);
                assert_eq!(actual, expected);
                assert_eq!(actual.len(), append_len(&request));
                assert_eq!(actual.capacity(), capacity);
                assert_eq!(
                    crate::http_codec::encode_append(&request)?,
                    crate::http_codec::encode("append", &expected)?
                );
            }
        }
        Ok(())
    }
    #[test]
    fn request_path_append_json_rejects_invalid_records_and_command_size()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut request = AppendRequest {
            path: crate::StreamPath::new("request/events")?,
            records: Vec::new(),
            if_tail: None,
            idempotency_key: None,
        };
        for records in [
            Vec::new(),
            vec![OwnedBytes::from(vec![0; crate::MAX_RECORD_BYTES + 1])],
            vec![OwnedBytes::from_static(b""); crate::MAX_ITEMS + 1],
            vec![
                OwnedBytes::from(vec![0; crate::MAX_RECORD_BYTES]);
                crate::MAX_COMMAND_BYTES / crate::MAX_RECORD_BYTES + 1
            ],
        ] {
            request.records = records;
            let mut bytes = Vec::with_capacity(append_len(&request));
            append(&mut bytes, &request);
            assert_eq!(
                crate::http_codec::encode_append(&request),
                Err("limit_exceeded")
            );
            assert_eq!(
                crate::http_codec::encode_append(&request),
                crate::http_codec::encode("append", &bytes)
            );
        }
        Ok(())
    }

    #[test]
    #[ignore = "opt-in debug-profile request-path measurement: npm run bench:clients"]
    fn request_path_measure_append_encoding() -> Result<(), Box<dyn std::error::Error>> {
        let request = AppendRequest {
            path: crate::StreamPath::new("request/events")?,
            records: vec![OwnedBytes::from(vec![7; 65_536])],
            if_tail: Some(0),
            idempotency_key: None,
        };
        let mut binary = Vec::with_capacity(append_len(&request));
        append(&mut binary, &request);
        assert_eq!(
            crate::http_codec::encode("append", &binary)?,
            crate::http_codec::encode_append(&request)?
        );
        for direct in [false, true] {
            let started = std::time::Instant::now();
            for _ in 0..256 {
                let encoded = if direct {
                    crate::http_codec::encode_append(&request)?
                } else {
                    let mut encoded = Vec::with_capacity(append_len(&request));
                    append(&mut encoded, &request);
                    crate::http_codec::encode("append", &encoded)?
                };
                std::hint::black_box(encoded);
            }
            eprintln!(
                "stream append JSON direct={direct} iterations=256 bytes=65536 elapsed_us={}",
                started.elapsed().as_micros()
            );
        }
        Ok(())
    }
}
