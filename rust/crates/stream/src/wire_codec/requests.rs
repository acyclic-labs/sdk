use crate::{
    AppendRecord, AppendRequest, CommitCondition, CommitMutation, CommitRequest, ForkRequest,
    RecordAttribute,
};

pub(crate) fn block_len(bytes: usize) -> usize {
    bytes.saturating_add(1 + prost::length_delimiter_len(bytes))
}
fn uint_len(value: Option<u64>) -> usize {
    value.map_or(0, |value| 1 + prost::encoding::encoded_len_varint(value))
}
fn attribute_len(attribute: RecordAttribute<'_>) -> usize {
    block_len(attribute.name.len()).saturating_add(if attribute.value.is_empty() {
        0
    } else {
        block_len(attribute.value.len())
    })
}
fn append_record_len(record: &AppendRecord) -> usize {
    let value = if record.value.is_empty() {
        0
    } else {
        block_len(record.value.len())
    };
    crate::changes::wire_keys(&record.headers)
        .iter()
        .map(|key| block_len(key.len()))
        .chain(
            record
                .headers
                .attributes()
                .iter()
                .map(|attribute| block_len(attribute_len(attribute))),
        )
        .fold(value, usize::saturating_add)
}
fn records_len(records: &[AppendRecord]) -> usize {
    records_len_with(records, &append_record_len)
}
fn records_len_with(records: &[AppendRecord], length: &impl Fn(&AppendRecord) -> usize) -> usize {
    records
        .iter()
        .map(|record| block_len(length(record)))
        .fold(0_usize, usize::saturating_add)
}

pub(crate) fn append_len(request: &AppendRequest) -> usize {
    block_len(request.path.as_str().len())
        + records_len(&request.records)
        + uint_len(request.if_tail)
        + block_len(request.idempotency_key.as_bytes().len())
}
pub(crate) fn fork_len(request: &ForkRequest) -> usize {
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
fn condition_len(condition: &CommitCondition) -> usize {
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
#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
fn mutation_len(mutation: &CommitMutation) -> usize {
    mutation_len_with(mutation, &append_record_len)
}
fn mutation_len_with(mutation: &CommitMutation, length: &impl Fn(&AppendRecord) -> usize) -> usize {
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
pub(crate) fn commit_len(request: &CommitRequest) -> usize {
    commit_len_with(request, &append_record_len)
}
fn commit_len_with(request: &CommitRequest, length: &impl Fn(&AppendRecord) -> usize) -> usize {
    request
        .conditions
        .iter()
        .map(|condition| block_len(condition_len(condition)))
        .fold(0_usize, usize::saturating_add)
        .saturating_add(
            request
                .mutations
                .iter()
                .map(|mutation| block_len(mutation_len_with(mutation, length)))
                .fold(0_usize, usize::saturating_add),
        )
        .saturating_add(block_len(request.idempotency_key.as_bytes().len()))
        .saturating_add(uint_len(request.deadline_unix_millis))
}

/// A prepared single-record command admits unknown value and header bytes.
/// Count every possible field, then use the ordinary nesting calculation so
/// length-delimiter growth is included at every protobuf boundary.
#[cfg(feature = "local")]
pub(crate) fn prepared_commit_command_len(request: &CommitRequest, maximum: usize) -> usize {
    let fields = 1 + crate::MAX_RECORD_KEYS + 3 * crate::MAX_RECORD_ATTRIBUTES;
    let field_overhead = 1 + prost::length_delimiter_len(crate::MAX_COMMAND_BYTES);
    let record = maximum.saturating_add(fields.saturating_mul(field_overhead));
    block_len(commit_len_with(request, &|_| record))
}

#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(test, feature = "http"),
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
pub(crate) fn header(out: &mut Vec<u8>, tag: u32, length: usize) {
    prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, out);
    prost::encoding::encode_varint(length as u64, out);
}
#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(test, feature = "http"),
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
fn bytes(out: &mut Vec<u8>, tag: u32, value: &[u8]) {
    header(out, tag, value.len());
    out.extend_from_slice(value);
}
#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(test, feature = "http"),
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
fn uint(out: &mut Vec<u8>, tag: u32, value: Option<u64>) {
    if let Some(value) = value {
        prost::encoding::encode_key(tag, prost::encoding::WireType::Varint, out);
        prost::encoding::encode_varint(value, out);
    }
}
#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(test, feature = "http"),
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
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

#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(test, feature = "http"),
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
pub(crate) fn append(out: &mut Vec<u8>, request: &AppendRequest) {
    bytes(out, 1, request.path.as_str().as_bytes());
    records(out, 2, &request.records);
    uint(out, 3, request.if_tail);
    bytes(out, 4, request.idempotency_key.as_bytes());
}
#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
pub(crate) fn fork(out: &mut Vec<u8>, request: &ForkRequest) {
    for path in &request.exclude {
        bytes(out, 5, path.as_str().as_bytes());
    }
    bytes(out, 1, request.source.as_str().as_bytes());
    bytes(out, 2, request.destination.as_str().as_bytes());
    uint(out, 3, request.at_tail);
    bytes(out, 4, request.idempotency_key.as_bytes());
}
#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
pub(crate) fn commit(out: &mut Vec<u8>, request: &CommitRequest) {
    for condition in &request.conditions {
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
    bytes(out, 3, request.idempotency_key.as_bytes());
    uint(out, 4, request.deadline_unix_millis);
}

#[cfg(test)]
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::OwnedBytes;
    use prost::Message;

    #[test]
    #[cfg(feature = "local")]
    fn commit_size_matches_generated_protobuf_at_wire_boundaries()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = crate::StreamPath::new("request/events")?;
        let destination = crate::StreamPath::new("request/fork")?;
        for tail in [0, 127, 128, 16_383, 16_384, u64::MAX] {
            for length in [0, 127, 128, 16_383, 16_384, crate::MAX_RECORD_BYTES] {
                let records: Vec<AppendRecord> = vec![OwnedBytes::from(vec![7; length]).into()];
                let request = CommitRequest {
                    deadline_unix_millis: (tail != 0).then_some(tail),
                    conditions: vec![
                        CommitCondition::Present { path: path.clone() },
                        CommitCondition::Absent {
                            path: destination.clone(),
                        },
                        CommitCondition::Tail {
                            path: path.clone(),
                            expected: tail,
                        },
                        CommitCondition::Head {
                            path: path.clone(),
                            expected: tail,
                            commit_id: crate::CommitId::from_bytes([3; 32]),
                        },
                    ],
                    mutations: vec![
                        CommitMutation::Append {
                            path: path.clone(),
                            records: records.clone(),
                        },
                        CommitMutation::Fork {
                            exclude: vec![
                                crate::StreamPath::new("children")?,
                                crate::StreamPath::new("volumes/cache")?,
                            ],
                            source: path.clone(),
                            destination: destination.clone(),
                            at_tail: tail,
                            records,
                        },
                    ],
                    idempotency_key: crate::IdempotencyKey::new(bytes::Bytes::from_static(
                        b"size",
                    ))?,
                };
                let expected = crate::wire_codec::commit_to_wire(&request).encode_to_vec();
                let mut actual = Vec::new();
                commit(&mut actual, &request);
                assert_eq!(actual, expected);
                assert_eq!(request.wire_encoded_len(), expected.len());
            }
        }
        Ok(())
    }

    #[test]
    fn request_path_append_encoding_matches_generated_protobuf()
    -> Result<(), Box<dyn std::error::Error>> {
        for tail in [None, Some(0), Some(u64::MAX)] {
            for key in [
                crate::IdempotencyKey::new(bytes::Bytes::from_static(b"k"))?,
                crate::IdempotencyKey::new(bytes::Bytes::from_static(b"request-key"))?,
            ] {
                let request = AppendRequest {
                    path: crate::StreamPath::new("request/events")?,
                    records: vec![
                        OwnedBytes::from_static(b"").into(),
                        OwnedBytes::from(vec![7; 65_536]).into(),
                    ],
                    if_tail: tail,
                    idempotency_key: key,
                };
                let expected = crate::wire::AppendRequest {
                    path: request.path.to_string(),
                    records: request
                        .records
                        .iter()
                        .cloned()
                        .map(crate::wire_codec::append_record_wire)
                        .collect(),
                    if_tail: tail,
                    idempotency_key: bytes::Bytes::copy_from_slice(
                        request.idempotency_key.as_bytes(),
                    ),
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
            idempotency_key: crate::IdempotencyKey::unique(),
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
            request.records = records.into_iter().map(Into::into).collect();
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
    fn request_path_measure_append_encoding() -> Result<(), Box<dyn std::error::Error>> {
        let request = AppendRequest {
            path: crate::StreamPath::new("request/events")?,
            records: vec![OwnedBytes::from(vec![7; 65_536]).into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
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
