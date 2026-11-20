pub use acyclic_stream_contract_core::request::len::{append_len, block_len, commit_len, fork_len};
use acyclic_stream_contract_core::request::len::{record_body_len, uint_len};

/// Encoded length of `record_wire(record, followed)`.
pub fn record_len(
    record: &acyclic_stream_contract_core::Record,
    followed: Option<&acyclic_stream_contract_core::StreamPath>,
) -> usize {
    record_body_len(&record.value, &record.headers)
        .saturating_add(
            super::spelled_origin(record, followed)
                .map_or(0, |origin| block_len(origin.as_str().len())),
        )
        .saturating_add(uint_len((record.sequence != 0).then_some(record.sequence)))
        .saturating_add(block_len(record.commit_id.as_bytes().len()))
        .saturating_add(uint_len(
            (record.committed_at_micros != 0).then_some(record.committed_at_micros),
        ))
}

/// The encoded size of an inherited origin.
pub fn origin_len(
    origin: &acyclic_stream_contract_core::Provenance<
        acyclic_stream_contract_core::StreamPath,
        acyclic_stream_contract_core::ForkCut,
    >,
) -> usize {
    block_len(origin.source.as_str().len())
        .saturating_add(uint_len(
            (origin.inherited.forked_at != 0).then_some(origin.inherited.forked_at),
        ))
        .saturating_add(block_len(origin.inherited.commit_id.as_bytes().len()))
}

/// The encoded size of one child.
pub fn child_len(child: &acyclic_stream_contract_core::Child) -> usize {
    block_len(child.path.as_str().len())
        .saturating_add(uint_len(child.tail))
        .saturating_add(
            child
                .tail_record
                .as_ref()
                .map_or(0, |record| block_len(record_len(record, None))),
        )
        .saturating_add(
            child
                .origin
                .as_ref()
                .map_or(0, |origin| block_len(origin_len(origin))),
        )
}

fn revision_len(revision: &acyclic_stream_contract_core::Revision) -> usize {
    (if revision.scope.as_bytes().is_empty() {
        0
    } else {
        block_len(revision.scope.as_bytes().len())
    })
    .saturating_add(block_len(16))
    .saturating_add(uint_len((revision.index != 0).then_some(revision.index)))
}

/// Encoded page length from its framed children and optional continuation.
pub fn children_page_len(
    children_bytes: usize,
    next_after: Option<&acyclic_stream_contract_core::StreamPath>,
    scanned: u32,
) -> usize {
    block_len(32)
        .saturating_add(children_bytes)
        .saturating_add(next_after.map_or(0, |path| block_len(path.as_str().len())))
        .saturating_add(uint_len((scanned != 0).then_some(u64::from(scanned))))
}

/// The encoded size of a heads view.
pub fn heads_len(view: &acyclic_stream_contract_core::Heads) -> usize {
    heads_len_with_durability(view, false)
}

/// Reserves deferred durable watermarks without claiming them during capture.
pub fn heads_len_with_durability(
    view: &acyclic_stream_contract_core::Heads,
    deferred: bool,
) -> usize {
    heads_frame_len(view, deferred, true)
}

/// The frame without its children page, which a page budget charges row by row.
pub fn heads_len_without_children(
    view: &acyclic_stream_contract_core::Heads,
    deferred: bool,
) -> usize {
    heads_frame_len(view, deferred, false)
}

fn heads_frame_len(
    view: &acyclic_stream_contract_core::Heads,
    deferred: bool,
    children: bool,
) -> usize {
    let snapshot = revision_len(&view.snapshot);
    let named = view
        .heads
        .iter()
        .map(|head| {
            block_len(head.as_ref().map_or(0, |head| {
                let durable = if deferred { head.tail } else { head.durable };
                block_len(
                    uint_len((head.tail != 0).then_some(head.tail))
                        .saturating_add(uint_len((head.changed_at != 0).then_some(head.changed_at)))
                        .saturating_add(
                            head.last
                                .as_ref()
                                .map_or(0, |record| block_len(record_len(record, None))),
                        )
                        .saturating_add(uint_len((durable != 0).then_some(durable)))
                        .saturating_add(uint_len(head.retained_through))
                        .saturating_add(
                            head.origin
                                .as_ref()
                                .map_or(0, |origin| block_len(origin_len(origin))),
                        ),
                )
            }))
        })
        .fold(0usize, usize::saturating_add);
    let children = view
        .children
        .as_ref()
        .filter(|_| children)
        .map_or(0, |page| {
            block_len(children_page_len(
                page.children
                    .iter()
                    .map(|child| block_len(child_len(child)))
                    .fold(0usize, usize::saturating_add),
                page.next_after.as_ref(),
                page.scanned,
            ))
        });
    block_len(snapshot)
        .saturating_add(block_len(revision_len(&view.applied)))
        .saturating_add(
            view.retained
                .iter()
                .map(|observation| {
                    block_len(block_len(observation.path.as_str().len()).saturating_add(
                        observation.record.as_ref().map_or(0, |record| {
                            block_len(record_len(record, Some(&observation.path)))
                        }),
                    ))
                })
                .fold(0usize, usize::saturating_add),
        )
        .saturating_add(named)
        .saturating_add(children)
        .saturating_add(uint_len(match view.projection {
            acyclic_stream_contract_core::HeadProjection::History => None,
            acyclic_stream_contract_core::HeadProjection::Watermarks => Some(1),
            acyclic_stream_contract_core::HeadProjection::Tail => Some(2),
        }))
}

/// A prepared single-record command admits unknown value and header bytes.
/// Count every possible field, then use the ordinary nesting calculation so
/// length-delimiter growth is included at every protobuf boundary.
#[cfg(not(target_arch = "wasm32"))]
pub fn prepared_commit_command_len(
    request: &acyclic_stream_contract_core::CommitRequest,
    capacity: acyclic_stream_contract_core::PreparedRecordCapacity,
) -> usize {
    let fields = 1_usize
        .saturating_add(capacity.keys)
        .saturating_add(3_usize.saturating_mul(capacity.attributes));
    let field_overhead =
        1 + prost::length_delimiter_len(acyclic_stream_contract_core::MAX_COMMAND_BYTES);
    let record = capacity
        .payload_bytes
        .saturating_add(fields.saturating_mul(field_overhead));
    block_len(acyclic_stream_contract_core::request::len::commit_len_with(
        request,
        &|_| record,
    ))
}

// An accepted reply adds the original envelope to the compact acknowledgement.
// Keep the existing acknowledgement budget, then fund the envelope from this
// admitted command budget: unchanged payload/path/header bytes, 34 commit-id bytes plus two
// eleven-byte u64 fields per record, and at most four extra delimiter bytes.
// Each mutation adds at most three u64 fields and two growing delimiters (45
// bytes); the envelope adds its commit id, timestamp and enclosing delimiter.
// Count vector lengths, never records, payload bytes or unrelated retained history.
#[cfg(any(
    not(target_arch = "wasm32"),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// The largest accepted response the request's effects can produce.
pub fn accepted_response_limit(
    input: &acyclic_stream_wire::wire::CommitRequest,
) -> Result<usize, acyclic_stream_contract_core::StreamError> {
    let mut maximum = 2 * acyclic_stream_contract_core::MAX_COMMAND_BYTES + 64;
    for mutation in input.effects.iter().flat_map(|effect| &effect.mutations) {
        let records = match mutation.mutation.as_ref() {
            Some(acyclic_stream_wire::wire::commit_mutation::Mutation::Append(append)) => {
                append.records.len()
            }
            Some(acyclic_stream_wire::wire::commit_mutation::Mutation::Fork(fork)) => {
                fork.records.len()
            }
            None => return Err(acyclic_stream_contract_core::StreamError::InvalidArgument),
        };
        maximum = records
            .checked_mul(60)
            .and_then(|bytes| bytes.checked_add(45))
            .and_then(|bytes| maximum.checked_add(bytes))
            .ok_or(acyclic_stream_contract_core::StreamError::LimitExceeded)?;
    }
    Ok(maximum)
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests {
    use acyclic_stream_contract_core::request::encode::commit;
    use acyclic_stream_contract_core::{
        AppendRecord, CommitCondition, CommitEffect, CommitMutation, CommitRequest, OwnedBytes,
    };
    use prost::Message;

    #[test]
    #[cfg(not(target_arch = "wasm32"))]
    fn commit_size_matches_generated_protobuf_at_wire_boundaries()
    -> Result<(), Box<dyn std::error::Error>> {
        let path = acyclic_stream_contract_core::StreamPath::new("request/events")?;
        let destination = acyclic_stream_contract_core::StreamPath::new("request/fork")?;
        let inbox = acyclic_stream_contract_core::StreamPath::new("request/inbox")?;
        for tail in [0, 127, 128, 16_383, 16_384, u64::MAX] {
            for length in [
                0,
                127,
                128,
                16_383,
                16_384,
                acyclic_stream_contract_core::MAX_RECORD_BYTES,
            ] {
                let records: Vec<AppendRecord> = vec![OwnedBytes::from(vec![7; length]).into()];
                let request = CommitRequest {
                    deadline_unix_millis: (tail != 0).then_some(tail),
                    effects: vec![
                        CommitEffect::hard(
                            vec![
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
                                    commit_id: acyclic_stream_contract_core::CommitId::from_bytes(
                                        [3; 32],
                                    ),
                                },
                            ],
                            vec![
                                CommitMutation::Append {
                                    path: path.clone(),
                                    records: records.clone(),
                                },
                                CommitMutation::Fork {
                                    exclude: vec![
                                        acyclic_stream_contract_core::StreamPath::new("children")?,
                                        acyclic_stream_contract_core::StreamPath::new(
                                            "volumes/cache",
                                        )?,
                                    ],
                                    source: path.clone(),
                                    destination: destination.clone(),
                                    at_tail: tail,
                                    records: records.clone(),
                                },
                            ],
                        ),
                        CommitEffect {
                            conditions: vec![CommitCondition::Present {
                                path: inbox.clone(),
                            }],
                            mutations: vec![CommitMutation::Append {
                                path: inbox.clone(),
                                records,
                            }],
                            soft: true,
                        },
                    ],
                    idempotency_key: acyclic_stream_contract_core::IdempotencyKey::new(
                        bytes::Bytes::from_static(b"size"),
                    )?,
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
}
