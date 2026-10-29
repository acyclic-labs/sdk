use super::*;

/// Validates the canonical mixed observation request before provider admission.
pub fn heads_from_wire(value: wire::HeadsRequest) -> Result<crate::HeadsRequest, StreamError> {
    let request = crate::HeadsRequest {
        retained: value
            .retained
            .into_iter()
            .map(|selected| {
                Ok(crate::RetainedRecord {
                    commit_id: commit_id(&selected.commit_id)?,
                    path: StreamPath::new(selected.path)?,
                    sequence: selected.sequence,
                })
            })
            .collect::<Result<_, StreamError>>()?,
        durable_through: value.durable_through,
        projection: projection_from_wire(value.projection)
            .map_err(|_| StreamError::InvalidArgument)?,
        paths: value
            .paths
            .into_iter()
            .map(StreamPath::new)
            .collect::<Result<_, _>>()?,
        children: value.children.map(children_page_from_wire).transpose()?,
        limit: value.limit,
        at_least: cut_from_wire(value.at_least)?,
    };
    request.validate()?;
    Ok(request)
}

/// Encodes one mixed observation without splitting it into separate requests.
pub fn heads_request_to_wire(value: crate::HeadsRequest) -> wire::HeadsRequest {
    wire::HeadsRequest {
        retained: value
            .retained
            .into_iter()
            .map(|selected| wire::RetainedRecord {
                commit_id: Bytes::copy_from_slice(selected.commit_id.as_bytes()),
                path: selected.path.to_string(),
                sequence: selected.sequence,
            })
            .collect(),
        durable_through: value.durable_through,
        projection: projection_to_wire(value.projection),
        paths: value
            .paths
            .into_iter()
            .map(|path| path.to_string())
            .collect(),
        children: value.children.map(children_page_request_to_wire),
        limit: value.limit,
        at_least: cut_to_wire(&value.at_least),
    }
}

/// Encodes the exact authority snapshot and its ordered observations.
pub fn heads_to_wire(value: crate::Heads) -> wire::HeadsResponse {
    wire::HeadsResponse {
        retained: value
            .retained
            .into_iter()
            .map(|selected| wire::RetainedRecordObservation {
                path: selected.path.to_string(),
                record: selected.record.map(record_wire),
            })
            .collect(),
        projection: projection_to_wire(value.projection),
        snapshot: Some(revision_to_wire(&value.snapshot)),
        heads: value
            .heads
            .into_iter()
            .map(|head| wire::HeadObservation {
                head: head.map(head_to_wire),
            })
            .collect(),
        children: value.children.map(children_page_to_wire),
    }
}

/// Validates every head and the shared authority cut before exposing the view.
pub fn heads_from_response(value: wire::HeadsResponse) -> Result<crate::Heads, StreamError> {
    let projection = projection_from_wire(value.projection)?;
    Ok(crate::Heads {
        retained: value
            .retained
            .into_iter()
            .map(|selected| {
                Ok(crate::RetainedRecordObservation {
                    path: StreamPath::new(selected.path)
                        .map_err(|_| StreamError::InvalidResponse)?,
                    record: selected.record.map(record).transpose()?,
                })
            })
            .collect::<Result<_, _>>()?,
        projection,
        snapshot: issued_from_wire(value.snapshot)?,
        heads: value
            .heads
            .into_iter()
            .map(|head| {
                head.head
                    .map(|head| head_observation_from_wire(head, projection))
                    .transpose()
            })
            .collect::<Result<_, _>>()?,
        children: value
            .children
            .map(children_page_from_response)
            .transpose()?,
    })
}

/// Validates an atomic head without reconstructing it through other operations.
pub fn head_from_wire(value: wire::PathHead) -> Result<crate::PathHead, StreamError> {
    head_observation_from_wire(value, crate::HeadProjection::History)
}

fn head_observation_from_wire(
    value: wire::PathHead,
    projection: crate::HeadProjection,
) -> Result<crate::PathHead, StreamError> {
    if projection != crate::HeadProjection::History
        && (value.last.is_some() || value.origin.is_some())
    {
        return Err(StreamError::InvalidResponse);
    }
    let mut head = crate::PathHead {
        retained_through: value.retained_through,
        tail: value.tail,
        last: value.last.map(record).transpose()?,
        durable: value.durable,
        origin: None,
    };
    // Preserve refusal ordering before decoding an independently malformed origin.
    head.validate_projection(projection)?;
    head.origin = value.origin.map(fork_origin_from_wire).transpose()?;
    head.validate_projection(projection)?;
    Ok(head)
}

fn projection_from_wire(value: i32) -> Result<crate::HeadProjection, StreamError> {
    match wire::HeadProjection::try_from(value) {
        Ok(wire::HeadProjection::History) => Ok(crate::HeadProjection::History),
        Ok(wire::HeadProjection::Watermarks) => Ok(crate::HeadProjection::Watermarks),
        Ok(wire::HeadProjection::Tail) => Ok(crate::HeadProjection::Tail),
        Err(_) => Err(StreamError::InvalidResponse),
    }
}

fn projection_to_wire(value: crate::HeadProjection) -> i32 {
    match value {
        crate::HeadProjection::History => wire::HeadProjection::History as i32,
        crate::HeadProjection::Watermarks => wire::HeadProjection::Watermarks as i32,
        crate::HeadProjection::Tail => wire::HeadProjection::Tail as i32,
    }
}

/// Projects one provider observation, retaining the original record bytes.
pub fn head_to_wire(value: crate::PathHead) -> wire::PathHead {
    wire::PathHead {
        retained_through: value.retained_through,
        tail: value.tail,
        last: value.last.map(record_wire),
        durable: value.durable,
        origin: value.origin.map(fork_origin_to_wire),
    }
}

/// Decodes the shared inherited-cut descriptor used by heads and child pages.
pub fn fork_origin_from_wire(origin: wire::ForkOrigin) -> Result<crate::ForkOrigin, StreamError> {
    Ok(crate::ForkOrigin {
        source: StreamPath::new(origin.source)?,
        forked_at: origin.forked_at,
        commit_id: commit_id(&origin.commit_id)?,
    })
}

/// Projects the shared inherited-cut descriptor without reconstructing history.
pub fn fork_origin_to_wire(origin: crate::ForkOrigin) -> wire::ForkOrigin {
    wire::ForkOrigin {
        source: origin.source.to_string(),
        forked_at: origin.forked_at,
        commit_id: Bytes::copy_from_slice(origin.commit_id.as_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use prost::Message;

    proptest! {
        #[test]
        fn retained_positions_preserve_order_identity_and_sequence(
            positions in prop::collection::vec((any::<[u8; 32]>(), "[a-z]{1,24}", any::<u64>()), 0..8),
        ) {
            let request = crate::HeadsRequest {
                retained: positions.into_iter().map(|(id, path, sequence)| crate::RetainedRecord {
                    commit_id: crate::CommitId::from_bytes(id), path: StreamPath::new(path).unwrap(), sequence,
                }).collect(),
                paths: Default::default(), children: None, projection: crate::HeadProjection::Tail,
                durable_through: None, limit: 8, at_least: Default::default(),
            };
            let wire = heads_request_to_wire(request.clone());
            let decoded = wire::HeadsRequest::decode(wire.encode_to_vec().as_slice()).unwrap();
            prop_assert_eq!(heads_from_wire(decoded).unwrap(), request.clone());
            let view = crate::Heads {
                retained: request.retained.iter().map(|selected| crate::RetainedRecordObservation {
                    path: selected.path.clone(),
                    record: (selected.sequence % 2 == 0).then(|| crate::Record {
                        sequence: selected.sequence, value: selected.path.as_str().as_bytes().to_vec().into(),
                        commit_id: selected.commit_id, committed_at_micros: 1,
                        headers: Default::default(),
                    }),
                }).collect(),
                projection: request.projection,
                snapshot: crate::Revision { scope: crate::RevisionScope::new(Vec::new()), incarnation: 1, index: 0 },
                heads: Default::default(), children: None,
            };
            prop_assert_eq!(view.validate_for(&request), Ok(()));
            let response = heads_to_wire(view.clone());
            prop_assert_eq!(crate::wire_codec::requests::heads_len(&view), response.encoded_len());
            let decoded = wire::HeadsResponse::decode(response.encode_to_vec().as_slice()).unwrap();
            prop_assert_eq!(heads_from_response(decoded).unwrap(), view);
            if !response.retained.is_empty() {
                let mut malformed = response;
                malformed.retained[0].path.clear();
                prop_assert_eq!(heads_from_response(malformed), Err(StreamError::InvalidResponse));
            }
            if !wire.retained.is_empty() {
                let mut malformed = wire.clone();
                malformed.retained[0].commit_id = Bytes::new();
                prop_assert!(heads_from_wire(malformed).is_err());
                let mut too_small = wire;
                too_small.limit = too_small.retained.len() as u32 - 1;
                prop_assert_eq!(heads_from_wire(too_small), Err(StreamError::InvalidArgument));
            }
        }
    }
}
