use super::*;

/// Validates the canonical mixed observation request before provider admission.
pub fn heads_from_wire(value: wire::HeadsRequest) -> Result<crate::HeadsRequest, StreamError> {
    let request = crate::HeadsRequest {
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
    Ok(crate::Heads {
        snapshot: issued_from_wire(value.snapshot)?,
        heads: value
            .heads
            .into_iter()
            .map(|head| head.head.map(head_from_wire).transpose())
            .collect::<Result<_, _>>()?,
        children: value
            .children
            .map(children_page_from_response)
            .transpose()?,
    })
}

/// Validates an atomic head without reconstructing it through other operations.
pub fn head_from_wire(value: wire::PathHead) -> Result<crate::PathHead, StreamError> {
    let mut head = crate::PathHead {
        tail: value.tail,
        last: value.last.map(record).transpose()?,
        durable: value.durable,
        origin: None,
    };
    // Preserve refusal ordering before decoding an independently malformed origin.
    head.validate()?;
    head.origin = value.origin.map(fork_origin_from_wire).transpose()?;
    head.validate()?;
    Ok(head)
}

/// Projects one provider observation, retaining the original record bytes.
pub fn head_to_wire(value: crate::PathHead) -> wire::PathHead {
    wire::PathHead {
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
