use super::*;

/// Validates an atomic head without reconstructing it through other operations.
pub fn head_from_wire(value: wire::PathHead) -> Result<crate::PathHead, StreamError> {
    let last = value.last.map(record).transpose()?;
    if value.durable > value.tail
        || match &last {
            Some(record) => record.sequence.checked_add(1) != Some(value.tail),
            None => value.tail != 0,
        }
    {
        return Err(StreamError::InvalidResponse);
    }
    let origin = value.origin.map(|origin| -> Result<crate::ForkOrigin, StreamError> {
        if origin.forked_at > value.tail {
            return Err(StreamError::InvalidResponse);
        }
        Ok(crate::ForkOrigin {
            source: StreamPath::new(origin.source)?,
            forked_at: origin.forked_at,
            commit_id: commit_id(&origin.commit_id)?,
        })
    }).transpose()?;
    Ok(crate::PathHead { tail: value.tail, last, durable: value.durable, origin })
}

/// Projects one provider observation, retaining the original record bytes.
pub fn head_to_wire(value: crate::PathHead) -> wire::PathHead {
    wire::PathHead {
        tail: value.tail,
        last: value.last.map(record_wire),
        durable: value.durable,
        origin: value.origin.map(|origin| wire::ForkOrigin {
            source: origin.source.to_string(),
            forked_at: origin.forked_at,
            commit_id: Bytes::copy_from_slice(origin.commit_id.as_bytes()),
        }),
    }
}
