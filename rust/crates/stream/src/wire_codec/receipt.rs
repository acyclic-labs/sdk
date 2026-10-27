use super::*;

/// Decode a compact publication receipt without constructing record values.
pub fn receipt_from_wire(value: wire::CommitReceipt) -> Result<crate::CommitReceipt, StreamError> {
    let id = commit_id(&value.commit_id)?;
    let appends = value.appends.into_iter().map(|range| {
        if range.start >= range.tail {
            return Err(StreamError::Unavailable);
        }
        Ok(crate::AppendRange { path: path(range.path)?, start: range.start, tail: range.tail })
    }).collect::<Result<Vec<_>, StreamError>>()?;
    let forks = value.forks.into_iter().map(|fork| {
        let fork = fork_receipt_from_wire(fork)?;
        if fork.commit_id != id || fork.forked_at > fork.tail {
            return Err(StreamError::Unavailable);
        }
        Ok(fork)
    }).collect::<Result<Vec<_>, StreamError>>()?;
    if appends.is_empty() && forks.is_empty() {
        return Err(StreamError::Unavailable);
    }
    let receipt = crate::CommitReceipt { commit_id: id, committed_at_micros: value.committed_at_micros, appends, forks };
    receipt.validate().map_err(|_| StreamError::Unavailable)?;
    Ok(receipt)
}

/// Decode the same fork identity used by standalone and coordinated commits.
pub fn fork_receipt_from_wire(value: wire::ForkReceipt) -> Result<ForkReceipt, StreamError> {
    if value.forked_at > value.tail {
        return Err(StreamError::Unavailable);
    }
    Ok(ForkReceipt {
        source: path(value.source)?, destination: path(value.destination)?,
        forked_at: value.forked_at, tail: value.tail, commit_id: commit_id(&value.commit_id)?,
    })
}

/// Encode acknowledgment metadata; full facts use the separate envelope codec.
pub fn receipt_to_wire(value: crate::CommitReceipt) -> wire::CommitReceipt {
    wire::CommitReceipt {
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        committed_at_micros: value.committed_at_micros,
        appends: value.appends.into_iter().map(|range| wire::AppendRange {
            path: range.path.to_string(), start: range.start, tail: range.tail,
        }).collect(),
        forks: value.forks.iter().map(fork_receipt_to_wire).collect(),
    }
}
