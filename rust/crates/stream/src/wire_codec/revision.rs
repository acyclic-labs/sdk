//! The one wire spelling of the shared applied-revision token.
use crate::{Revision, RevisionCut, RevisionScope, StreamError, wire};

/// Encodes one revision.
#[must_use]
pub fn revision_to_wire(value: &Revision) -> wire::Revision {
    wire::Revision {
        scope: bytes::Bytes::copy_from_slice(value.scope.as_bytes()),
        incarnation: bytes::Bytes::copy_from_slice(&value.incarnation.to_be_bytes()),
        index: value.index,
    }
}

/// Decodes one revision; an incarnation other than 16 bytes is invalid.
pub fn revision_from_wire(value: wire::Revision) -> Result<Revision, StreamError> {
    let incarnation = <[u8; 16]>::try_from(value.incarnation.as_ref())
        .map_err(|_| StreamError::InvalidArgument)?;
    Ok(Revision {
        scope: RevisionScope::new(value.scope.to_vec()),
        incarnation: u128::from_be_bytes(incarnation),
        index: value.index,
    })
}

/// Decodes a peer-issued revision that the response contract requires.
pub fn issued_from_wire(value: Option<wire::Revision>) -> Result<Revision, StreamError> {
    value
        .ok_or(StreamError::Unavailable)
        .and_then(revision_from_wire)
        .map_err(|_| StreamError::Unavailable)
}

/// Encodes a minimum cut.
#[must_use]
pub fn cut_to_wire(value: &RevisionCut) -> Vec<wire::Revision> {
    value.iter().map(revision_to_wire).collect()
}

/// Decodes a minimum cut, keeping one participant per scope.
pub fn cut_from_wire(value: Vec<wire::Revision>) -> Result<RevisionCut, StreamError> {
    Ok(RevisionCut::new(
        value
            .into_iter()
            .map(revision_from_wire)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

/// Serves one wire tail: waits for its minimum cut first, so the returned
/// tail covers it, and reports the applied revision served. Every host
/// shares this one handler.
pub async fn tail_response<P: crate::StreamProvider + ?Sized>(
    provider: &P,
    request: wire::TailRequest,
) -> Result<wire::TailResponse, StreamError> {
    let path = crate::StreamPath::new(request.path)?;
    let at_least = cut_from_wire(request.at_least)?;
    let applied = crate::revision::served(provider, &at_least)
        .await?
        .as_ref()
        .map(revision_to_wire);
    if !request.accepted {
        return Ok(wire::TailResponse {
            tail: provider.tail(path).await?,
            durable: None,
            applied,
        });
    }
    let durable = match request.durable_through {
        Some(through) => Some(
            provider
                .durable_receipt(path.clone(), through)
                .await?
                .map_or(0, |receipt| receipt.through()),
        ),
        None => None,
    };
    let watermarks = provider.watermarks(path).await?;
    Ok(wire::TailResponse {
        tail: watermarks.accepted,
        durable: Some(durable.unwrap_or(watermarks.durable)),
        applied,
    })
}
