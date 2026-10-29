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

/// Decodes a minimum cut, refusing incompatible incarnations of one scope.
pub fn cut_from_wire(value: Vec<wire::Revision>) -> Result<RevisionCut, StreamError> {
    RevisionCut::new(
        value
            .into_iter()
            .map(revision_from_wire)
            .collect::<Result<Vec<_>, _>>()?,
    )
}

/// Serves one wire tail: waits for its minimum cut first, so the returned
/// tail covers it, and reports the applied revision served. Every host
/// shares this one handler.
pub async fn tail_response<P: crate::StreamProvider + ?Sized>(
    provider: &P,
    request: wire::TailRequest,
) -> Result<wire::TailResponse, StreamError> {
    let path = crate::StreamPath::new(request.path)?;
    let projection = if request.accepted {
        crate::HeadProjection::Watermarks
    } else {
        crate::HeadProjection::Tail
    };
    let through = request
        .accepted
        .then_some(request.durable_through)
        .flatten();
    let observation = crate::HeadsRequest {
        retained: Default::default(),
        durable_through: through,
        projection,
        paths: [path].into(),
        children: None,
        limit: 1,
        at_least: cut_from_wire(request.at_least)?,
    };
    let mut view = provider.heads(observation.clone()).await?;
    view.validate_for(&observation)?;
    let head = view.heads.pop().flatten().ok_or(StreamError::NotFound)?;
    Ok(wire::TailResponse {
        tail: head.tail,
        durable: request
            .accepted
            .then_some(through.map_or(head.durable, |_| head.retained_through.unwrap_or(0))),
        applied: Some(revision_to_wire(&view.snapshot)),
    })
}
