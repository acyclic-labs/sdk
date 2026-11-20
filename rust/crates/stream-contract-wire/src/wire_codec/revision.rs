//! The one wire spelling of the shared applied-revision token.
use acyclic_stream_contract_core::revision::revision_to_wire;
pub use acyclic_stream_contract_core::revision::{
    cut_from_wire, cut_to_wire, revision_from, revision_from_wire,
};
use acyclic_stream_contract_core::{Revision, StreamError};
use acyclic_stream_wire::wire;

/// Encodes the choices made by an original stream open.
#[must_use]
pub fn selection_to_wire(
    value: &acyclic_stream_contract_core::StreamSelection,
) -> wire::SessionSelection {
    wire::SessionSelection {
        from: value.from,
        end: value.end,
        preceding_commit_id: value.preceding.map_or_else(bytes::Bytes::new, |id| {
            bytes::Bytes::copy_from_slice(id.as_bytes())
        }),
        has_bootstrap: value.bootstrap.is_some(),
    }
}

/// Projects the original provider receipt without polling or sampling again.
#[must_use]
pub fn opened_to_wire<T, E>(
    value: &acyclic_stream_contract_core::Opened<T, E>,
) -> wire::SessionOpened {
    wire::SessionOpened {
        resume: Some(wire::SessionCursor {
            next: value.selection.from,
            opaque: bytes::Bytes::new(),
        }),
        preceding_commit_id: value
            .selection
            .preceding
            .map_or_else(bytes::Bytes::new, |id| {
                bytes::Bytes::copy_from_slice(id.as_bytes())
            }),
        applied: Some(revision_to_wire(&value.snapshot)),
        selection: Some(selection_to_wire(&value.selection)),
    }
}

/// Decodes the mandatory metadata of a successful stream opening receipt.
pub fn selection_from_opened(
    value: &wire::SessionOpened,
) -> Result<
    (
        Revision,
        acyclic_stream_contract_core::StreamSelection,
        bool,
    ),
    StreamError,
> {
    let snapshot = revision_from(value.applied.as_ref().ok_or(StreamError::InvalidResponse)?)
        .map_err(|source| StreamError::InvalidResponse.with_source(source))?;
    let selected = value
        .selection
        .as_ref()
        .ok_or(StreamError::InvalidResponse)?;
    let preceding = if selected.preceding_commit_id.is_empty() {
        None
    } else {
        Some(
            super::commit_id(&selected.preceding_commit_id)
                .map_err(|source| StreamError::InvalidResponse.with_source(source))?,
        )
    };
    Ok((
        snapshot,
        acyclic_stream_contract_core::StreamSelection {
            from: selected.from,
            preceding,
            end: selected.end,
            bootstrap: None,
        },
        selected.has_bootstrap,
    ))
}

#[cfg(any(
    not(target_arch = "wasm32"),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Admits the original selection before data or its revision can be observed.
/// Sessions and injected provider ports use the same request/receipt contract.
pub fn selection_for_follow(
    value: &wire::SessionOpened,
    follow: &wire::SessionFollow,
    from: u64,
) -> Result<
    (
        Revision,
        acyclic_stream_contract_core::StreamSelection,
        bool,
    ),
    StreamError,
> {
    let (snapshot, selection, has_bootstrap) = selection_from_opened(value)?;
    let bootstrap = follow.bootstrap_head && from == 0;
    let preceding = if follow.preceding_commit_id.is_empty() {
        None
    } else {
        Some(super::commit_id(&follow.preceding_commit_id)?)
    };
    if !cut_proven_by(&follow.at_least, Some(&snapshot))
        || (!bootstrap && selection.from != from)
        || (!bootstrap && selection.preceding != preceding)
        || (has_bootstrap && (!bootstrap || selection.from == 0 || selection.preceding.is_none()))
        || selection.end.is_some() != follow.until.is_some()
        || selection
            .end
            .zip(follow.until)
            .is_some_and(|(selected, requested)| selected > requested)
    {
        return Err(StreamError::InvalidResponse);
    }
    Ok((snapshot, selection, has_bootstrap))
}

/// Decodes a peer-issued revision that the response contract requires.
pub fn issued_from_wire(value: Option<wire::Revision>) -> Result<Revision, StreamError> {
    value
        .ok_or(StreamError::Unavailable)
        .and_then(|value| revision_from_wire(&value))
        .map_err(|_| StreamError::Unavailable)
}

/// Checks the retained wire minimum without decoding or canonicalizing it
/// again. Proving every supplied entry also proves their greatest indices;
/// malformed or conflicting incarnations cannot match one applied revision.
/// An empty minimum is proven even when `applied` is absent.
#[must_use]
pub fn cut_proven_by(minimum: &[wire::Revision], applied: Option<&Revision>) -> bool {
    let Some(applied) = applied else {
        return minimum.is_empty();
    };
    let incarnation = applied.incarnation.to_be_bytes();
    minimum.iter().all(|minimum| {
        minimum.scope.as_ref() == applied.scope.as_bytes()
            && minimum.incarnation.as_ref() == incarnation
            && minimum.index <= applied.index
    })
}

#[cfg(all(test, not(loom), not(target_arch = "wasm32")))]
#[path = "revision_tests.rs"]
mod tests;

/// Serves one wire tail: waits for its minimum cut first, so the returned
/// tail covers it, and reports the applied revision served. Every host
/// shares this one handler.
pub async fn tail_response<P: acyclic_stream_contract_core::StreamProvider + ?Sized>(
    provider: &P,
    request: wire::TailRequest,
) -> Result<wire::TailResponse, StreamError> {
    let path = acyclic_stream_contract_core::StreamPath::new(request.path)?;
    let projection = if request.accepted {
        acyclic_stream_contract_core::HeadProjection::Watermarks
    } else {
        acyclic_stream_contract_core::HeadProjection::Tail
    };
    let through = request
        .accepted
        .then_some(request.durable_through)
        .flatten();
    let observation = acyclic_stream_contract_core::HeadsRequest {
        at: None,
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
