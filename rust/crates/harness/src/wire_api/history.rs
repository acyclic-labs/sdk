use super::*;
use crate::{core::AuthorityVerifier, store::HistoryReader, wire_codec::encode_event_envelope};
use acyclic_stream::StreamProvider;
use futures::{StreamExt, stream};
use std::sync::Arc;

/// Projects verified provider appends into individual client deliveries.
/// An absent pre-admission owner at revision zero returns an empty snapshot;
/// admission must reopen it after creating that owner.
pub async fn replay_history<P: StreamProvider>(
    provider: &Arc<P>,
    verifier: &AuthorityVerifier,
    cursor: wire::ReplayCursor,
    maximum_event_bytes: u64,
    mode: ReplayMode,
) -> Result<BoxStream<'static, Result<wire::Delivery>>> {
    if maximum_event_bytes == 0 {
        return Err(Error::Invalid(
            "history event bound must be positive".into(),
        ));
    }
    let authority = decode_authority(
        cursor
            .authority
            .clone()
            .ok_or_else(|| Error::Invalid("cursor authority missing".into()))?,
        wire::ErrorCode::Invalid,
        &None,
    )?;
    if cursor.generation.is_empty() {
        return Err(Error::Invalid("cursor generation missing".into()));
    }
    let reader = HistoryReader::new(provider, &authority, verifier, &mut None)?;
    let remaining = if mode == ReplayMode::Snapshot {
        let cut = match reader.pin(cursor.revision, None).await {
            Ok(cut) => cut,
            Err(Error::NotFound(_)) if cursor.revision == 0 => {
                return Ok(Box::pin(stream::empty()));
            }
            Err(error) => return Err(error),
        };
        let count = usize::try_from(cut.through_revision - cursor.revision)
            .map_err(|_| Error::Invalid("snapshot event count exceeds address space".into()))?;
        if count == 0 {
            return Ok(Box::pin(stream::empty()));
        }
        Some(count)
    } else {
        None
    };
    let history = match reader.follow(cursor.revision, maximum_event_bytes).await {
        Ok(history) => history,
        Err(Error::NotFound(_)) if cursor.revision == 0 => return Ok(Box::pin(stream::empty())),
        Err(error) => return Err(error),
    };
    let deliveries = history.map(move |event| {
        let event = event?;
        Ok(wire::Delivery {
            authority: cursor.authority.clone(),
            generation: cursor.generation.clone(),
            from_revision: event.revision - 1,
            through_revision: event.revision,
            events: vec![encode_event_envelope(&authority, &event, &mut None)?],
            live: mode == ReplayMode::Follow,
        })
    });
    Ok(match remaining {
        Some(count) => Box::pin(deliveries.take(count)),
        None => Box::pin(deliveries),
    })
}
