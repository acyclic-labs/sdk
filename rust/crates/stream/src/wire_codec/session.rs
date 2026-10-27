//! Session receive-credit accounting shared by transport adapters.

use crate::{CommitId, RecordStream, StreamError, StreamProvider, wire};
use prost::Message as _;

mod resource;
pub use resource::{stream_path, stream_resource, validate_resource};

/// Finite canonical Actors response plus its enclosing product result. The
/// same bound applies to every finite one-shot reply, including a children page.
pub const MAX_ACTOR_CALL_REPLY_BYTES: usize = crate::MAX_COMMAND_BYTES + 128;

/// `SessionClosed.code` of a finite channel that delivered its whole range.
/// It is the only non-error close code; any other code is a [`StreamError`].
pub const END_CODE: &str = "end";

/// One fragment of a finite one-shot reply: an Actors call or a children page.
pub fn reply_chunk(item: Option<&wire::session_data::Item>) -> Option<&bytes::Bytes> {
    match item {
        Some(
            wire::session_data::Item::ActorCall(chunk)
            | wire::session_data::Item::ChildrenPage(chunk),
        ) => Some(chunk),
        _ => None,
    }
}

/// Actor-call chunks carry the immutable total byte length as an eight-byte
/// little-endian opaque cursor. The record cursor counts consecutive chunks.
pub fn actor_call_total(data: &wire::SessionData) -> Result<usize, StreamError> {
    let opaque = &data
        .after
        .as_ref()
        .ok_or(StreamError::InvalidResponse)?
        .opaque;
    let bytes: [u8; 8] = opaque
        .as_ref()
        .try_into()
        .map_err(|_| StreamError::InvalidResponse)?;
    usize::try_from(u64::from_le_bytes(bytes))
        .ok()
        .filter(|total| *total > 0 && *total <= MAX_ACTOR_CALL_REPLY_BYTES)
        .ok_or(StreamError::InvalidResponse)
}

/// Admit one ordered, bounded fragment without returning credit or advancing
/// application progress. The entire response belongs to this one generation.
pub fn actor_call_sequence(
    next: u64,
    previous_opaque: &[u8],
    data: &wire::SessionData,
) -> Result<u64, StreamError> {
    let after = data.after.as_ref().ok_or(StreamError::InvalidResponse)?;
    let total = actor_call_total(data)?;
    let chunk = reply_chunk(data.item.as_ref()).ok_or(StreamError::InvalidResponse)?;
    let start = usize::try_from(next)
        .ok()
        .and_then(|index| index.checked_mul(crate::MAX_RECORD_BYTES))
        .filter(|start| *start < total)
        .ok_or(StreamError::InvalidResponse)?;
    if chunk.is_empty()
        || chunk.len() > crate::MAX_RECORD_BYTES
        || chunk.len() > total
        || chunk.len() != (total - start).min(crate::MAX_RECORD_BYTES)
        || (!previous_opaque.is_empty() && previous_opaque != after.opaque.as_ref())
        || after.next != next.checked_add(1).ok_or(StreamError::OutOfRange)?
        || data.accepted != after.next
        || data.durable != 0
    {
        return Err(StreamError::InvalidResponse);
    }
    Ok(next)
}

/// Decodes a bounded peer session message; protocol validation remains in the pool.
pub fn decode_frame(bytes: &[u8]) -> Result<wire::SessionFrame, StreamError> {
    if bytes.len() > crate::MAX_COMMAND_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    wire::SessionFrame::decode(bytes).map_err(|_| StreamError::InvalidResponse)
}

/// Validates a logical Streams follow without creating transport state.
pub fn validate_follow(
    follow: &wire::SessionFollow,
    resume: &wire::SessionCursor,
) -> Result<(), StreamError> {
    validate_resource(&follow.resource)?;
    if !resume.opaque.is_empty()
        || !matches!(
            wire::SessionDelivery::try_from(follow.delivery),
            Ok(wire::SessionDelivery::All | wire::SessionDelivery::Latest)
        )
    {
        return Err(StreamError::InvalidArgument);
    }
    preceding_follow(follow, resume)?;
    follow_bound(follow)?;
    Ok(())
}

/// Decodes the optional exclusive end of a bounded follow (the session read).
/// Only an ALL follow of a `streams:/` resource can be bounded: LATEST conflates
/// away the positions a bound counts, and product resources own their cursors.
pub fn follow_bound(follow: &wire::SessionFollow) -> Result<Option<u64>, StreamError> {
    if follow.until == 0 {
        return Ok(None);
    }
    if follow.delivery != wire::SessionDelivery::All as i32
        || !follow.resource.starts_with("streams:/")
    {
        return Err(StreamError::InvalidArgument);
    }
    Ok(Some(follow.until))
}

/// Decodes the optional atomic history guard of a full-fidelity follow.
pub fn preceding_follow(
    follow: &wire::SessionFollow,
    resume: &wire::SessionCursor,
) -> Result<Option<crate::CommitId>, StreamError> {
    if follow.preceding_commit_id.is_empty() {
        return Ok(None);
    }
    if follow.delivery != wire::SessionDelivery::All as i32
        || resume.next == 0
        || !resume.opaque.is_empty()
    {
        return Err(StreamError::InvalidArgument);
    }
    super::commit_id(&follow.preceding_commit_id)
        .map(Some)
        .map_err(|_| StreamError::InvalidArgument)
}

/// Requires affirmative provider enforcement before a checked channel gets credit.
pub fn confirm_follow(
    preceding: Option<crate::CommitId>,
    opened: &wire::SessionOpened,
) -> Result<(), StreamError> {
    if let Some(preceding) = preceding
        && opened.preceding_commit_id.as_ref() != preceding.as_bytes()
    {
        return Err(StreamError::Unsupported);
    }
    Ok(())
}

/// Complete record continuation at a nonzero cut, including its immutable anchor.
/// The provider-owned `opaque` field carries the 32-byte commit ID of `next - 1`.
/// ALL-record route adapters resume this token through [`follow_records`], and
/// retain it unchanged on placement moves. No new frame or cursor schema is used.
pub fn record_cursor(next: u64, preceding: CommitId) -> Result<wire::SessionCursor, StreamError> {
    if next == 0 {
        return Err(StreamError::InvalidArgument);
    }
    Ok(wire::SessionCursor {
        next,
        opaque: preceding.as_bytes().to_vec().into(),
    })
}

/// Validates the canonical ALL-record cursor without normalizing malformed input.
pub fn record_anchor(cursor: &wire::SessionCursor) -> Result<Option<CommitId>, StreamError> {
    if cursor.next == 0 {
        return if cursor.opaque.is_empty() {
            Ok(None)
        } else {
            Err(StreamError::InvalidArgument)
        };
    }
    let anchor = cursor
        .opaque
        .as_ref()
        .try_into()
        .map_err(|_| StreamError::InvalidArgument)?;
    Ok(Some(CommitId::from_bytes(anchor)))
}

/// Opens the platform's ALL-record session route. Anchor checking and subscription
/// are one provider operation, so accepted-history replacement cannot splice cuts.
pub async fn follow_records<P: StreamProvider + ?Sized>(
    provider: &P,
    follow: &wire::SessionFollow,
    cursor: &wire::SessionCursor,
) -> Result<RecordStream, StreamError> {
    if follow.delivery != wire::SessionDelivery::All as i32 {
        return Err(StreamError::InvalidArgument);
    }
    let path = stream_path(&follow.resource)?;
    match record_anchor(cursor)? {
        Some(anchor) => {
            provider
                .follow_checked(path, cursor.next, anchor, crate::FollowFilter::default())
                .await
        }
        None => {
            provider
                .follow(path, 0, crate::FollowFilter::default())
                .await
        }
    }
}

/// Validates a peer's ALL/LATEST data against the next received cursor.
/// LATEST can skip conflated values; ALL must remain consecutive. The caller
/// advances application progress only when this admitted delivery is consumed.
pub fn follow_sequence(
    delivery: i32,
    next: u64,
    data: &wire::SessionData,
) -> Result<u64, StreamError> {
    use wire::{SessionDelivery, session_data::Item};
    let after = data.after.as_ref().ok_or(StreamError::Unavailable)?;
    if !after.opaque.is_empty() || data.accepted < after.next || data.durable > data.accepted {
        return Err(StreamError::Unavailable);
    }
    match (SessionDelivery::try_from(delivery), &data.item) {
        (Ok(SessionDelivery::All), Some(Item::Record(record)))
            if record.commit_id.len() == 32 && record.sequence == next =>
        {
            if after.next
                != record
                    .sequence
                    .checked_add(1)
                    .ok_or(StreamError::OutOfRange)?
            {
                return Err(StreamError::Unavailable);
            }
            Ok(record.sequence)
        }
        (Ok(SessionDelivery::Latest), Some(Item::LatestValue(value)))
            if !value.key.is_empty() && after.next > next =>
        {
            Ok(after.next - 1)
        }
        _ => Err(StreamError::Unavailable),
    }
}

/// Adds a receive-credit grant without partially applying an overflowing grant.
pub fn grant_credit(
    remaining: &mut wire::SessionWindow,
    grant: &wire::SessionWindow,
) -> Result<(), StreamError> {
    let bytes = remaining
        .bytes
        .checked_add(grant.bytes)
        .ok_or(StreamError::LimitExceeded)?;
    let frames = remaining
        .frames
        .checked_add(grant.frames)
        .ok_or(StreamError::LimitExceeded)?;
    remaining.bytes = bytes;
    remaining.frames = frames;
    Ok(())
}

/// Debits one encoded data frame; control frames never consume data credit.
pub fn consume_credit(
    remaining: &mut wire::SessionWindow,
    frame: &wire::SessionFrame,
) -> Result<(), StreamError> {
    match frame.body.as_ref() {
        None => return Err(StreamError::InvalidArgument),
        Some(wire::session_frame::Body::Data(_)) if frame.channel_id == 0 => {
            return Err(StreamError::InvalidArgument);
        }
        Some(wire::session_frame::Body::Data(_)) => {}
        Some(_) => return Ok(()),
    }
    let length = u64::try_from(frame.encoded_len()).map_err(|_| StreamError::LimitExceeded)?;
    let bytes = remaining
        .bytes
        .checked_sub(length)
        .ok_or(StreamError::Capacity)?;
    let frames = remaining
        .frames
        .checked_sub(1)
        .ok_or(StreamError::Capacity)?;
    remaining.bytes = bytes;
    remaining.frames = frames;
    Ok(())
}

#[cfg(test)]
mod tests;
