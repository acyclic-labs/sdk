//! Session receive-credit accounting shared by transport adapters.

use crate::{CommitId, RecordStream, StreamError, StreamPath, StreamProvider, wire};
use prost::Message as _;

/// Decodes a bounded physical session message; protocol validation remains in the pool.
pub fn decode_frame(bytes: &[u8]) -> Result<wire::SessionFrame, StreamError> {
    if bytes.len() > crate::MAX_COMMAND_BYTES {
        return Err(StreamError::LimitExceeded);
    }
    wire::SessionFrame::decode(bytes).map_err(|_| StreamError::InvalidArgument)
}

/// Validates a logical Streams follow without creating transport state.
pub fn validate_follow(
    follow: &wire::SessionFollow,
    resume: &wire::SessionCursor,
) -> Result<(), StreamError> {
    StreamPath::validate(&follow.path)?;
    if !resume.opaque.is_empty()
        || !matches!(
            wire::SessionDelivery::try_from(follow.delivery),
            Ok(wire::SessionDelivery::All | wire::SessionDelivery::Latest)
        )
    {
        return Err(StreamError::InvalidArgument);
    }
    preceding_follow(follow, resume)?;
    Ok(())
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
    let path = StreamPath::new(&follow.path)?;
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
