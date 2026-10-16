//! Session receive-credit accounting shared by transport adapters.

use crate::{CommitId, RecordStream, StreamError, StreamPath, StreamProvider, wire};
use prost::Message as _;

/// Complete record continuation at a nonzero cut, including its immutable anchor.
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
        Some(anchor) => provider.follow_checked(path, cursor.next, anchor).await,
        None => provider.follow(path, 0).await,
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
