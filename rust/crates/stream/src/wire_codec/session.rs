//! Session receive-credit accounting shared by transport adapters.

use crate::{StreamError, wire};
use prost::Message as _;

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
    super::commit_id(&follow.preceding_commit_id).map(Some)
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
