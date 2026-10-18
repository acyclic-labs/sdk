use acyclic_stream::wire_codec::session::validate_follow;
use acyclic_stream::{StreamError, wire, wire_codec::session::follow_sequence};
use wire::{SessionDelivery, session_data::Item};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault {
    None,
    StrictLatest,
    SkipAll,
    EmptyKey,
    Watermark,
    Opaque,
    Commit,
    WrongMode,
}

// This oracle describes received wire positions directly. It never encodes a
// cursor or calls the production inverse to derive the expected sequence.
fn expected(delivery: i32, next: u64, data: &wire::SessionData) -> Result<u64, StreamError> {
    let Some(after) = &data.after else {
        return Err(StreamError::Unavailable);
    };
    if data.durable > data.accepted || after.next > data.accepted || !after.opaque.is_empty() {
        return Err(StreamError::Unavailable);
    }
    match (&data.item, delivery) {
        (Some(Item::Record(record)), 1)
            if record.commit_id.len() == 32 && record.sequence == next =>
        {
            let successor = u128::from(record.sequence) + 1;
            if successor > u128::from(u64::MAX) {
                return Err(StreamError::OutOfRange);
            }
            if u128::from(after.next) != successor {
                return Err(StreamError::Unavailable);
            }
            Ok(record.sequence)
        }
        (Some(Item::LatestValue(value)), 2)
            if !value.key.is_empty()
                && after.next != 0
                && after.next >= next.saturating_add(1)
                && next != u64::MAX =>
        {
            Ok(after.next - 1)
        }
        _ => Err(StreamError::Unavailable),
    }
}

pub(super) fn check(
    delivery: i32,
    next: u64,
    data: &wire::SessionData,
    fault: Fault,
) -> Result<(), &'static str> {
    let wanted = expected(delivery, next, data);
    let mut changed = data.clone();
    if fault == Fault::Watermark {
        changed.accepted = u64::MAX;
        changed.durable = 0;
    }
    if fault == Fault::Opaque
        && let Some(after) = &mut changed.after
    {
        after.opaque.clear();
    }
    if fault == Fault::Commit
        && let Some(Item::Record(record)) = &mut changed.item
    {
        record.commit_id = vec![7; 32].into();
    }
    let mut actual = follow_sequence(delivery, next, &changed);
    if let Some(after) = &data.after {
        match fault {
            Fault::StrictLatest
                if delivery == SessionDelivery::Latest as i32
                    && after.next > next.saturating_add(1)
                    && actual.is_ok() =>
            {
                actual = Err(StreamError::Unavailable);
            }
            Fault::SkipAll if delivery == SessionDelivery::All as i32 => {
                if let Some(Item::Record(record)) = &data.item
                    && record.sequence != next
                    && record.sequence.checked_add(1) == Some(after.next)
                {
                    actual = Ok(record.sequence);
                }
            }
            Fault::EmptyKey | Fault::WrongMode if after.next > next => {
                if let Some(Item::LatestValue(value)) = &data.item
                    && ((fault == Fault::EmptyKey && value.key.is_empty() && delivery == 2)
                        || (fault == Fault::WrongMode && delivery == 1))
                {
                    actual = Ok(after.next - 1);
                }
            }
            _ => {}
        }
    }
    if actual == wanted {
        Ok(())
    } else {
        Err("follow data contract")
    }
}
