//! Retire the consumed driver prefix without losing its original byte custody.
use super::*;

/// Positions in the current working tail, never message-content identities.
/// Retired steering moves here until the same turn's output is staged.
#[derive(Default)]
pub(in crate::executor) struct SteeringHistory {
    ranges: Vec<std::ops::Range<usize>>,
    retired: Vec<ModelMessage>,
}

impl SteeringHistory {
    pub(in crate::executor) fn admit(
        &mut self,
        range: std::ops::Range<usize>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if !range.is_empty() {
            stock_vec_push(&mut self.ranges, range, io)?;
        }
        Ok(())
    }

    pub(in crate::executor) fn into_retired(self) -> Vec<ModelMessage> {
        self.retired
    }
}

fn steering_at(ranges: &mut &[std::ops::Range<usize>], index: usize) -> bool {
    while ranges.first().is_some_and(|range| range.end <= index) {
        *ranges = &ranges[1..];
    }
    ranges.first().is_some_and(|range| range.contains(&index))
}

pub(super) fn retire_prefix(
    messages: &mut Vec<ModelMessage>,
    offset: &mut u64,
    through: u64,
    mut steering: Option<&mut SteeringHistory>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let invalid = || {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "working prefix exceeds retained history",
        )
    };
    let count = through
        .checked_sub(*offset)
        .and_then(|count| usize::try_from(count).ok())
        .filter(|count| *count <= messages.len())
        .ok_or_else(invalid)?;
    if count == 0 {
        return Ok(());
    }
    let ranges = steering
        .as_ref()
        .map(|steering| steering.ranges.as_slice())
        .unwrap_or(&[]);
    let preserved = ranges
        .iter()
        .take_while(|range| range.start < count)
        .map(|range| range.end.min(count) - range.start)
        .sum::<usize>();
    if let Some(steering) = steering.as_mut() {
        let capacity = steering
            .retired
            .len()
            .checked_add(preserved)
            .ok_or_else(invalid)?;
        if capacity > steering.retired.capacity() {
            let capacity = capacity.max(
                steering
                    .retired
                    .capacity()
                    .checked_mul(2)
                    .unwrap_or(capacity),
            );
            stock_vec_reserve(&mut steering.retired, capacity, io)?;
        }
    }
    #[cfg(feature = "filesystem")]
    let retirement = {
        let mut ranges = steering
            .as_ref()
            .map(|steering| steering.ranges.as_slice())
            .unwrap_or(&[]);
        let bytes = messages
            .get(..count)
            .ok_or_else(invalid)?
            .iter()
            .enumerate()
            .try_fold(0_u64, |bytes, (index, message)| {
                if steering_at(&mut ranges, index) {
                    return Ok(bytes);
                }
                bytes
                    .checked_add(message.content.retained_payload_bytes()?)
                    .ok_or_else(invalid)
            })?;
        io.map(|io| io.guard_owned(bytes)).transpose()?
    };
    // Vector capacity belongs to the next working tail. Nested payloads must
    // die before their funding; acquiring that funding is fallible and first.
    if let Some(steering) = steering {
        let mut ranges = steering.ranges.as_slice();
        for (index, message) in messages.drain(..count).enumerate() {
            if steering_at(&mut ranges, index) {
                steering.retired.push(message);
            }
        }
        steering.ranges.retain_mut(|range| {
            range.start = range.start.saturating_sub(count);
            range.end = range.end.saturating_sub(count);
            !range.is_empty()
        });
    } else {
        drop(messages.drain(..count));
    }
    *offset = through;
    #[cfg(feature = "filesystem")]
    drop(retirement);
    Ok(())
}

#[cfg(all(test, feature = "filesystem"))]
mod tests;
