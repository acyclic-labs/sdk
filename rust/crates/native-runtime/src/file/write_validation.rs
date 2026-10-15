use crate::OwnedWrite;
use std::io;

pub(crate) fn validate_write_batch(writes: &[OwnedWrite]) -> io::Result<()> {
    let mut previous_end = 0;
    for write in writes {
        let end = write
            .offset
            .checked_add(write.bytes.len() as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;
        if write.offset == u64::MAX {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if !write.bytes.is_empty() {
            if write.offset < previous_end {
                return validate_unordered_write_batch(writes);
            }
            previous_end = end;
        }
    }
    Ok(())
}

fn validate_unordered_write_batch(writes: &[OwnedWrite]) -> io::Result<()> {
    let mut ranges = Vec::new();
    ranges.try_reserve_exact(writes.len())?;
    for write in writes {
        let end = write
            .offset
            .checked_add(write.bytes.len() as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;
        if write.offset == u64::MAX {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if !write.bytes.is_empty() {
            ranges.push((write.offset, end));
        }
    }
    ranges.sort_unstable();
    if ranges
        .windows(2)
        .any(|pair| matches!(pair, [left, right] if left.1 > right.0))
    {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    Ok(())
}
