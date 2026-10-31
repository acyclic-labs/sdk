//! Byte-progress I/O: only delivered bytes advance an operation.
use std::io::{self, IoSlice, Read};

/// Writes each original slice without copying it or reissuing a failed call.
pub(crate) fn write_all<'a>(
    mut buffers: &mut [IoSlice<'a>],
    mut write: impl FnMut(&[IoSlice<'a>]) -> io::Result<usize>,
) -> io::Result<()> {
    let mut remaining = buffers.iter().try_fold(0_usize, |total, buffer| {
        total
            .checked_add(buffer.len())
            .ok_or(io::ErrorKind::InvalidInput)
    })?;
    while remaining != 0 {
        let count = write(buffers)?;
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        remaining = remaining
            .checked_sub(count)
            .ok_or(io::ErrorKind::InvalidData)?;
        IoSlice::advance_slices(&mut buffers, count);
    }
    Ok(())
}

/// Returns the first read failure, retaining every byte already stored.
pub(crate) fn read_to_end(reader: &mut impl Read, bytes: &mut Vec<u8>) -> io::Result<()> {
    let mut filled = bytes.len();
    let result = (|| {
        loop {
            if filled == bytes.len() {
                // Probe before growing, so EOF at capacity allocates nothing.
                // Only this single boundary byte needs an intermediate copy.
                let mut next = [0];
                match reader.read(&mut next)? {
                    0 => return Ok(()),
                    1 => {
                        // Allocation chunks split storage growth, never limit a report.
                        bytes
                            .try_reserve(4096)
                            .map_err(|_| io::ErrorKind::OutOfMemory)?;
                        bytes.push(next[0]);
                        filled += 1;
                        bytes.resize(bytes.capacity(), 0);
                    }
                    _ => return Err(io::ErrorKind::InvalidData.into()),
                }
            }
            let output = bytes.get_mut(filled..).ok_or(io::ErrorKind::InvalidData)?;
            let count = reader.read(output)?;
            if count == 0 {
                return Ok(());
            }
            if count > output.len() {
                return Err(io::ErrorKind::InvalidData.into());
            }
            filled += count;
        }
    })();
    bytes.truncate(filled);
    result
}
