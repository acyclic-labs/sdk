use crate::*;

pub(crate) fn validate_prepared_read_range(offset: u64, length: usize) -> io::Result<()> {
    let end = offset
        .checked_add(length as u64)
        .ok_or(io::ErrorKind::InvalidInput)?;
    #[cfg(windows)]
    if length != 0 && (length > u32::MAX as usize || offset == u64::MAX) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    #[cfg(unix)]
    if length != 0 && (offset > i64::MAX as u64 || end > i64::MAX as u64) {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    #[cfg(not(unix))]
    let _ = end;
    Ok(())
}

pub(crate) fn read_bounded_slice(
    file: &File,
    offset: u64,
    data: &mut [u8],
    require_full: bool,
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    let length = data.len();
    let mut filled = 0;
    while filled < length {
        meter.check()?;
        let destination = &mut data[filled..];
        #[cfg(windows)]
        let result =
            windows::read_at_known_overlapped(file, offset + filled as u64, destination, meter);
        #[cfg(unix)]
        let result = read_declared_source_bytes(file, offset + filled as u64, destination, meter);
        let count = match result {
            Ok(count) => count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        meter.check()?;
        if count == 0 {
            break;
        }
        filled = filled
            .checked_add(count)
            .filter(|count| *count <= length)
            .ok_or(io::ErrorKind::InvalidData)?;
    }
    meter.finish_read(length, filled)?;
    if require_full && filled != length {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(filled)
}

/// An owned file range with an advancing cursor.
pub struct AsyncRangeReader {
    file: Arc<NativeFile>,
    offset: u64,
    remaining: u64,
}

impl AsyncRangeReader {
    /// Limits reads to length bytes starting at offset.
    pub const fn new(file: Arc<NativeFile>, offset: u64, length: u64) -> Self {
        Self {
            file,
            offset,
            remaining: length,
        }
    }

    /// Bytes remaining in this range.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Reads a bounded chunk, returning its bytes before their allocation custody.
    pub async fn read(
        &mut self,
        maximum: usize,
        meter: Arc<dyn NativeReadMeter>,
        require_full: bool,
    ) -> io::Result<(Bytes, Option<ReadAllocation>)> {
        meter.check()?;
        let length = usize::try_from(self.remaining.min(maximum as u64))
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        if length == 0 {
            return Ok((Bytes::new(), None));
        }
        let result = self
            .file
            .read_bounded_async(self.offset, length, meter, require_full)?
            .await?;
        self.offset = self
            .offset
            .checked_add(result.0.len() as u64)
            .ok_or(io::ErrorKind::InvalidInput)?;
        self.remaining -= result.0.len() as u64;
        Ok(result)
    }
}
