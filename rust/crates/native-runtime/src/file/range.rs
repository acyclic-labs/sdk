use crate::*;
use std::borrow::Borrow;

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
    read_bounded(length, require_full, meter, |filled| {
        let destination = data.get_mut(filled..).ok_or(io::ErrorKind::InvalidData)?;
        #[cfg(windows)]
        return windows::read_at_known_overlapped(file, offset + filled as u64, destination, meter);
        #[cfg(unix)]
        read_declared_source_bytes(file, offset + filled as u64, destination, meter)
    })
}

pub(crate) fn read_bounded_spare(
    file: &File,
    offset: u64,
    data: &mut Vec<u8>,
    length: usize,
    require_full: bool,
    meter: &dyn NativeReadMeter,
) -> io::Result<usize> {
    if !data.is_empty() || length > data.capacity() {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    read_bounded(length, require_full, meter, |filled| {
        #[cfg(windows)]
        return windows::read_spare_known_overlapped(
            file,
            offset + filled as u64,
            data,
            length - filled,
            meter,
        );
        #[cfg(unix)]
        super::operations::read_declared_source_spare(
            file,
            offset + filled as u64,
            data,
            length - filled,
            meter,
        )
    })
}

fn read_bounded(
    length: usize,
    require_full: bool,
    meter: &dyn NativeReadMeter,
    mut read: impl FnMut(usize) -> io::Result<usize>,
) -> io::Result<usize> {
    let mut filled = 0;
    while filled < length {
        meter.check()?;
        let count = match read(filled) {
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

/// A bounded file range with an advancing cursor.
pub struct AsyncRangeReader<F = Arc<NativeFile>> {
    file: F,
    offset: u64,
    remaining: u64,
}

impl<F: Borrow<NativeFile>> AsyncRangeReader<F> {
    /// Limits reads to length bytes starting at offset, borrowing or owning the file.
    pub const fn new(file: F, offset: u64, length: u64) -> Self {
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

    /// Reads owned storage, advancing only after the consumer accepts the result.
    pub async fn read_prepared<B, T, E>(
        &mut self,
        storage: B,
        maximum: usize,
        meter: Arc<dyn NativeReadMeter>,
        require_full: bool,
        accept: impl FnOnce(io::Result<(B, usize)>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        B: AsMut<[u8]> + Send + 'static,
    {
        let result = async {
            meter.check()?;
            let length = usize::try_from(self.remaining.min(maximum as u64))
                .map_err(|_| io::ErrorKind::InvalidInput)?;
            if length == 0 {
                return Ok((storage, 0));
            }
            self.file
                .borrow()
                .read_prepared_bounded_async(self.offset, storage, length, require_full, meter)?
                .await
        }
        .await;
        self.accept_prepared(result, accept)
    }

    /// Reads into spare capacity and commits only the consumer-accepted prefix.
    pub async fn read_spare<G, T, E>(
        &mut self,
        storage: (Vec<u8>, G),
        maximum: usize,
        meter: Arc<dyn NativeReadMeter>,
        require_full: bool,
        accept: impl FnOnce(io::Result<((Vec<u8>, G), usize)>) -> Result<T, E>,
    ) -> Result<T, E>
    where
        G: Send + 'static,
    {
        let result = async {
            meter.check()?;
            let length = usize::try_from(self.remaining.min(maximum as u64))
                .map_err(|_| io::ErrorKind::InvalidInput)?;
            if length == 0 {
                return Ok((storage, 0));
            }
            self.file
                .borrow()
                .read_spare_bounded_async(self.offset, storage, length, require_full, meter)?
                .await
        }
        .await;
        self.accept_prepared(result, accept)
    }

    fn accept_prepared<B, T, E>(
        &mut self,
        result: io::Result<(B, usize)>,
        accept: impl FnOnce(io::Result<(B, usize)>) -> Result<T, E>,
    ) -> Result<T, E> {
        let filled = result.as_ref().map_or(0, |(_, filled)| *filled as u64);
        let (Some(offset), Some(remaining)) = (
            self.offset.checked_add(filled),
            self.remaining.checked_sub(filled),
        ) else {
            return accept(Err(io::ErrorKind::InvalidInput.into()));
        };
        let value = accept(result)?;
        self.offset = offset;
        self.remaining = remaining;
        Ok(value)
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
            .borrow()
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
