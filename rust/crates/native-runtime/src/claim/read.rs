//! Metered reads of the shared persisted claim format.

use super::record::{CHECKSUM_BYTES, CHECKSUM_INITIAL, Contents, MAGIC, checksum, checksum_chunk};

/// Complete record storage; its buffer is destroyed before its original credit.
pub(super) struct ReadRecord {
    bytes: Vec<u8>,
    status: Option<Status>,
    _allocation: Option<crate::ReadAllocation>,
}

#[derive(Clone, Copy)]
enum Status {
    Empty,
    Torn,
    Malformed,
    Verified,
}

impl ReadRecord {
    #[cfg(all(test, loom))]
    pub(super) fn from_bytes(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            status: None,
            _allocation: None,
        }
    }

    pub(super) fn contents(&self) -> Contents<'_> {
        match self.status {
            Some(Status::Empty) => Contents::Empty,
            Some(Status::Torn) => Contents::Torn,
            Some(Status::Malformed) => Contents::Malformed,
            Some(Status::Verified) => Contents::decode_inner(&self.bytes, false),
            None => Contents::decode(&self.bytes),
        }
    }

    // Inspect framing and checksum in constant scratch space before allocating.
    // A large sparse/corrupt claim file therefore cannot request its file size
    // from the allocator. The same original meter pays for each actual read.
    pub(super) fn read(
        file: &mut (impl std::io::Read + std::io::Seek),
        meter: Option<&std::sync::Arc<dyn crate::NativeReadMeter>>,
    ) -> std::io::Result<Self> {
        use std::io::{self, SeekFrom};
        let mut result = Self {
            bytes: Vec::new(),
            status: None,
            _allocation: None,
        };
        let length = native_operation(meter, || file.seek(SeekFrom::End(0)))?;
        native_operation(meter, || file.seek(SeekFrom::Start(0)))?;
        if length == 0 {
            result.status = Some(Status::Empty);
            return Ok(result);
        }
        let mut magic = [0_u8; MAGIC.len()];
        let prefix = usize::try_from(length.min(MAGIC.len() as u64))
            .map_err(|_| io::ErrorKind::InvalidData)?;
        read_exact(file, &mut magic[..prefix], meter)?;
        if magic[..prefix] != MAGIC[..prefix] {
            result.status = Some(Status::Malformed);
            return Ok(result);
        }
        if prefix < MAGIC.len() {
            result.status = Some(Status::Torn);
            return Ok(result);
        }
        let mut position = MAGIC.len() as u64;
        for index in 0..5 {
            if length.saturating_sub(position) < 4 {
                result.status = Some(Status::Torn);
                return Ok(result);
            }
            let mut size = [0_u8; 4];
            read_exact(file, &mut size, meter)?;
            position += 4;
            let size = u64::from(u32::from_le_bytes(size));
            if (index == 0 && size != 4) || (index == 4 && size != 8) {
                result.status = Some(Status::Malformed);
                return Ok(result);
            }
            if size > length.saturating_sub(position) {
                result.status = Some(Status::Torn);
                return Ok(result);
            }
            position += size;
            native_operation(meter, || file.seek(SeekFrom::Start(position)))?;
        }
        match length
            .saturating_sub(position)
            .cmp(&(CHECKSUM_BYTES as u64))
        {
            std::cmp::Ordering::Less => {
                result.status = Some(Status::Torn);
                return Ok(result);
            }
            std::cmp::Ordering::Greater => {
                result.status = Some(Status::Malformed);
                return Ok(result);
            }
            std::cmp::Ordering::Equal => {}
        }
        native_operation(meter, || file.seek(SeekFrom::Start(0)))?;
        let mut remaining = length - CHECKSUM_BYTES as u64;
        let mut scratch = [0_u8; 16 * 1024];
        let mut hash = CHECKSUM_INITIAL;
        while remaining != 0 {
            let count = usize::try_from(remaining.min(scratch.len() as u64))
                .map_err(|_| io::ErrorKind::InvalidData)?;
            read_exact(file, &mut scratch[..count], meter)?;
            if let Some(meter) = meter {
                meter.admit_codec(0, 0, count as u64)?;
            }
            hash = checksum_chunk(hash, &scratch[..count]);
            if let Some(meter) = meter {
                meter.record_codec(0, 0, count as u64)?;
            }
            remaining -= count as u64;
        }
        let mut sum = [0_u8; CHECKSUM_BYTES];
        read_exact(file, &mut sum, meter)?;
        if hash.to_le_bytes() != sum {
            result.status = Some(Status::Torn);
            return Ok(result);
        }
        let count = usize::try_from(length).map_err(|_| io::ErrorKind::InvalidData)?;
        result._allocation = meter
            .map(|meter| crate::ReadAllocation::new(meter, count))
            .transpose()?;
        result
            .bytes
            .try_reserve_exact(count)
            .map_err(|error| match meter {
                Some(meter) => meter.allocation_failed(error),
                None => io::Error::from(io::ErrorKind::OutOfMemory),
            })?;
        if let Some(allocation) = &mut result._allocation {
            allocation.reconcile_capacity(result.bytes.capacity())?;
        }
        result.bytes.resize(count, 0);
        native_operation(meter, || file.seek(SeekFrom::Start(0)))?;
        read_exact(file, &mut result.bytes, meter)?;
        // Revalidate the owned snapshot too, even if an external writer ignores
        // the protocol lock. Contents then borrows only this verified buffer.
        let body = result.bytes.len() - CHECKSUM_BYTES;
        if let Some(meter) = meter {
            meter.admit_codec(0, 0, body as u64)?;
        }
        let valid = checksum(&result.bytes[..body]) == result.bytes[body..];
        if let Some(meter) = meter {
            meter.record_codec(0, 0, body as u64)?;
        }
        result.status = Some(if valid {
            Status::Verified
        } else {
            Status::Torn
        });
        Ok(result)
    }
}

fn native_operation<T>(
    meter: Option<&std::sync::Arc<dyn crate::NativeReadMeter>>,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> std::io::Result<T> {
    crate::meter::read_operation(meter, operation)?
}

fn read_exact(
    file: &mut impl std::io::Read,
    mut bytes: &mut [u8],
    meter: Option<&std::sync::Arc<dyn crate::NativeReadMeter>>,
) -> std::io::Result<()> {
    use std::io;
    while !bytes.is_empty() {
        let mut transfer = meter
            .map(|meter| {
                crate::NativeByteReservation::new(
                    meter.clone(),
                    crate::NativeOperationKind::Read,
                    bytes.len(),
                )
            })
            .transpose()?;
        let result = native_operation(meter, || file.read(bytes));
        if let Some(transfer) = &mut transfer {
            transfer.record(result.as_ref().copied().unwrap_or(0))?;
        }
        match result {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(count) => bytes = &mut bytes[count..],
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
