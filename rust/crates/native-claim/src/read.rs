//! Metered reads of the shared persisted claim format.

use super::record::{CHECKSUM_BYTES, CHECKSUM_INITIAL, Contents, MAGIC, checksum, checksum_chunk};

/// Scratch that holds a whole claim record, which then costs one read.
const SCRATCH_BYTES: usize = 16 * 1024;

/// Complete record storage; its buffer is destroyed before its original credit.
pub(super) struct ReadRecord {
    bytes: Vec<u8>,
    status: Option<Status>,
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
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
    // from the allocator. The same original meter pays for each actual read:
    // the length and a rewind, then one read of a file that fits scratch.
    pub(super) fn read(
        file: &mut (impl std::io::Read + std::io::Seek),
        meter: Option<&std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
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
        let mut scratch = [0_u8; SCRATCH_BYTES];
        if let Some(whole) = usize::try_from(length)
            .ok()
            .and_then(|count| scratch.get_mut(..count))
        {
            // A record that fits scratch is read once and classified there.
            read_exact(file, whole, meter)?;
            if let Some(meter) = meter {
                meter.admit_codec(0, 0, length)?;
            }
            let status = match Contents::decode(whole) {
                Contents::Empty => Status::Empty,
                Contents::Torn => Status::Torn,
                Contents::Malformed => Status::Malformed,
                Contents::Record(_) => Status::Verified,
            };
            if let Some(meter) = meter {
                meter.record_codec(0, 0, length)?;
            }
            if let Status::Verified = status {
                result.allocate(whole.len(), meter)?;
                result.bytes.extend_from_slice(whole);
            }
            result.status = Some(status);
            return Ok(result);
        }
        // A longer file streams its framing and checksum through scratch first.
        let mut magic = [0_u8; MAGIC.len()];
        let prefix = usize::try_from(length.min(MAGIC.len() as u64))
            .map_err(|_| io::ErrorKind::InvalidData)?;
        let magic = magic.get_mut(..prefix).ok_or(io::ErrorKind::InvalidData)?;
        read_exact(file, magic, meter)?;
        if !MAGIC.starts_with(magic) {
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
        let mut hash = CHECKSUM_INITIAL;
        while remaining != 0 {
            let count = usize::try_from(remaining.min(scratch.len() as u64))
                .map_err(|_| io::ErrorKind::InvalidData)?;
            let chunk = scratch.get_mut(..count).ok_or(io::ErrorKind::InvalidData)?;
            read_exact(file, chunk, meter)?;
            if let Some(meter) = meter {
                meter.admit_codec(0, 0, count as u64)?;
            }
            hash = checksum_chunk(hash, chunk);
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
        result.allocate(count, meter)?;
        result.bytes.resize(count, 0);
        native_operation(meter, || file.seek(SeekFrom::Start(0)))?;
        read_exact(file, &mut result.bytes, meter)?;
        // Revalidate the owned snapshot too, even if an external writer ignores
        // the protocol lock. Contents then borrows only this verified buffer.
        let (body, sum) = result
            .bytes
            .split_last_chunk::<CHECKSUM_BYTES>()
            .ok_or(io::ErrorKind::InvalidData)?;
        if let Some(meter) = meter {
            meter.admit_codec(0, 0, body.len() as u64)?;
        }
        let valid = checksum(body) == *sum;
        if let Some(meter) = meter {
            meter.record_codec(0, 0, body.len() as u64)?;
        }
        result.status = Some(if valid {
            Status::Verified
        } else {
            Status::Torn
        });
        Ok(result)
    }
}

impl ReadRecord {
    /// Admits, then reserves, the owned buffer of a `count`-byte record.
    fn allocate(
        &mut self,
        count: usize,
        meter: Option<&std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
    ) -> std::io::Result<()> {
        self._allocation = meter
            .map(|meter| acyclic_native_runtime::ReadAllocation::new(meter, count))
            .transpose()?;
        self.bytes
            .try_reserve_exact(count)
            .map_err(|error| match meter {
                Some(meter) => meter.allocation_failed(error),
                None => std::io::Error::from(std::io::ErrorKind::OutOfMemory),
            })?;
        if let Some(allocation) = &mut self._allocation {
            allocation.reconcile_capacity(self.bytes.capacity())?;
        }
        Ok(())
    }
}

fn native_operation<T>(
    meter: Option<&std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
    operation: impl FnOnce() -> std::io::Result<T>,
) -> std::io::Result<T> {
    acyclic_native_runtime::read_operation(meter, operation)?
}

fn read_exact(
    file: &mut impl std::io::Read,
    mut bytes: &mut [u8],
    meter: Option<&std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
) -> std::io::Result<()> {
    use std::io;
    while !bytes.is_empty() {
        let mut transfer = meter
            .map(|meter| {
                acyclic_native_runtime::NativeByteReservation::new(
                    meter.clone(),
                    acyclic_native_runtime::NativeOperationKind::Read,
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
            // A reader reporting more than it was given breaks Read's contract.
            Ok(count) => {
                bytes = std::mem::take(&mut bytes)
                    .get_mut(count..)
                    .ok_or(io::ErrorKind::InvalidData)?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
