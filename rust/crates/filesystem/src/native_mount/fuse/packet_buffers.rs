//! Original-funded packet storage and one shared write-size admission policy.

use super::*;
use crate::NativeMaterializationWork;

pub(super) struct PacketBuffers {
    pub(super) input: FuseOwned<Vec<u8>>,
    pub(super) output: FuseOwned<Vec<u8>>,
}

impl PacketBuffers {
    pub(super) fn new(
        capacity: usize,
        work: &NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let buffer = || -> Result<FuseOwned<Vec<u8>>, NativeMountError> {
            let mut bytes = fuse_vec(capacity, work)?;
            bytes.resize(capacity, 0);
            Ok(bytes)
        };
        Ok(Self {
            input: buffer()?,
            output: buffer()?,
        })
    }
}

pub(super) fn maximum_write(
    work: &NativeMaterializationWork,
    workers: std::num::NonZeroU32,
    page_size: std::num::NonZeroU32,
) -> Result<u32, NativeMountError> {
    // Two buffers per worker consume at most half the remaining allocation
    // capacity. Source operations and custody metadata retain the other half.
    // Negotiation shrinks writes rather than increasing the original limits.
    let per_buffer = work.remaining()?.peak_allocation_bytes / (4 * u64::from(workers.get()));
    let unit = u64::from(page_size.get().max(4096));
    let maximum = per_buffer
        .saturating_sub(4096)
        .min(u64::from(super::packet_session::MAXIMUM_WRITE))
        / unit
        * unit;
    let maximum = u32::try_from(maximum).map_err(|_| libc::EOVERFLOW)?;
    // Linux xattr requests can carry 64 KiB independently of max_write.
    if maximum < 64 * 1024 {
        return Err(libc::ENOMEM.into());
    }
    Ok(maximum)
}
