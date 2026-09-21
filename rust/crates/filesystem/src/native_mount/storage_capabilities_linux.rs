//! Linux volume geometry used to admit native storage operations.

use super::{NativeStorageCapabilities, NativeStorageCapabilityError};
use std::fs::File;
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[allow(unsafe_code)]
pub(super) fn probe(
    root: &Path,
) -> Result<NativeStorageCapabilities, NativeStorageCapabilityError> {
    let directory = File::open(root)
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: the directory is live and the kernel fills the result on success.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
        return Err(NativeStorageCapabilityError::Platform(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: fstatfs succeeded and initialized the result.
    let stats = unsafe { stats.assume_init() };
    // libc models statfs::f_type as signed for glibc and unsigned for musl.
    // Fallible normalization keeps one boundary valid for both ABIs and
    // rejects an impossible negative filesystem identifier.
    let kind =
        u64::try_from(stats.f_type).map_err(|_| NativeStorageCapabilityError::UnsupportedTarget)?;
    let filesystem = match kind {
        0xef53 => "ext",
        0x9123_683e => "btrfs",
        0x5846_5342 => "xfs",
        _ => return Err(NativeStorageCapabilityError::UnsupportedTarget),
    };
    let device = directory
        .metadata()
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?
        .dev();
    let sysfs = PathBuf::from(format!(
        "/sys/dev/block/{}:{}",
        libc::major(device),
        libc::minor(device)
    ));
    let mut device_path = std::fs::canonicalize(sysfs)
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    let (logical, physical) = loop {
        let queue = device_path.join("queue");
        if queue.is_dir() {
            break (
                sector_size(&queue, "logical_block_size")?,
                sector_size(&queue, "physical_block_size")?,
            );
        }
        if !device_path.pop() {
            return Err(NativeStorageCapabilityError::UnsupportedTarget);
        }
    };
    let allocation_unit_bytes =
        u64::try_from(stats.f_bsize).map_err(|_| NativeStorageCapabilityError::InvalidGeometry)?;
    if logical == 0
        || physical == 0
        || allocation_unit_bytes == 0
        || allocation_unit_bytes % u64::from(logical) != 0
    {
        return Err(NativeStorageCapabilityError::InvalidGeometry);
    }
    let sectors_per_allocation_unit = u32::try_from(allocation_unit_bytes / u64::from(logical))
        .map_err(|_| NativeStorageCapabilityError::InvalidGeometry)?;
    if sectors_per_allocation_unit == 0 {
        return Err(NativeStorageCapabilityError::InvalidGeometry);
    }
    Ok(NativeStorageCapabilities {
        filesystem: filesystem.to_owned(),
        logical_bytes_per_sector: logical,
        // Linux exposes physical block size, but no portable per-volume
        // atomic-write guarantee. This is the conservative device unit.
        physical_bytes_per_sector_for_atomicity: physical,
        physical_bytes_per_sector_for_performance: physical,
        effective_physical_bytes_per_sector_for_atomicity: physical,
        sectors_per_allocation_unit,
        allocation_unit_bytes,
        sparse_files: true,
        // XFS reflink support is a per-volume feature. The live probe below
        // records its actual availability instead of guessing from f_type.
        block_cloning: filesystem == "btrfs",
    })
}

fn sector_size(queue: &Path, name: &str) -> Result<u32, NativeStorageCapabilityError> {
    std::fs::read_to_string(queue.join(name))
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?
        .trim()
        .parse::<u32>()
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_geometry_is_consistent() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let facts = match super::super::probe_native_storage_capabilities(directory.path()) {
            Ok(facts) => facts,
            Err(NativeStorageCapabilityError::UnsupportedTarget) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        assert!(!facts.filesystem.is_empty());
        assert!(facts.logical_bytes_per_sector > 0);
        assert_eq!(
            facts.allocation_unit_bytes,
            u64::from(facts.logical_bytes_per_sector)
                * u64::from(facts.sectors_per_allocation_unit)
        );
        Ok(())
    }
}
