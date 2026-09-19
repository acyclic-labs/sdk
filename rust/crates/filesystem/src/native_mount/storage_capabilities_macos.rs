//! Volume facts for the directory containing a macOS native root.

use super::{NativeStorageCapabilities, NativeStorageCapabilityError};
use std::ffi::CStr;
use std::fs::File;
use std::os::fd::AsRawFd;
use std::path::Path;

#[repr(C)]
struct VolumeCapabilities {
    length: u32,
    values: libc::vol_capabilities_attr_t,
}

#[allow(unsafe_code)]
pub(super) fn probe(
    root: &Path,
) -> Result<NativeStorageCapabilities, NativeStorageCapabilityError> {
    let directory = File::open(root)
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: the directory descriptor remains live and fstatfs fills the
    // exclusively borrowed output structure before success is returned.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
        return Err(NativeStorageCapabilityError::Platform(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: fstatfs succeeded and initialized every field.
    let stats = unsafe { stats.assume_init() };
    // SAFETY: Darwin defines f_fstypename as a NUL-terminated fixed array.
    let filesystem = unsafe { CStr::from_ptr(stats.f_fstypename.as_ptr()) }
        .to_str()
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?
        .to_owned();
    let block_bytes = stats.f_bsize;
    if filesystem.is_empty() || block_bytes == 0 {
        return Err(NativeStorageCapabilityError::InvalidGeometry);
    }
    let mut requested = libc::attrlist {
        bitmapcount: libc::ATTR_BIT_MAP_COUNT,
        reserved: 0,
        commonattr: 0,
        volattr: libc::ATTR_VOL_INFO | libc::ATTR_VOL_CAPABILITIES,
        dirattr: 0,
        fileattr: 0,
        forkattr: 0,
    };
    let mut reported = VolumeCapabilities {
        length: 0,
        values: libc::vol_capabilities_attr_t {
            capabilities: [0; 4],
            valid: [0; 4],
        },
    };
    // SAFETY: the live directory descriptor identifies the volume, and both
    // repr(C) structures remain exclusively borrowed for the synchronous call.
    if unsafe {
        libc::fgetattrlist(
            directory.as_raw_fd(),
            (&raw mut requested).cast(),
            (&raw mut reported).cast(),
            std::mem::size_of::<VolumeCapabilities>(),
            0,
        )
    } != 0
    {
        return Err(NativeStorageCapabilityError::Platform(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    if usize::try_from(reported.length).ok() != Some(std::mem::size_of::<VolumeCapabilities>()) {
        return Err(NativeStorageCapabilityError::InvalidGeometry);
    }
    let sparse_bit = libc::VOL_CAP_FMT_SPARSE_FILES;
    let clone_bit = libc::VOL_CAP_INT_CLONE;
    let sparse_files = reported.values.valid[0] & sparse_bit != 0
        && reported.values.capabilities[0] & sparse_bit != 0;
    let block_cloning = reported.values.valid[1] & clone_bit != 0
        && reported.values.capabilities[1] & clone_bit != 0;
    // f_bsize is the reported volume allocation block. It provides the
    // conservative offset granularity where raw-device geometry is hidden.
    Ok(NativeStorageCapabilities {
        filesystem,
        logical_bytes_per_sector: block_bytes,
        physical_bytes_per_sector_for_atomicity: block_bytes,
        physical_bytes_per_sector_for_performance: block_bytes,
        effective_physical_bytes_per_sector_for_atomicity: block_bytes,
        sectors_per_allocation_unit: 1,
        allocation_unit_bytes: u64::from(block_bytes),
        sparse_files,
        block_cloning,
    })
}
