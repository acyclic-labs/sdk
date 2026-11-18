//! Win32 storage geometry for an existing native root.

use super::{NativeStorageCapabilities, NativeStorageCapabilityError};
use std::path::Path;

#[allow(unsafe_code, reason = "reads volume information through Win32 calls")]
pub(super) fn probe(
    root: &Path,
) -> Result<NativeStorageCapabilities, NativeStorageCapabilityError> {
    use std::os::windows::ffi::OsStrExt;
    use windows::Win32::Storage::FileSystem::{
        GetDiskFreeSpaceW, GetVolumeInformationW, GetVolumePathNameW,
    };
    use windows::Win32::System::SystemServices::{
        FILE_SUPPORTS_BLOCK_REFCOUNTING, FILE_SUPPORTS_SPARSE_FILES,
    };
    use windows::core::PCWSTR;

    let canonical = std::fs::canonicalize(root)
        .map_err(|error| NativeStorageCapabilityError::InvalidRoot(error.to_string()))?;
    let mut encoded = canonical.as_os_str().encode_wide().collect::<Vec<_>>();
    encoded.push(0);
    let mut volume_path = vec![0_u16; 32_768];
    // SAFETY: both buffers are live, NUL-terminated where required, and the
    // output slice has its exact writable length for the duration of the call.
    unsafe {
        GetVolumePathNameW(PCWSTR(encoded.as_ptr()), &mut volume_path)
            .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    }
    let volume_length = volume_path
        .iter()
        .position(|value| *value == 0)
        .ok_or_else(|| {
            NativeStorageCapabilityError::Platform("volume path was not NUL-terminated".to_owned())
        })?;
    volume_path.truncate(volume_length + 1);

    let mut sectors_per_allocation_unit = 0_u32;
    let mut logical_bytes_per_sector = 0_u32;
    // SAFETY: the volume path remains NUL-terminated and every supplied output
    // pointer references a live `u32` for the complete synchronous call.
    unsafe {
        GetDiskFreeSpaceW(
            PCWSTR(volume_path.as_ptr()),
            Some(&raw mut sectors_per_allocation_unit),
            Some(&raw mut logical_bytes_per_sector),
            None,
            None,
        )
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    }
    let storage = probe_root_storage(root)?;
    let mut flags = 0_u32;
    let mut filesystem_name = vec![0_u16; 256];
    // SAFETY: the input path is NUL-terminated; the output slice and flags
    // pointer are valid and exclusively borrowed for this synchronous call.
    unsafe {
        GetVolumeInformationW(
            PCWSTR(volume_path.as_ptr()),
            None,
            None,
            None,
            Some(&raw mut flags),
            Some(&mut filesystem_name),
        )
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    }
    let filesystem_length = filesystem_name
        .iter()
        .position(|value| *value == 0)
        .unwrap_or(filesystem_name.len());
    let filesystem = String::from_utf16(
        filesystem_name
            .get(..filesystem_length)
            .ok_or(NativeStorageCapabilityError::InvalidGeometry)?,
    )
    .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    let allocation_unit_bytes = u64::from(logical_bytes_per_sector)
        .checked_mul(u64::from(sectors_per_allocation_unit))
        .ok_or(NativeStorageCapabilityError::InvalidGeometry)?;
    if logical_bytes_per_sector == 0
        || sectors_per_allocation_unit == 0
        || allocation_unit_bytes == 0
        || storage.LogicalBytesPerSector != logical_bytes_per_sector
        || storage.PhysicalBytesPerSectorForAtomicity == 0
        || storage.PhysicalBytesPerSectorForPerformance == 0
        || storage.FileSystemEffectivePhysicalBytesPerSectorForAtomicity == 0
        || filesystem.is_empty()
    {
        return Err(NativeStorageCapabilityError::InvalidGeometry);
    }
    Ok(NativeStorageCapabilities {
        filesystem,
        logical_bytes_per_sector,
        physical_bytes_per_sector_for_atomicity: storage.PhysicalBytesPerSectorForAtomicity,
        physical_bytes_per_sector_for_performance: storage.PhysicalBytesPerSectorForPerformance,
        effective_physical_bytes_per_sector_for_atomicity: storage
            .FileSystemEffectivePhysicalBytesPerSectorForAtomicity,
        sectors_per_allocation_unit,
        allocation_unit_bytes,
        sparse_files: flags & FILE_SUPPORTS_SPARSE_FILES != 0,
        block_cloning: flags & FILE_SUPPORTS_BLOCK_REFCOUNTING != 0,
    })
}

#[allow(
    unsafe_code,
    reason = "reads root storage information through Win32 calls"
)]
fn probe_root_storage(
    root: &Path,
) -> Result<windows::Win32::Storage::FileSystem::FILE_STORAGE_INFO, NativeStorageCapabilityError> {
    use std::mem::size_of;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ,
        FILE_SHARE_WRITE, FILE_STORAGE_INFO, FileStorageInfo, GetFileInformationByHandleEx,
    };

    // FileStorageInfo is available on a directory handle. Querying the
    // existing root avoids a temporary file, filesystem mutation, and USN
    // journal advance on every capability probe.
    let directory = std::fs::OpenOptions::new()
        .access_mode(FILE_READ_ATTRIBUTES.0)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0)
        .open(root)
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    let mut storage = FILE_STORAGE_INFO::default();
    // SAFETY: the handle is borrowed from the live root directory and the output
    // pointer names one correctly sized, exclusively borrowed structure.
    unsafe {
        GetFileInformationByHandleEx(
            HANDLE(directory.as_raw_handle()),
            FileStorageInfo,
            (&raw mut storage).cast(),
            u32::try_from(size_of::<FILE_STORAGE_INFO>())
                .map_err(|_| NativeStorageCapabilityError::InvalidGeometry)?,
        )
    }
    .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    Ok(storage)
}
