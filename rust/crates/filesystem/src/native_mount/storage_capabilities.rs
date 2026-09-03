//! Native filesystem capability discovery for qualification and adapter admission.

use std::path::Path;
use thiserror::Error;

/// Exact immutable facts reported by the filesystem hosting one native root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NativeStorageCapabilities {
    /// Stable filesystem implementation name reported by the host.
    pub filesystem: String,
    /// Logical sector bytes used by filesystem offsets and allocation units.
    pub logical_bytes_per_sector: u32,
    /// Physical sector bytes required for atomic writes by the storage stack.
    pub physical_bytes_per_sector_for_atomicity: u32,
    /// Physical sector bytes preferred for storage-stack performance.
    pub physical_bytes_per_sector_for_performance: u32,
    /// Filesystem-adjusted physical atomicity sector used for qualification.
    pub effective_physical_bytes_per_sector_for_atomicity: u32,
    /// Logical sectors in one filesystem allocation unit.
    pub sectors_per_allocation_unit: u32,
    /// Checked product of sector size and sectors per allocation unit.
    pub allocation_unit_bytes: u64,
    /// Whether the host advertises durable sparse-file allocation semantics.
    pub sparse_files: bool,
    /// Whether the host advertises same-volume block-reference cloning.
    pub block_cloning: bool,
}

/// Native capability discovery failed before a mount or qualification run.
#[derive(Debug, Error)]
pub enum NativeStorageCapabilityError {
    /// This target has no implemented native capability probe yet.
    #[error("native storage capability discovery is unavailable on this target")]
    UnsupportedTarget,
    /// The supplied root is missing, inaccessible, or not a directory.
    #[error("native storage capability root is invalid: {0}")]
    InvalidRoot(String),
    /// The operating system rejected a volume-information request.
    #[error("native storage capability probe failed: {0}")]
    Platform(String),
    /// Reported geometry was zero or could not be represented exactly.
    #[error("native storage capability geometry is invalid")]
    InvalidGeometry,
}

/// Discovers exact non-destructive storage facts for the volume containing
/// `root`.
///
/// # Errors
///
/// Rejects a missing/non-directory root, unsupported target, platform query
/// failure, zero geometry, or allocation-unit overflow.
pub fn probe_native_storage_capabilities(
    root: &Path,
) -> Result<NativeStorageCapabilities, NativeStorageCapabilityError> {
    let metadata = std::fs::symlink_metadata(root)
        .map_err(|error| NativeStorageCapabilityError::InvalidRoot(error.to_string()))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(NativeStorageCapabilityError::InvalidRoot(
            "root must be an existing non-link directory".to_owned(),
        ));
    }
    probe_platform(root)
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn probe_platform(root: &Path) -> Result<NativeStorageCapabilities, NativeStorageCapabilityError> {
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
    let storage = probe_file_storage(root)?;
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
    let filesystem = String::from_utf16(&filesystem_name[..filesystem_length])
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

#[cfg(windows)]
#[allow(unsafe_code)]
fn probe_file_storage(
    root: &Path,
) -> Result<windows::Win32::Storage::FileSystem::FILE_STORAGE_INFO, NativeStorageCapabilityError> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use std::time::{SystemTime, UNIX_EPOCH};
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_STORAGE_INFO, FileStorageInfo, GetFileInformationByHandleEx,
    };

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?
        .as_nanos();
    let probe_path = root.join(format!(
        ".acyclic-fs-storage-capability-{}-{nonce}",
        std::process::id()
    ));
    let probe_file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&probe_path)
        .map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    let mut storage = FILE_STORAGE_INFO::default();
    // SAFETY: the handle is borrowed from the live probe file and the output
    // pointer names one correctly sized, exclusively borrowed structure.
    let storage_result = unsafe {
        GetFileInformationByHandleEx(
            HANDLE(probe_file.as_raw_handle()),
            FileStorageInfo,
            (&raw mut storage).cast(),
            u32::try_from(size_of::<FILE_STORAGE_INFO>())
                .map_err(|_| NativeStorageCapabilityError::InvalidGeometry)?,
        )
    };
    drop(probe_file);
    let remove_result = std::fs::remove_file(&probe_path);
    storage_result.map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    remove_result.map_err(|error| NativeStorageCapabilityError::Platform(error.to_string()))?;
    Ok(storage)
}

#[cfg(not(windows))]
fn probe_platform(_root: &Path) -> Result<NativeStorageCapabilities, NativeStorageCapabilityError> {
    Err(NativeStorageCapabilityError::UnsupportedTarget)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[test]
    fn windows_probe_reports_nonzero_exact_volume_geometry()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let capabilities = probe_native_storage_capabilities(directory.path())?;
        assert!(!capabilities.filesystem.is_empty());
        assert!(capabilities.logical_bytes_per_sector > 0);
        assert!(capabilities.physical_bytes_per_sector_for_atomicity > 0);
        assert!(capabilities.physical_bytes_per_sector_for_performance > 0);
        assert!(capabilities.effective_physical_bytes_per_sector_for_atomicity > 0);
        assert!(capabilities.sectors_per_allocation_unit > 0);
        assert_eq!(
            capabilities.allocation_unit_bytes,
            u64::from(capabilities.logical_bytes_per_sector)
                * u64::from(capabilities.sectors_per_allocation_unit)
        );
        Ok(())
    }

    #[test]
    fn non_directory_root_is_rejected_before_platform_discovery()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let file = directory.path().join("file");
        std::fs::write(&file, b"not-a-root")?;
        assert!(matches!(
            probe_native_storage_capabilities(&file),
            Err(NativeStorageCapabilityError::InvalidRoot(_))
        ));
        Ok(())
    }
}
