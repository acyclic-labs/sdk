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
#[path = "storage_capabilities_windows.rs"]
mod windows_probe;
#[cfg(windows)]
use windows_probe::probe as probe_platform;

#[cfg(target_os = "macos")]
#[path = "storage_capabilities_macos.rs"]
mod macos;

#[cfg(target_os = "macos")]
fn probe_platform(root: &Path) -> Result<NativeStorageCapabilities, NativeStorageCapabilityError> {
    macos::probe(root)
}

#[cfg(target_os = "linux")]
#[path = "storage_capabilities_linux.rs"]
mod linux;

#[cfg(target_os = "linux")]
fn probe_platform(root: &Path) -> Result<NativeStorageCapabilities, NativeStorageCapabilityError> {
    linux::probe(root)
}

#[cfg(not(any(windows, target_os = "macos", target_os = "linux")))]
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
