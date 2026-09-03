//! Live native sparse-allocation and block-clone capability execution.

use crate::{NativeStorageCapabilityError, probe_native_storage_capabilities};
use std::path::Path;
use thiserror::Error;

/// Exact result of executing the native sparse-allocation operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeSparseAccelerationEvidence {
    /// Sparse allocation executed and retained an observable physical hole.
    Available {
        /// Logical bytes retained by the probe file.
        logical_bytes: u64,
        /// Physically allocated bytes retained by the probe file.
        allocated_bytes: u64,
    },
    /// The operating system explicitly rejected sparse allocation as unsupported.
    Unavailable {
        /// Exact failing HRESULT represented as its signed 32-bit value.
        platform_error_code: i32,
    },
}

/// Exact result of executing native same-volume block-reference cloning.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeBlockCloneAccelerationEvidence {
    /// Block cloning executed and retained copy-on-write isolation.
    Available {
        /// Bytes cloned by the native operation.
        cloned_bytes: u64,
        /// Whether the clone retained its original bytes after source mutation.
        copy_on_write_isolation: bool,
    },
    /// The operating system explicitly rejected block cloning as unsupported.
    Unavailable {
        /// Exact failing HRESULT represented as its signed 32-bit value.
        platform_error_code: i32,
    },
}

/// Exact evidence from independent live sparse and block-clone probes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeStorageAccelerationEvidence {
    /// Sparse-allocation execution result.
    pub sparse: NativeSparseAccelerationEvidence,
    /// Same-volume block-clone execution result.
    pub block_clone: NativeBlockCloneAccelerationEvidence,
}

/// A live native storage acceleration probe failed or was unavailable.
#[derive(Debug, Error)]
pub enum NativeStorageAccelerationError {
    /// Static capability discovery failed before mutation began.
    #[error(transparent)]
    Capability(#[from] NativeStorageCapabilityError),
    /// This target has no implemented live acceleration probe.
    #[error("native storage acceleration probing is unavailable on this target")]
    UnsupportedTarget,
    /// Probe fixture creation, synchronization, or cleanup failed.
    #[error("native storage acceleration I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// The operating system rejected a native sparse/clone operation.
    #[error("native storage acceleration probe failed: {0}")]
    Platform(String),
    /// Native success did not preserve the required sparse or COW semantics.
    #[error("native storage acceleration evidence is invalid: {0}")]
    Invalid(&'static str),
}

/// Independently executes sparse allocation and same-volume block cloning
/// below `root`.
/// Temporary fixtures are removed before success returns.
///
/// # Errors
///
/// Returns exact unavailable outcomes only for operating-system errors that
/// unambiguously mean unsupported. Rejects unsupported targets, every other
/// native API failure, byte or allocation divergence, COW aliasing,
/// synchronization failure, or cleanup failure.
pub fn probe_native_storage_accelerations(
    root: &Path,
) -> Result<NativeStorageAccelerationEvidence, NativeStorageAccelerationError> {
    let capabilities = probe_native_storage_capabilities(root)?;
    probe_platform(root, capabilities.allocation_unit_bytes)
}

#[cfg(windows)]
struct ProbeDirectory(std::path::PathBuf);

#[cfg(windows)]
impl ProbeDirectory {
    fn create(root: &Path) -> Result<Self, std::io::Error> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(std::io::Error::other)?
            .as_nanos();
        let path = root.join(format!(
            ".acyclic-fs-storage-acceleration-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn remove(self) -> Result<(), std::io::Error> {
        std::fs::remove_dir_all(&self.0)?;
        std::mem::forget(self);
        Ok(())
    }
}

#[cfg(windows)]
impl Drop for ProbeDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn file_allocation_bytes(file: &std::fs::File) -> Result<u64, NativeStorageAccelerationError> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{
        FILE_STANDARD_INFO, FileStandardInfo, GetFileInformationByHandleEx,
    };

    let mut information = FILE_STANDARD_INFO::default();
    // SAFETY: the handle is borrowed from a live file and the output pointer
    // names one correctly sized, exclusively borrowed structure.
    unsafe {
        GetFileInformationByHandleEx(
            HANDLE(file.as_raw_handle()),
            FileStandardInfo,
            (&raw mut information).cast(),
            u32::try_from(size_of::<FILE_STANDARD_INFO>())
                .map_err(|_| NativeStorageAccelerationError::Invalid("file info size overflow"))?,
        )
        .map_err(|error| NativeStorageAccelerationError::Platform(error.to_string()))?;
    }
    u64::try_from(information.AllocationSize)
        .map_err(|_| NativeStorageAccelerationError::Invalid("negative allocation size"))
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn mark_sparse(file: &std::fs::File) -> windows::core::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    // SAFETY: the handle is borrowed from a live file. This control accepts no
    // input or output buffer and is synchronous because no OVERLAPPED exists.
    unsafe {
        DeviceIoControl(
            HANDLE(file.as_raw_handle()),
            FSCTL_SET_SPARSE,
            None,
            0,
            None,
            0,
            None,
            None,
        )
    }
}

#[cfg(windows)]
fn unavailable_platform_code(error: &windows::core::Error) -> Option<i32> {
    use windows::Win32::Foundation::{ERROR_INVALID_FUNCTION, ERROR_NOT_SUPPORTED};
    use windows::core::HRESULT;

    let code = error.code();
    if code == HRESULT::from_win32(ERROR_INVALID_FUNCTION.0)
        || code == HRESULT::from_win32(ERROR_NOT_SUPPORTED.0)
    {
        Some(code.0)
    } else {
        None
    }
}

#[cfg(windows)]
fn unexpected_platform_error(
    operation: &str,
    error: &windows::core::Error,
) -> NativeStorageAccelerationError {
    NativeStorageAccelerationError::Platform(format!(
        "{operation} returned HRESULT {}: {error}",
        error.code().0
    ))
}

#[cfg(windows)]
fn sparse_probe(
    directory: &Path,
    allocation_unit_bytes: u64,
) -> Result<NativeSparseAccelerationEvidence, NativeStorageAccelerationError> {
    use std::io::{Read, Seek, SeekFrom, Write};

    let logical_bytes = allocation_unit_bytes
        .checked_mul(256)
        .ok_or(NativeStorageAccelerationError::Invalid(
            "sparse logical length overflow",
        ))?
        .max(1024 * 1024);
    let path = directory.join("sparse.bin");
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)?;
    if let Err(error) = mark_sparse(&file) {
        return unavailable_platform_code(&error).map_or_else(
            || Err(unexpected_platform_error("sparse allocation", &error)),
            |platform_error_code| {
                Ok(NativeSparseAccelerationEvidence::Unavailable {
                    platform_error_code,
                })
            },
        );
    }
    file.set_len(logical_bytes)?;
    let edge = [0xA5_u8; 4_096];
    file.write_all(&edge)?;
    file.seek(SeekFrom::Start(logical_bytes - 4_096))?;
    file.write_all(&edge)?;
    file.sync_all()?;
    let allocated_bytes = file_allocation_bytes(&file)?;
    if allocated_bytes == 0 || allocated_bytes >= logical_bytes {
        return Err(NativeStorageAccelerationError::Invalid(
            "sparse file did not retain a physical hole",
        ));
    }
    file.seek(SeekFrom::Start(logical_bytes / 2))?;
    let mut hole = [1_u8; 4_096];
    file.read_exact(&mut hole)?;
    if hole.iter().any(|byte| *byte != 0) {
        return Err(NativeStorageAccelerationError::Invalid(
            "sparse hole did not read as logical zero bytes",
        ));
    }
    Ok(NativeSparseAccelerationEvidence::Available {
        logical_bytes,
        allocated_bytes,
    })
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn block_clone_probe(
    directory: &Path,
    allocation_unit_bytes: u64,
) -> Result<NativeBlockCloneAccelerationEvidence, NativeStorageAccelerationError> {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::{DUPLICATE_EXTENTS_DATA, FSCTL_DUPLICATE_EXTENTS_TO_FILE};

    let clone_bytes = allocation_unit_bytes
        .checked_mul(256)
        .ok_or(NativeStorageAccelerationError::Invalid(
            "block clone byte count overflow",
        ))?
        .max(1024 * 1024);
    let length = usize::try_from(clone_bytes).map_err(|_| {
        NativeStorageAccelerationError::Invalid("block clone byte count is not addressable")
    })?;
    let mut expected = Vec::new();
    expected.try_reserve_exact(length).map_err(|_| {
        NativeStorageAccelerationError::Invalid("block clone source allocation failed")
    })?;
    expected.extend((0..clone_bytes).map(|offset| (offset.wrapping_mul(131) % 251) as u8));
    let source_path = directory.join("clone-source.bin");
    let target_path = directory.join("clone-target.bin");
    let mut source = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(source_path)?;
    source.write_all(&expected)?;
    source.sync_all()?;
    let mut target = std::fs::OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(target_path)?;
    target.set_len(clone_bytes)?;
    target.sync_all()?;
    let duplicate = DUPLICATE_EXTENTS_DATA {
        FileHandle: HANDLE(source.as_raw_handle()),
        SourceFileOffset: 0,
        TargetFileOffset: 0,
        ByteCount: i64::try_from(clone_bytes).map_err(|_| {
            NativeStorageAccelerationError::Invalid("block clone byte count exceeds i64")
        })?,
    };
    // SAFETY: both handles remain live; the input pointer names one complete
    // immutable structure and the synchronous control returns no output bytes.
    let clone_result = unsafe {
        DeviceIoControl(
            HANDLE(target.as_raw_handle()),
            FSCTL_DUPLICATE_EXTENTS_TO_FILE,
            Some((&raw const duplicate).cast()),
            u32::try_from(size_of::<DUPLICATE_EXTENTS_DATA>()).map_err(|_| {
                NativeStorageAccelerationError::Invalid("block clone control size overflow")
            })?,
            None,
            0,
            None,
            None,
        )
    };
    if let Err(error) = clone_result {
        return unavailable_platform_code(&error).map_or_else(
            || Err(unexpected_platform_error("block cloning", &error)),
            |platform_error_code| {
                Ok(NativeBlockCloneAccelerationEvidence::Unavailable {
                    platform_error_code,
                })
            },
        );
    }
    target.sync_all()?;
    let mut observed = Vec::new();
    observed.try_reserve_exact(length).map_err(|_| {
        NativeStorageAccelerationError::Invalid("block clone result allocation failed")
    })?;
    target.seek(SeekFrom::Start(0))?;
    target.read_to_end(&mut observed)?;
    if observed != expected {
        return Err(NativeStorageAccelerationError::Invalid(
            "block clone bytes diverged",
        ));
    }
    source.seek(SeekFrom::Start(0))?;
    source.write_all(&[0x5A_u8; 4_096])?;
    source.sync_all()?;
    target.seek(SeekFrom::Start(0))?;
    let mut isolated = [0_u8; 4_096];
    target.read_exact(&mut isolated)?;
    if isolated.as_slice() != &expected[..4_096] {
        return Err(NativeStorageAccelerationError::Invalid(
            "block clone did not preserve copy-on-write isolation",
        ));
    }
    Ok(NativeBlockCloneAccelerationEvidence::Available {
        cloned_bytes: clone_bytes,
        copy_on_write_isolation: true,
    })
}

#[cfg(windows)]
fn probe_platform(
    root: &Path,
    allocation_unit_bytes: u64,
) -> Result<NativeStorageAccelerationEvidence, NativeStorageAccelerationError> {
    let directory = ProbeDirectory::create(root)?;
    let sparse = sparse_probe(directory.path(), allocation_unit_bytes)?;
    let block_clone = block_clone_probe(directory.path(), allocation_unit_bytes)?;
    directory.remove()?;
    Ok(NativeStorageAccelerationEvidence {
        sparse,
        block_clone,
    })
}

#[cfg(not(windows))]
fn probe_platform(
    _root: &Path,
    _allocation_unit_bytes: u64,
) -> Result<NativeStorageAccelerationEvidence, NativeStorageAccelerationError> {
    Err(NativeStorageAccelerationError::UnsupportedTarget)
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[test]
    fn windows_accelerations_execute_or_return_exact_unsupported_evidence()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let evidence = probe_native_storage_accelerations(directory.path())?;
        match evidence.sparse {
            NativeSparseAccelerationEvidence::Available {
                logical_bytes,
                allocated_bytes,
            } => assert!(allocated_bytes < logical_bytes),
            NativeSparseAccelerationEvidence::Unavailable {
                platform_error_code,
            } => assert_ne!(platform_error_code, 0),
        }
        match evidence.block_clone {
            NativeBlockCloneAccelerationEvidence::Available {
                cloned_bytes,
                copy_on_write_isolation,
            } => {
                assert!(cloned_bytes > 0);
                assert!(copy_on_write_isolation);
            }
            NativeBlockCloneAccelerationEvidence::Unavailable {
                platform_error_code,
            } => assert_ne!(platform_error_code, 0),
        }
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn only_unambiguous_unsupported_hresult_values_are_classified_unavailable() {
        use windows::Win32::Foundation::{
            ERROR_INVALID_FUNCTION, ERROR_INVALID_PARAMETER, ERROR_NOT_SUPPORTED,
        };
        use windows::core::{Error, HRESULT};

        let invalid_function = Error::from_hresult(HRESULT::from_win32(ERROR_INVALID_FUNCTION.0));
        let not_supported = Error::from_hresult(HRESULT::from_win32(ERROR_NOT_SUPPORTED.0));
        let invalid_parameter = Error::from_hresult(HRESULT::from_win32(ERROR_INVALID_PARAMETER.0));
        assert_eq!(
            unavailable_platform_code(&invalid_function),
            Some(invalid_function.code().0)
        );
        assert_eq!(
            unavailable_platform_code(&not_supported),
            Some(not_supported.code().0)
        );
        assert_eq!(unavailable_platform_code(&invalid_parameter), None);
    }
}
