//! Bounded live APFS sparse and copy-on-write clone probes.

use super::{
    NativeBlockCloneAccelerationEvidence, NativeStorageAccelerationError,
    NativeStorageAccelerationEvidence, probe_directory::ProbeDirectory, sparse_probe,
};
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

fn probe_bytes(allocation_unit_bytes: u64) -> Result<u64, NativeStorageAccelerationError> {
    let bytes = allocation_unit_bytes
        .checked_mul(256)
        .ok_or(NativeStorageAccelerationError::Invalid(
            "probe byte count overflow",
        ))?
        .max(1024 * 1024);
    if bytes > 16 * 1024 * 1024 {
        return Err(NativeStorageAccelerationError::Invalid(
            "allocation unit exceeds bounded probe size",
        ));
    }
    Ok(bytes)
}

#[allow(unsafe_code, reason = "probes copy-on-write cloning with clonefile")]
fn block_clone_probe(
    directory: &ProbeDirectory,
    cloned_bytes: u64,
) -> Result<NativeBlockCloneAccelerationEvidence, NativeStorageAccelerationError> {
    let source_path = directory.path().join("clone-source.bin");
    let target_path = directory.path().join("clone-target.bin");
    let mut source = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&source_path)?;
    let length = usize::try_from(cloned_bytes).map_err(|_| {
        NativeStorageAccelerationError::Invalid("clone byte count is not addressable")
    })?;
    #[allow(clippy::cast_possible_truncation, reason = "modulo 251 fits in u8")]
    let expected: Vec<u8> = (0..length)
        .map(|offset| (offset.wrapping_mul(131) % 251) as u8)
        .collect();
    source.write_all(&expected)?;
    source.sync_all()?;
    let source_name = CString::new(source_path.as_os_str().as_bytes())
        .map_err(|_| NativeStorageAccelerationError::Invalid("source path contains NUL"))?;
    let target_name = CString::new(target_path.as_os_str().as_bytes())
        .map_err(|_| NativeStorageAccelerationError::Invalid("target path contains NUL"))?;
    // SAFETY: both NUL-terminated paths and the source file remain live for
    // this synchronous call; clonefile creates the target on the same volume.
    if unsafe { libc::clonefile(source_name.as_ptr(), target_name.as_ptr(), 0) } != 0 {
        let error = std::io::Error::last_os_error();
        return if matches!(error.raw_os_error(), Some(libc::ENOTSUP | libc::ENOSYS)) {
            Ok(NativeBlockCloneAccelerationEvidence::Unavailable {
                platform_error_code: error.raw_os_error().unwrap_or_default(),
            })
        } else {
            Err(NativeStorageAccelerationError::Platform(format!(
                "clonefile: {error}"
            )))
        };
    }
    let mut target = File::open(&target_path)?;
    if target.metadata()?.len() != cloned_bytes {
        return Err(NativeStorageAccelerationError::Invalid(
            "clone length diverged",
        ));
    }
    let mut observed = vec![0_u8; length];
    target.read_exact(&mut observed)?;
    if observed != expected {
        return Err(NativeStorageAccelerationError::Invalid(
            "clone bytes diverged",
        ));
    }
    source.seek(SeekFrom::Start(0))?;
    source.write_all(&[0x5A_u8; 4_096])?;
    source.sync_all()?;
    target.seek(SeekFrom::Start(0))?;
    let mut isolated = [0_u8; 4_096];
    target.read_exact(&mut isolated)?;
    if !expected.starts_with(&isolated) {
        return Err(NativeStorageAccelerationError::Invalid(
            "clone did not retain copy-on-write isolation",
        ));
    }
    Ok(NativeBlockCloneAccelerationEvidence::Available {
        cloned_bytes,
        copy_on_write_isolation: true,
    })
}

pub(super) fn probe(
    root: &Path,
    allocation_unit_bytes: u64,
) -> Result<NativeStorageAccelerationEvidence, NativeStorageAccelerationError> {
    let bytes = probe_bytes(allocation_unit_bytes)?;
    let directory = ProbeDirectory::create(root)?;
    let sparse = sparse_probe(directory.path(), bytes)?;
    let block_clone = block_clone_probe(&directory, bytes / 16)?;
    directory.remove()?;
    Ok(NativeStorageAccelerationEvidence {
        sparse,
        block_clone,
    })
}

#[cfg(test)]
mod tests {
    use super::super::NativeSparseAccelerationEvidence;
    use super::*;

    #[test]
    fn apfs_accelerations_preserve_sparse_and_clone_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let capabilities = crate::probe_native_storage_capabilities(directory.path())?;
        if capabilities.filesystem != "apfs" {
            return Ok(());
        }
        assert!(capabilities.sparse_files);
        assert!(capabilities.block_cloning);
        let evidence = crate::probe_native_storage_accelerations(directory.path())?;
        assert!(matches!(
            evidence.sparse,
            NativeSparseAccelerationEvidence::Available { .. }
        ));
        assert!(matches!(
            evidence.block_clone,
            NativeBlockCloneAccelerationEvidence::Available {
                copy_on_write_isolation: true,
                ..
            }
        ));
        Ok(())
    }
}
