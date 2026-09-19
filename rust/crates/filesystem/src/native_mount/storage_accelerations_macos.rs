//! Bounded live APFS sparse and copy-on-write clone probes.

use super::{
    NativeBlockCloneAccelerationEvidence, NativeSparseAccelerationEvidence,
    NativeStorageAccelerationError, NativeStorageAccelerationEvidence,
};
use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

struct ProbeDirectory(PathBuf);

impl ProbeDirectory {
    fn create(root: &Path) -> Result<Self, NativeStorageAccelerationError> {
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

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    fn remove(self) -> Result<(), NativeStorageAccelerationError> {
        for name in ["sparse.bin", "clone-source.bin", "clone-target.bin"] {
            let path = self.path(name);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }
        std::fs::remove_dir(&self.0)?;
        std::mem::forget(self);
        Ok(())
    }
}

impl Drop for ProbeDirectory {
    fn drop(&mut self) {
        for name in ["sparse.bin", "clone-source.bin", "clone-target.bin"] {
            let _ = std::fs::remove_file(self.path(name));
        }
        let _ = std::fs::remove_dir(&self.0);
    }
}

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

fn sparse_probe(
    directory: &ProbeDirectory,
    logical_bytes: u64,
) -> Result<NativeSparseAccelerationEvidence, NativeStorageAccelerationError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(directory.path("sparse.bin"))?;
    file.set_len(logical_bytes)?;
    let edge = [0xA5_u8; 4_096];
    file.write_all(&edge)?;
    file.seek(SeekFrom::Start(logical_bytes - edge.len() as u64))?;
    file.write_all(&edge)?;
    // APFS can allocate a zero tail after ftruncate and a later write.
    crate::native_host::punch_hole(
        &file,
        edge.len() as u64,
        logical_bytes - 2 * edge.len() as u64,
    )?;
    file.sync_all()?;
    let allocated_bytes = file.metadata()?.blocks().checked_mul(512).ok_or(
        NativeStorageAccelerationError::Invalid("allocated byte count overflow"),
    )?;
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
            "sparse hole did not read as zero bytes",
        ));
    }
    Ok(NativeSparseAccelerationEvidence::Available {
        logical_bytes,
        allocated_bytes,
    })
}

#[allow(unsafe_code)]
fn block_clone_probe(
    directory: &ProbeDirectory,
    cloned_bytes: u64,
) -> Result<NativeBlockCloneAccelerationEvidence, NativeStorageAccelerationError> {
    let source_path = directory.path("clone-source.bin");
    let target_path = directory.path("clone-target.bin");
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
    let sparse = sparse_probe(&directory, bytes)?;
    let block_clone = block_clone_probe(&directory, bytes / 16)?;
    directory.remove()?;
    Ok(NativeStorageAccelerationEvidence {
        sparse,
        block_clone,
    })
}

#[cfg(test)]
mod tests {
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
