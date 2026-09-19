//! Bounded live sparse-file and reflink checks on the destination volume.

use super::{
    NativeBlockCloneAccelerationEvidence, NativeSparseAccelerationEvidence,
    NativeStorageAccelerationError, NativeStorageAccelerationEvidence,
};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

struct ProbeDirectory(PathBuf);

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

    fn remove(self) -> Result<(), std::io::Error> {
        std::fs::remove_dir_all(&self.0)?;
        std::mem::forget(self);
        Ok(())
    }
}

impl Drop for ProbeDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) fn probe(
    root: &Path,
    allocation_unit_bytes: u64,
) -> Result<NativeStorageAccelerationEvidence, NativeStorageAccelerationError> {
    let directory = ProbeDirectory::create(root)?;
    let bytes = allocation_unit_bytes
        .checked_mul(16)
        .ok_or(NativeStorageAccelerationError::Invalid(
            "probe length overflow",
        ))?
        .max(64 * 1024);
    if bytes > 1024 * 1024 {
        return Err(NativeStorageAccelerationError::Invalid(
            "probe length exceeds 1 MiB",
        ));
    }
    let sparse = sparse_probe(&directory.0, bytes)?;
    let block_clone = block_clone_probe(&directory.0, bytes)?;
    directory.remove()?;
    Ok(NativeStorageAccelerationEvidence {
        sparse,
        block_clone,
    })
}

fn sparse_probe(
    directory: &Path,
    logical_bytes: u64,
) -> Result<NativeSparseAccelerationEvidence, NativeStorageAccelerationError> {
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(directory.join("sparse.bin"))?;
    file.set_len(logical_bytes)?;
    let edge = [0xa5_u8; 4096];
    file.write_all(&edge)?;
    file.seek(SeekFrom::Start(logical_bytes - edge.len() as u64))?;
    file.write_all(&edge)?;
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
    let mut hole = [1_u8; 4096];
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

#[allow(unsafe_code)]
fn block_clone_probe(
    directory: &Path,
    clone_bytes: u64,
) -> Result<NativeBlockCloneAccelerationEvidence, NativeStorageAccelerationError> {
    let mut source = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(directory.join("clone-source.bin"))?;
    let mut target = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(directory.join("clone-target.bin"))?;
    let mut chunk = [0_u8; 4096];
    for (index, byte) in chunk.iter_mut().enumerate() {
        *byte = u8::try_from((index * 131) % 251)
            .map_err(|_| NativeStorageAccelerationError::Invalid("probe pattern overflow"))?;
    }
    let mut remaining = clone_bytes;
    while remaining != 0 {
        let count = usize::try_from(remaining.min(chunk.len() as u64))
            .map_err(|_| NativeStorageAccelerationError::Invalid("probe chunk overflow"))?;
        let slice = chunk
            .get(..count)
            .ok_or(NativeStorageAccelerationError::Invalid(
                "probe chunk bounds",
            ))?;
        source.write_all(slice)?;
        remaining -= count as u64;
    }
    source.sync_all()?;
    // SAFETY: both descriptors are live regular files on the same volume.
    let result = unsafe { libc::ioctl(target.as_raw_fd(), libc::FICLONE as _, source.as_raw_fd()) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        let code = error.raw_os_error().unwrap_or_default();
        if matches!(
            code,
            libc::EOPNOTSUPP | libc::ENOTTY | libc::ENOSYS | libc::EXDEV
        ) {
            return Ok(NativeBlockCloneAccelerationEvidence::Unavailable {
                platform_error_code: code,
            });
        }
        return Err(NativeStorageAccelerationError::Platform(format!(
            "FICLONE returned errno {code}: {error}"
        )));
    }
    target.sync_all()?;
    let mut observed = [0_u8; 4096];
    remaining = clone_bytes;
    while remaining != 0 {
        let count = usize::try_from(remaining.min(observed.len() as u64))
            .map_err(|_| NativeStorageAccelerationError::Invalid("probe chunk overflow"))?;
        let observed_slice =
            observed
                .get_mut(..count)
                .ok_or(NativeStorageAccelerationError::Invalid(
                    "probe chunk bounds",
                ))?;
        target.read_exact(observed_slice)?;
        if observed_slice
            != chunk
                .get(..count)
                .ok_or(NativeStorageAccelerationError::Invalid(
                    "probe chunk bounds",
                ))?
        {
            return Err(NativeStorageAccelerationError::Invalid(
                "block clone bytes diverged",
            ));
        }
        remaining -= count as u64;
    }
    source.seek(SeekFrom::Start(0))?;
    source.write_all(&[0x5a_u8; 4096])?;
    source.sync_all()?;
    target.seek(SeekFrom::Start(0))?;
    target.read_exact(&mut observed)?;
    if observed != chunk {
        return Err(NativeStorageAccelerationError::Invalid(
            "block clone did not preserve copy-on-write isolation",
        ));
    }
    Ok(NativeBlockCloneAccelerationEvidence::Available {
        cloned_bytes: clone_bytes,
        copy_on_write_isolation: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_probe_bounds_work_and_cleans_up() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let evidence = match super::super::probe_native_storage_accelerations(directory.path()) {
            Ok(evidence) => evidence,
            Err(NativeStorageAccelerationError::Capability(
                crate::NativeStorageCapabilityError::UnsupportedTarget,
            )) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        assert!(matches!(
            evidence.sparse,
            NativeSparseAccelerationEvidence::Available {
                logical_bytes: 65536,
                allocated_bytes: 1..=65535,
            }
        ));
        assert!(matches!(
            evidence.block_clone,
            NativeBlockCloneAccelerationEvidence::Available { .. }
                | NativeBlockCloneAccelerationEvidence::Unavailable { .. }
        ));
        assert_eq!(std::fs::read_dir(directory.path())?.count(), 0);
        Ok(())
    }
}
