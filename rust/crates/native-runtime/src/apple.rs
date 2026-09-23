//! Positional I/O for arbitrary Apple file handles.
//!
//! These functions run on the runtime's bounded I/O workers, never on the
//! service executor. Dispatch I/O is not valid for an arbitrary `File` here:
//! its random-channel offsets are relative to the descriptor's cursor, and
//! `/dev/fd/N` aliases that cursor rather than creating an independent one.

use crate::{OwnedRead, OwnedWrite};
use bytes::Bytes;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt as _;
use std::os::unix::fs::MetadataExt as _;

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(super) struct FileIdentity {
    device: u64,
    inode: u64,
}

pub(super) fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let metadata = file.metadata()?;
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

pub(super) fn file_identity_capacity() -> usize {
    let limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
    usize::try_from(limit.current.unwrap_or(u64::MAX))
        .unwrap_or(usize::MAX)
        .saturating_div(4)
        .clamp(1, 4096)
}

fn validate_range(offset: u64, length: usize) -> io::Result<()> {
    let length = u64::try_from(length)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "I/O length is too large"))?;
    let end = offset
        .checked_add(length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "I/O range overflow"))?;
    i64::try_from(end)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "I/O range is too large"))?;
    Ok(())
}

pub(super) fn read_batch(file: &File, reads: &[OwnedRead]) -> io::Result<Vec<Bytes>> {
    for read in reads {
        validate_range(read.offset, read.length)?;
    }
    let mut results = Vec::new();
    results.try_reserve_exact(reads.len())?;
    for read in reads {
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(read.length)?;
        bytes.resize(read.length, 0);
        let count = file.read_at(&mut bytes, read.offset)?;
        bytes.truncate(count);
        results.push(Bytes::from(bytes));
    }
    Ok(results)
}

#[allow(clippy::needless_pass_by_value)]
pub(super) fn write_all_batch_owned(file: &File, writes: Vec<OwnedWrite>) -> io::Result<()> {
    // Reject an invalid batch before it can partially change the file.
    for write in &writes {
        validate_range(write.offset, write.bytes.len())?;
    }
    for write in writes {
        file.write_all_at(&write.bytes, write.offset)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;

    #[test]
    fn append_handle_preserves_positional_write_semantics() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("append-positional");
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .append(true)
            .open(&path)?;
        write_all_batch_owned(
            &file,
            vec![OwnedWrite {
                offset: 0,
                bytes: Bytes::from_static(b"AB"),
            }],
        )?;
        write_all_batch_owned(
            &file,
            vec![OwnedWrite {
                offset: 0,
                bytes: Bytes::from_static(b"Z"),
            }],
        )?;
        assert_eq!(std::fs::read(path)?, b"ZB");
        Ok(())
    }
}
