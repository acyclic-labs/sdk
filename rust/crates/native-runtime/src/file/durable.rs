use crate::*;
use std::io::Write;

/// Writes once to an original durable writer, preserving short-write results.
pub fn write_file(writer: &mut impl Write, bytes: &[u8]) -> io::Result<usize> {
    write_file_with(bytes, |bytes| writer.write(bytes))
}

/// Runs one write together with its original completion accounting. A lost
/// acknowledgement is injected only after the physical result was recorded.
pub fn write_file_with(
    bytes: &[u8],
    write: impl FnOnce(&[u8]) -> io::Result<usize>,
) -> io::Result<usize> {
    #[cfg(feature = "test-support")]
    return testing::durable_write("file:write", bytes, write);
    #[cfg(not(feature = "test-support"))]
    write(bytes)
}

/// Writes all bytes through the same per-call durable boundary.
pub fn write_file_all(writer: &mut impl Write, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        let count = write_file(writer, bytes)?;
        if count == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }
        bytes = bytes.get(count..).ok_or(io::ErrorKind::InvalidData)?;
    }
    Ok(())
}

/// Retains the caller's original data-only flush policy.
pub fn flush_file_data(file: &File) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:sync_data", || flush_file_policy(file, true));
    #[cfg(not(feature = "test-support"))]
    flush_file_policy(file, true)
}

/// Retains the caller's original data-and-metadata flush policy.
pub fn flush_file_all(file: &File) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:sync_all", || flush_file_policy(file, false));
    #[cfg(not(feature = "test-support"))]
    flush_file_policy(file, false)
}

#[cfg(windows)]
fn flush_file_policy(file: &File, _data_only: bool) -> io::Result<()> {
    use std::os::windows::io::AsHandle as _;
    crate::sync_descriptor_once(file.as_handle())
}

#[cfg(not(windows))]
pub(super) fn flush_file_policy(file: &File, data_only: bool) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    if data_only {
        use std::os::fd::AsFd as _;
        return crate::held_sync::sync_data_descriptor_once(file.as_fd(), Durability::Full);
    }
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        use std::os::fd::AsFd as _;
        #[cfg(target_vendor = "apple")]
        let _ = data_only;
        crate::sync_descriptor_once(file.as_fd())
    }
    #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
    if data_only {
        file.sync_data()
    } else {
        file.sync_all()
    }
}

/// Renames with the original platform semantics and no additional flush.
pub fn rename_file(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:rename", || std::fs::rename(from, to));
    #[cfg(not(feature = "test-support"))]
    std::fs::rename(from, to)
}

/// Truncates the original held file without changing synchronization policy.
pub fn truncate_file(file: &File, length: u64) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:truncate", || {
        super::operations::resize_file(file, length, None)
    });
    #[cfg(not(feature = "test-support"))]
    super::operations::resize_file(file, length, None)
}

#[cfg(test)]
#[path = "durable_tests.rs"]
mod tests;
