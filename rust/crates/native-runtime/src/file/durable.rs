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
pub fn write_file_all(writer: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    #[cfg(not(feature = "test-support"))]
    return writer.write_all(bytes);
    #[cfg(feature = "test-support")]
    {
        let mut bytes = bytes;
        while !bytes.is_empty() {
            match write_file(writer, bytes) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(count) => bytes = bytes.get(count..).ok_or(io::ErrorKind::InvalidData)?,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}

/// Retains the caller's original data-only flush policy.
pub fn flush_file_data(file: &File) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:sync_data", || file.sync_data());
    #[cfg(not(feature = "test-support"))]
    file.sync_data()
}

/// Retains the caller's original data-and-metadata flush policy.
pub fn flush_file_all(file: &File) -> io::Result<()> {
    #[cfg(feature = "test-support")]
    return testing::durable_operation("file:sync_all", || file.sync_all());
    #[cfg(not(feature = "test-support"))]
    file.sync_all()
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
    return testing::durable_operation("file:truncate", || file.set_len(length));
    #[cfg(not(feature = "test-support"))]
    file.set_len(length)
}
