use super::*;

/// One admitted guest-file range and its original absolute deadline.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileAccess {
    pub(crate) path: String,
    pub(crate) offset: u64,
    pub(crate) deadline_unix_ms: u64,
}

impl FileAccess {
    /// Paths address the admitted guest, never the client filesystem.
    pub fn new(path: String, offset: u64, deadline_unix_ms: u64) -> Result<Self, ProviderError> {
        if !path.starts_with('/') || path.contains('\0') || deadline_unix_ms == 0 {
            return Err(ProviderError::Invalid("guest file admission failed; recovery: supply an absolute NUL-free guest path and the original positive deadline".into()));
        }
        Ok(Self {
            path,
            offset,
            deadline_unix_ms,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn deadline_unix_ms(&self) -> u64 {
        self.deadline_unix_ms
    }

    /// Checks a single transport chunk without bounding the whole file.
    pub fn validate_range(&self, length: usize) -> Result<(), ProviderError> {
        if length > MAX_OUTPUT_READ_BYTES as usize
            || self.offset.checked_add(length as u64).is_none()
        {
            return Err(ProviderError::Invalid("guest file range admission failed; recovery: split into at most 65536-byte chunks without offset overflow".into()));
        }
        Ok(())
    }
}

/// A native file read completes with exact bytes (empty at EOF) or POSIX errno.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileRead {
    Data(Vec<u8>),
    Errno(NonZeroU32),
}

/// A native file write may be short. Lost completion never permits redispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileWrite {
    Written(u32),
    Errno(NonZeroU32),
}
