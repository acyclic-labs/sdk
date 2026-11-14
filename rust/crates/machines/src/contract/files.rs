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
        let bytes = path.as_bytes();
        let absolute = path.starts_with('/')
            || path.starts_with("\\\\")
            || matches!(bytes, [drive, b':', b'/' | b'\\', ..] if drive.is_ascii_alphabetic());
        if !absolute || path.contains('\0') || deadline_unix_ms == 0 {
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

/// A native file read completes with exact bytes (empty at EOF) or its OS-native error code.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileRead {
    Data(Vec<u8>),
    Errno(NonZeroU32),
}

/// A lazy whole-file transfer. Each poll admits one transport chunk; dropping
/// the stream cancels the unread suffix. The original file deadline covers it.
pub type FileStream =
    acyclic_stream::BoxProviderStream<'static, Result<bytes::Bytes, ProviderError>>;

/// A whole-file provider retains either its byte stream or its native errno.
pub enum FileDownload {
    Data(FileStream),
    Errno(NonZeroU32),
}

impl fmt::Debug for FileDownload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Data(_) => formatter.write_str("Data(<file stream>)"),
            Self::Errno(errno) => formatter.debug_tuple("Errno").field(errno).finish(),
        }
    }
}

impl FileDownload {
    /// Explicitly materializes a whole file for whole-file consumers such as
    /// replacement writes. Range readers consume the stream directly instead.
    pub async fn collect(self) -> Result<FileRead, ProviderError> {
        match self {
            Self::Data(mut stream) => {
                let mut bytes = Vec::new();
                while let Some(chunk) = stream.next().await {
                    bytes.extend_from_slice(&chunk?);
                }
                Ok(FileRead::Data(bytes))
            }
            Self::Errno(errno) => Ok(FileRead::Errno(errno)),
        }
    }
}

/// A native file write may be short. Lost completion never permits redispatch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileWrite {
    Written(u32),
    Errno(NonZeroU32),
}
