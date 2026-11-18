//! Shared multipart and request bounds for the filesystem S3 protocol.

/// The supplied S3 limits contain a zero bound or an inconsistent part size.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid filesystem S3 limits")]
pub struct InvalidS3Limits;

/// Streaming chunk sizes and optional caller policies for the S3 edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilesystemS3Limits {
    /// Largest single request body accepted before publication.
    pub maximum_request_bytes: Option<u64>,
    /// Largest response chunk returned from the canonical range reader.
    pub response_chunk_bytes: u64,
    /// Largest combined object/prefix page.
    pub maximum_list_keys: u32,
    /// Largest number of parts retained by one durable multipart upload.
    pub maximum_multipart_parts: Option<u32>,
    /// Largest logical byte count admitted for one multipart part.
    pub maximum_multipart_part_bytes: Option<u64>,
    /// Minimum size of every completed part except the final part.
    pub minimum_multipart_part_bytes: u64,
}

/// Optional caller policy for discovering active multipart roots before reclamation.
/// Absence admits the complete scan under the supplied work budget.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct S3MultipartRetentionLimits {
    /// Largest number of durable upload registry entries examined.
    pub maximum_uploads: Option<u64>,
    /// Largest number of distinct staged immutable objects retained.
    pub maximum_objects: Option<u64>,
    /// Largest cumulative canonical bytes authenticated while tracing roots.
    pub maximum_object_bytes: Option<u64>,
}

impl S3MultipartRetentionLimits {
    /// Rejects zero limits and inconsistent multipart bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured bounds cannot admit valid requests.
    pub fn validate(self) -> Result<Self, InvalidS3Limits> {
        if self.maximum_uploads == Some(0)
            || self.maximum_objects == Some(0)
            || self.maximum_object_bytes == Some(0)
        {
            return Err(InvalidS3Limits);
        }
        Ok(self)
    }
}

impl Default for FilesystemS3Limits {
    fn default() -> Self {
        Self {
            maximum_request_bytes: None,
            response_chunk_bytes: 8 * 1024 * 1024,
            maximum_list_keys: 1_000,
            maximum_multipart_parts: None,
            maximum_multipart_part_bytes: None,
            minimum_multipart_part_bytes: 5 * 1024 * 1024,
        }
    }
}

impl FilesystemS3Limits {
    /// Validates every protocol bound before a listener is created.
    /// Rejects zero limits and inconsistent multipart bounds.
    ///
    /// # Errors
    ///
    /// Returns an error when the configured bounds cannot admit valid requests.
    pub fn validate(self) -> Result<Self, InvalidS3Limits> {
        if self.maximum_request_bytes == Some(0)
            || self.response_chunk_bytes == 0
            || self.maximum_list_keys == 0
            || self.maximum_multipart_parts == Some(0)
            || self.maximum_multipart_part_bytes == Some(0)
            || self
                .maximum_multipart_part_bytes
                .is_some_and(|maximum| self.minimum_multipart_part_bytes > maximum)
        {
            return Err(InvalidS3Limits);
        }
        Ok(self)
    }
}
