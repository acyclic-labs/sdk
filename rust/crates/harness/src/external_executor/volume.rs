use super::ExternalRequest;
use crate::{
    JsonBounds,
    task_volume::{Component, Error, FileKey, FileValue, Result, TaskFile},
};

// Writes and reads share the bounded JSON decoder's structural ceiling.
const BOUNDS: JsonBounds = JsonBounds {
    maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
    maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64 + 1,
    maximum_depth: 256,
};

pub(crate) fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    crate::json_bytes_bounded(value, BOUNDS, None, &mut |_| Ok(()))
        .map_err(|_| Error::LimitExceeded)
}

pub(crate) fn decode<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    crate::json_from_slice_bounded(
        std::marker::PhantomData::<T>,
        bytes,
        BOUNDS,
        bytes.len() as u64,
        None,
        None,
        &mut |_| Ok(()),
    )
    .map_err(|_| Error::InvalidSegment)
}

impl ExternalRequest {
    /// Encodes the immutable request file for ordinary volume publication.
    pub fn to_file(&self, task: Component) -> Result<FileValue> {
        if !self.valid() {
            return Err(Error::InvalidSegment);
        }
        Ok(FileValue::new(self.request_key(task)?, encode(self)?))
    }

    /// Immutable request-file identity, without encoding or copying its payload.
    pub fn request_key(&self, task: Component) -> Result<FileKey> {
        let mut name = [0; 36];
        let operation = uuid::Uuid::from_bytes(self.operation_id.into_bytes());
        let request = Component::new(operation.hyphenated().encode_lower(&mut name))?;
        Ok(FileKey::Task {
            task,
            file: TaskFile::Request(request),
        })
    }

    /// Decodes a request belonging to this task, checking both file and hook identities.
    pub fn from_file(expected_task: &Component, file: &FileValue) -> Result<Self> {
        let FileKey::Task {
            task,
            file: TaskFile::Request(request),
        } = &file.key
        else {
            return Err(Error::InvalidSegment);
        };
        if task != expected_task || file.channel.is_some() {
            return Err(Error::InvalidSegment);
        }
        let decoded: Self = decode(&file.bytes)?;
        let mut name = [0; 36];
        let operation = uuid::Uuid::from_bytes(decoded.operation_id.into_bytes());
        if request.as_str() != operation.hyphenated().encode_lower(&mut name) || !decoded.valid() {
            return Err(Error::InvalidSegment);
        }
        Ok(decoded)
    }
}
