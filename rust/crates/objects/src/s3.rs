//! S3-compatible medium: one physical object per key under a private prefix.
//!
//! Requires conditional create (`If-None-Match: *`), read-after-write
//! consistency, ranged GETs and lexical listing with a start-after offset.
use crate::{Medium, MediumError};
use acyclic_stream::OwnedBytes;
use futures::TryStreamExt as _;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt as _, PutMode, path::Path};
use std::ops::Range;
use std::sync::Arc;

/// An S3-compatible bucket prefix holding every account's blobs.
#[derive(Clone)]
pub struct S3Medium {
    store: Arc<dyn ObjectStore>,
    prefix: String,
}

impl S3Medium {
    /// Uses `store` exclusively below `prefix`.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: &str) -> Self {
        let prefix = prefix.trim_matches('/');
        Self {
            store,
            prefix: if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            },
        }
    }

    fn path(&self, key: &str) -> Path {
        Path::from(format!("{}{key}", self.prefix))
    }
}

fn failed(error: impl std::fmt::Display) -> MediumError {
    MediumError::Failed(std::io::Error::other(error.to_string()))
}

#[async_trait::async_trait]
impl Medium for S3Medium {
    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let payload = object_store::PutPayload::from(bytes::Bytes::copy_from_slice(&bytes));
        match self
            .store
            .put_opts(&self.path(key), payload, PutMode::Create.into())
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(false),
            // The request may have reached the bucket.
            Err(error) => Err(MediumError::Ambiguous(std::io::Error::other(error.to_string()))),
        }
    }

    async fn read(
        &self,
        key: &str,
        range: Option<Range<u64>>,
    ) -> Result<Option<OwnedBytes>, MediumError> {
        let options = GetOptions {
            range: range.clone().map(GetRange::Bounded),
            ..GetOptions::default()
        };
        if range.as_ref().is_some_and(Range::is_empty) {
            return match self.store.head(&self.path(key)).await {
                Ok(_) => Ok(Some(OwnedBytes::new())),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(error) => Err(failed(error)),
            };
        }
        match self.store.get_opts(&self.path(key), options).await {
            Ok(result) => Ok(Some(OwnedBytes::from(
                result.bytes().await.map_err(failed)?,
            ))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(failed(error)),
        }
    }

    async fn remove(&self, key: &str) -> Result<bool, MediumError> {
        let path = self.path(key);
        match self.store.head(&path).await {
            Ok(_) => {}
            Err(object_store::Error::NotFound { .. }) => return Ok(false),
            Err(error) => return Err(failed(error)),
        }
        match self.store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(true),
            Err(error) => Err(MediumError::Ambiguous(std::io::Error::other(error.to_string()))),
        }
    }

    async fn list(
        &self,
        prefix: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, u64)>, MediumError> {
        let base = self.path(prefix.trim_end_matches('/'));
        let offset = after.map(|after| self.path(after));
        let listing = match &offset {
            Some(offset) => self.store.list_with_offset(Some(&base), offset),
            None => self.store.list(Some(&base)),
        };
        let mut out = listing
            .map_ok(|meta| {
                let key = meta
                    .location
                    .as_ref()
                    .strip_prefix(self.prefix.as_str())
                    .unwrap_or(meta.location.as_ref())
                    .to_owned();
                (key, meta.size)
            })
            .try_collect::<Vec<_>>()
            .await
            .map_err(failed)?;
        // Some endpoints ignore the offset or return pages out of order.
        out.retain(|(key, _)| key.starts_with(prefix) && after.is_none_or(|after| key.as_str() > after));
        out.sort_unstable();
        out.truncate(limit);
        Ok(out)
    }
}
