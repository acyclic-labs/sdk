//! S3-compatible medium: one physical object per key under a private prefix.
//!
//! Requires conditional create (`If-None-Match: *`), conditional overwrite
//! and delete on an observed ETag (`If-Match`), read-after-write consistency,
//! ranged GETs and lexical listing with a start-after offset.
use crate::{Medium, MediumError};
use acyclic_stream::OwnedBytes;
use futures::TryStreamExt as _;
use object_store::signer::{SignedUrlOptions, Signer};
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt as _, PutMode, UpdateVersion, path::Path,
};
use reqwest_s3::header::{HeaderValue, IF_MATCH};
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// The request types a [`Signer`] names.
pub use reqwest_s3::{Method, Url};

/// A bucket that also presigns requests: `object_store` has no conditional
/// delete, so [`S3Medium`] signs an `If-Match` DELETE itself.
pub trait Bucket: ObjectStore + Signer {}
impl<T: ObjectStore + Signer> Bucket for T {}

/// An S3-compatible bucket prefix holding every account's blobs.
#[derive(Clone)]
pub struct S3Medium {
    store: Arc<dyn Bucket>,
    prefix: String,
    http: Arc<OnceLock<reqwest_s3::Client>>,
}

impl S3Medium {
    /// Uses `store` exclusively below `prefix`.
    #[must_use]
    pub fn new(store: Arc<dyn Bucket>, prefix: &str) -> Self {
        let prefix = prefix.trim_matches('/');
        Self {
            store,
            http: Arc::default(),
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

    /// The ETag of `key` while it holds exactly `expected`.
    async fn observed(&self, path: &Path, expected: &[u8]) -> Result<Option<String>, MediumError> {
        let result = match self.store.get_opts(path, GetOptions::default()).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(failed(error)),
        };
        let e_tag = result.meta.e_tag.clone();
        let bytes = result.bytes().await.map_err(failed)?;
        if bytes.as_ref() != expected {
            return Ok(None);
        }
        e_tag
            .map(Some)
            .ok_or_else(|| failed("conditional removal needs an ETag"))
    }

    fn http(&self) -> Result<&reqwest_s3::Client, MediumError> {
        if let Some(client) = self.http.get() {
            return Ok(client);
        }
        let client = reqwest_s3::Client::builder().build().map_err(failed)?;
        Ok(self.http.get_or_init(|| client))
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
            Err(error) => Err(MediumError::Ambiguous(std::io::Error::other(
                error.to_string(),
            ))),
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
            Err(error) => Err(MediumError::Ambiguous(std::io::Error::other(
                error.to_string(),
            ))),
        }
    }

    async fn replace(
        &self,
        key: &str,
        expected: &[u8],
        bytes: OwnedBytes,
    ) -> Result<bool, MediumError> {
        let path = self.path(key);
        let Some(e_tag) = self.observed(&path, expected).await? else {
            return Ok(false);
        };
        let payload = object_store::PutPayload::from(bytes::Bytes::copy_from_slice(&bytes));
        let mode = PutMode::Update(UpdateVersion {
            e_tag: Some(e_tag),
            version: None,
        });
        match self.store.put_opts(&path, payload, mode.into()).await {
            Ok(_) => Ok(true),
            Err(
                object_store::Error::Precondition { .. } | object_store::Error::NotFound { .. },
            ) => Ok(false),
            Err(error) => Err(MediumError::Ambiguous(std::io::Error::other(
                error.to_string(),
            ))),
        }
    }

    async fn remove_if(&self, key: &str, expected: &[u8]) -> Result<bool, MediumError> {
        let path = self.path(key);
        let Some(e_tag) = self.observed(&path, expected).await? else {
            return Ok(false);
        };
        let e_tag = HeaderValue::from_str(&e_tag).map_err(failed)?;
        let url = self
            .store
            .signed_url_opts(
                Method::DELETE,
                &path,
                Duration::from_secs(300),
                &SignedUrlOptions::new().with_signed_header(IF_MATCH, e_tag.clone()),
            )
            .await
            .map_err(failed)?;
        let ambiguous = |error: &dyn std::fmt::Display| {
            MediumError::Ambiguous(std::io::Error::other(error.to_string()))
        };
        let response = self
            .http()?
            .delete(url)
            .header(IF_MATCH, e_tag)
            .send()
            .await
            .map_err(|error| ambiguous(&error))?;
        match response.status().as_u16() {
            200 | 204 => Ok(true),
            404 | 412 => Ok(false),
            status => Err(ambiguous(&format!("conditional delete answered {status}"))),
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
        let mut listing = listing.map_ok(|meta| {
            let key = meta
                .location
                .as_ref()
                .strip_prefix(self.prefix.as_str())
                .unwrap_or(meta.location.as_ref())
                .to_owned();
            (key, meta.size)
        });
        // Keep the `limit` least matches, stopping once a lexical listing
        // (required above) has them; names at or before an offset the
        // endpoint ignored are skipped. A listing seen out of order is read
        // to its end, still holding at most `limit` names.
        let mut kept = std::collections::BTreeMap::new();
        let mut previous: Option<String> = None;
        let mut ordered = true;
        while let Some((key, size)) = listing.try_next().await.map_err(failed)? {
            ordered &= previous
                .as_deref()
                .is_none_or(|previous| previous < key.as_str());
            previous = Some(key.clone());
            if !key.starts_with(prefix) || after.is_some_and(|after| key.as_str() <= after) {
                continue;
            }
            kept.insert(key, size);
            if kept.len() > limit {
                kept.pop_last();
            }
            if ordered && kept.len() == limit {
                break;
            }
        }
        Ok(kept.into_iter().collect())
    }
}
