//! S3-compatible medium: one physical object per key under a private prefix.
//!
//! Requires conditional create (`If-None-Match: *`), conditional overwrite
//! and delete on an observed ETag (`If-Match`), read-after-write consistency,
//! ranged GETs and lexical listing with a start-after offset.
use crate::{Medium, MediumError};
use acyclic_stream::OwnedBytes;
use futures::TryStreamExt as _;
use object_store::multipart::MultipartStore;
use object_store::signer::{SignedUrlOptions, Signer};
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt as _, PutMode, UpdateVersion, path::Path,
};
use reqwest_s3::header::{HeaderValue, IF_MATCH, IF_NONE_MATCH};
use std::ops::Range;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// The request types a [`Signer`] names.
pub use reqwest_s3::{Method, Url};

/// A bucket that also presigns requests: `object_store` has no conditional
/// delete, so [`S3Medium`] signs an `If-Match` DELETE itself.
pub trait Bucket: ObjectStore + Signer + MultipartStore {}
impl<T: ObjectStore + Signer + MultipartStore> Bucket for T {}

/// Deployment capabilities, independent of vendor names. Required flags fail closed.
#[derive(Clone, Copy)]
pub struct Capabilities {
    /// Atomic If-None-Match creation and If-Match replacement.
    pub conditional_writes: bool,
    /// Atomic If-Match deletion, including a signed DELETE request.
    pub conditional_delete: bool,
    /// Committed changes are immediately visible to independent clients.
    pub read_after_write: bool,
    /// GET honors half-open byte selections through HTTP ranges.
    pub range_reads: bool,
    /// Listing returns keys in ascending byte order.
    pub lexical_listing: bool,
    /// Use conditional multipart completion for bodies larger than 5 MiB.
    pub multipart: bool,
}
impl Capabilities {
    /// Assert only after qualifying these operations on the target endpoint.
    pub const REQUIRED: Self = Self {
        conditional_writes: true,
        conditional_delete: true,
        read_after_write: true,
        range_reads: true,
        lexical_listing: true,
        multipart: true,
    };
}

/// An S3-compatible bucket prefix holding every account's blobs.
#[derive(Clone)]
pub struct S3Medium {
    store: Arc<dyn Bucket>,
    prefix: String,
    http: Arc<OnceLock<reqwest_s3::Client>>,
    multipart: bool,
}

impl S3Medium {
    /// Uses `store` exclusively below `prefix`.
    pub fn new(
        store: Arc<dyn Bucket>,
        prefix: &str,
        capabilities: Capabilities,
    ) -> Result<Self, MediumError> {
        if !(capabilities.conditional_writes
            && capabilities.conditional_delete
            && capabilities.read_after_write
            && capabilities.range_reads
            && capabilities.lexical_listing)
        {
            return Err(MediumError::Failed(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "STEP s3-capabilities refused: conditional writes/delete, read-after-write, ranges and lexical listing required; recovery: qualify the endpoint and pass its supported capabilities",
            )));
        }
        let prefix = prefix.trim_matches('/');
        Ok(Self {
            store,
            http: Arc::default(),
            multipart: capabilities.multipart,
            prefix: if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            },
        })
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

    async fn multipart_create(&self, path: &Path, bytes: OwnedBytes) -> Result<bool, MediumError> {
        let id = self.store.create_multipart(path).await.map_err(failed)?;
        let result = async {
            let mut xml = String::from("<CompleteMultipartUpload>");
            let part_size = bytes.len().div_ceil(10_000).max(5 * 1024 * 1024);
            for (index, part) in bytes.chunks(part_size).enumerate() {
                let part = self
                    .store
                    .put_part(path, &id, index, bytes::Bytes::copy_from_slice(part).into())
                    .await
                    .map_err(ambiguous)?;
                xml.push_str(&format!(
                    "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag></Part>",
                    index + 1,
                    quick_xml::escape::escape(&part.content_id)
                ));
            }
            xml.push_str("</CompleteMultipartUpload>");
            let condition = HeaderValue::from_static("*");
            let options = SignedUrlOptions::new()
                .with_query([("uploadId", id.as_str())])
                .with_signed_header(IF_NONE_MATCH, condition.clone());
            let url = self
                .store
                .signed_url_opts(Method::POST, path, Duration::from_secs(300), &options)
                .await
                .map_err(failed)?;
            let response = self
                .http()?
                .post(url)
                .header(IF_NONE_MATCH, condition)
                .body(xml)
                .send()
                .await
                .map_err(|error| ambiguous(error.without_url()))?;
            match response.status().as_u16() {
                412 => return Ok(false),
                200 => {}
                status => {
                    return Err(ambiguous(format!(
                        "conditional multipart completion answered {status}"
                    )));
                }
            }
            // S3 can return an Error XML document with HTTP 200. Parse to EOF.
            let body = response.bytes().await.map_err(ambiguous)?;
            let mut reader = quick_xml::Reader::from_reader(body.as_ref());
            let mut complete = false;
            loop {
                match reader.read_event().map_err(ambiguous)? {
                    quick_xml::events::Event::Start(tag)
                        if tag.local_name().as_ref() == b"Error" =>
                    {
                        return Err(ambiguous("multipart completion returned an error document"));
                    }
                    quick_xml::events::Event::End(tag)
                        if tag.local_name().as_ref() == b"CompleteMultipartUploadResult" =>
                    {
                        complete = true
                    }
                    quick_xml::events::Event::Eof => break,
                    _ => {}
                }
            }
            if complete {
                Ok(true)
            } else {
                Err(ambiguous("invalid multipart completion document"))
            }
        }
        .await;
        if !matches!(result, Ok(true)) {
            // Abort only this upload ID; never delete the destination after an uncertain completion.
            let _ = self.store.abort_multipart(path, &id).await;
        }
        result
    }
}

fn failed(error: impl std::fmt::Display) -> MediumError {
    MediumError::Failed(std::io::Error::other(error.to_string()))
}
fn ambiguous(error: impl std::fmt::Display) -> MediumError {
    MediumError::Ambiguous(std::io::Error::other(error.to_string()))
}

#[async_trait::async_trait]
impl Medium for S3Medium {
    // An acknowledged S3 write is durable.
    async fn flush(&self, _keys: &[String]) -> Result<(), MediumError> {
        Ok(())
    }

    fn volatile(&self) -> bool {
        false
    }

    fn recovered(&self) -> bool {
        false
    }

    fn settle(&self) {}

    async fn create(&self, key: &str, bytes: OwnedBytes) -> Result<bool, MediumError> {
        if self.multipart && bytes.len() > 5 * 1024 * 1024 {
            return self.multipart_create(&self.path(key), bytes).await;
        }
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
        let response = self
            .http()?
            .delete(url)
            .header(IF_MATCH, e_tag)
            .send()
            .await
            .map_err(|error| ambiguous(error.without_url()))?;
        match response.status().as_u16() {
            200 | 204 => Ok(true),
            404 | 412 => Ok(false),
            status => Err(ambiguous(format!("conditional delete answered {status}"))),
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
        let mut kept = Vec::new();
        while kept.len() < limit {
            let Some((key, size)) = listing.try_next().await.map_err(failed)? else {
                break;
            };
            if key.starts_with(prefix) && after.is_none_or(|after| key.as_str() > after) {
                kept.push((key, size));
            }
        }
        Ok(kept)
    }
}
