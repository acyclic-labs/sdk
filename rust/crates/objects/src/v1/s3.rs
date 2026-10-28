//! S3-compatible physical transport for the shared Objects root publisher.
use super::memory::remote::publication::{Publisher, Store};
use super::{Error, wire};
use crate::body::ImmutableBodies;
use acyclic_stream::OwnedBytes;
use futures::future::BoxFuture;
use object_store::{
    GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, UpdateVersion, path::Path,
};
use std::sync::Arc;
const PART: usize = 5 * 1024 * 1024;
/// Endpoint capabilities, configured by the host rather than a vendor name.
#[derive(Clone, Copy, Debug)]
pub struct Capabilities {
    /// Atomic If-None-Match: * and If-Match conditional PUTs.
    pub conditional_put: bool,
    /// Successful PUTs are immediately visible to subsequent GETs.
    pub read_after_write: bool,
    /// Multipart uploads with at least 5 MiB nonfinal parts.
    pub multipart: bool,
    /// HTTP byte-range GET support.
    pub range_reads: bool,
}

/// Physical S3 IO used by the shared durable Objects core.
pub struct Transport {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    capabilities: Capabilities,
}

/// Logical Objects provider backed by an S3-compatible physical bucket.
pub type S3Objects = Publisher<Transport>;
fn backend(error: impl std::fmt::Display) -> Error {
    tracing::error!(%error, "S3 Objects IO failed; reopen S3Objects::open with the same physical bucket and prefix");
    wire::ErrorCode::Unavailable.into()
}
impl Publisher<Transport> {
    /// Opens an exclusive prefix; endpoint differences are explicit capabilities.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        prefix: Path,
        capabilities: Capabilities,
    ) -> Result<Self, Error> {
        if !capabilities.conditional_put || !capabilities.read_after_write {
            return Err(wire::ErrorCode::Unsupported.into());
        }
        Publisher::attach(Arc::new(Transport {
            store,
            prefix,
            capabilities,
        }))
        .await
    }
}
impl Transport {
    fn path(&self, key: &str) -> Path {
        key.split('/')
            .fold(self.prefix.clone(), |path, segment| path.join(segment))
    }

    async fn write(&self, kind: &str, node: &str, bytes: bytes::Bytes) -> Result<(), Error> {
        let path = self.path(&format!("{kind}/{node}"));
        if !self.capabilities.multipart || bytes.len() < PART {
            self.store.put(&path, bytes.into()).await.map_err(backend)?;
            return Ok(());
        }
        let mut upload = self.store.put_multipart(&path).await.map_err(backend)?;
        let result = async {
            let mut start = 0;
            while start < bytes.len() {
                let end = (start + PART).min(bytes.len());
                upload
                    .put_part(bytes.slice(start..end).into())
                    .await
                    .map_err(backend)?;
                start = end;
            }
            upload.complete().await.map_err(backend)?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = upload.abort().await;
        }
        result
    }
}

impl Store for Transport {
    type Version = UpdateVersion;
    fn head(&self) -> BoxFuture<'_, Result<(Vec<u8>, UpdateVersion), Error>> {
        Box::pin(async move {
            let result = self.store.get(&self.path("head")).await.map_err(backend)?;
            if result.meta.size > 1024 {
                return Err(backend("oversized head"));
            }
            let version = UpdateVersion {
                e_tag: result.meta.e_tag.clone(),
                version: result.meta.version.clone(),
            };
            if version.e_tag.is_none() {
                return Err(backend("conditional publication requires an ETag"));
            }
            Ok((result.bytes().await.map_err(backend)?.to_vec(), version))
        })
    }
    fn publish<'a>(
        &'a self,
        version: Option<&'a UpdateVersion>,
        bytes: Vec<u8>,
    ) -> BoxFuture<'a, Result<bool, Error>> {
        Box::pin(async move {
            let mode = version.map_or(PutMode::Create, |version| PutMode::Update(version.clone()));
            match self
                .store
                .put_opts(&self.path("head"), bytes.into(), mode.into())
                .await
            {
                Ok(_) => Ok(true),
                Err(
                    object_store::Error::Precondition { .. }
                    | object_store::Error::AlreadyExists { .. },
                ) => Ok(false),
                Err(error) => Err(backend(error)),
            }
        })
    }
    fn get<'a>(
        &'a self,
        kind: &'a str,
        id: &'a str,
        maximum: usize,
    ) -> BoxFuture<'a, Result<Vec<u8>, Error>> {
        Box::pin(async move {
            let result = self
                .store
                .get(&self.path(&format!("{kind}/{id}")))
                .await
                .map_err(backend)?;
            if result.meta.size > maximum as u64 {
                return Err(backend("oversized metadata page"));
            }
            Ok(result.bytes().await.map_err(backend)?.to_vec())
        })
    }
    fn put<'a>(
        &'a self,
        kind: &'a str,
        id: &'a str,
        bytes: bytes::Bytes,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(self.write(kind, id, bytes))
    }
}
impl ImmutableBodies for Transport {
    fn put(&self, id: [u8; 32], bytes: OwnedBytes) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(async move {
            self.write(
                "bodies",
                &hex::encode(id),
                bytes::Bytes::copy_from_slice(&bytes),
            )
            .await
        })
    }
    fn get(
        &self,
        id: [u8; 32],
        length: usize,
        start: usize,
        end: usize,
    ) -> BoxFuture<'_, Result<OwnedBytes, Error>> {
        Box::pin(async move {
            let options = GetOptions {
                range: self
                    .capabilities
                    .range_reads
                    .then_some(GetRange::Bounded(start as u64..end as u64)),
                ..Default::default()
            };
            let result = self
                .store
                .get_opts(&self.path(&format!("bodies/{}", hex::encode(id))), options)
                .await
                .map_err(backend)?;
            if result.meta.size != length as u64 {
                return Err(backend("immutable body length mismatch"));
            }
            let bytes = result.bytes().await.map_err(backend)?;
            if self.capabilities.range_reads {
                Ok(bytes.into())
            } else {
                if bytes.len() != length {
                    return Err(backend("truncated immutable body"));
                }
                Ok(bytes.slice(start..end).into())
            }
        })
    }
}
