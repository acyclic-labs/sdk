//! Black-box logical Objects conformance for deterministic, disposable test providers.

use super::{Error, ObjectsProvider, wire};
use acyclic_stream::testing::{Counted, RequestLedger, Requests};
use acyclic_stream::{OwnedBytes, UnixMillisClock as _};

/// Backend setup and dispatch instrumentation; assertions belong to this suite.
/// Each factory must retain fresh disposable storage for the complete invocation.
pub trait Backend: Send + Sync {
    /// The same public logical provider used by consumers.
    fn provider(&self) -> &dyn ObjectsProvider;
    /// A namespace unused by any previous invocation or concurrent writer.
    fn namespace(&self) -> &str;
    /// Calls at the backend's declared dispatch boundary, including failures.
    fn requests(&self) -> RequestLedger;
}

/// A counted concrete provider and its disposable namespace. Backends measuring
/// physical transport traffic can implement [`Backend`] using a transport ledger.
pub struct Fixture<P> {
    provider: Counted<P>,
    namespace: String,
}

impl<P> Fixture<P> {
    /// Retains this provider and namespace for one complete suite invocation.
    pub fn new(provider: P, namespace: impl Into<String>) -> Self {
        Self {
            provider: Counted::new(provider),
            namespace: namespace.into(),
        }
    }
}

impl<P: ObjectsProvider> Backend for Fixture<P> {
    fn provider(&self) -> &dyn ObjectsProvider {
        &self.provider
    }
    fn namespace(&self) -> &str {
        &self.namespace
    }
    fn requests(&self) -> RequestLedger {
        self.provider.ledger()
    }
}

impl<P: ObjectsProvider> Backend for Counted<P> {
    fn provider(&self) -> &dyn ObjectsProvider {
        self
    }
    fn namespace(&self) -> &str {
        "conformance"
    }
    fn requests(&self) -> RequestLedger {
        self.ledger()
    }
}

/// Register the complete suite with an async factory returning a fresh backend.
pub use crate::objects_conformance_suite as suite;

/// Implementation of [`suite!`].
#[macro_export]
macro_rules! objects_conformance_suite {
    ($factory:path) => {
        $crate::objects_conformance_suite!(objects_conformance, $factory);
    };
    ($name:ident, $factory:path) => {
        #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
        #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test::wasm_bindgen_test)]
        async fn $name() -> std::result::Result<(), Box<dyn std::error::Error>> {
            let backend = $factory().await?;
            $crate::v1::conformance::verify(&backend).await?;
            Ok(())
        }
    };
}

/// Run the complete logical contract and steady-state request budgets.
pub async fn verify(backend: &dyn Backend) -> Result<(), Failure> {
    verify_provider(backend.provider(), backend.namespace()).await?;
    steady(backend).await
}

/// Canonical inventory of the executable logical provider walkthrough.
pub const SUITE: &[u8] = include_bytes!("../../conformance/objects-v1.json");

/// A provider failure or a violated logical Objects invariant.
#[derive(Debug, thiserror::Error)]
pub enum Failure {
    /// The provider rejected an operation required by the fixture.
    #[error(transparent)]
    Provider(#[from] Error),
    /// A successful operation or expected failure contradicted the contract.
    #[error("Objects v1 conformance: {0}")]
    Invariant(&'static str),
    /// A steady-state data operation issued control requests or excess data calls.
    #[error("Objects {case} exceeded its request budget: {requests:?}")]
    Budget {
        /// Operation under measurement.
        case: &'static str,
        /// Calls made between the operation's snapshots.
        requests: Requests,
    },
}

fn ensure(condition: bool, message: &'static str) -> Result<(), Failure> {
    if condition {
        Ok(())
    } else {
        Err(Failure::Invariant(message))
    }
}

fn expected_error<T>(
    result: Result<T, Error>,
    code: wire::ErrorCode,
    message: &'static str,
) -> Result<(), Failure> {
    ensure(
        result.err().is_some_and(|error| error.code == code),
        message,
    )
}

fn put(bucket: &wire::BucketRef, key: &str) -> wire::PutObjectHeader {
    wire::PutObjectHeader {
        bucket: Some(bucket.clone()),
        object_key: key.into(),
        ..Default::default()
    }
}

fn get(bucket: &wire::BucketRef, key: &str) -> wire::GetObjectRequest {
    wire::GetObjectRequest {
        bucket: Some(bucket.clone()),
        object_key: key.into(),
        ..Default::default()
    }
}

fn identity(namespace: &str, operation: &str) -> Option<wire::MutationIdentity> {
    Some(wire::MutationIdentity {
        idempotency_key: format!(
            "{}-{operation}",
            blake3::hash(namespace.as_bytes()).to_hex()
        ),
    })
}

/// Exercises every public logical Objects operation in a fresh caller-owned namespace.
#[allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    reason = "one ordered provider lifecycle qualifies every public operation"
)]
async fn verify_provider<P: ObjectsProvider + ?Sized>(
    provider: &P,
    namespace: &str,
) -> Result<(), Failure> {
    let create = wire::CreateBucketRequest {
        name: namespace.into(),
        mutation: identity(namespace, "create"),
    };
    let created = provider.create_bucket(create.clone()).await?;
    let bucket = created.bucket.clone().ok_or(Failure::Invariant(
        "created bucket omitted its logical name",
    ))?;
    ensure(
        bucket.name == namespace,
        "create substituted the caller's logical bucket",
    )?;
    ensure(
        provider.create_bucket(create).await? == created,
        "bucket retry did not replay its exact response",
    )?;
    ensure(
        provider
            .head_bucket(wire::HeadBucketRequest {
                bucket: Some(bucket.clone()),
            })
            .await?
            == created,
        "head bucket differs from creation",
    )?;
    expected_error(
        provider
            .create_bucket(wire::CreateBucketRequest {
                name: if namespace == "other" {
                    "alternate"
                } else {
                    "other"
                }
                .into(),
                mutation: identity(namespace, "create"),
            })
            .await,
        wire::ErrorCode::IdempotencyMismatch,
        "bucket retry identity was rebound",
    )?;

    let body = OwnedBytes::from(vec![b'x'; 2 * 65_536 + 17]);
    let mut first_request = put(&bucket, "dir/value");
    first_request.preconditions = Some(wire::Preconditions {
        condition: Some(wire::preconditions::Condition::IfAbsent(true)),
    });
    first_request.mutation = identity(namespace, "put");
    let first = provider.put(first_request.clone(), body.clone()).await?;
    ensure(
        first.size == body.len() as u64 && !first.etag.is_empty(),
        "put omitted exact length or validator",
    )?;
    let mut denied = first_request.clone();
    denied.mutation = None;
    expected_error(
        provider.put(denied, body.clone()).await,
        wire::ErrorCode::PreconditionFailed,
        "IfAbsent overwrote a current key",
    )?;
    expected_error(
        provider
            .put(first_request.clone(), OwnedBytes::from_static(b"different"))
            .await,
        wire::ErrorCode::IdempotencyMismatch,
        "put retry identity admitted different bytes",
    )?;

    let query = get(&bucket, "dir/value");
    let read = provider.get(query.clone(), body.len() as u64).await?;
    ensure(
        read.body() == &body && read.header().object.as_ref() == Some(&first),
        "get changed the complete representation or its metadata",
    )?;
    expected_error(
        provider.get(query.clone(), 1).await,
        wire::ErrorCode::QuotaExceeded,
        "read allocation bound was ignored",
    )?;
    let mut conditional = query.clone();
    conditional.if_none_match = first.etag.clone();
    expected_error(
        provider.get(conditional, u64::MAX).await,
        wire::ErrorCode::NotModified,
        "matching conditional read returned a body",
    )?;
    let mut ranged = query.clone();
    ranged.range = Some(wire::ByteRange {
        selection: Some(wire::byte_range::Selection::SuffixLength(17)),
    });
    let selected = provider.get(ranged, 17).await?;
    ensure(
        selected.body().as_ref() == [b'x'; 17] && selected.header().object.as_ref() == Some(&first),
        "suffix range changed the selected bytes or complete metadata",
    )?;
    ensure(
        provider
            .head(wire::HeadObjectRequest {
                bucket: Some(bucket.clone()),
                object_key: "dir/value".into(),
                ..Default::default()
            })
            .await?
            .object
            == Some(first.clone()),
        "head changed publication metadata",
    )?;

    let newer = provider
        .put(put(&bucket, "dir/value"), OwnedBytes::from_static(b"new"))
        .await?;
    ensure(
        provider.put(first_request, body.clone()).await? == first,
        "retry after replacement lost its original receipt",
    )?;
    ensure(
        provider.get(query, 3).await?.body().as_ref() == b"new",
        "old retry restored superseded bytes",
    )?;
    expected_error(
        provider
            .delete(wire::DeleteObjectRequest {
                bucket: Some(bucket.clone()),
                object_key: "dir/value".into(),
                preconditions: Some(wire::Preconditions {
                    condition: Some(wire::preconditions::Condition::IfMatch(first.etag)),
                }),
                mutation: None,
            })
            .await,
        wire::ErrorCode::PreconditionFailed,
        "stale validator deleted a later value",
    )?;

    provider
        .put(put(&bucket, "dir/other"), OwnedBytes::from_static(b"other"))
        .await?;
    let listing = wire::ListObjectsRequest {
        bucket: Some(bucket.clone()),
        prefix: "dir/".into(),
        page_size: 1,
        ..Default::default()
    };
    let page = provider.list(listing.clone()).await?;
    ensure(
        page.entries.len() == 1
            && page
                .entries
                .first()
                .is_some_and(|entry| entry.object_key == "dir/other")
            && !page.continuation_token.is_empty(),
        "first listing page is not ordered and bounded",
    )?;
    let mut continuation = listing;
    continuation.continuation_token = page.continuation_token;
    let mut rebound = continuation.clone();
    rebound.prefix = "different/".into();
    expected_error(
        provider.list(rebound).await,
        wire::ErrorCode::InvalidArgument,
        "listing cursor admitted another query",
    )?;
    let page = provider.list(continuation).await?;
    ensure(
        page.entries.len() == 1
            && page.entries.first().is_some_and(|entry| {
                entry.object_key == "dir/value" && entry.object.as_ref() == Some(&newer)
            })
            && page.continuation_token.is_empty(),
        "listing repeated history or omitted the current representation",
    )?;
    let grouped = provider
        .list(wire::ListObjectsRequest {
            bucket: Some(bucket.clone()),
            delimiter: "/".into(),
            ..Default::default()
        })
        .await?;
    ensure(
        grouped.entries.is_empty() && grouped.common_prefixes == ["dir/"],
        "delimiter grouping exposed nested objects",
    )?;

    let upload = provider
        .create_multipart(wire::CreateMultipartRequest {
            bucket: Some(bucket.clone()),
            object_key: "multipart".into(),
            mutation: identity(namespace, "multipart"),
            ..Default::default()
        })
        .await?;
    let part_header = wire::UploadPartHeader {
        bucket: Some(bucket.clone()),
        object_key: "multipart".into(),
        upload_id: upload.upload_id.clone(),
        part_number: 1,
        mutation: identity(namespace, "part"),
    };
    let part = provider
        .upload_part(part_header.clone(), body.clone())
        .await?;
    ensure(
        provider.upload_part(part_header, body.clone()).await? == part,
        "part retry changed its receipt",
    )?;
    ensure(
        provider
            .list_parts(wire::ListPartsRequest {
                bucket: Some(bucket.clone()),
                object_key: "multipart".into(),
                upload_id: upload.upload_id.clone(),
                ..Default::default()
            })
            .await?
            .parts
            == [part.clone()],
        "staged part listing differs from its receipt",
    )?;
    let complete = wire::CompleteMultipartRequest {
        bucket: Some(bucket.clone()),
        object_key: "multipart".into(),
        upload_id: upload.upload_id.clone(),
        parts: vec![part],
        mutation: identity(namespace, "complete"),
        ..Default::default()
    };
    let published = provider.complete_multipart(complete.clone()).await?;
    ensure(
        provider.complete_multipart(complete).await? == published,
        "completed upload retry did not recover its receipt",
    )?;
    ensure(
        provider
            .get(get(&bucket, "multipart"), body.len() as u64)
            .await?
            .body()
            == &body,
        "completed multipart bytes differ",
    )?;
    ensure(
        !provider
            .abort_multipart(wire::AbortMultipartRequest {
                bucket: Some(bucket.clone()),
                object_key: "multipart".into(),
                upload_id: upload.upload_id,
                mutation: None,
            })
            .await?
            .existed,
        "completed upload remained abortable",
    )?;

    let abandoned = provider
        .create_multipart(wire::CreateMultipartRequest {
            bucket: Some(bucket.clone()),
            object_key: "abandoned".into(),
            ..Default::default()
        })
        .await?;
    provider
        .upload_part(
            wire::UploadPartHeader {
                bucket: Some(bucket.clone()),
                object_key: "abandoned".into(),
                upload_id: abandoned.upload_id.clone(),
                part_number: 1,
                mutation: identity(namespace, "abandoned-part"),
            },
            body.clone(),
        )
        .await?;
    expected_error(
        provider
            .get(get(&bucket, "abandoned"), body.len() as u64)
            .await,
        wire::ErrorCode::NotFound,
        "staged multipart bytes became a public object",
    )?;
    ensure(
        provider
            .abort_multipart(wire::AbortMultipartRequest {
                bucket: Some(bucket.clone()),
                object_key: "abandoned".into(),
                upload_id: abandoned.upload_id,
                mutation: None,
            })
            .await?
            .existed,
        "active upload was not aborted",
    )?;
    expected_error(
        provider
            .get(get(&bucket, "abandoned"), body.len() as u64)
            .await,
        wire::ErrorCode::NotFound,
        "aborted multipart bytes became a public object",
    )?;
    expected_error(
        provider
            .delete_bucket(wire::DeleteBucketRequest {
                bucket: Some(bucket.clone()),
                mutation: None,
            })
            .await,
        wire::ErrorCode::PreconditionFailed,
        "nonempty bucket was deleted",
    )?;
    for key in ["dir/value", "dir/other", "multipart"] {
        ensure(
            provider
                .delete(wire::DeleteObjectRequest {
                    bucket: Some(bucket.clone()),
                    object_key: key.into(),
                    ..Default::default()
                })
                .await?
                .existed,
            "current key was not deleted",
        )?;
        expected_error(
            provider.get(get(&bucket, key), 1).await,
            wire::ErrorCode::NotFound,
            "deleted key exposed retained bytes",
        )?;
    }
    ensure(
        provider
            .list(wire::ListObjectsRequest {
                bucket: Some(bucket.clone()),
                ..Default::default()
            })
            .await?
            .entries
            .is_empty(),
        "deleted keys remained publicly listed",
    )?;
    ensure(
        provider
            .delete_bucket(wire::DeleteBucketRequest {
                bucket: Some(bucket.clone()),
                mutation: None,
            })
            .await?
            .existed,
        "empty bucket was not deleted",
    )?;
    expected_error(
        provider
            .head_bucket(wire::HeadBucketRequest {
                bucket: Some(bucket),
            })
            .await,
        wire::ErrorCode::NotFound,
        "deleted bucket remained public",
    )?;
    Ok(())
}

fn budget(case: &'static str, before: Requests, ledger: &RequestLedger) -> Result<(), Failure> {
    let requests = ledger.requests().since(before);
    if requests.control() != 0 || requests.data() > 1 {
        return Err(Failure::Budget { case, requests });
    }
    Ok(())
}

async fn steady(backend: &dyn Backend) -> Result<(), Failure> {
    let provider = backend.provider();
    let ledger = backend.requests();
    let bucket = provider
        .create_bucket(wire::CreateBucketRequest {
            name: format!("{}-budget", backend.namespace()),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or(Failure::Invariant("budget bucket omitted identity"))?;
    let value = OwnedBytes::from_static(b"request-budget");
    let before = ledger.requests();
    provider.put(put(&bucket, "value"), value.clone()).await?;
    budget("put", before, &ledger)?;
    let before = ledger.requests();
    let object = provider
        .get(get(&bucket, "value"), value.len() as u64)
        .await?;
    budget("get", before, &ledger)?;
    ensure(object.body() == &value, "budget get changed payload")?;
    let before = ledger.requests();
    let missing = provider.get(get(&bucket, "missing"), 1).await;
    budget("missing get", before, &ledger)?;
    expected_error(
        missing,
        wire::ErrorCode::NotFound,
        "budget missing get changed error",
    )?;
    provider
        .delete(wire::DeleteObjectRequest {
            bucket: Some(bucket.clone()),
            object_key: "value".into(),
            ..Default::default()
        })
        .await?;
    provider
        .delete_bucket(wire::DeleteBucketRequest {
            bucket: Some(bucket),
            mutation: None,
        })
        .await?;
    Ok(())
}

/// Measurements for the same checked workload on every backend.
#[derive(Debug)]
pub struct Measurement {
    /// Completed puts and gets, each with an exact payload check.
    pub operations: usize,
    /// Elapsed wall-clock time at millisecond resolution. Native performance
    /// runners can additionally time the invocation with a monotonic clock.
    pub elapsed: std::time::Duration,
    /// Calls after bucket setup, at the backend's declared dispatch boundary.
    pub requests: Requests,
}

/// An on-demand 256-round put/get workload, excluding bucket setup. Every
/// operation enforces zero control requests and at most one data request.
/// Supply fresh storage independently of [`verify`]. No scheduler invokes it.
pub async fn bench(backend: &dyn Backend) -> Result<Measurement, Failure> {
    let provider = backend.provider();
    let ledger = backend.requests();
    let bucket = provider
        .create_bucket(wire::CreateBucketRequest {
            name: format!("{}-bench", backend.namespace()),
            mutation: None,
        })
        .await?
        .bucket
        .ok_or(Failure::Invariant("bench bucket omitted identity"))?;
    let value = OwnedBytes::from(vec![0x5a; 4096]);
    let before = ledger.requests();
    let clock = acyclic_stream::SystemUnixMillisClock;
    let started = clock.now_unix_millis();
    for _ in 0..256 {
        let before = ledger.requests();
        provider.put(put(&bucket, "value"), value.clone()).await?;
        budget("bench put", before, &ledger)?;
        let before = ledger.requests();
        let object = provider
            .get(get(&bucket, "value"), value.len() as u64)
            .await?;
        budget("bench get", before, &ledger)?;
        ensure(object.body() == &value, "benchmark get changed payload")?;
    }
    let requests = ledger.requests().since(before);
    let elapsed = std::time::Duration::from_millis(clock.now_unix_millis().saturating_sub(started));
    provider
        .delete(wire::DeleteObjectRequest {
            bucket: Some(bucket.clone()),
            object_key: "value".into(),
            ..Default::default()
        })
        .await?;
    provider
        .delete_bucket(wire::DeleteBucketRequest {
            bucket: Some(bucket),
            mutation: None,
        })
        .await?;
    Ok(Measurement {
        operations: 512,
        elapsed,
        requests,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn memory() -> Result<Fixture<super::super::MemoryObjects>, Failure> {
        Ok(Fixture::new(
            super::super::MemoryObjects::new(Default::default())?,
            "suite-macro",
        ))
    }
    suite!(memory_backend, memory);

    #[tokio::test]
    async fn shared_bench_checks_values_and_zero_control_budget() -> Result<(), Failure> {
        let backend = memory().await?;
        let sample = bench(&backend).await?;
        assert_eq!(sample.operations, 512);
        assert_eq!(sample.requests.control(), 0);
        assert_eq!(sample.requests.data(), 512);
        Ok(())
    }
}
