use super::*;
include!("access_tests.rs");

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn ordinary_http_wire_retains_canonical_allocation_through_last_chunk()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)?;
    let baseline = work.live_allocation_bytes();
    let data = b"canonical HTTP chunk".to_vec();
    let allocation = {
        let claim_bytes = data.capacity() as u64;
        work.claim(
            claim_bytes,
            u64::from(claim_bytes != 0),
            crate::WorkCounters::UNCHARGED,
        )
    }?;
    let carrier =
        acyclic_stream::OwnedBytes::from_vec_with_guard(data, allocation, work.runtime_meter())
            .unwrap_or_else(|failure| panic!("ordinary test carrier: {:?}", failure.into_error()));
    let pointer = carrier.as_slice().as_ptr();
    let retained = work.live_allocation_bytes();
    let mut response = body(Bytes::from_owner(carrier));
    assert_eq!(work.live_allocation_bytes(), retained);
    let chunk = response
        .next()
        .await
        .ok_or("missing wire chunk")?
        .map_err(std::io::Error::other)?;
    assert_eq!(chunk.as_ptr(), pointer);
    assert_eq!(chunk.as_ref(), b"canonical HTTP chunk");
    let alias = chunk.slice(1..);
    drop(chunk);
    drop(response);
    assert_eq!(work.live_allocation_bytes(), retained);
    drop(alias);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}
const SIGNATURE_DATE_HEADER: &str = "x-amz-date";
const SIGNATURE_ALGORITHM: &str = "AWS4-HMAC-SHA256";
const AUTHORIZATION_HEADER: &str = "authorization";
use crate::{Fs, MemoryAuthorityBackend, MemoryObjectBackend};
use futures::TryStreamExt;
use hmac::{Hmac, KeyInit, Mac};
use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};
use s3s::auth::Credentials;
use s3s::dto::{
    AbortMultipartUploadInput, CompleteMultipartUploadInput, CompletedMultipartUpload,
    CompletedPart, CreateMultipartUploadInput, GetObjectInput, ListObjectsV2Input, ListPartsInput,
    PutObjectInput, UploadPartInput,
};
use s3s::service::S3ServiceBuilder;
use sha2::{Digest as ShaDigest, Sha256};
use std::fmt::Write as _;
use tower::ServiceExt;

struct Resolver {
    principal: FilesystemS3Principal<MemoryAuthorityBackend, MemoryObjectBackend>,
}

#[async_trait]
impl FilesystemS3Resolver<MemoryAuthorityBackend, MemoryObjectBackend> for Resolver {
    async fn resolve_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        if access_key != "access" {
            return Err(s3s::s3_error!(InvalidAccessKeyId));
        }
        Ok(self.principal.secret_key.clone())
    }

    async fn resolve_principal(
        &self,
        access_key: &str,
        _session_token: Option<&str>,
        bucket: &str,
    ) -> S3Result<FilesystemS3Principal<MemoryAuthorityBackend, MemoryObjectBackend>> {
        if access_key != "access" || bucket != self.principal.bucket {
            return Err(s3s::s3_error!(InvalidAccessKeyId));
        }
        Ok(self.principal.clone())
    }
}

fn request<T>(input: T) -> S3Request<T> {
    let mut headers = HeaderMap::new();
    headers.insert(
        IDEMPOTENCY_HEADER,
        HeaderValue::from_static("test-operation"),
    );
    S3Request {
        input,
        method: Method::PUT,
        uri: Uri::from_static("http://localhost/bucket/key"),
        headers,
        extensions: Extensions::new(),
        credentials: Some(Credentials {
            access_key: "access".to_owned(),
            secret_key: SecretKey::from("secret"),
        }),
        region: None,
        service: Some("s3".to_owned()),
        trailing_headers: None,
    }
}

async fn adapter(
    writable: bool,
) -> S3Result<FilesystemS3Adapter<Resolver, MemoryAuthorityBackend, MemoryObjectBackend>> {
    adapter_with_limits(
        writable,
        FilesystemS3Limits {
            minimum_multipart_part_bytes: 1,
            ..FilesystemS3Limits::default()
        },
    )
    .await
}

async fn adapter_with_limits(
    writable: bool,
    limits: FilesystemS3Limits,
) -> S3Result<FilesystemS3Adapter<Resolver, MemoryAuthorityBackend, MemoryObjectBackend>> {
    let filesystem = Fs::memory();
    let workspace = filesystem
        .create_workspace_with_operation::<_, (), crate::WorkspaceError>(
            "s3-http",
            crate::VolumeInitialization {
                config: crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                access_policy: Some(crate::access::INITIAL_POLICY),
            },
            crate::OperationId::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            None,
        )
        .await
        .map_err(|_| s3s::s3_error!(InternalError))?
        .value
        .workspace();
    FilesystemS3Adapter::new(
        Arc::new(Resolver {
            principal: FilesystemS3Principal {
                secret_key: SecretKey::from("secret"),
                bucket: "bucket".to_owned(),
                workspace,
                generation: None,
                writable,
            },
        }),
        limits,
    )
}

fn body(bytes: Bytes) -> StreamingBlob {
    StreamingBlob::wrap(stream::iter([Ok::<Bytes, std::io::Error>(bytes)]))
}

/// The `x-amz-date` value `offset` from now, and its date.
fn signature_time(offset: time::Duration) -> (String, String) {
    let at = time::OffsetDateTime::now_utc() + offset;
    let date = format!("{:04}{:02}{:02}", at.year(), u8::from(at.month()), at.day());
    let timestamp = format!(
        "{date}T{:02}{:02}{:02}Z",
        at.hour(),
        at.minute(),
        at.second()
    );
    (timestamp, date)
}

fn hmac(key: &[u8], value: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)?;
    mac.update(value);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn signed_head_bucket(
    timestamp: &str,
    scope_date: &str,
) -> Result<http::Request<axum::body::Body>, Box<dyn std::error::Error>> {
    signed_bucket_request(Method::HEAD, timestamp, scope_date)
}

fn signed_bucket_request(
    method: Method,
    timestamp: &str,
    scope_date: &str,
) -> Result<http::Request<axum::body::Body>, Box<dyn std::error::Error>> {
    let path = "/bucket";
    let payload_hash = hex::encode(Sha256::digest([]));
    let signed_headers = "host;x-amz-content-sha256;x-amz-date";
    let mut canonical_headers = String::new();
    writeln!(canonical_headers, "host:localhost")?;
    writeln!(canonical_headers, "x-amz-content-sha256:{payload_hash}")?;
    writeln!(canonical_headers, "x-amz-date:{timestamp}")?;
    let canonical_request =
        format!("{method}\n{path}\n\n{canonical_headers}\n{signed_headers}\n{payload_hash}");
    // Signed consistently for the stated scope, so a scope date that
    // differs from `timestamp` fails only the date rule, not the signature.
    let scope = format!("{scope_date}/auto/s3/aws4_request");
    let string_to_sign = format!(
        "{SIGNATURE_ALGORITHM}\n{timestamp}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let date_key = hmac(b"AWS4secret", scope_date.as_bytes())?;
    let region_key = hmac(&date_key, b"auto")?;
    let service_key = hmac(&region_key, b"s3")?;
    let signing_key = hmac(&service_key, b"aws4_request")?;
    let signature = hex::encode(hmac(&signing_key, string_to_sign.as_bytes())?);
    let authorization = format!(
        "{SIGNATURE_ALGORITHM} Credential=access/{scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );
    Ok(http::Request::builder()
        .method(method)
        .uri(path)
        .header("host", "localhost")
        .header("x-amz-content-sha256", payload_hash)
        .header(SIGNATURE_DATE_HEADER, timestamp)
        .header(AUTHORIZATION_HEADER, authorization)
        .body(axum::body::Body::empty())?)
}

fn presigned_head_bucket_with_encoded_signature_name()
-> Result<http::Request<axum::body::Body>, Box<dyn std::error::Error>> {
    let now = time::OffsetDateTime::now_utc();
    let scope_date = format!(
        "{:04}{:02}{:02}",
        now.year(),
        u8::from(now.month()),
        now.day()
    );
    let timestamp = format!(
        "{scope_date}T{:02}{:02}{:02}Z",
        now.hour(),
        now.minute(),
        now.second()
    );
    let credential = format!("access/{scope_date}/auto/s3/aws4_request");
    let canonical_query = format!(
        "X-Amz-Algorithm={SIGNATURE_ALGORITHM}&X-Amz-Credential=access%2F{scope_date}%2Fauto%2Fs3%2Faws4_request&X-Amz-Date={timestamp}&X-Amz-Expires=900&X-Amz-SignedHeaders=host"
    );
    let canonical_request =
        format!("HEAD\n/bucket\n{canonical_query}\nhost:localhost\n\nhost\nUNSIGNED-PAYLOAD");
    let string_to_sign = format!(
        "{SIGNATURE_ALGORITHM}\n{timestamp}\n{scope_date}/auto/s3/aws4_request\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes()))
    );
    let date_key = hmac(b"AWS4secret", scope_date.as_bytes())?;
    let region_key = hmac(&date_key, b"auto")?;
    let service_key = hmac(&region_key, b"s3")?;
    let signing_key = hmac(&service_key, b"aws4_request")?;
    let signature = hex::encode(hmac(&signing_key, string_to_sign.as_bytes())?);
    let uri = format!("/bucket?{canonical_query}&%58-Amz-Signature={signature}");
    Ok(http::Request::builder()
            .method(Method::HEAD)
            .uri(uri)
            .header("host", "localhost")
            .header(
                AUTHORIZATION_HEADER,
                format!(
                    "{SIGNATURE_ALGORITHM} Credential={credential}, SignedHeaders=host;x-amz-date, Signature=invalid"
                ),
            )
            .body(axum::body::Body::empty())?)
}

#[test]
fn sha256_and_hmac_match_known_answers() -> Result<(), Box<dyn std::error::Error>> {
    // FIPS 180-2 "abc" and RFC 4231 test case 2 pin the digest crates.
    assert_eq!(
        hex::encode(Sha256::digest(b"abc")),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
        hex::encode(hmac(b"Jefe", b"what do ya want for nothing?")?),
        "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
    );
    Ok(())
}

#[tokio::test]
async fn authenticated_service_enforces_signature_time() -> Result<(), Box<dyn std::error::Error>> {
    let adapter = adapter(true).await?;
    let authentication = FilesystemS3Authentication::new(Arc::clone(&adapter.resolver));
    let mut builder = S3ServiceBuilder::new(adapter);
    builder.set_auth(authentication);
    let service = builder.build();

    // s3s allows 15 minutes of skew either way.
    for minutes in [-14, 14] {
        let (timestamp, date) = signature_time(time::Duration::minutes(minutes));
        let response = service
            .clone()
            .oneshot(signed_head_bucket(&timestamp, &date)?)
            .await
            .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
        assert_eq!(response.status(), http::StatusCode::OK);
    }
    for minutes in [-16, 16] {
        let (timestamp, date) = signature_time(time::Duration::minutes(minutes));
        let response = service
            .clone()
            .oneshot(signed_head_bucket(&timestamp, &date)?)
            .await
            .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
        assert_eq!(response.status(), http::StatusCode::FORBIDDEN);
    }
    let (timestamp, _) = signature_time(time::Duration::ZERO);
    let mismatched_scope = service
        .oneshot(signed_bucket_request(Method::GET, &timestamp, "20000101")?)
        .await
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
    assert_eq!(mismatched_scope.status(), http::StatusCode::FORBIDDEN);
    let body = mismatched_scope
        .into_body()
        .store_all_limited(64 * 1024)
        .await
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    assert!(
        String::from_utf8_lossy(&body).contains("credential scope date does not match x-amz-date"),
        "{}",
        String::from_utf8_lossy(&body)
    );
    Ok(())
}

#[tokio::test]
async fn authenticated_service_honors_percent_encoded_presigned_auth_precedence()
-> Result<(), Box<dyn std::error::Error>> {
    let adapter = adapter(true).await?;
    let authentication = FilesystemS3Authentication::new(Arc::clone(&adapter.resolver));
    let mut builder = S3ServiceBuilder::new(adapter);
    builder.set_auth(authentication);
    let response = builder
        .build()
        .oneshot(presigned_head_bucket_with_encoded_signature_name()?)
        .await
        .map_err(|error| std::io::Error::other(format!("{error:?}")))?;
    assert_eq!(response.status(), http::StatusCode::OK);
    Ok(())
}

#[tokio::test]
async fn browser_form_uploads_are_explicitly_unsupported() -> S3Result {
    let Err(error) = adapter(true)
        .await?
        .post_object(request(PostObjectInput::default()))
        .await
    else {
        return Err(s3s::s3_error!(InternalError));
    };
    assert_eq!(error.code(), &s3s::S3ErrorCode::NotImplemented);
    Ok(())
}

#[tokio::test]
async fn adapter_uses_one_workspace_and_generation_stable_pagination() -> S3Result {
    let adapter = adapter(true).await?;
    for (operation, key) in [("one", "a"), ("two", "b")] {
        let mut request = request(PutObjectInput {
            body: Some(body(Bytes::copy_from_slice(key.as_bytes()))),
            bucket: "bucket".to_owned(),
            content_length: Some(1),
            key: key.to_owned(),
            ..PutObjectInput::default()
        });
        request.headers.insert(
            IDEMPOTENCY_HEADER,
            HeaderValue::from_str(operation).map_err(|_| s3s::s3_error!(InternalError))?,
        );
        S3::put_object(&adapter, request).await?;
    }
    let first = S3::list_objects_v2(
        &adapter,
        request(ListObjectsV2Input {
            bucket: "bucket".to_owned(),
            max_keys: Some(1),
            ..ListObjectsV2Input::default()
        }),
    )
    .await?
    .output;
    let continuation = first
        .next_continuation_token
        .ok_or_else(|| s3s::s3_error!(InternalError))?;
    let mut later = request(PutObjectInput {
        body: Some(body(Bytes::from_static(b"later"))),
        bucket: "bucket".to_owned(),
        content_length: Some(5),
        key: "aa".to_owned(),
        ..PutObjectInput::default()
    });
    later.headers.insert(
        IDEMPOTENCY_HEADER,
        HeaderValue::from_static("later-operation"),
    );
    S3::put_object(&adapter, later).await?;
    let second = S3::list_objects_v2(
        &adapter,
        request(ListObjectsV2Input {
            bucket: "bucket".to_owned(),
            continuation_token: Some(continuation),
            max_keys: Some(10),
            ..ListObjectsV2Input::default()
        }),
    )
    .await?
    .output;
    let keys = second
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|entry| entry.key)
        .collect::<Vec<_>>();
    assert_eq!(keys, vec!["b"]);
    Ok(())
}

#[tokio::test]
async fn read_only_credentials_reject_mutation_before_polling_the_body() -> S3Result {
    let adapter = adapter(false).await?;
    let result = S3::put_object(
        &adapter,
        request(PutObjectInput {
            body: Some(body(Bytes::from_static(b"not-consumed"))),
            bucket: "bucket".to_owned(),
            content_length: Some(12),
            key: "denied".to_owned(),
            ..PutObjectInput::default()
        }),
    )
    .await;
    assert!(result.is_err());
    Ok(())
}

#[tokio::test]
async fn get_streams_bounded_chunks_from_one_immutable_generation() -> S3Result {
    let adapter = adapter_with_limits(
        true,
        FilesystemS3Limits {
            maximum_request_bytes: 16,
            response_chunk_bytes: 2,
            ..FilesystemS3Limits::default()
        },
    )
    .await?;
    let mut put = request(PutObjectInput {
        body: Some(body(Bytes::from_static(b"abcde"))),
        bucket: "bucket".to_owned(),
        content_length: Some(5),
        key: "file".to_owned(),
        ..PutObjectInput::default()
    });
    put.headers
        .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("initial"));
    S3::put_object(&adapter, put).await?;

    let response = S3::get_object(
        &adapter,
        request(GetObjectInput {
            bucket: "bucket".to_owned(),
            key: "file".to_owned(),
            ..GetObjectInput::default()
        }),
    )
    .await?;

    let mut replace = request(PutObjectInput {
        body: Some(body(Bytes::from_static(b"vwxyz"))),
        bucket: "bucket".to_owned(),
        content_length: Some(5),
        key: "file".to_owned(),
        ..PutObjectInput::default()
    });
    replace
        .headers
        .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("replace"));
    S3::put_object(&adapter, replace).await?;

    let chunks = response
        .output
        .body
        .ok_or_else(|| s3s::s3_error!(InternalError))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|_| s3s::s3_error!(InternalError))?;
    assert_eq!(
        chunks,
        vec![Bytes::from("ab"), Bytes::from("cd"), Bytes::from("e")]
    );
    Ok(())
}

#[tokio::test]
async fn generation_scoped_credentials_are_immutable_snapshots() -> S3Result {
    let writable = adapter(true).await?;
    let workspace = writable.resolver.principal.workspace.clone();
    let mut initial = request(PutObjectInput {
        body: Some(body(Bytes::from_static(b"old"))),
        bucket: "bucket".to_owned(),
        content_length: Some(3),
        key: "file".to_owned(),
        ..PutObjectInput::default()
    });
    initial
        .headers
        .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("old"));
    S3::put_object(&writable, initial).await?;
    let generation = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|_| s3s::s3_error!(InternalError))?
        .value;

    let mut replacement = request(PutObjectInput {
        body: Some(body(Bytes::from_static(b"new"))),
        bucket: "bucket".to_owned(),
        content_length: Some(3),
        key: "file".to_owned(),
        ..PutObjectInput::default()
    });
    replacement
        .headers
        .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("new"));
    S3::put_object(&writable, replacement).await?;

    let pinned = FilesystemS3Adapter::new(
        Arc::new(Resolver {
            principal: FilesystemS3Principal {
                secret_key: SecretKey::from("secret"),
                bucket: "bucket".to_owned(),
                workspace,
                generation: Some(generation.id()),
                writable: false,
            },
        }),
        FilesystemS3Limits::default(),
    )?;
    let response = S3::get_object(
        &pinned,
        request(GetObjectInput {
            bucket: "bucket".to_owned(),
            key: "file".to_owned(),
            ..GetObjectInput::default()
        }),
    )
    .await?;
    let bytes = response
        .output
        .body
        .ok_or_else(|| s3s::s3_error!(InternalError))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|_| s3s::s3_error!(InternalError))?
        .concat();
    assert_eq!(bytes, b"old");
    Ok(())
}

#[tokio::test]
async fn multipart_is_stream_durable_stateless_and_exactly_retryable() -> S3Result {
    let adapter = adapter(true).await?;
    let created = S3::create_multipart_upload(
        &adapter,
        request(CreateMultipartUploadInput {
            bucket: "bucket".to_owned(),
            key: "large/object".to_owned(),
            ..CreateMultipartUploadInput::default()
        }),
    )
    .await?
    .output;
    let upload_id = created
        .upload_id
        .ok_or_else(|| s3s::s3_error!(InternalError))?;

    let mut first_request = request(UploadPartInput {
        body: Some(body(Bytes::from_static(b"first-"))),
        bucket: "bucket".to_owned(),
        content_length: Some(6),
        key: "large/object".to_owned(),
        part_number: 1,
        upload_id: upload_id.clone(),
        ..UploadPartInput::default()
    });
    first_request
        .headers
        .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("part-one"));
    let first_etag = S3::upload_part(&adapter, first_request)
        .await?
        .output
        .e_tag
        .ok_or_else(|| s3s::s3_error!(InternalError))?;

    let mut retry = request(UploadPartInput {
        body: Some(body(Bytes::from_static(b"first-"))),
        bucket: "bucket".to_owned(),
        content_length: Some(6),
        key: "large/object".to_owned(),
        part_number: 1,
        upload_id: upload_id.clone(),
        ..UploadPartInput::default()
    });
    retry
        .headers
        .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("part-one"));
    assert_eq!(
        S3::upload_part(&adapter, retry).await?.output.e_tag,
        Some(first_etag.clone())
    );

    let restarted =
        FilesystemS3Adapter::new(Arc::clone(&adapter.resolver), FilesystemS3Limits::default())?;
    let mut second_request = request(UploadPartInput {
        body: Some(body(Bytes::from_static(b"second"))),
        bucket: "bucket".to_owned(),
        content_length: Some(6),
        key: "large/object".to_owned(),
        part_number: 2,
        upload_id: upload_id.clone(),
        ..UploadPartInput::default()
    });
    second_request
        .headers
        .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("part-two"));
    let second_etag = S3::upload_part(&restarted, second_request)
        .await?
        .output
        .e_tag
        .ok_or_else(|| s3s::s3_error!(InternalError))?;

    let mut bounded_discovery = request(CreateMultipartUploadInput {
        bucket: "bucket".to_owned(),
        key: "bounded-discovery".to_owned(),
        ..CreateMultipartUploadInput::default()
    });
    bounded_discovery.headers.insert(
        IDEMPOTENCY_HEADER,
        HeaderValue::from_static("bounded-discovery"),
    );
    S3::create_multipart_upload(&restarted, bounded_discovery).await?;

    let stream = restarted
        .resolver
        .principal
        .workspace
        .volume
        .fs
        .authority()
        .stream_provider();
    let cancellation = CancellationToken::new();
    assert!(
        active_s3_multipart_objects(
            stream.as_ref(),
            restarted.resolver.principal.workspace.volume.fs.objects(),
            FilesystemS3Limits::default(),
            S3MultipartRetentionLimits {
                maximum_uploads: 1,
                ..S3MultipartRetentionLimits::default()
            },
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .is_err()
    );
    assert!(
        active_s3_multipart_objects(
            stream.as_ref(),
            restarted.resolver.principal.workspace.volume.fs.objects(),
            FilesystemS3Limits::default(),
            S3MultipartRetentionLimits {
                maximum_object_bytes: 1,
                ..S3MultipartRetentionLimits::default()
            },
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .is_err()
    );
    let active = active_s3_multipart_objects(
        stream.as_ref(),
        restarted.resolver.principal.workspace.volume.fs.objects(),
        FilesystemS3Limits::default(),
        S3MultipartRetentionLimits::default(),
        crate::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?;
    assert!(active.value.len() >= 4);
    assert!(active.work.authority_records_read >= 4);
    assert!(active.work.authority_bytes_read > 0);
    assert!(active.work.backend_read_operations >= 5);
    assert!(active.work.object_bytes_read > 0);

    let listed = S3::list_parts(
        &restarted,
        request(ListPartsInput {
            bucket: "bucket".to_owned(),
            key: "large/object".to_owned(),
            upload_id: upload_id.clone(),
            ..ListPartsInput::default()
        }),
    )
    .await?
    .output;
    assert_eq!(listed.parts.as_ref().map(Vec::len), Some(2));

    let mut complete = request(CompleteMultipartUploadInput {
        bucket: "bucket".to_owned(),
        key: "large/object".to_owned(),
        multipart_upload: Some(CompletedMultipartUpload {
            parts: Some(vec![
                CompletedPart {
                    e_tag: Some(first_etag),
                    part_number: Some(1),
                    ..CompletedPart::default()
                },
                CompletedPart {
                    e_tag: Some(second_etag),
                    part_number: Some(2),
                    ..CompletedPart::default()
                },
            ]),
        }),
        upload_id: upload_id.clone(),
        ..CompleteMultipartUploadInput::default()
    });
    complete.headers.insert(
        IDEMPOTENCY_HEADER,
        HeaderValue::from_static("complete-upload"),
    );
    S3::complete_multipart_upload(&restarted, complete).await?;
    assert!(
        active_s3_multipart_objects(
            stream.as_ref(),
            restarted.resolver.principal.workspace.volume.fs.objects(),
            FilesystemS3Limits::default(),
            S3MultipartRetentionLimits::default(),
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value
        .is_empty()
    );

    let response = S3::get_object(
        &restarted,
        request(GetObjectInput {
            bucket: "bucket".to_owned(),
            key: "large/object".to_owned(),
            ..GetObjectInput::default()
        }),
    )
    .await?;
    let bytes = response
        .output
        .body
        .ok_or_else(|| s3s::s3_error!(InternalError))?
        .try_collect::<Vec<_>>()
        .await
        .map_err(|_| s3s::s3_error!(InternalError))?
        .concat();
    assert_eq!(bytes, b"first-second");
    Ok(())
}

#[tokio::test]
async fn multipart_abort_is_durable_and_fences_parts() -> S3Result {
    let adapter = adapter(true).await?;
    let upload_id = S3::create_multipart_upload(
        &adapter,
        request(CreateMultipartUploadInput {
            bucket: "bucket".to_owned(),
            key: "aborted".to_owned(),
            ..CreateMultipartUploadInput::default()
        }),
    )
    .await?
    .output
    .upload_id
    .ok_or_else(|| s3s::s3_error!(InternalError))?;
    let mut staged = request(UploadPartInput {
        body: Some(body(Bytes::from_static(b"collectable-after-abort"))),
        bucket: "bucket".to_owned(),
        content_length: Some(23),
        key: "aborted".to_owned(),
        part_number: 1,
        upload_id: upload_id.clone(),
        ..UploadPartInput::default()
    });
    staged
        .headers
        .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("staged"));
    S3::upload_part(&adapter, staged).await?;
    let stream = adapter
        .resolver
        .principal
        .workspace
        .volume
        .fs
        .authority()
        .stream_provider();
    let cancellation = CancellationToken::new();
    assert!(
        !active_s3_multipart_objects(
            stream.as_ref(),
            adapter.resolver.principal.workspace.volume.fs.objects(),
            FilesystemS3Limits::default(),
            S3MultipartRetentionLimits::default(),
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value
        .is_empty()
    );
    let mut abort = request(AbortMultipartUploadInput {
        bucket: "bucket".to_owned(),
        key: "aborted".to_owned(),
        upload_id: upload_id.clone(),
        ..AbortMultipartUploadInput::default()
    });
    abort
        .headers
        .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("abort"));
    S3::abort_multipart_upload(&adapter, abort).await?;
    assert!(
        active_s3_multipart_objects(
            stream.as_ref(),
            adapter.resolver.principal.workspace.volume.fs.objects(),
            FilesystemS3Limits::default(),
            S3MultipartRetentionLimits::default(),
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value
        .is_empty()
    );
    let mut upload = request(UploadPartInput {
        body: Some(body(Bytes::from_static(b"late"))),
        bucket: "bucket".to_owned(),
        content_length: Some(4),
        key: "aborted".to_owned(),
        part_number: 1,
        upload_id,
        ..UploadPartInput::default()
    });
    upload
        .headers
        .insert(IDEMPOTENCY_HEADER, HeaderValue::from_static("late"));
    assert!(S3::upload_part(&adapter, upload).await.is_err());
    Ok(())
}
