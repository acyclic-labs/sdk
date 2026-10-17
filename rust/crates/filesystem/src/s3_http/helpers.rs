fn reject_create_multipart_extensions(input: &CreateMultipartUploadInput) -> S3Result {
    if input.acl.is_some()
        || input.bucket_key_enabled.is_some()
        || input.cache_control.is_some()
        || input.checksum_algorithm.is_some()
        || input.checksum_type.is_some()
        || input.content_disposition.is_some()
        || input.content_encoding.is_some()
        || input.content_language.is_some()
        || input.content_type.is_some()
        || input.expected_bucket_owner.is_some()
        || input.expires.is_some()
        || input.grant_full_control.is_some()
        || input.grant_read.is_some()
        || input.grant_read_acp.is_some()
        || input.grant_write_acp.is_some()
        || input.metadata.is_some()
        || input.object_lock_legal_hold_status.is_some()
        || input.object_lock_mode.is_some()
        || input.object_lock_retain_until_date.is_some()
        || input.request_payer.is_some()
        || input.sse_customer_algorithm.is_some()
        || input.sse_customer_key.is_some()
        || input.sse_customer_key_md5.is_some()
        || input.ssekms_encryption_context.is_some()
        || input.ssekms_key_id.is_some()
        || input.server_side_encryption.is_some()
        || input.storage_class.is_some()
        || input.tagging.is_some()
        || input.website_redirect_location.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

fn reject_upload_part_extensions(input: &UploadPartInput) -> S3Result {
    if input
        .checksum_algorithm
        .as_ref()
        .is_some_and(|value| value.as_str() != "CRC32")
        || input.checksum_algorithm.is_some() && input.checksum_crc32.is_none()
        || input.checksum_crc32c.is_some()
        || input.checksum_crc64nvme.is_some()
        || input.checksum_sha1.is_some()
        || input.checksum_sha256.is_some()
        || input.content_md5.is_some()
        || input.expected_bucket_owner.is_some()
        || input.request_payer.is_some()
        || input.sse_customer_algorithm.is_some()
        || input.sse_customer_key.is_some()
        || input.sse_customer_key_md5.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

fn reject_complete_multipart_extensions(input: &CompleteMultipartUploadInput) -> S3Result {
    if input.checksum_crc32.is_some()
        || input.checksum_crc32c.is_some()
        || input.checksum_crc64nvme.is_some()
        || input.checksum_sha1.is_some()
        || input.checksum_sha256.is_some()
        || input.checksum_type.is_some()
        || input.expected_bucket_owner.is_some()
        || input.if_match.is_some()
        || input.if_none_match.is_some()
        || input.mpu_object_size.is_some()
        || input.request_payer.is_some()
        || input.sse_customer_algorithm.is_some()
        || input.sse_customer_key.is_some()
        || input.sse_customer_key_md5.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

fn check_conditions(
    etag_value: &str,
    if_match: Option<&s3s::dto::ETagCondition>,
    if_none_match: Option<&s3s::dto::ETagCondition>,
) -> S3Result {
    let current = etag(etag_value)?;
    if let Some(value) = if_match
        && !value.is_any()
        && value
            .as_etag()
            .is_none_or(|value| !value.strong_cmp(&current))
    {
        return Err(s3s::s3_error!(PreconditionFailed));
    }
    if let Some(value) = if_none_match
        && (value.is_any()
            || value
                .as_etag()
                .is_some_and(|value| value.weak_cmp(&current)))
    {
        return Err(s3s::s3_error!(NotModified));
    }
    Ok(())
}

fn committed_generation<A, O>(
    commit: crate::TransactionCommit<A, O>,
) -> S3Result<crate::Generation<A, O>> {
    match commit {
        crate::TransactionCommit::Committed(generation)
        | crate::TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
        crate::TransactionCommit::Conflict { .. }
        | crate::TransactionCommit::Fenced
        | crate::TransactionCommit::IdempotencyConflict => Err(s3s::s3_error!(PreconditionFailed)),
    }
}

fn etag(value: &str) -> S3Result<s3s::dto::ETag> {
    s3s::dto::ETag::parse_http_header(value.as_bytes()).map_err(|_| s3s::s3_error!(InternalError))
}

fn etag_for_generation(generation: crate::GenerationId, key: &str) -> S3Result<s3s::dto::ETag> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-s3-etag-v1\0");
    hasher.update(generation.digest().as_bytes());
    hasher.update(key.as_bytes());
    etag(&format!("\"{}\"", hasher.finalize().to_hex()))
}

fn request_operation<T>(request: &S3Request<T>) -> S3Result<IdempotencyKey> {
    let supplied = request
        .headers
        .get(IDEMPOTENCY_HEADER)
        .or_else(|| request.headers.get(SDK_INVOCATION_HEADER))
        .ok_or_else(|| s3s::s3_error!(InvalidRequest))?
        .to_str()
        .map_err(|_| s3s::s3_error!(InvalidRequest))?;
    if supplied.is_empty() || supplied.len() > 1_024 {
        return Err(s3s::s3_error!(InvalidRequest));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-s3-operation-v1\0");
    hasher.update(supplied.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    Ok(IdempotencyKey::from_bytes(bytes))
}

fn reject_put_extensions(input: &PutObjectInput) -> S3Result {
    if input
        .checksum_algorithm
        .as_ref()
        .is_some_and(|value| value.as_str() != "CRC32")
        || input.checksum_algorithm.is_some() && input.checksum_crc32.is_none()
        || input.acl.is_some()
        || input.bucket_key_enabled.is_some()
        || input.cache_control.is_some()
        || input.checksum_crc32c.is_some()
        || input.checksum_crc64nvme.is_some()
        || input.checksum_sha1.is_some()
        || input.checksum_sha256.is_some()
        || input.content_disposition.is_some()
        || input.content_encoding.is_some()
        || input.content_language.is_some()
        || input.content_md5.is_some()
        || input.content_type.is_some()
        || input.expected_bucket_owner.is_some()
        || input.expires.is_some()
        || input.grant_full_control.is_some()
        || input.grant_read.is_some()
        || input.grant_read_acp.is_some()
        || input.grant_write_acp.is_some()
        || input.if_match.is_some()
        || input.if_none_match.is_some()
        || input.metadata.is_some()
        || input.object_lock_legal_hold_status.is_some()
        || input.object_lock_mode.is_some()
        || input.object_lock_retain_until_date.is_some()
        || input.request_payer.is_some()
        || input.sse_customer_algorithm.is_some()
        || input.sse_customer_key.is_some()
        || input.sse_customer_key_md5.is_some()
        || input.ssekms_encryption_context.is_some()
        || input.ssekms_key_id.is_some()
        || input.server_side_encryption.is_some()
        || input.storage_class.is_some()
        || input.tagging.is_some()
        || input.website_redirect_location.is_some()
        || input.write_offset_bytes.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

fn encode_s3_key(value: &str) -> String {
    urlencoding::encode(value).replace("%2F", "/")
}

fn validate_bucket_name(name: &str) -> S3Result {
    let bytes = name.as_bytes();
    let looks_like_ipv4 =
        name.split('.').count() == 4 && name.split('.').all(|part| part.parse::<u8>().is_ok());
    if !(3..=63).contains(&bytes.len())
        || !bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        || !bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(*byte, b'.' | b'-')
        })
        || name.contains("..")
        || name.contains(".-")
        || name.contains("-.")
        || looks_like_ipv4
    {
        return Err(s3s::s3_error!(InvalidBucketName));
    }
    Ok(())
}

fn s3_error(error: &S3Error) -> s3s::S3Error {
    match error {
        S3Error::InvalidKey
        | S3Error::InvalidRequest(_)
        | S3Error::InvalidContinuation
        | S3Error::IdempotencyConflict => {
            s3s::s3_error!(InvalidArgument)
        }
        S3Error::NotFound => s3s::s3_error!(NoSuchKey),
        S3Error::NotRegularFile => s3s::s3_error!(InvalidObjectState),
        S3Error::ListLimit | S3Error::MultipartLimit | S3Error::WriteConflict => {
            s3s::s3_error!(SlowDown)
        }
        S3Error::MissingPart(_) => s3s::s3_error!(InvalidPart),
        S3Error::UnsupportedNamespace => s3s::s3_error!(NotImplemented),
        S3Error::ForeignGeneration | S3Error::ForeignTransaction => {
            s3s::s3_error!(AccessDenied)
        }
        S3Error::Workspace(crate::WorkspaceError::AccessDenied) => s3s::s3_error!(AccessDenied),
        S3Error::InvalidObject | S3Error::Workspace(_) => s3s::s3_error!(InternalError),
    }
}
