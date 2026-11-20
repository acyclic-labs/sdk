// Non-generic S3 request translation and response construction.
//
// Every function here is independent of the workspace backend types, so the
// generic adapter methods stay thin and each backend instantiation does not
// recompile the request and response logic.

#[inline(never)]
pub(super) fn list_objects_v2_request(
    input: &ListObjectsV2Input,
    limits: &FilesystemS3Limits,
) -> S3Result<(S3ListOptions, bool, u32)> {
    if input
        .encoding_type
        .as_ref()
        .is_some_and(|value| value.as_str() != "url")
        || input.fetch_owner == Some(true)
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    let encode = input.encoding_type.is_some();
    let maximum_keys = input.max_keys.unwrap_or(1_000);
    let maximum_keys = u32::try_from(maximum_keys)
        .map_err(|_| s3s::s3_error!(InvalidArgument))?
        .min(limits.maximum_list_keys);
    let delimiter = match input.delimiter.as_deref() {
        None | Some("") => None,
        Some("/") => Some('/'),
        Some(_) => return Err(s3s::s3_error!(NotImplemented)),
    };
    let continuation = input
        .continuation_token
        .as_deref()
        .map(S3ListCursor::decode)
        .transpose()
        .map_err(|error| s3_error(&error))?;
    let options = S3ListOptions {
        prefix: input.prefix.clone().unwrap_or_default(),
        delimiter,
        start_after: input.start_after.clone(),
        continuation,
        maximum_keys,
    };
    Ok((options, encode, maximum_keys))
}

#[inline(never)]
pub(super) fn list_objects_v2_response(
    input: ListObjectsV2Input,
    page: S3List,
    encode: bool,
    maximum_keys: u32,
) -> S3Result<ListObjectsV2Output> {
    let key_count = page
        .objects
        .len()
        .saturating_add(page.common_prefixes.len());
    let contents = page
        .objects
        .into_iter()
        .map(|value| {
            Ok(Object {
                e_tag: Some(etag(&value.etag)?),
                key: Some(if encode {
                    encode_s3_key(&value.key)
                } else {
                    value.key
                }),
                size: Some(
                    i64::try_from(value.content_length)
                        .map_err(|_| s3s::s3_error!(InternalError))?,
                ),
                ..Object::default()
            })
        })
        .collect::<S3Result<Vec<_>>>()?;
    let common_prefixes = page
        .common_prefixes
        .into_iter()
        .map(|prefix| CommonPrefix {
            prefix: Some(if encode {
                encode_s3_key(&prefix)
            } else {
                prefix
            }),
        })
        .collect::<Vec<_>>();
    let next = page.next_continuation.map(|cursor| cursor.encode());
    Ok(ListObjectsV2Output {
        common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
        contents: (!contents.is_empty()).then_some(contents),
        continuation_token: input.continuation_token,
        delimiter: input
            .delimiter
            .map(|value| if encode { encode_s3_key(&value) } else { value }),
        encoding_type: input.encoding_type,
        is_truncated: Some(next.is_some()),
        key_count: Some(i32::try_from(key_count).map_err(|_| s3s::s3_error!(InternalError))?),
        max_keys: Some(i32::try_from(maximum_keys).map_err(|_| s3s::s3_error!(InternalError))?),
        name: Some(input.bucket),
        next_continuation_token: next,
        prefix: input
            .prefix
            .map(|value| if encode { encode_s3_key(&value) } else { value }),
        start_after: input
            .start_after
            .map(|value| if encode { encode_s3_key(&value) } else { value }),
        ..ListObjectsV2Output::default()
    })
}

#[inline(never)]
pub(super) fn list_objects_request(
    input: &ListObjectsInput,
    limits: &FilesystemS3Limits,
) -> S3Result<(S3ListOptions, bool, u32, Option<char>)> {
    if input
        .encoding_type
        .as_ref()
        .is_some_and(|value| value.as_str() != "url")
        || input.expected_bucket_owner.is_some()
        || input.optional_object_attributes.is_some()
        || input.request_payer.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    let encode = input.encoding_type.is_some();
    let delimiter = match input.delimiter.as_deref() {
        None | Some("") => None,
        Some("/") => Some('/'),
        Some(_) => return Err(s3s::s3_error!(NotImplemented)),
    };
    let maximum_keys = u32::try_from(input.max_keys.unwrap_or(1_000))
        .map_err(|_| s3s::s3_error!(InvalidArgument))?
        .min(limits.maximum_list_keys);
    let options = S3ListOptions {
        prefix: input.prefix.clone().unwrap_or_default(),
        delimiter,
        start_after: input.marker.clone(),
        maximum_keys,
        ..S3ListOptions::default()
    };
    Ok((options, encode, maximum_keys, delimiter))
}

#[inline(never)]
pub(super) fn list_objects_response(
    input: ListObjectsInput,
    page: S3List,
    encode: bool,
    maximum_keys: u32,
    delimiter: Option<char>,
) -> S3Result<ListObjectsOutput> {
    let truncated = page.next_continuation.is_some();
    let next_marker = if truncated {
        page.objects
            .last()
            .map(|item| item.key.as_str())
            .into_iter()
            .chain(page.common_prefixes.last().map(String::as_str))
            .max()
            .map(str::to_owned)
    } else {
        None
    };
    let contents = page
        .objects
        .into_iter()
        .map(|item| {
            Ok(Object {
                e_tag: Some(etag(&item.etag)?),
                key: Some(if encode {
                    encode_s3_key(&item.key)
                } else {
                    item.key
                }),
                size: Some(
                    i64::try_from(item.content_length)
                        .map_err(|_| s3s::s3_error!(InternalError))?,
                ),
                ..Object::default()
            })
        })
        .collect::<S3Result<Vec<_>>>()?;
    let common_prefixes = page
        .common_prefixes
        .into_iter()
        .map(|prefix| CommonPrefix {
            prefix: Some(if encode {
                encode_s3_key(&prefix)
            } else {
                prefix
            }),
        })
        .collect::<Vec<_>>();
    Ok(ListObjectsOutput {
        common_prefixes: (!common_prefixes.is_empty()).then_some(common_prefixes),
        contents: (!contents.is_empty()).then_some(contents),
        delimiter: input.delimiter,
        encoding_type: input.encoding_type,
        is_truncated: Some(truncated),
        marker: input
            .marker
            .map(|value| if encode { encode_s3_key(&value) } else { value }),
        max_keys: Some(i32::try_from(maximum_keys).map_err(|_| s3s::s3_error!(InternalError))?),
        name: Some(input.bucket),
        next_marker: next_marker
            .filter(|_| delimiter.is_some())
            .map(|value| if encode { encode_s3_key(&value) } else { value }),
        prefix: input
            .prefix
            .map(|value| if encode { encode_s3_key(&value) } else { value }),
        ..ListObjectsOutput::default()
    })
}

#[inline(never)]
pub(super) fn list_object_versions_request(
    input: &ListObjectVersionsInput,
    limits: &FilesystemS3Limits,
) -> S3Result<(S3ListOptions, bool, u32)> {
    if input.delimiter.is_some()
        || input
            .encoding_type
            .as_ref()
            .is_some_and(|value| value.as_str() != "url")
        || input.expected_bucket_owner.is_some()
        || input.optional_object_attributes.is_some()
        || input.request_payer.is_some()
        || input
            .version_id_marker
            .as_deref()
            .is_some_and(|id| id != "null")
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    let encode = input.encoding_type.is_some();
    let maximum_keys = u32::try_from(input.max_keys.unwrap_or(1_000))
        .map_err(|_| s3s::s3_error!(InvalidArgument))?
        .min(limits.maximum_list_keys);
    let options = S3ListOptions {
        prefix: input.prefix.clone().unwrap_or_default(),
        delimiter: None,
        start_after: input.key_marker.clone(),
        maximum_keys,
        ..S3ListOptions::default()
    };
    Ok((options, encode, maximum_keys))
}

#[inline(never)]
pub(super) fn list_object_versions_response(
    input: ListObjectVersionsInput,
    page: S3List,
    encode: bool,
    maximum_keys: u32,
) -> S3Result<ListObjectVersionsOutput> {
    let truncated = page.next_continuation.is_some();
    let next_key_marker = truncated
        .then(|| page.objects.last().map(|item| item.key.clone()))
        .flatten()
        .map(|key| if encode { encode_s3_key(&key) } else { key });
    let versions = page
        .objects
        .into_iter()
        .map(|item| {
            Ok(ObjectVersion {
                e_tag: Some(etag(&item.etag)?),
                is_latest: Some(true),
                key: Some(if encode {
                    encode_s3_key(&item.key)
                } else {
                    item.key
                }),
                size: Some(
                    i64::try_from(item.content_length)
                        .map_err(|_| s3s::s3_error!(InternalError))?,
                ),
                version_id: Some("null".to_owned()),
                ..ObjectVersion::default()
            })
        })
        .collect::<S3Result<Vec<_>>>()?;
    Ok(ListObjectVersionsOutput {
        is_truncated: Some(truncated),
        encoding_type: input.encoding_type,
        key_marker: input
            .key_marker
            .map(|value| if encode { encode_s3_key(&value) } else { value }),
        max_keys: Some(i32::try_from(maximum_keys).map_err(|_| s3s::s3_error!(InternalError))?),
        name: Some(input.bucket),
        next_key_marker,
        next_version_id_marker: truncated.then(|| "null".to_owned()),
        prefix: input
            .prefix
            .map(|value| if encode { encode_s3_key(&value) } else { value }),
        versions: Some(versions),
        ..ListObjectVersionsOutput::default()
    })
}

#[inline(never)]
pub(super) fn head_object_response(
    input: &HeadObjectInput,
    etag_value: &str,
    content_length: u64,
) -> S3Result<HeadObjectOutput> {
    check_conditions(
        etag_value,
        input.if_match.as_ref(),
        input.if_none_match.as_ref(),
    )?;
    let selected = select_range(content_length, input.range.as_ref())?;
    Ok(HeadObjectOutput {
        accept_ranges: Some("bytes".to_owned()),
        content_length: Some(
            i64::try_from(selected.length).map_err(|_| s3s::s3_error!(InternalError))?,
        ),
        content_range: selected.content_range,
        e_tag: Some(etag(etag_value)?),
        ..HeadObjectOutput::default()
    })
}

#[inline(never)]
pub(super) fn list_parts_response(
    input: ListPartsInput,
    snapshot: &MultipartSnapshot,
    maximum_list_keys: u32,
) -> S3Result<ListPartsOutput> {
    let marker = input.part_number_marker.unwrap_or(0);
    let maximum = input
        .max_parts
        .unwrap_or(1_000)
        .clamp(0, i32::try_from(maximum_list_keys).unwrap_or(i32::MAX));
    let maximum = usize::try_from(maximum).map_err(|_| s3s::s3_error!(InvalidArgument))?;
    let mut selected = snapshot.parts.iter().filter(|(number, _)| {
        i32::try_from(**number)
            .ok()
            .is_some_and(|value| value > marker)
    });
    let values = selected
        .by_ref()
        .take(maximum)
        .map(|(number, part)| {
            Ok(Part {
                e_tag: Some(etag(&part.etag)?),
                part_number: Some(
                    i32::try_from(*number).map_err(|_| s3s::s3_error!(InternalError))?,
                ),
                size: Some(
                    i64::try_from(part.staged.logical_bytes())
                        .map_err(|_| s3s::s3_error!(InternalError))?,
                ),
                ..Part::default()
            })
        })
        .collect::<S3Result<Vec<_>>>()?;
    let truncated = selected.next().is_some();
    let next_marker = truncated
        .then(|| values.last().and_then(|part| part.part_number))
        .flatten();
    Ok(ListPartsOutput {
        bucket: Some(input.bucket),
        key: Some(input.key),
        upload_id: Some(input.upload_id),
        max_parts: Some(i32::try_from(maximum).map_err(|_| s3s::s3_error!(InternalError))?),
        part_number_marker: Some(marker),
        next_part_number_marker: next_marker,
        is_truncated: Some(truncated),
        parts: (!values.is_empty()).then_some(values),
        ..ListPartsOutput::default()
    })
}

#[inline(never)]
pub(super) fn delete_objects_keys(input: &DeleteObjectsInput) -> S3Result<Vec<String>> {
    input
        .delete
        .objects
        .iter()
        .map(|object| {
            if object.version_id.as_deref().is_some_and(|id| id != "null") {
                return Err(s3s::s3_error!(NoSuchVersion));
            }
            Ok(object.key.clone())
        })
        .collect()
}

#[inline(never)]
pub(super) fn delete_objects_response(input: &DeleteObjectsInput) -> DeleteObjectsOutput {
    let deleted = (!input.delete.quiet.unwrap_or(false)).then(|| {
        input
            .delete
            .objects
            .iter()
            .map(|object| DeletedObject {
                key: Some(object.key.clone()),
                version_id: object.version_id.clone(),
                ..DeletedObject::default()
            })
            .collect()
    });
    DeleteObjectsOutput {
        deleted,
        ..DeleteObjectsOutput::default()
    }
}

#[inline(never)]
pub(super) fn copy_object_source(input: &CopyObjectInput) -> S3Result<(&str, &str)> {
    match &input.copy_source {
        s3s::dto::CopySource::Bucket {
            bucket,
            key,
            version_id: None,
        } => Ok((bucket.as_ref(), key.as_ref())),
        s3s::dto::CopySource::Bucket {
            version_id: Some(_),
            ..
        }
        | s3s::dto::CopySource::AccessPoint { .. }
        | s3s::dto::CopySource::Outpost { .. } => Err(s3s::s3_error!(NotImplemented)),
    }
}

#[inline(never)]
pub(super) fn copy_object_response(
    input: &CopyObjectInput,
    generation: acyclic_fs_format::object_reference::GenerationId,
) -> S3Result<CopyObjectOutput> {
    Ok(CopyObjectOutput {
        copy_object_result: Some(CopyObjectResult {
            e_tag: Some(etag_for_generation(generation, &input.key)?),
            ..CopyObjectResult::default()
        }),
        ..CopyObjectOutput::default()
    })
}

#[inline(never)]
pub(super) fn create_multipart_upload_response(
    input: CreateMultipartUploadInput,
    upload_id: String,
) -> CreateMultipartUploadOutput {
    CreateMultipartUploadOutput {
        bucket: Some(input.bucket),
        key: Some(input.key),
        upload_id: Some(upload_id),
        ..CreateMultipartUploadOutput::default()
    }
}

#[inline(never)]
pub(super) fn list_buckets_response(mut names: Vec<String>) -> S3Result<ListBucketsOutput> {
    names.sort_unstable();
    if names.windows(2).any(|pair| pair.first() == pair.last()) {
        return Err(s3s::s3_error!(InternalError));
    }
    Ok(ListBucketsOutput {
        buckets: Some(
            names
                .into_iter()
                .map(|name| Bucket {
                    name: Some(name),
                    ..Bucket::default()
                })
                .collect(),
        ),
        ..ListBucketsOutput::default()
    })
}

#[inline(never)]
pub(super) fn create_bucket_validate(input: &CreateBucketInput) -> S3Result {
    if input.acl.is_some()
        || input.create_bucket_configuration.is_some()
        || input.grant_full_control.is_some()
        || input.grant_read.is_some()
        || input.grant_read_acp.is_some()
        || input.grant_write.is_some()
        || input.grant_write_acp.is_some()
        || input.object_lock_enabled_for_bucket.is_some()
        || input.object_ownership.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

#[inline(never)]
pub(super) fn list_buckets_validate(input: &ListBucketsInput) -> S3Result {
    if input.bucket_region.is_some()
        || input.continuation_token.is_some()
        || input.max_buckets.is_some()
        || input.prefix.is_some()
    {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

#[inline(never)]
pub(super) fn upload_part_validate(
    input: &UploadPartInput,
    limits: &FilesystemS3Limits,
) -> S3Result<u32> {
    let part_number =
        u32::try_from(input.part_number).map_err(|_| s3s::s3_error!(InvalidArgument))?;
    if part_number == 0
        || limits
            .maximum_multipart_parts
            .is_some_and(|maximum| part_number > maximum)
    {
        return Err(s3s::s3_error!(InvalidArgument));
    }
    Ok(part_number)
}

#[inline(never)]
pub(super) fn upload_part_response(
    input: &UploadPartInput,
    etag_value: &str,
) -> S3Result<UploadPartOutput> {
    Ok(UploadPartOutput {
        checksum_crc32: input.checksum_crc32.clone(),
        e_tag: Some(etag(etag_value)?),
        ..UploadPartOutput::default()
    })
}

#[inline(never)]
pub(super) fn complete_multipart_response(
    input: &CompleteMultipartUploadInput,
    generation: acyclic_fs_format::object_reference::GenerationId,
) -> S3Result<CompleteMultipartUploadOutput> {
    Ok(CompleteMultipartUploadOutput {
        bucket: Some(input.bucket.clone()),
        key: Some(input.key.clone()),
        e_tag: Some(etag_for_generation(generation, &input.key)?),
        ..CompleteMultipartUploadOutput::default()
    })
}

#[inline(never)]
pub(super) fn delete_bucket_validate(input: &DeleteBucketInput) -> S3Result {
    if input.expected_bucket_owner.is_some() {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}

#[inline(never)]
pub(super) fn abort_multipart_validate(input: &AbortMultipartUploadInput) -> S3Result {
    if input.if_match_initiated_time.is_some() {
        return Err(s3s::s3_error!(NotImplemented));
    }
    Ok(())
}
