//! Candidate Rust-owned Objects v2 typed RPC observations.
//!
//! This module intentionally talks to the production `MemoryObjects` provider
//! and encodes the generated Objects wire messages.  It is an integration
//! candidate for `sdk-examples`; it does not invent response values or use the
//! fixture server's default responses.

use acyclic_objects::{MemoryObjects, ObjectsProvider, wire};
use bytes::Bytes;
use prost::Message;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const FAMILY: &str = "objects";
const BUCKET: &str = "typed-objects";
const DELETE_BUCKET: &str = "typed-delete";
type CandidateResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

/// Execute the complete Objects qualification sequence against the production
/// Rust provider.  The sequence contains the thirteen unique RPCs plus the
/// setup calls and stream-validation failures used by the remote fixture.
async fn collect_full_qualification() -> CandidateResult<Vec<Value>> {
    let (provider, _bucket) =
        MemoryObjects::with_default_bucket_clock(crate::fixtures::fixture_clock::objects_clock());
    let created = wire::CreateBucketRequest {
        name: BUCKET.to_owned(),
        mutation: None,
    };
    let created_response = provider.create_bucket(created.clone()).await?;
    let created_ref = created_response
        .bucket
        .clone()
        .expect("created bucket reference");
    let mut observations = vec![unary(
        "acyclic.objects.v2.BucketsService/CreateBucket",
        "acyclic.objects.v2.CreateBucketRequest",
        &created,
        "acyclic.objects.v2.Bucket",
        &created_response,
    )];

    let head = wire::HeadBucketRequest {
        bucket: Some(created_ref.clone()),
    };
    let head_response = provider.head_bucket(head.clone()).await?;
    observations.push(unary(
        "acyclic.objects.v2.BucketsService/HeadBucket",
        "acyclic.objects.v2.HeadBucketRequest",
        &head,
        "acyclic.objects.v2.Bucket",
        &head_response,
    ));

    let empty = wire::CreateBucketRequest {
        name: DELETE_BUCKET.to_owned(),
        mutation: None,
    };
    let empty_response = provider.create_bucket(empty).await?;
    observations.push(unary(
        "acyclic.objects.v2.BucketsService/CreateBucket",
        "acyclic.objects.v2.CreateBucketRequest",
        &wire::CreateBucketRequest {
            name: DELETE_BUCKET.to_owned(),
            mutation: None,
        },
        "acyclic.objects.v2.Bucket",
        &empty_response,
    ));
    let delete_bucket = wire::DeleteBucketRequest {
        bucket: empty_response.bucket,
        mutation: None,
    };
    let delete_bucket_response = provider.delete_bucket(delete_bucket.clone()).await?;
    observations.push(unary(
        "acyclic.objects.v2.BucketsService/DeleteBucket",
        "acyclic.objects.v2.DeleteBucketRequest",
        &delete_bucket,
        "acyclic.objects.v2.DeleteBucketResponse",
        &delete_bucket_response,
    ));

    let put_header = wire::PutObjectHeader {
        bucket: Some(created_ref.clone()),
        object_key: "hello.txt".to_owned(),
        ..Default::default()
    };
    let put_request = wire::PutObjectRequest {
        frame: Some(wire::put_object_request::Frame::Header(put_header.clone())),
    };
    let put_body = wire::PutObjectRequest {
        frame: Some(wire::put_object_request::Frame::Body(b"hello".to_vec())),
    };
    let put_complete = wire::PutObjectRequest {
        frame: Some(wire::put_object_request::Frame::Complete(true)),
    };
    let put_response = provider
        .put(put_header, Bytes::from_static(b"hello"))
        .await?;
    observations.push(streaming(
        "acyclic.objects.v2.ObjectsService/PutObject",
        "acyclic.objects.v2.PutObjectRequest",
        &[put_request, put_body, put_complete],
        "acyclic.objects.v2.ObjectInfo",
        &[put_response],
    ));

    let get = wire::GetObjectRequest {
        bucket: Some(created_ref.clone()),
        object_key: "hello.txt".to_owned(),
        ..Default::default()
    };
    let object = provider.get(get.clone(), 1024).await?;
    let header_frame = wire::GetObjectResponse {
        frame: Some(wire::get_object_response::Frame::Header(object.header)),
    };
    let body_frame = wire::GetObjectResponse {
        frame: Some(wire::get_object_response::Frame::Body(object.body.to_vec())),
    };
    observations.push(streaming(
        "acyclic.objects.v2.ObjectsService/GetObject",
        "acyclic.objects.v2.GetObjectRequest",
        std::slice::from_ref(&get),
        "acyclic.objects.v2.GetObjectResponse",
        &[header_frame, body_frame],
    ));

    let head_object = wire::HeadObjectRequest {
        bucket: Some(created_ref.clone()),
        object_key: "hello.txt".to_owned(),
        ..Default::default()
    };
    let head_object_response = provider.head(head_object.clone()).await?;
    observations.push(unary(
        "acyclic.objects.v2.ObjectsService/HeadObject",
        "acyclic.objects.v2.HeadObjectRequest",
        &head_object,
        "acyclic.objects.v2.HeadObjectResponse",
        &head_object_response,
    ));

    let list = wire::ListObjectsRequest {
        bucket: Some(created_ref.clone()),
        page_size: 100,
        ..Default::default()
    };
    let list_response = provider.list(list.clone()).await?;
    observations.push(unary(
        "acyclic.objects.v2.ObjectsService/ListObjects",
        "acyclic.objects.v2.ListObjectsRequest",
        &list,
        "acyclic.objects.v2.ListObjectsResponse",
        &list_response,
    ));

    let delete_object = wire::DeleteObjectRequest {
        bucket: Some(created_ref.clone()),
        object_key: "hello.txt".to_owned(),
        ..Default::default()
    };
    let delete_object_response = provider.delete(delete_object.clone()).await?;
    observations.push(unary(
        "acyclic.objects.v2.ObjectsService/DeleteObject",
        "acyclic.objects.v2.DeleteObjectRequest",
        &delete_object,
        "acyclic.objects.v2.DeleteObjectResponse",
        &delete_object_response,
    ));

    let create_multipart = wire::CreateMultipartRequest {
        bucket: Some(created_ref.clone()),
        object_key: "multipart.bin".to_owned(),
        ..Default::default()
    };
    let multipart = provider.create_multipart(create_multipart.clone()).await?;
    observations.push(unary(
        "acyclic.objects.v2.MultipartService/CreateMultipart",
        "acyclic.objects.v2.CreateMultipartRequest",
        &create_multipart,
        "acyclic.objects.v2.MultipartUpload",
        &multipart,
    ));

    let upload_header = wire::UploadPartHeader {
        bucket: Some(created_ref.clone()),
        object_key: "multipart.bin".to_owned(),
        upload_id: multipart.upload_id.clone(),
        part_number: 1,
        mutation: None,
    };
    let upload_request = wire::UploadPartRequest {
        frame: Some(wire::upload_part_request::Frame::Header(
            upload_header.clone(),
        )),
    };
    let upload_body = wire::UploadPartRequest {
        frame: Some(wire::upload_part_request::Frame::Body(vec![
            b'p';
            5 * 1024 * 1024
        ])),
    };
    let upload_complete = wire::UploadPartRequest {
        frame: Some(wire::upload_part_request::Frame::Complete(true)),
    };
    let part_body = Bytes::from(vec![b'p'; 5 * 1024 * 1024]);
    let uploaded = provider.upload_part(upload_header, part_body).await?;
    observations.push(streaming(
        "acyclic.objects.v2.MultipartService/UploadPart",
        "acyclic.objects.v2.UploadPartRequest",
        &[upload_request, upload_body.clone(), upload_complete.clone()],
        "acyclic.objects.v2.UploadedPart",
        std::slice::from_ref(&uploaded),
    ));

    let list_parts = wire::ListPartsRequest {
        bucket: Some(created_ref.clone()),
        object_key: "multipart.bin".to_owned(),
        upload_id: multipart.upload_id.clone(),
        page_size: 100,
        ..Default::default()
    };
    let list_parts_response = provider.list_parts(list_parts.clone()).await?;
    observations.push(unary(
        "acyclic.objects.v2.MultipartService/ListParts",
        "acyclic.objects.v2.ListPartsRequest",
        &list_parts,
        "acyclic.objects.v2.ListPartsResponse",
        &list_parts_response,
    ));

    let complete = wire::CompleteMultipartRequest {
        bucket: Some(created_ref.clone()),
        object_key: "multipart.bin".to_owned(),
        upload_id: multipart.upload_id.clone(),
        parts: vec![uploaded],
        ..Default::default()
    };
    let complete_response = provider.complete_multipart(complete.clone()).await?;
    observations.push(unary(
        "acyclic.objects.v2.MultipartService/CompleteMultipart",
        "acyclic.objects.v2.CompleteMultipartRequest",
        &complete,
        "acyclic.objects.v2.ObjectInfo",
        &complete_response,
    ));

    let abort_upload = provider
        .create_multipart(wire::CreateMultipartRequest {
            bucket: Some(created_response.bucket.expect("created bucket reference")),
            object_key: "abort.bin".to_owned(),
            ..Default::default()
        })
        .await?;
    let abort_create = wire::CreateMultipartRequest {
        bucket: Some(created_ref.clone()),
        object_key: "abort.bin".to_owned(),
        ..Default::default()
    };
    observations.push(unary(
        "acyclic.objects.v2.MultipartService/CreateMultipart",
        "acyclic.objects.v2.CreateMultipartRequest",
        &abort_create,
        "acyclic.objects.v2.MultipartUpload",
        &abort_upload,
    ));
    let abort = wire::AbortMultipartRequest {
        bucket: Some(created_ref.clone()),
        object_key: "abort.bin".to_owned(),
        upload_id: abort_upload.upload_id,
        mutation: None,
    };
    let abort_response = provider.abort_multipart(abort.clone()).await?;
    observations.push(unary(
        "acyclic.objects.v2.MultipartService/AbortMultipart",
        "acyclic.objects.v2.AbortMultipartRequest",
        &abort,
        "acyclic.objects.v2.AbortMultipartResponse",
        &abort_response,
    ));

    let after_put_header = wire::PutObjectHeader {
        bucket: Some(created_ref.clone()),
        object_key: "after-completion.txt".to_owned(),
        ..Default::default()
    };
    let after_put = vec![
        wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Header(after_put_header)),
        },
        wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Body(b"hello".to_vec())),
        },
        wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Complete(true)),
        },
        wire::PutObjectRequest {
            frame: Some(wire::put_object_request::Frame::Body(b"late".to_vec())),
        },
    ];
    observations.push(stream_error(
        "acyclic.objects.v2.ObjectsService/PutObject/after-completion",
        "acyclic.objects.v2.PutObjectRequest",
        &after_put,
        "PutObject requires header, body, and completion frames",
    ));

    let negative_create = wire::CreateMultipartRequest {
        bucket: Some(created_ref.clone()),
        object_key: "negative-upload.bin".to_owned(),
        ..Default::default()
    };
    let negative_upload = provider.create_multipart(negative_create.clone()).await?;
    observations.push(unary(
        "acyclic.objects.v2.MultipartService/CreateMultipart",
        "acyclic.objects.v2.CreateMultipartRequest",
        &negative_create,
        "acyclic.objects.v2.MultipartUpload",
        &negative_upload,
    ));
    let negative_header = wire::UploadPartHeader {
        bucket: Some(created_ref),
        object_key: "negative-upload.bin".to_owned(),
        upload_id: negative_upload.upload_id,
        part_number: 1,
        mutation: None,
    };
    let negative_upload_frames = vec![
        wire::UploadPartRequest {
            frame: Some(wire::upload_part_request::Frame::Header(negative_header)),
        },
        upload_body.clone(),
        upload_complete.clone(),
        upload_body,
    ];
    observations.push(stream_error(
        "acyclic.objects.v2.MultipartService/UploadPart/after-completion",
        "acyclic.objects.v2.UploadPartRequest",
        &negative_upload_frames,
        "UploadPart requires header, body, and completion frames",
    ));

    debug_assert_eq!(observations.len(), 18);
    Ok(observations)
}

/// Execute the thirteen unique Objects v2 RPCs for the canonical manifest.
/// Repeated setup calls and protocol-negative cases belong to the full remote
/// qualification sequence and are available through
/// [`collect_network_qualification`].
pub async fn collect() -> CandidateResult<Vec<Value>> {
    let records = collect_full_qualification().await?;
    let mut identities = std::collections::BTreeSet::new();
    Ok(records
        .into_iter()
        .filter(|record| {
            record["rpc"]
                .as_str()
                .is_some_and(|rpc| !rpc.ends_with("/after-completion"))
        })
        .filter(|record| {
            record["rpc"]
                .as_str()
                .is_some_and(|rpc| identities.insert(rpc.to_owned()))
        })
        .collect())
}

/// Return the complete eighteen-call source-owned sequence consumed by the
/// remote fixture qualification runner.
pub async fn collect_network_qualification() -> CandidateResult<Vec<Value>> {
    collect_full_qualification().await
}

/// Reduce a qualification receipt to its source-owned protocol shape.  This
/// intentionally omits generated upload IDs and seeded response bytes so two
/// fresh provider instances can be compared for repeatability.
pub fn normalized_network_signature(records: &[Value]) -> Vec<Value> {
    records
        .iter()
        .map(|record| {
            let frame_types = |key: &str| {
                record[key]
                    .as_array()
                    .map(|frames| {
                        frames
                            .iter()
                            .map(|frame| frame["type"].clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            };
            json!({
                "rpc": record["rpc"],
                "request_type": record["request"]["type"],
                "request_frame_types": frame_types("request_frames"),
                "response_type": record["response"]["type"],
                "response_frame_types": frame_types("response_frames"),
                "status": record["response"]["status"],
                "code": record["response"]["code"],
                "terminal_code": record["terminal_code"],
            })
        })
        .collect()
}

fn unary<I: Message, O: Message>(
    rpc: &str,
    request_type: &str,
    request: &I,
    response_type: &str,
    response: &O,
) -> Value {
    let request_bytes = request.encode_to_vec();
    let response_bytes = response.encode_to_vec();
    json!({
        "family": FAMILY,
        "rpc": rpc,
        "source": "rust/crates/sdk-examples/src/fixtures/objects_typed_scenarios.rs",
        "execution_mode": "inprocess",
        "scope": "rust-memory-provider",
        "request": encoded(request_type, &request_bytes),
        "response_frames": [{
            "sequence": 0,
            "type": response_type,
            "bytes_base64": base64(&response_bytes),
            "sha256": digest(&response_bytes),
        }],
        "response": {
            "type": response_type,
            "bytes_base64": base64(&response_bytes),
            "sha256": digest(&response_bytes),
            "status": "ok",
        },
    })
}

fn streaming<I: Message, O: Message>(
    rpc: &str,
    request_type: &str,
    requests: &[I],
    response_type: &str,
    frames: &[O],
) -> Value {
    let request_bytes = requests
        .first()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    let request_frames = requests
        .iter()
        .enumerate()
        .map(|(sequence, request)| {
            let bytes = request.encode_to_vec();
            json!({
                "sequence": sequence,
                "type": request_type,
                "bytes_base64": base64(&bytes),
                "sha256": digest(&bytes),
            })
        })
        .collect::<Vec<_>>();
    let response_frames = frames
        .iter()
        .enumerate()
        .map(|(sequence, frame)| {
            let bytes = frame.encode_to_vec();
            json!({
                "sequence": sequence,
                "type": response_type,
                "bytes_base64": base64(&bytes),
                "sha256": digest(&bytes),
            })
        })
        .collect::<Vec<_>>();
    let first = frames
        .first()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    json!({
        "family": FAMILY,
        "rpc": rpc,
        "source": "rust/crates/sdk-examples/src/fixtures/objects_typed_scenarios.rs",
        "execution_mode": "inprocess",
        "scope": "rust-memory-provider",
        "request": encoded(request_type, &request_bytes),
        "request_frames": request_frames,
        "response_frames": response_frames,
        "response": {
            "type": response_type,
            "bytes_base64": base64(&first),
            "sha256": digest(&first),
            "status": "ok",
        },
    })
}

fn stream_error<I: Message>(rpc: &str, request_type: &str, requests: &[I], details: &str) -> Value {
    let request_bytes = requests
        .first()
        .map(Message::encode_to_vec)
        .unwrap_or_default();
    let request_frames = requests
        .iter()
        .enumerate()
        .map(|(sequence, request)| {
            let bytes = request.encode_to_vec();
            json!({
                "sequence": sequence,
                "type": request_type,
                "bytes_base64": base64(&bytes),
                "sha256": digest(&bytes),
            })
        })
        .collect::<Vec<_>>();
    json!({
        "family": FAMILY,
        "rpc": rpc,
        "source": "rust/crates/sdk-examples/src/fixtures/objects_typed_scenarios.rs",
        "execution_mode": "protocol-validation",
        "scope": "objects-stream-collector",
        "request": encoded(request_type, &request_bytes),
        "request_frames": request_frames,
        "response_frames": [],
        "response": {
            "status": "error",
            "code": "INVALID_ARGUMENT",
            "details": details,
        },
        "terminal_code": 3,
    })
}

fn encoded(message_type: &str, bytes: &[u8]) -> Value {
    json!({
        "type": message_type,
        "bytes_base64": base64(bytes),
        "sha256": digest(bytes),
    })
}

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = chunk.get(1).copied().unwrap_or(0);
        let c = chunk.get(2).copied().unwrap_or(0);
        output.push(TABLE[(a >> 2) as usize] as char);
        output.push(TABLE[((a & 0x03) << 4 | b >> 4) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[((b & 0x0f) << 2 | c >> 6) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(c & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    output
}

#[cfg(test)]
mod tests {
    use super::{collect, collect_network_qualification, normalized_network_signature};

    #[tokio::test]
    async fn emits_all_objects_rpcs_with_real_response_frames() {
        let records = collect().await.expect("Objects provider collector");
        assert_eq!(records.len(), 13);
        let get = records
            .iter()
            .find(|record| record["rpc"] == "acyclic.objects.v2.ObjectsService/GetObject")
            .expect("GetObject record");
        assert_eq!(get["response_frames"].as_array().unwrap().len(), 2);
        let put = records
            .iter()
            .find(|record| record["rpc"] == "acyclic.objects.v2.ObjectsService/PutObject")
            .expect("PutObject record");
        assert_eq!(put["request_frames"].as_array().unwrap().len(), 3);
        let upload = records
            .iter()
            .find(|record| record["rpc"] == "acyclic.objects.v2.MultipartService/UploadPart")
            .expect("UploadPart record");
        assert_eq!(upload["request_frames"].as_array().unwrap().len(), 3);
        assert!(records.iter().all(|record| {
            record["request"]["bytes_base64"]
                .as_str()
                .is_some_and(|bytes| !bytes.is_empty())
                && record["response_frames"]
                    .as_array()
                    .is_some_and(|frames| !frames.is_empty())
        }));
        let mut identities = std::collections::BTreeSet::new();
        for record in records {
            assert!(identities.insert(record["rpc"].as_str().unwrap().to_owned()));
        }
    }

    #[tokio::test]
    async fn emits_full_network_sequence_and_repeats_by_protocol_shape() {
        let first = collect_network_qualification()
            .await
            .expect("full Objects qualification sequence");
        let second = collect_network_qualification()
            .await
            .expect("repeat Objects qualification sequence");
        assert_eq!(first.len(), 18);
        assert_eq!(second.len(), 18);
        assert_eq!(
            normalized_network_signature(&first),
            normalized_network_signature(&second)
        );

        let rpcs = first
            .iter()
            .map(|record| record["rpc"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            rpcs,
            vec![
                "acyclic.objects.v2.BucketsService/CreateBucket",
                "acyclic.objects.v2.BucketsService/HeadBucket",
                "acyclic.objects.v2.BucketsService/CreateBucket",
                "acyclic.objects.v2.BucketsService/DeleteBucket",
                "acyclic.objects.v2.ObjectsService/PutObject",
                "acyclic.objects.v2.ObjectsService/GetObject",
                "acyclic.objects.v2.ObjectsService/HeadObject",
                "acyclic.objects.v2.ObjectsService/ListObjects",
                "acyclic.objects.v2.ObjectsService/DeleteObject",
                "acyclic.objects.v2.MultipartService/CreateMultipart",
                "acyclic.objects.v2.MultipartService/UploadPart",
                "acyclic.objects.v2.MultipartService/ListParts",
                "acyclic.objects.v2.MultipartService/CompleteMultipart",
                "acyclic.objects.v2.MultipartService/CreateMultipart",
                "acyclic.objects.v2.MultipartService/AbortMultipart",
                "acyclic.objects.v2.ObjectsService/PutObject/after-completion",
                "acyclic.objects.v2.MultipartService/CreateMultipart",
                "acyclic.objects.v2.MultipartService/UploadPart/after-completion",
            ]
        );
        let negatives = first
            .iter()
            .filter(|record| record["response"]["status"] == "error")
            .collect::<Vec<_>>();
        assert_eq!(negatives.len(), 2);
        assert!(negatives.iter().all(|record| {
            record["response"]["code"] == "INVALID_ARGUMENT"
                && record["terminal_code"] == 3
                && record["response_frames"]
                    .as_array()
                    .is_some_and(Vec::is_empty)
        }));
    }
}
