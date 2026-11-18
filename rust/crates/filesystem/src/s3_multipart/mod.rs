//! Persisted multipart upload records and object retention.

use std::collections::BTreeMap;

use acyclic_fs_format::s3_limits::{FilesystemS3Limits, S3MultipartRetentionLimits};
use acyclic_stream::StreamProviderExt as _;
use bytes::Bytes;
use futures::StreamExt as _;

use crate::kernel::{ClosureLimits, DecodeLimits};
use crate::model::FilesystemProfile;
use crate::{AsyncObjectStore, CancellationToken, IdempotencyKey, ObjectId, StagedContent};

pub const MULTIPART_RECORD_VERSION: u8 = 1;

#[derive(Clone, Debug)]
pub struct MultipartPart {
    pub staged: StagedContent,
    pub etag: String,
}

#[derive(Clone, Debug)]
pub enum MultipartTerminal {
    Completing(Vec<(u32, String)>),
    Completed(crate::GenerationId),
    Aborted,
}

#[derive(Clone, Debug)]
pub struct MultipartSnapshot {
    pub key: String,
    pub tail: u64,
    pub minimum_part_bytes: u64,
    pub maximum_parts: Option<u32>,
    pub maximum_part_bytes: Option<u64>,
    pub parts: BTreeMap<u32, MultipartPart>,
    pub operations: BTreeMap<[u8; 16], (u32, String)>,
    pub terminal: Option<MultipartTerminal>,
    pub work: crate::WorkCounters,
}

include!("multipart_record.rs");
include!("multipart_retention.rs");
