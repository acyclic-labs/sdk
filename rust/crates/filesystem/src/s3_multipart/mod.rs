//! Persisted multipart upload records and object retention.

use std::{collections::BTreeMap, sync::Arc};

use acyclic_fs_format::s3_limits::{FilesystemS3Limits, S3MultipartRetentionLimits};
use acyclic_stream::StreamProviderExt as _;
use bytes::Bytes;
use futures::StreamExt as _;

use crate::kernel::{ClosureLimits, DecodeLimits};
use crate::model::FilesystemProfile;
use crate::{
    AsyncAuthorityStore, AsyncObjectStore, CancellationToken, IdempotencyKey, ObjectId,
    StagedContent, StreamAuthorityStore, Workspace,
};

/// Authority adapter that retains filesystem state in the canonical public hierarchical Stream provider.
pub trait StreamBackedAuthority: AsyncAuthorityStore {
    /// Exact Stream implementation used by this deployment.
    type Stream: acyclic_stream::StreamProvider;

    /// Returns the shared provider; no shadow log or multipart catalog is created.
    fn stream_provider(&self) -> Arc<Self::Stream>;
}

impl<P: crate::distributed::AuthorityProvider> StreamBackedAuthority for StreamAuthorityStore<P> {
    type Stream = P;

    fn stream_provider(&self) -> Arc<Self::Stream> {
        self.provider()
    }
}

impl<A: StreamBackedAuthority, O: AsyncObjectStore> Workspace<A, O> {
    /// Returns the canonical Stream provider that stores multipart upload records.
    pub fn multipart_stream_provider(&self) -> Arc<A::Stream> {
        self.volume.fs.authority().stream_provider()
    }

    /// Returns the upload record path under this workspace's multipart namespace.
    pub fn multipart_path(
        &self,
        upload_id: &str,
    ) -> MultipartStorageResult<acyclic_stream::StreamPath> {
        if upload_id.len() != 32 || !upload_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(MultipartStorageError::NoSuchUpload);
        }
        acyclic_stream::StreamPath::new(format!(
            "fs/s3-multipart/{}/{}",
            hex::encode(self.id().into_bytes()),
            upload_id.to_ascii_lowercase()
        ))
        .map_err(|_| MultipartStorageError::Malformed)
    }

    /// Appends an upload record at the observed tail in this workspace.
    pub async fn append_multipart_record(
        &self,
        upload_id: &str,
        expected_tail: u64,
        record: Bytes,
        operation: acyclic_stream::IdempotencyKey,
    ) -> MultipartStorageResult<bool> {
        match self
            .multipart_stream_provider()
            .append(acyclic_stream::AppendRequest {
                path: self.multipart_path(upload_id)?,
                records: vec![record.into()],
                if_tail: Some(expected_tail),
                idempotency_key: operation,
            })
            .await
            .map_err(MultipartStorageError::StreamWrite)?
        {
            acyclic_stream::AppendOutcome::Committed(_) => Ok(true),
            acyclic_stream::AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }

    /// Replays the requested upload in this workspace.
    pub async fn multipart_snapshot(
        &self,
        upload_id: &str,
        limits: FilesystemS3Limits,
    ) -> MultipartStorageResult<MultipartSnapshot> {
        read_multipart_snapshot(
            self.multipart_stream_provider().as_ref(),
            self.multipart_path(upload_id)?,
            limits,
        )
        .await
    }

    /// Authenticates the immutable objects retained by active multipart uploads.
    pub async fn active_multipart_objects(
        &self,
        protocol_limits: FilesystemS3Limits,
        retention_limits: S3MultipartRetentionLimits,
        budget: crate::WorkBudget,
        cancellation: &CancellationToken,
    ) -> MultipartStorageResult<crate::OperationReceipt<Vec<ObjectId>>> {
        active_s3_multipart_objects(
            self.multipart_stream_provider().as_ref(),
            self.volume.fs.objects(),
            protocol_limits,
            retention_limits,
            budget,
            cancellation,
        )
        .await
    }
}

/// Version byte for persisted multipart upload records.
pub const MULTIPART_RECORD_VERSION: u8 = 1;

/// One uploaded part with its staged content and entity tag.
#[derive(Clone, Debug)]
pub struct MultipartPart {
    /// Canonical staged content referenced by the record.
    pub staged: StagedContent,
    /// Entity tag of the uploaded part.
    pub etag: String,
}

/// The current completion or abort state of an upload.
#[derive(Clone, Debug)]
pub enum MultipartTerminal {
    /// Completion intent with the selected part numbers and entity tags.
    Completing(Vec<(u32, String)>),
    /// Published generation for a completed upload.
    Completed(crate::GenerationId),
    /// Durable abort that fences further parts and completion.
    Aborted,
}

/// Current multipart state reconstructed from the upload stream.
#[derive(Clone, Debug)]
pub struct MultipartSnapshot {
    /// Object key to which the upload is bound.
    pub key: String,
    /// Stream tail used for the next conditional append.
    pub tail: u64,
    /// Minimum byte length for non-final parts.
    pub minimum_part_bytes: u64,
    /// Maximum allowed part count, if configured.
    pub maximum_parts: Option<u32>,
    /// Maximum bytes allowed per part, if configured.
    pub maximum_part_bytes: Option<u64>,
    /// Current part by number, after applying replacements.
    pub parts: BTreeMap<u32, MultipartPart>,
    /// Part upload operation identities and their accepted part and tag.
    pub operations: BTreeMap<[u8; 16], (u32, String)>,
    /// Latest completion or abort state, if any.
    pub terminal: Option<MultipartTerminal>,
    /// Provider reads and record work charged by replay.
    pub work: crate::WorkCounters,
}

include!("multipart_record.rs");
include!("multipart_retention.rs");
