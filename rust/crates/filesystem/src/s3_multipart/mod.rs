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

    /// Returns the shared provider for the filesystem's multipart storage.
    /// Implementations use the authority's original Stream provider so no
    /// shadow log or multipart catalog is created.
    ///
    /// This method lets authority implementations supply the provider behind
    /// the shared storage API. HTTP handlers use workspace operations and
    /// have no provider accessor on the workspace.
    fn stream_provider(&self) -> Arc<Self::Stream>;
}

impl<P: crate::distributed::AuthorityProvider> StreamBackedAuthority for StreamAuthorityStore<P> {
    type Stream = P;

    fn stream_provider(&self) -> Arc<Self::Stream> {
        self.provider()
    }
}

const MULTIPART_DOMAIN: &[u8] = b"acyclic-fs-s3-multipart-v1\0";

/// Derives the retry identity for a conditional upload record.
fn multipart_cas_operation(
    upload_id: &str,
    phase: &[u8],
    operation: IdempotencyKey,
    expected_tail: u64,
) -> MultipartStorageResult<acyclic_stream::IdempotencyKey> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(MULTIPART_DOMAIN);
    hasher.update(phase);
    hasher.update(upload_id.as_bytes());
    hasher.update(&operation.into_bytes());
    hasher.update(&expected_tail.to_le_bytes());
    acyclic_stream::IdempotencyKey::new(Bytes::copy_from_slice(hasher.finalize().as_bytes()))
        .map_err(|_| MultipartStorageError::Malformed)
}

impl<A: StreamBackedAuthority, O: AsyncObjectStore> Workspace<A, O> {
    /// Returns the canonical Stream provider that stores multipart upload records.
    fn multipart_stream_provider(&self) -> Arc<A::Stream> {
        self.volume.fs.authority().stream_provider()
    }

    /// Returns the upload record path under this workspace's multipart namespace.
    fn multipart_path(
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

    /// Derives the upload ID from this workspace and operation.
    pub fn multipart_upload_id(&self, operation: IdempotencyKey) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(MULTIPART_DOMAIN);
        hasher.update(b"upload-id\0");
        hasher.update(&self.id().into_bytes());
        hasher.update(&operation.into_bytes());
        hex::encode(&hasher.finalize().as_bytes()[..16])
    }

    fn multipart_registry_path(
        upload_id: &str,
    ) -> MultipartStorageResult<acyclic_stream::StreamPath> {
        let shard = upload_id
            .get(..2)
            .filter(|value| value.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .ok_or(MultipartStorageError::Malformed)?;
        acyclic_stream::StreamPath::new(format!(
            "fs/s3-multipart/registry/{}",
            shard.to_ascii_lowercase()
        ))
        .map_err(|_| MultipartStorageError::Malformed)
    }

    /// Atomically creates the upload stream and its registry entry.
    pub async fn create_multipart_record(
        &self,
        upload_id: &str,
        record: MultipartCreateRecord,
        operation: IdempotencyKey,
    ) -> MultipartStorageResult<bool> {
        let path = self.multipart_path(upload_id)?;
        let provider = self.multipart_stream_provider();
        let registry = Self::multipart_registry_path(upload_id)?;
        let (registry_condition, registry_tail) = match provider.tail(registry.clone()).await {
            Ok(tail) => (
                acyclic_stream::CommitCondition::Tail {
                    path: registry.clone(),
                    expected: tail,
                },
                tail,
            ),
            Err(error) if error.category() == acyclic_stream::StreamCategory::NotFound => (
                acyclic_stream::CommitCondition::Absent {
                    path: registry.clone(),
                },
                0,
            ),
            Err(error) => return Err(MultipartStorageError::Stream(error)),
        };
        let attempt = multipart_cas_operation(upload_id, b"create\0", operation, registry_tail)?;
        let outcome = provider
            .commit(acyclic_stream::CommitRequest {
                deadline_unix_millis: None,
                effects: vec![acyclic_stream::CommitEffect::hard(
                    vec![
                        registry_condition,
                        acyclic_stream::CommitCondition::Absent { path: path.clone() },
                    ],
                    vec![
                        acyclic_stream::CommitMutation::Append {
                            path: registry,
                            records: vec![Bytes::copy_from_slice(path.as_str().as_bytes()).into()],
                        },
                        acyclic_stream::CommitMutation::Append {
                            path,
                            records: vec![record.into()],
                        },
                    ],
                )],
                idempotency_key: attempt,
            })
            .await
            .map_err(MultipartStorageError::StreamWrite)?;
        Ok(matches!(
            outcome,
            acyclic_stream::CommitOutcome::Committed(_)
        ))
    }

    /// Appends an upload record at the observed tail in this workspace.
    pub async fn append_multipart_record(
        &self,
        upload_id: &str,
        expected_tail: u64,
        record: MultipartUpdateRecord,
        operation: IdempotencyKey,
    ) -> MultipartStorageResult<bool> {
        let phase: &[u8] = match RecordCursor::new(record.as_ref()).any_header()? {
            PART_RECORD => b"part\0",
            COMPLETING_RECORD => b"complete-intent\0",
            COMPLETED_RECORD => b"complete-terminal\0",
            ABORTED_RECORD => b"abort\0",
            _ => return Err(MultipartStorageError::Malformed),
        };
        let idempotency_key = multipart_cas_operation(upload_id, phase, operation, expected_tail)?;
        match self
            .multipart_stream_provider()
            .append(acyclic_stream::AppendRequest {
                path: self.multipart_path(upload_id)?,
                records: vec![record.into()],
                if_tail: Some(expected_tail),
                idempotency_key,
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
}

impl<A: StreamBackedAuthority, O: AsyncObjectStore> crate::Fs<A, O> {
    /// Authenticates the immutable objects retained by active multipart uploads in this filesystem.
    pub async fn active_multipart_objects(
        &self,
        protocol_limits: FilesystemS3Limits,
        retention_limits: S3MultipartRetentionLimits,
        budget: crate::WorkBudget,
        cancellation: &CancellationToken,
    ) -> MultipartStorageResult<crate::OperationReceipt<Vec<ObjectId>>> {
        active_s3_multipart_objects(
            self.authority().stream_provider().as_ref(),
            self.objects(),
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
    Completed(acyclic_fs_format::object_reference::GenerationId),
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

/// Codec checks and corpus samples for the canonical multipart record format.
#[cfg(feature = "test-support")]
pub mod codec_fuzz;
