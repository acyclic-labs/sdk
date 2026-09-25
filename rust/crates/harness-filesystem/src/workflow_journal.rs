//! Ref-only Stream journal for pinned resumable workflow transitions.

use super::{FilesystemHost, InternalContentClass};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_harness::{
    Error, IdempotencyKey, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    workflow::{WorkflowCommitOutcome, WorkflowJournal, WorkflowRecord},
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::{TryStreamExt as _, future::BoxFuture};
use std::{collections::HashSet, sync::Arc};

const PAGE_SIZE: u32 = 1_024;
const MAX_RECORDS: u64 = 1_000_000;

/// One workflow's atomic transition ledger; Streams never carry state or command bodies.
pub struct FilesystemWorkflowJournal<P, A, O> {
    stream: StreamClient<P>,
    host: Arc<FilesystemHost<A, O>>,
    workflow_id: String,
    path: String,
    volume: VolumeRef,
    verifier: AuthorityVerifier,
    scope: Scope,
    maximum_payload_bytes: u64,
}

impl<P, A, O> FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider + Send + Sync,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Binds a validated workflow identity and its exact private-volume grants.
    pub fn new(
        stream: StreamClient<P>,
        host: Arc<FilesystemHost<A, O>>,
        workflow_id: &str,
        volume: VolumeRef,
        verifier: AuthorityVerifier,
        scope: Scope,
        maximum_payload_bytes: u64,
    ) -> Result<Self> {
        if workflow_id.is_empty()
            || workflow_id.len() > 128
            || workflow_id == "."
            || workflow_id == ".."
            || workflow_id.contains('/')
            || workflow_id.contains('\\')
            || workflow_id.chars().any(char::is_control)
            || volume.class() != VolumeClass::AgentPrivate
            || volume.provider() != &host.provider
            || maximum_payload_bytes == 0
        {
            return Err(Error::Invalid("workflow journal binding is invalid".into()));
        }
        ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Read)?;
        ContentGrant::verify(&verifier, &scope, &volume, VolumeOperation::Write)?;
        let path = format!("harness/v2/workflows/{workflow_id}");
        stream
            .stream(&path)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(Self {
            stream,
            host,
            workflow_id: workflow_id.to_owned(),
            path,
            volume,
            verifier,
            scope,
            maximum_payload_bytes,
        })
    }

    async fn read_record(&self, reference: &FileRef) -> Result<WorkflowRecord> {
        if reference.volume() != &self.volume
            || reference.descriptor().media_type() != "application/json"
            || !reference
                .path()
                .starts_with(&format!(".system/workflows/{}/", self.workflow_id))
        {
            return Err(Error::Unauthorized(
                "workflow record belongs to another private volume".into(),
            ));
        }
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Read,
        )?;
        let bytes = self
            .host
            .read_content(reference, &grant, self.maximum_payload_bytes)
            .await?;
        let record: WorkflowRecord = serde_json::from_slice(&bytes)
            .map_err(|error| Error::Storage(format!("workflow record is invalid: {error}")))?;
        record.validate()?;
        self.verify_command_payloads(&record).await?;
        Ok(record)
    }

    async fn verify_command_payloads(&self, record: &WorkflowRecord) -> Result<()> {
        let read = ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Read,
        )?;
        for command in &record.transition.commands {
            if command.payload.volume() != &self.volume {
                return Err(Error::Unauthorized(
                    "workflow command payload belongs to another volume".into(),
                ));
            }
            self.host
                .read_content(&command.payload, &read, self.maximum_payload_bytes)
                .await?;
        }
        Ok(())
    }

    async fn stage(&self, record: &WorkflowRecord) -> Result<FileRef> {
        // A committed transition may expose its commands immediately on
        // recovery. Admit their immutable argument versions first.
        self.verify_command_payloads(record).await?;
        let bytes =
            serde_json::to_vec(record).map_err(|error| Error::Invalid(error.to_string()))?;
        if bytes.len() as u64 > self.maximum_payload_bytes {
            return Err(Error::Invalid(
                "workflow transition exceeds private-file limit".into(),
            ));
        }
        let grant = ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Write,
        )?;
        self.host
            .put_internal_content(
                &self.volume,
                &grant,
                &format!(
                    ".system/workflows/{}/{:020}-{}.json",
                    self.workflow_id, record.prior.revision, record.operation_id
                ),
                &bytes,
                "application/json",
                "transition.json",
                self.maximum_payload_bytes,
                &IdempotencyKey::new(format!(
                    "workflow:{}:{}",
                    self.workflow_id, record.operation_id
                ))?,
                InternalContentClass::Workflow,
            )
            .await
    }

    async fn reconcile(&self, record: &WorkflowRecord) -> Result<Option<WorkflowCommitOutcome>> {
        for existing in self.replay().await? {
            if existing.operation_id == record.operation_id {
                return if existing == *record {
                    Ok(Some(WorkflowCommitOutcome::Replayed(existing)))
                } else {
                    Err(Error::Conflict("workflow operation identity reused".into()))
                };
            }
        }
        Ok(None)
    }
}

impl<P, A, O> WorkflowJournal for FilesystemWorkflowJournal<P, A, O>
where
    P: StreamProvider + Send + Sync,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn replay(&self) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>> {
        Box::pin(async move {
            let stream = self
                .stream
                .stream(&self.path)
                .map_err(|error| Error::Storage(error.to_string()))?;
            let mut from = 0_u64;
            let mut result = Vec::new();
            let mut keys = HashSet::new();
            loop {
                let records = match stream.read(from, PAGE_SIZE).await {
                    Ok(records) => records,
                    Err(StreamError::NotFound) if from == 0 => break,
                    Err(error) => return Err(Error::Storage(error.to_string())),
                };
                let page = records
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|error| Error::Storage(error.to_string()))?;
                if page.is_empty() {
                    break;
                }
                for entry in page {
                    if entry.sequence != from || from >= MAX_RECORDS {
                        return Err(Error::Storage(
                            "workflow sequence or retention bound is invalid".into(),
                        ));
                    }
                    let reference: FileRef =
                        serde_json::from_slice(&entry.value).map_err(|error| {
                            Error::Storage(format!("workflow reference is invalid: {error}"))
                        })?;
                    let record = self.read_record(&reference).await?;
                    if record.prior.revision != from || record.next.revision != from + 1 {
                        return Err(Error::Storage(
                            "workflow checkpoint revision is discontinuous".into(),
                        ));
                    }
                    if !keys.insert(record.idempotency_key.clone()) {
                        return Err(Error::Storage(
                            "workflow retry identity is duplicated".into(),
                        ));
                    }
                    result.push(record);
                    from += 1;
                }
            }
            Ok(result)
        })
    }

    fn commit<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxFuture<'a, Result<WorkflowCommitOutcome>> {
        Box::pin(async move {
            IdempotencyKey::new(idempotency_key.0.clone())?;
            record.validate()?;
            if record.idempotency_key != idempotency_key
                || record.prior.revision != expected_revision
                || record.next.revision
                    != expected_revision
                        .checked_add(1)
                        .ok_or_else(|| Error::Invalid("workflow revision exhausted".into()))?
                || expected_revision >= MAX_RECORDS
            {
                return Err(Error::Invalid(
                    "workflow commit identity or revision is invalid".into(),
                ));
            }
            let reference = self.stage(&record).await?;
            if self.read_record(&reference).await? != record {
                return Err(Error::Conflict(
                    "staged workflow record disagrees with its transition".into(),
                ));
            }
            let bytes = serde_json::to_vec(&reference)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid(
                    "workflow reference exceeds Stream limit".into(),
                ));
            }
            let digest =
                blake3::hash(format!("{}:{}", self.path, idempotency_key.as_str()).as_bytes());
            let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let stream = self
                .stream
                .stream(&self.path)
                .map_err(|error| Error::Storage(error.to_string()))?;
            match stream
                .append_batch(vec![Bytes::from(bytes)], Some(expected_revision), Some(key))
                .await
            {
                Ok(AppendOutcome::Committed(receipt))
                    if receipt.start == expected_revision
                        && receipt.end == expected_revision + 1 =>
                {
                    Ok(WorkflowCommitOutcome::Applied(record))
                }
                Ok(AppendOutcome::Committed(_)) => {
                    Err(Error::Storage("workflow append receipt is invalid".into()))
                }
                Ok(AppendOutcome::TailConflict { .. }) => match self.reconcile(&record).await? {
                    Some(previous) => Ok(previous),
                    None => Err(Error::Conflict(
                        "workflow checkpoint revision is stale".into(),
                    )),
                },
                Err(StreamError::Unavailable) => match self.reconcile(&record).await {
                    Ok(Some(previous)) => Ok(previous),
                    Ok(None) | Err(Error::Storage(_)) => {
                        Err(Error::Indeterminate(record.operation_id))
                    }
                    Err(error) => Err(error),
                },
                Err(StreamError::IdempotencyMismatch) => {
                    Err(Error::Conflict("workflow retry identity reused".into()))
                }
                Err(error) => Err(Error::Storage(error.to_string())),
            }
        })
    }
}
