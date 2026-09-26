//! Ref-only Stream journal for pinned resumable workflow transitions.

use super::{FilesystemHost, InternalContentClass};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_harness::{
    Error, IdempotencyKey, Result,
    conversation::{ContentGrant, FileRef, VolumeClass, VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
    workflow::{
        MachineCheckpoint, MachineStatus, WorkflowAdmission, WorkflowCommitOutcome,
        WorkflowJournal, WorkflowRecord,
    },
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamKey, StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::{TryStreamExt as _, future::BoxFuture};
use std::{
    collections::{BTreeSet, HashSet},
    sync::{Arc, Mutex},
};

const PAGE_SIZE: u32 = 1_024;
// Reopening must be bounded even when every transition and command is valid.
const MAX_RECORDS: u64 = 4_096;
const MAX_RESERVED_IDENTITIES: usize = 65_536;

struct VerifiedSummary {
    admission: WorkflowAdmission,
    revision: u64,
    checkpoint: MachineCheckpoint,
    reserved: BTreeSet<acyclic_harness::OperationId>,
    terminal: bool,
}

impl VerifiedSummary {
    fn require_next(&self, record: &WorkflowRecord) -> Result<()> {
        if record.prior != self.checkpoint || record.prior.revision != self.revision {
            return Err(Error::Conflict(
                "workflow transition differs from admission".into(),
            ));
        }
        if self.terminal {
            return Err(Error::Conflict(
                "workflow cannot advance after a terminal transition".into(),
            ));
        }
        if self
            .reserved
            .len()
            .saturating_add(record.transition.commands.len() + 1)
            > MAX_RESERVED_IDENTITIES
        {
            return Err(Error::Invalid(
                "workflow operation identity bound is exhausted".into(),
            ));
        }
        if record
            .operation_ids()
            .any(|identity| self.reserved.contains(&identity))
        {
            return Err(Error::Conflict(
                "workflow command or transition identity is already bound".into(),
            ));
        }
        Ok(())
    }

    fn advance(&mut self, record: &WorkflowRecord) {
        for identity in record.operation_ids() {
            self.reserved.insert(identity);
        }
        self.revision = record.next.revision;
        self.checkpoint = record.next.clone();
        self.terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
    }
}

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
    verified: Mutex<Option<VerifiedSummary>>,
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
            verified: Mutex::new(None),
        })
    }

    async fn read_record(
        &self,
        reference: &FileRef,
        verify_payloads: bool,
    ) -> Result<WorkflowRecord> {
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
        if verify_payloads {
            self.verify_command_payloads(&record).await?;
        }
        Ok(record)
    }

    async fn replay_records(&self, verify_payloads: bool) -> Result<Vec<WorkflowRecord>> {
        let stream = self
            .stream
            .stream(&self.path)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let mut from = 0_u64;
        let mut result = Vec::new();
        let mut keys = HashSet::new();
        let mut reserved = BTreeSet::new();
        let mut identities = 0_usize;
        let mut terminal = false;
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
                let reference: FileRef = serde_json::from_slice(&entry.value).map_err(|error| {
                    Error::Storage(format!("workflow reference is invalid: {error}"))
                })?;
                let record = self.read_record(&reference, verify_payloads).await?;
                if terminal {
                    return Err(Error::Storage(
                        "workflow history continues after a terminal transition".into(),
                    ));
                }
                if record
                    .operation_ids()
                    .any(|operation_id| !reserved.insert(operation_id))
                {
                    return Err(Error::Storage(
                        "workflow history reuses a command or transition identity".into(),
                    ));
                }
                identities = identities.saturating_add(record.transition.commands.len() + 1);
                if identities > MAX_RESERVED_IDENTITIES {
                    return Err(Error::Storage(
                        "workflow identity retention bound is invalid".into(),
                    ));
                }
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
                terminal = !matches!(&record.transition.status, MachineStatus::Suspended);
                result.push(record);
                from += 1;
            }
        }
        Ok(result)
    }

    async fn verified_history(&self, admission: &WorkflowAdmission) -> Result<Vec<WorkflowRecord>> {
        // Identity validation needs the immutable record envelopes, not every
        // historic command body. Public replay still verifies every payload.
        let history = self.replay_records(false).await?;
        let mut summary = VerifiedSummary {
            admission: admission.clone(),
            revision: 0,
            checkpoint: admission.initial.clone(),
            reserved: BTreeSet::new(),
            terminal: false,
        };
        for record in &history {
            summary.require_next(record).map_err(|_| {
                Error::Storage(
                    "workflow history diverges from its admission or reuses an identity".into(),
                )
            })?;
            summary.advance(record);
        }
        if let Ok(mut cached) = self.verified.lock()
            && cached
                .as_ref()
                .is_none_or(|current| current.revision <= summary.revision)
        {
            *cached = Some(summary);
        }
        Ok(history)
    }

    fn cached_preflight(
        &self,
        admission: &WorkflowAdmission,
        record: &WorkflowRecord,
    ) -> Result<bool> {
        let cached = self
            .verified
            .lock()
            .map_err(|_| Error::Storage("workflow summary cache is poisoned".into()))?;
        let Some(summary) = cached.as_ref() else {
            return Ok(false);
        };
        if summary.admission != *admission || summary.revision != record.prior.revision {
            return Ok(false);
        }
        summary.require_next(record)?;
        Ok(true)
    }

    fn advance_cached(&self, admission: &WorkflowAdmission, record: &WorkflowRecord) {
        if let Ok(mut cached) = self.verified.lock()
            && let Some(summary) = cached.as_mut()
            && summary.admission == *admission
            && summary.revision == record.prior.revision
        {
            summary.advance(record);
        }
    }

    fn admission_path(&self) -> String {
        format!("harness/v2/workflow-admissions/{}", self.workflow_id)
    }

    async fn has_transition(&self) -> Result<bool> {
        let stream = self
            .stream
            .stream(&self.path)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let entries = match stream.read(0, 1).await {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => return Ok(false),
            Err(error) => return Err(Error::Storage(error.to_string())),
        }
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
        Ok(!entries.is_empty())
    }

    async fn read_admission(&self) -> Result<Option<WorkflowAdmission>> {
        let stream = self
            .stream
            .stream(self.admission_path())
            .map_err(|error| Error::Storage(error.to_string()))?;
        let entries = match stream.read(0, 2).await {
            Ok(entries) => entries,
            Err(StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(Error::Storage(error.to_string())),
        }
        .try_collect::<Vec<_>>()
        .await
        .map_err(|error| Error::Storage(error.to_string()))?;
        let [entry] = entries.as_slice() else {
            if entries.is_empty() {
                return Ok(None);
            }
            return Err(Error::Storage(
                "workflow admission stream is invalid".into(),
            ));
        };
        if entry.sequence != 0 {
            return Err(Error::Storage(
                "workflow admission sequence is invalid".into(),
            ));
        }
        let reference: FileRef = serde_json::from_slice(&entry.value).map_err(|error| {
            Error::Storage(format!("workflow admission reference is invalid: {error}"))
        })?;
        if reference.volume() != &self.volume
            || reference.path() != format!(".system/workflows/{}/admission.json", self.workflow_id)
            || reference.descriptor().media_type() != "application/json"
        {
            return Err(Error::Unauthorized(
                "workflow admission belongs to another volume".into(),
            ));
        }
        let read = ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Read,
        )?;
        let bytes = self
            .host
            .read_content(&reference, &read, self.maximum_payload_bytes)
            .await?;
        let admission: WorkflowAdmission = serde_json::from_slice(&bytes)
            .map_err(|error| Error::Storage(format!("workflow admission is invalid: {error}")))?;
        admission.validate()?;
        Ok(Some(admission))
    }

    async fn stage_admission(&self, admission: &WorkflowAdmission) -> Result<FileRef> {
        let bytes =
            serde_json::to_vec(admission).map_err(|error| Error::Invalid(error.to_string()))?;
        if bytes.len() as u64 > self.maximum_payload_bytes {
            return Err(Error::Invalid(
                "workflow admission exceeds private-file limit".into(),
            ));
        }
        let write = ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Write,
        )?;
        self.host
            .put_internal_content(
                &self.volume,
                &write,
                &format!(".system/workflows/{}/admission.json", self.workflow_id),
                &bytes,
                "application/json",
                "admission.json",
                self.maximum_payload_bytes,
                &IdempotencyKey::new(format!("workflow:{}:admission", self.workflow_id))?,
                InternalContentClass::Workflow,
            )
            .await
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
        let admission = self.read_admission().await?.ok_or_else(|| {
            Error::Conflict("workflow transition has no retained admission".into())
        })?;
        let requested: BTreeSet<_> = record.operation_ids().collect();
        for existing in self.verified_history(&admission).await? {
            if existing.operation_id == record.operation_id {
                return if existing == *record {
                    self.verify_command_payloads(&existing).await?;
                    Ok(Some(WorkflowCommitOutcome::Replayed(existing)))
                } else {
                    Err(Error::Conflict("workflow operation identity reused".into()))
                };
            }
            if existing
                .operation_ids()
                .any(|operation_id| requested.contains(&operation_id))
            {
                return Err(Error::Conflict(
                    "workflow command or transition identity is already bound".into(),
                ));
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
    fn admit<'a>(
        &'a self,
        admission: WorkflowAdmission,
    ) -> BoxFuture<'a, Result<WorkflowAdmission>> {
        Box::pin(async move {
            admission.validate()?;
            if let Some(existing) = self.read_admission().await? {
                return if existing == admission {
                    Ok(existing)
                } else {
                    Err(Error::Conflict(
                        "workflow admission identity is already bound".into(),
                    ))
                };
            }
            if self.has_transition().await? {
                return Err(Error::Conflict(
                    "workflow transitions exist without admission".into(),
                ));
            }
            let reference = self.stage_admission(&admission).await?;
            let bytes = serde_json::to_vec(&reference)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
                return Err(Error::Invalid(
                    "workflow admission reference exceeds Stream limit".into(),
                ));
            }
            let digest = blake3::hash(format!("{}:admission", self.admission_path()).as_bytes());
            let key = StreamKey::new(Bytes::copy_from_slice(digest.as_bytes()))
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let stream = self
                .stream
                .stream(self.admission_path())
                .map_err(|error| Error::Storage(error.to_string()))?;
            match stream
                .append_batch(vec![Bytes::from(bytes)], Some(0), Some(key))
                .await
            {
                Ok(AppendOutcome::Committed(receipt)) if receipt.start == 0 && receipt.end == 1 => {
                    Ok(admission)
                }
                Ok(AppendOutcome::Committed(_)) => Err(Error::Storage(
                    "workflow admission receipt is invalid".into(),
                )),
                Ok(AppendOutcome::TailConflict { .. }) | Err(StreamError::IdempotencyMismatch) => {
                    match self.read_admission().await? {
                        Some(existing) if existing == admission => Ok(existing),
                        Some(_) => Err(Error::Conflict(
                            "workflow admission identity is already bound".into(),
                        )),
                        None => Err(Error::Indeterminate(admission.operation_id)),
                    }
                }
                Err(StreamError::Unavailable) => match self.read_admission().await {
                    Ok(Some(existing)) if existing == admission => Ok(existing),
                    Ok(Some(_)) => Err(Error::Conflict(
                        "workflow admission identity is already bound".into(),
                    )),
                    Ok(None) | Err(Error::Storage(_)) => {
                        Err(Error::Indeterminate(admission.operation_id))
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(Error::Storage(error.to_string())),
            }
        })
    }

    fn admission(&self) -> BoxFuture<'_, Result<Option<WorkflowAdmission>>> {
        Box::pin(self.read_admission())
    }

    fn replay(&self) -> BoxFuture<'_, Result<Vec<WorkflowRecord>>> {
        Box::pin(self.replay_records(true))
    }

    fn commit<'a>(
        &'a self,
        expected_revision: u64,
        idempotency_key: IdempotencyKey,
        record: WorkflowRecord,
    ) -> BoxFuture<'a, Result<WorkflowCommitOutcome>> {
        Box::pin(async move {
            let admission = self.read_admission().await?.ok_or_else(|| {
                Error::Conflict("workflow transition has no retained admission".into())
            })?;
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
            if !self.cached_preflight(&admission, &record)? {
                // Reopening or reconciling an old retry scans the authoritative
                // chain once. The normal next step uses its verified summary;
                // Stream's tail CAS fences concurrent writers after preflight.
                let history = self.verified_history(&admission).await?;
                if let Some(existing) = history
                    .iter()
                    .find(|existing| existing.operation_id == record.operation_id)
                {
                    return if existing == &record {
                        self.verify_command_payloads(existing).await?;
                        Ok(WorkflowCommitOutcome::Replayed(existing.clone()))
                    } else {
                        Err(Error::Conflict(
                            "workflow operation identity is already bound".into(),
                        ))
                    };
                }
                if history.len() as u64 != expected_revision {
                    return Err(Error::Conflict(
                        "workflow checkpoint revision is stale".into(),
                    ));
                }
                self.cached_preflight(&admission, &record)?;
            }
            let reference = self.stage(&record).await?;
            if self.read_record(&reference, true).await? != record {
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
                    self.advance_cached(&admission, &record);
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
