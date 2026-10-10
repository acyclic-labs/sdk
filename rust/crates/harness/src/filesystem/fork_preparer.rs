//! Provider-owned fork preparation journal and exact Filesystem captures.
//!
//! The journal is a separate workspace: putting it in the child private or
//! project volume would leak implementation metadata into the published fork.
//!
//! Task-bound allocations retain the original creation fence in those same
//! journals. A configured `FilesystemResourceCreationProvider` authenticates
//! the complete request, seed and original generations before deriving exact
//! created-volume IO; each resulting handle reauthenticates on every use.
//! Ordinary preparation attempts aggregate measured Filesystem counter receipts
//! under their admitted finite work/byte envelope, including failed boundaries.

use super::{FilesystemHost, ParentProjectController, WorkspaceMutation, map_error, workspace_ref};
use crate::contract::capability;
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentGrant, ContentResidencyVerifier, VolumeOperation, VolumeRef},
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkPreparer, ForkReport, ForkRequest,
        ForkSeed, ForkSelection, InheritedConversationPrefix, PreparedForkHistory,
        PreparedSummaryFork, ReferenceGrant, ResourceRevision, SharedGrant,
    },
    resource_preparation::{ResourceCreationOrigin, TaskResourcePreparation},
    resources::{ProviderRef, ResourceRef, WorkspaceRef},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

mod created_access;
mod bounded_io;
pub(crate) use bounded_io::PreparationIo;
pub use created_access::FilesystemResourceCreationProvider;

const MAX_REQUEST_BYTES: u64 = 4 * 1024 * 1024;
const MAX_REPORT_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CAPTURE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ForkAllocationRole { Scratch, Project, Result }

fn creation_volumes(request: &ForkRequest) -> impl Iterator<Item=(&VolumeRef,ForkAllocationRole)> {
    [
        (Some(&request.preparation.child_private_volume), ForkAllocationRole::Scratch),
        (Some(&request.preparation.child_project_volume), ForkAllocationRole::Project),
        (request.preparation.child_result_volume.as_ref(), ForkAllocationRole::Result),
    ].into_iter().filter_map(|(volume, role)| volume.map(|volume| (volume,role)))
}

fn seed_creation_volumes(seed: &ForkSeed) -> Result<impl Iterator<Item=(&VolumeRef,ForkAllocationRole)>> {
    let project = seed.resources.iter().find_map(|resource| match &resource.revision {
        ResourceRevision::Project { volume, .. } => Some(volume),
        _ => None,
    }).ok_or_else(|| Error::Invalid("fork has no child project".into()))?;
    Ok([
        (Some(&seed.child_private_volume), ForkAllocationRole::Scratch),
        (Some(project), ForkAllocationRole::Project),
        (seed.child_result_volume.as_ref(), ForkAllocationRole::Result),
    ].into_iter().filter_map(|(volume,role)| volume.map(|volume| (volume,role))))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AllocationClaim {
    operation_id: OperationId,
    preparation_digest: [u8; 32],
    task_origin: Option<ResourceCreationOrigin>,
    parent: Authority,
    child: Authority,
    volume: VolumeRef,
    role: ForkAllocationRole,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SeedBinding {
    digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureAttempt {
    operation_id: OperationId,
    selection: ForkSelection,
    attempt: Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaptureResult {
    selection: ForkSelection,
    capture: Capture,
}

/// Idempotent local capture of an exact Stream-backed parent snapshot and its
/// Filesystem project/private resources. Other providers remain explicit
/// unsupported captures; no cross-provider transaction is implied.
pub struct FilesystemForkPreparer<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    parent: Reducer,
    verifier: AuthorityVerifier,
    scope: Scope,
    project: VolumeRef,
    stream_provider: ProviderRef,
    resolver: Arc<dyn ContentResidencyVerifier>,
    capture_providers: Vec<Arc<dyn ForkCaptureProvider>>,
    summary: Option<BoundSummary>,
    history: Option<PreparedForkHistory>,
    task_preparation: Option<Arc<TaskResourcePreparation>>,
}

struct BoundSummary {
    prepared: PreparedSummaryFork,
    journal: Arc<dyn crate::executor::ExecutionJournal>,
}

struct PreparationAttempt<'a, A, O> {
    preparer: &'a FilesystemForkPreparer<A, O>,
    io: Option<PreparationIo>,
}

impl<A, O> std::ops::Deref for PreparationAttempt<'_, A, O> {
    type Target = FilesystemForkPreparer<A, O>;
    fn deref(&self) -> &Self::Target { self.preparer }
}

impl<A, O> FilesystemForkPreparer<A, O> {
    /// Binds a persisted parent revision, authenticated parent authority, and
    /// the providers that own the selected history and project resources.
    #[allow(
        clippy::too_many_arguments,
        reason = "every provider and authority is bound explicitly"
    )]
    pub fn new(
        host: Arc<FilesystemHost<A, O>>,
        parent: Reducer,
        verifier: AuthorityVerifier,
        scope: Scope,
        project: VolumeRef,
        stream_provider: ProviderRef,
        resolver: Arc<dyn ContentResidencyVerifier>,
    ) -> Result<Self> {
        stream_provider.validate()?;
        if stream_provider.family() != "stream" {
            return Err(Error::Invalid(
                "fork preparer requires a Stream history provider".into(),
            ));
        }
        ParentProjectController::new(&host, &parent, &verifier, &scope, project.clone())?;
        Ok(Self {
            host,
            parent,
            verifier,
            scope,
            project,
            stream_provider,
            resolver,
            capture_providers: Vec::new(),
            summary: None,
            history: None,
            task_preparation: None,
        })
    }

    /// Binds a finite archive selection produced by the original history reader.
    /// This does not grant file access or publish a child.
    pub fn with_history_context(mut self, history: PreparedForkHistory) -> Result<Self> {
        history.verify(&self.verifier)?;
        if history.parent() != self.parent.authority()
            || history.parent_revision() != self.parent.revision()
        {
            return Err(Error::Conflict("fork history differs from the bound parent".into()));
        }
        self.history = Some(history);
        Ok(self)
    }

    /// Binds an already verified original-parent Summary projection. This
    /// installs no grants and performs no model, allocation or publication IO.
    pub fn with_summary_context(
        mut self,
        prepared: PreparedSummaryFork,
        journal: Arc<dyn crate::executor::ExecutionJournal>,
    ) -> Result<Self> {
        if prepared.parent() != self.parent.authority()
            || prepared.parent_revision() != self.parent.revision()
            || journal.canonical_authority() != Some(self.parent.authority())
        {
            return Err(Error::Conflict(
                "summary capture differs from this original parent".into(),
            ));
        }
        self.summary = Some(BoundSummary { prepared, journal });
        Ok(self)
    }

    /// Pins an authenticated ordinary preparation task to this provider.
    /// The existing allocation records retain its origin; raw trusted
    /// preparation records cannot substitute for task-bound creation evidence.
    pub fn with_task_preparation(
        mut self,
        preparation: Arc<TaskResourcePreparation>,
    ) -> Result<Self> {
        if preparation.request().provider != self.host.provider
            || preparation.request().creation_grant != capability::FORK_PUBLISH
        {
            return Err(Error::Unauthorized(
                "fork preparation task belongs to another provider or creation authority".into(),
            ));
        }
        self.task_preparation = Some(preparation);
        Ok(self)
    }

    /// Registers an exact provider-owned resource capture. A provider may be
    /// bound once; unsupported selections remain explicit in the report.
    pub fn with_capture_provider(mut self, provider: Arc<dyn ForkCaptureProvider>) -> Result<Self> {
        provider.provider().validate()?;
        if self
            .capture_providers
            .iter()
            .any(|bound| bound.provider() == provider.provider())
        {
            return Err(Error::Invalid(
                "fork capture provider is registered twice".into(),
            ));
        }
        self.capture_providers.push(provider);
        Ok(self)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemForkPreparer<A, O> {
    /// Exact created identities for this provider's fork request, without IO.
    pub fn creation_resources(&self, request: &ForkRequest) -> Result<Vec<ResourceRef>> {
        request.validate()?;
        creation_volumes(request).map(|(volume,_)| {
            if volume.provider() != &self.host.provider {
                return Err(Error::Invalid(
                    "fork resource belongs to another provider".into(),
                ));
            }
            Ok(
                workspace_ref(self.host.provider.clone(), &volume.storage_name()?)?
                    .as_resource()
                    .clone(),
            )
        })
        .collect()
    }

    /// Actual Filesystem operation that created the child project, not the
    /// enclosing Harness operation. This pure identity grants no access.
    pub fn project_creation_operation(&self, request: &ForkRequest) -> Result<acyclic_fs::OperationId> {
        request.validate()?;
        self.creation_operation(request, ForkAllocationRole::Project)
    }

    /// Actual Filesystem operation that created the optional unmounted result slot.
    pub fn result_creation_operation(&self, request: &ForkRequest) -> Result<acyclic_fs::OperationId> {
        request.validate()?;
        if request.preparation.child_result_volume.is_none() {
            return Err(Error::Invalid("fork has no result allocation".into()));
        }
        self.creation_operation(request, ForkAllocationRole::Result)
    }

    fn creation_operation(&self, request: &ForkRequest, role: ForkAllocationRole) -> Result<acyclic_fs::OperationId> {
        struct DigestWriter { hash: blake3::Hasher, length: u64, maximum: u64 }
        impl std::io::Write for DigestWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let next = self.length.checked_add(bytes.len() as u64)
                    .ok_or_else(|| std::io::Error::other("fork request byte count overflows"))?;
                if next > self.maximum {
                    return Err(std::io::Error::other("fork request exceeds byte limit"));
                }
                self.hash.update(bytes);
                self.length = next;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let mut digest = DigestWriter {
            hash: blake3::Hasher::new(), length: 0,
            maximum: self.journal_limit(MAX_REQUEST_BYTES),
        };
        serde_json::to_writer(&mut digest, request).map_err(|error| Error::Invalid(error.to_string()))?;
        let suffix = match role { ForkAllocationRole::Scratch => "scratch", ForkAllocationRole::Project => "project", ForkAllocationRole::Result => "result" };
        let key = IdempotencyKey::new(format!("fork:{}:{}:{suffix}",request.operation_id,digest.hash.finalize()))?;
        Ok(acyclic_fs::OperationId::from_bytes(super::filesystem_key(&key).into_bytes()))
    }

    /// Builds exact allocation-journal references. These are request data,
    /// never access grants; only a configured creation verifier consumes them.
    pub fn creation_proofs(
        &self,
        request: &ForkRequest,
        preparation: crate::TaskId,
    ) -> Result<Vec<crate::resource_preparation::ResourceCreationProofRef>> {
        let resources = self.creation_resources(request)?;
        resources
            .into_iter()
            .zip(creation_volumes(request))
            .map(|(resource, (volume, _))| {
                Ok(crate::resource_preparation::ResourceCreationProofRef {
                    provider: self.host.provider.clone(),
                    preparation,
                    resource,
                    evidence: allocation_ref(self.host.provider.clone(), volume)?
                        .as_resource()
                        .clone(),
                })
            })
            .collect()
    }

    fn attempt(&self) -> Result<PreparationAttempt<'_, A, O>> {
        let io = self.task_preparation.as_ref()
            .map(|task| PreparationIo::new(task.request())).transpose()?;
        if let (Some(io), Some(task)) = (&io, &self.task_preparation) {
            io.encode(task.request(), task.request().maximum_bytes)?;
        }
        Ok(PreparationAttempt { preparer: self, io })
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> PreparationAttempt<'_, A, O> {

    fn journal_limit(&self, default: u64) -> u64 {
        self.task_preparation.as_ref().map_or(default, |task| {
            default.min(task.request().maximum_bytes)
        })
    }


    fn encode<T: Serialize>(&self, value: &T, maximum_bytes: u64) -> Result<Vec<u8>> {
        match &self.io {
            Some(io) => io.encode(value, maximum_bytes),
            None => encode_record(value, maximum_bytes),
        }
    }

    fn validate_task_request(&self, request: &ForkRequest) -> Result<()> {
        if let Some(preparation) = &self.task_preparation {
            let bound = preparation.request();
            let payload =
                serde_json::to_value(request).map_err(|error| Error::Invalid(error.to_string()))?;
            if bound.payload != payload || bound.resources != self.creation_resources(request)? {
                return Err(Error::Unauthorized(
                    "fork request or resources differ from the admitted preparation".into(),
                ));
            }
            if request.preparation.maximum_inherited_bytes > bound.maximum_bytes {
                return Err(Error::Invalid("fork preparation exceeds admitted IO allowance".into()));
            }
            crate::contract::validate_json_byte_bound(request, bound.maximum_bytes)?;
        }
        Ok(())
    }

    async fn verify_task_lease(&self, settlement: bool) -> Result<()> {
        if let Some(preparation) = &self.task_preparation {
            if settlement {
                preparation.verify_reconciliation().await?;
            } else {
                preparation.verify_creation().await?;
            }
        }
        Ok(())
    }

    async fn verify_task_allocations(&self, request: &ForkRequest) -> Result<()> {
        let Some(preparation) = &self.task_preparation else {
            return Ok(());
        };
        let digest = *blake3::hash(&self.encode(request, self.journal_limit(MAX_REQUEST_BYTES))?).as_bytes();
        for (volume, role) in creation_volumes(request) {
            let journal = allocation_ref(self.host.provider.clone(), volume)?;
            let retained = read_record_bounded::<A, O, AllocationClaim>(&self.host, &journal, "/claim.json", self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref())
            .await?
            .ok_or_else(|| Error::Unauthorized("task-bound allocation is missing".into()))?;
            let expected = AllocationClaim {
                operation_id: request.operation_id,
                preparation_digest: digest,
                task_origin: Some(preparation.origin().clone()),
                parent: request.parent.clone(),
                child: request.child.clone(),
                volume: volume.clone(),
                role,
            };
            if !same_allocation_claim(&retained, &expected) {
                return Err(Error::Unauthorized(
                    "allocation belongs to another preparation task".into(),
                ));
            }
        }
        Ok(())
    }

    fn authorize_request(&self, request: &ForkRequest) -> Result<()> {
        request.validate()?;
        self.validate_task_request(request)?;
        if request.parent != *self.parent.authority()
            || request.preparation.child_project_volume.provider() != &self.host.provider
        {
            return Err(Error::Conflict(
                "fork request does not match this parent/provider boundary".into(),
            ));
        }
        if request.boundary.is_some() {
            return Err(Error::Unsupported(
                "this preparer cannot attest a cross-provider boundary".into(),
            ));
        }
        if self.parent.conversation().and_then(|state| state.agent) == Some(request.child_agent) {
            return Err(Error::Invalid(
                "fork child must have a distinct agent identity".into(),
            ));
        }
        for selection in &request.selections {
            match &selection.revision {
                ResourceRevision::History(reference)
                    if reference.as_resource().provider() != &self.stream_provider =>
                {
                    return Err(Error::Invalid(
                        "fork history belongs to another Stream provider".into(),
                    ));
                }
                ResourceRevision::Project { volume, .. } if volume != &self.project => {
                    return Err(Error::Invalid(
                        "fork project is not the bound parent project".into(),
                    ));
                }
                ResourceRevision::PrivateVolume { volume, .. } => {
                    if volume.owner()
                        != &crate::conversation::VolumeOwner::Agent(self.scope.agent().ok_or_else(
                            || Error::Unauthorized("scratch fork requires a parent agent".into()),
                        )?)
                    {
                        return Err(Error::Unauthorized(
                            "scratch fork is not from the direct parent owner".into(),
                        ));
                    }
                    ContentGrant::verify(
                        &self.verifier,
                        &self.scope,
                        volume,
                        VolumeOperation::Read,
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn check_request(&self, request: &ForkRequest) -> Result<()> {
        self.authorize_request(request)?;
        if let Some(history) = &self.history {
            if history.through_sequence() != request.preparation.inherited_through_sequence {
                return Err(Error::Conflict("fork archive differs from its selected logical cut".into()));
            }
        } else if self.parent.conversation().is_some_and(|state|
            state.resident_after_sequence() > 0
                && request.preparation.inherited_through_sequence > 0)
        {
            return Err(Error::Invalid("archived fork requires its authenticated history capture".into()));
        }
        if let Some(selection) = &request.preparation.summary {
            let summary = self.summary.as_ref().ok_or_else(|| {
                Error::Unsupported(
                    "summary fork owner-bound checkpoint provider is not installed".into(),
                )
            })?;
            if summary.prepared.selection() != selection
                || summary.prepared.through_sequence()
                    != request.preparation.inherited_through_sequence
            {
                return Err(Error::Conflict(
                    "summary fork request differs from its original capture".into(),
                ));
            }
        }
        if request.parent_revision != self.parent.revision() {
            return Err(Error::Conflict(
                "fork preparation is not at the bound parent revision".into(),
            ));
        }
        Ok(())
    }

    fn journal_ref(&self, request: &ForkRequest) -> Result<WorkspaceRef> {
        workspace_ref(
            self.host.provider.clone(),
            &format!("harness-fork-preparation-{}", request.operation_id),
        )
    }

    async fn read_claim(&self, journal: &WorkspaceRef, request: &ForkRequest) -> Result<bool> {
        read_request_claim(&self.host, journal, request, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref()).await
    }

    async fn claim(&self, request: &ForkRequest) -> Result<WorkspaceRef> {
        let journal = self.journal_ref(request)?;
        let name = std::str::from_utf8(journal.as_resource().key())
            .map_err(|_| Error::Invalid("fork journal name is not UTF-8".into()))?;
        let creation_key = IdempotencyKey::new(format!("fork:{}:journal", request.operation_id))?;
        bounded_io::create(&self.host, name, &creation_key, self.io.as_ref()).await?;
        if self.read_claim(&journal, request).await? {
            return Ok(journal);
        }
        let bytes = self.encode(request, self.journal_limit(MAX_REQUEST_BYTES))?;
        let observed = bounded_io::resolve(&self.host, &journal, self.io.as_ref()).await?;
        let key = IdempotencyKey::new(format!("fork:{}:claim", request.operation_id))?;
        match bounded_io::apply(&self.host, &journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
            path: "/request.json".into(),
            bytes,
        }], &key, self.io.as_ref())
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
        if !self.read_claim(&journal, request).await? {
            return Err(Error::Indeterminate(request.operation_id));
        }
        Ok(journal)
    }

    async fn read_report(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
    ) -> Result<Option<ForkReport>> {
        read_preparation_report(&self.host, journal, request, self.journal_limit(MAX_REPORT_BYTES), self.io.as_ref()).await
    }

    async fn commit_report(
        &self,
        journal: &WorkspaceRef,
        report: &ForkReport,
    ) -> Result<ForkReport> {
        let bytes = self.encode(report, self.journal_limit(MAX_REPORT_BYTES))?;
        let observed = bounded_io::resolve(&self.host, journal, self.io.as_ref()).await?;
        let key = IdempotencyKey::new(format!("fork:{}:report", report.request.operation_id))?;
        match bounded_io::apply(&self.host, journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
            path: "/report.json".into(),
            bytes,
        }], &key, self.io.as_ref())
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
        match self.read_report(journal, &report.request).await? {
            Some(committed) if committed == *report => Ok(committed),
            Some(_) => Err(Error::Conflict(
                "fork operation has a different committed report".into(),
            )),
            None => Err(Error::Indeterminate(report.request.operation_id)),
        }
    }

    async fn capture_result(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
    ) -> Result<Option<Capture>> {
        let path = format!("/capture-{index}-result.json");
        let result =
            read_record_bounded::<A, O, CaptureResult>(&self.host, journal, &path, self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref())
                .await?;
        match result {
            Some(result) if result.selection == *selection => {
                let attempt_path = format!("/capture-{index}-attempt.json");
                let attempt = read_record_bounded::<A, O, CaptureAttempt>(&self.host, journal, &attempt_path, self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref())
                .await?
                .ok_or_else(|| Error::Storage("fork capture result has no durable start".into()))?;
                if attempt.operation_id != request.operation_id || attempt.selection != *selection {
                    return Err(Error::Conflict(
                        "fork capture result has another start identity".into(),
                    ));
                }
                validate_capture(selection, &result.capture)?;
                Ok(Some(result.capture))
            }
            Some(_) => Err(Error::Conflict(
                "fork capture result belongs to another selection".into(),
            )),
            None => Ok(None),
        }
    }

    /// Returns true only to the writer that durably won this attempt. Once a
    /// start record exists, all other callers reconcile instead of invoking
    /// an effectful provider again.
    async fn start_capture(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
    ) -> Result<bool> {
        let path = format!("/capture-{index}-attempt.json");
        if let Some(prior) =
            read_record_bounded::<A, O, CaptureAttempt>(&self.host, journal, &path, self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref())
                .await?
        {
            if prior.operation_id != request.operation_id || prior.selection != *selection {
                return Err(Error::Conflict(
                    "fork capture attempt belongs to another selection".into(),
                ));
            }
            return Ok(false);
        }
        let attempt = CaptureAttempt {
            operation_id: request.operation_id,
            selection: selection.clone(),
            attempt: Uuid::new_v4(),
        };
        let observed = bounded_io::resolve(&self.host, journal, self.io.as_ref()).await?;
        let key = IdempotencyKey::new(format!(
            "fork:{}:capture-attempt:{index}:{}",
            request.operation_id, attempt.attempt,
        ))?;
        match bounded_io::apply(&self.host, journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
            path: path.clone(),
            bytes: self.encode(&attempt, self.journal_limit(MAX_CAPTURE_BYTES))?,
        }], &key, self.io.as_ref())
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
        match read_record_bounded::<A, O, CaptureAttempt>(&self.host, journal, &path, self.journal_limit(MAX_CAPTURE_BYTES), self.io.as_ref())
            .await?
        {
            Some(prior) if prior == attempt => Ok(true),
            Some(prior)
                if prior.operation_id == request.operation_id && prior.selection == *selection =>
            {
                Ok(false)
            }
            Some(_) => Err(Error::Conflict(
                "fork capture attempt belongs to another selection".into(),
            )),
            None => Err(Error::Indeterminate(request.operation_id)),
        }
    }

    async fn commit_capture_result(
        &self,
        journal: &WorkspaceRef,
        request: &ForkRequest,
        index: usize,
        selection: &ForkSelection,
        capture: Capture,
    ) -> Result<Capture> {
        validate_capture(selection, &capture)?;
        if let Some(prior) = self
            .capture_result(journal, request, index, selection)
            .await?
        {
            return if prior == capture {
                Ok(prior)
            } else {
                Err(Error::Conflict(
                    "fork capture has another committed result".into(),
                ))
            };
        }
        let path = format!("/capture-{index}-result.json");
        let observed = bounded_io::resolve(&self.host, journal, self.io.as_ref()).await?;
        let key = IdempotencyKey::new(format!(
            "fork:{}:capture-result:{index}",
            request.operation_id,
        ))?;
        match bounded_io::apply(&self.host, journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
            path,
            bytes: self.encode(&CaptureResult {
                selection: selection.clone(),
                capture: capture.clone(),
            }, self.journal_limit(MAX_CAPTURE_BYTES))?,
        }], &key, self.io.as_ref())
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
        match self
            .capture_result(journal, request, index, selection)
            .await?
        {
            Some(committed) if committed == capture => Ok(committed),
            Some(_) => Err(Error::Conflict(
                "fork capture has another committed result".into(),
            )),
            None => Err(Error::Indeterminate(request.operation_id)),
        }
    }

    #[allow(
        clippy::too_many_lines,
        clippy::cognitive_complexity,
        reason = "one journaled fork preparation must bind each side effect to the same request"
    )]
    async fn prepare_inner(&self, request: ForkRequest) -> Result<ForkReport> {
        self.check_request(&request)?;
        self.verify_task_lease(false).await?;
        let journal = self.claim(&request).await?;
        if let Some(report) = self.read_report(&journal, &request).await? {
            self.verify_task_allocations(&request).await?;
            return Ok(report);
        }
        let source_generation = request
            .selections
            .iter()
            .find_map(|selection| match &selection.revision {
                ResourceRevision::Project { generation, .. } => Some(generation),
                _ => None,
            })
            .ok_or_else(|| Error::Invalid("fork has no project selection".into()))?;
        let request_bytes = self.encode(&request, self.journal_limit(MAX_REQUEST_BYTES))?;
        let request_digest = blake3::hash(&request_bytes);
        for (volume, role) in creation_volumes(&request) {
            self.verify_task_lease(false).await?;
            self.host
                .claim_fork_allocation_bounded(AllocationClaim {
                    operation_id: request.operation_id,
                    preparation_digest: *request_digest.as_bytes(),
                    task_origin: self
                        .task_preparation
                        .as_ref()
                        .map(|task| task.origin().clone()),
                    parent: request.parent.clone(),
                    child: request.child.clone(),
                    volume: volume.clone(),
                    role,
                }, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref())
                .await?;
        }
        let private = request.preparation.child_private_volume.clone();
        let (source_private, private_generation, paths) = request
            .selections
            .iter()
            .find_map(|selection| {
                if let ResourceRevision::PrivateVolume {
                    volume,
                    generation,
                    paths,
                } = &selection.revision
                {
                    Some((volume, generation, paths))
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::Invalid("fork has no parent scratch selection".into()))?;
        let private_key = IdempotencyKey::new(format!(
            "fork:{}:{request_digest}:scratch",
            request.operation_id
        ))?;
        self.verify_task_lease(false).await?;
        let private_observation = bounded_io::fork(
            &self.host, source_private, private_generation, &private,
            Some(paths.clone()), &private_key, self.io.as_ref(),
        ).await?;
        let controller = ParentProjectController::new(
            &self.host,
            &self.parent,
            &self.verifier,
            &self.scope,
            self.project.clone(),
        )?;
        let project_key = project_creation_key(request.operation_id, request_digest)?;
        self.verify_task_lease(false).await?;
        controller.require(capability::FORK_PUBLISH, VolumeOperation::Read)?;
        let child_project = bounded_io::fork(
            &self.host, &self.project, source_generation,
            &request.preparation.child_project_volume, None, &project_key, self.io.as_ref(),
        ).await?;
        let (child_result_volume, child_result_generation) = if let Some(volume) = &request.preparation.child_result_volume {
            self.verify_task_lease(false).await?;
            let key = IdempotencyKey::new(format!("fork:{}:{request_digest}:result",request.operation_id))?;
            let result = bounded_io::fork(
                &self.host, source_private, private_generation, volume,
                Some(Vec::new()), &key, self.io.as_ref(),
            ).await?;
            (Some(volume.clone()), Some(result.generation))
        } else { (None,None) };
        let (
            mut child_private_generation,
            mut inherited_context,
            mut reference_grants,
            attachment_manifests,
        ) = if request.preparation.inherited_through_sequence == 0 {
            (
                private_observation.generation.clone(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
        } else {
            let context_key = IdempotencyKey::new(format!(
                "fork:{}:{request_digest}:context",
                request.operation_id
            ))?;
            let continuation_keys = if request.preparation.summary.is_some() {
                super::summary_fork::summary_keys(&context_key)?.to_vec()
            } else {
                Vec::new()
            };
            let inherited = controller
                .materialize_inherited_conversation_continued(
                    &self.parent,
                    &private,
                    &private_observation.generation,
                    request.child_agent,
                    &request.attached_agents,
                    request.preparation.inherited_through_sequence,
                    usize::try_from(request.preparation.maximum_inherited_messages)
                        .map_err(|_| Error::Invalid("fork message bound is too large".into()))?,
                    request.preparation.maximum_inherited_bytes,
                    request.preparation.maximum_inherited_references as usize,
                    self.resolver.as_ref(),
                    &context_key,
                    &continuation_keys,
                    self.history.as_ref().map(PreparedForkHistory::conversation),
                )
                .await?;
            (
                inherited.generation,
                vec![inherited.file],
                inherited.reference_grants,
                inherited.attachment_manifests,
            )
        };
        let summary = if request.preparation.summary.is_some() {
            let bound = self.summary.as_ref().ok_or_else(|| {
                Error::Unsupported("summary fork provider is not installed".into())
            })?;
            let key = IdempotencyKey::new(format!(
                "fork:{}:{request_digest}:context",
                request.operation_id
            ))?;
            let prefix = inherited_context
                .first()
                .ok_or_else(|| Error::Invalid("summary fork omitted its logical prefix".into()))?;
            // Count the exact additional reader grants before either Summary
            // publication phase. Reuse original prefix grants when identical.
            let (_, summary_references) = super::summary_fork::split_references(&bound.prepared)?;
            let mut planned_grants = reference_grants
                .iter()
                .map(|grant| Ok((grant.capability()?, grant.reader)))
                .collect::<Result<BTreeSet<_>>>()?;
            for reader in
                std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied())
            {
                for file in summary_references.values() {
                    if file.volume().owner() != &crate::conversation::VolumeOwner::Agent(reader) {
                        planned_grants.insert((file.read_capability()?, reader));
                    }
                }
            }
            if planned_grants.len() > request.preparation.maximum_inherited_references as usize {
                return Err(Error::Invalid(
                    "summary fork reader grants exceed capture allowance".into(),
                ));
            }
            let (capture, generation) = controller
                .materialize_summary_context(
                    &self.parent,
                    &bound.prepared,
                    bound.journal.as_ref(),
                    &private,
                    &child_private_generation,
                    prefix,
                    request.preparation.maximum_inherited_bytes,
                    request.preparation.maximum_inherited_references as usize,
                    &key,
                )
                .await?;
            let mut granted = reference_grants
                .iter()
                .map(|grant| Ok((grant.capability()?, grant.reader)))
                .collect::<Result<BTreeSet<_>>>()?;
            for reader in
                std::iter::once(request.child_agent).chain(request.attached_agents.iter().copied())
            {
                for file in &capture.references {
                    if file.volume().owner() != &crate::conversation::VolumeOwner::Agent(reader)
                        && granted.insert((file.read_capability()?, reader))
                    {
                        reference_grants.push(ReferenceGrant {
                            file: file.clone(),
                            reader,
                            attachment_manifest: None,
                        });
                    }
                }
            }
            inherited_context.extend(capture.inherited_files().cloned());
            child_private_generation = generation;
            Some(capture)
        } else {
            None
        };
        let mut captures = Vec::with_capacity(request.selections.len());
        let mut shared_grants = Vec::new();
        for (index, selection) in request.selections.iter().enumerate() {
            self.verify_task_lease(false).await?;
            let capture = match &selection.revision {
                ResourceRevision::History(_) => Capture::Captured(CapturedResource {
                    source: selection.revision.clone(),
                    revision: selection.revision.clone(),
                }),
                ResourceRevision::Project { .. } => Capture::Captured(CapturedResource {
                    source: selection.revision.clone(),
                    revision: ResourceRevision::Project {
                        volume: request.preparation.child_project_volume.clone(),
                        generation: child_project.generation.clone(),
                    },
                }),
                ResourceRevision::PrivateVolume { paths, .. } => {
                    Capture::Captured(CapturedResource {
                        source: selection.revision.clone(),
                        revision: ResourceRevision::PrivateVolume {
                            volume: private.clone(),
                            generation: private_observation.generation.clone(),
                            paths: paths.clone(),
                        },
                    })
                }
                ResourceRevision::SharedVolume(volume)
                    if volume.provider() == &self.host.provider =>
                {
                    ContentGrant::verify(
                        &self.verifier,
                        &self.scope,
                        volume,
                        VolumeOperation::Read,
                    )?;
                    let reference =
                        workspace_ref(self.host.provider.clone(), &volume.storage_name()?)?;
                    bounded_io::resolve(&self.host, &reference, self.io.as_ref()).await?;
                    for reader in std::iter::once(request.child_agent)
                        .chain(request.attached_agents.iter().copied())
                    {
                        shared_grants.push(SharedGrant {
                            volume: volume.clone(),
                            child_agent: reader,
                            operations: BTreeSet::from([VolumeOperation::Read]),
                        });
                    }
                    Capture::Captured(CapturedResource {
                        source: selection.revision.clone(),
                        revision: selection.revision.clone(),
                    })
                }
                _ => {
                    if let Some(provider) = self
                        .capture_providers
                        .iter()
                        .find(|provider| provider.provider() == selection.revision.provider())
                    {
                        if let Some(committed) = self
                            .capture_result(&journal, &request, index, selection)
                            .await?
                        {
                            committed
                        } else {
                            let fresh = self
                                .start_capture(&journal, &request, index, selection)
                                .await?;
                            let captured = if fresh {
                                self.verify_task_lease(false).await?;
                                provider.capture(&request, selection).await?
                            } else {
                                provider
                                    .reconcile(&request, selection)
                                    .await?
                                    .ok_or(Error::Indeterminate(request.operation_id))?
                            };
                            self.commit_capture_result(
                                &journal, &request, index, selection, captured,
                            )
                            .await?
                        }
                    } else {
                        Capture::Unsupported("resource capture provider is not installed".into())
                    }
                }
            };
            captures.push(capture);
        }
        let inherited_through_sequence = request.preparation.inherited_through_sequence;
        let mut report = ForkReport {
            request,
            captures,
            child_private_volume: private.clone(),
            child_private_generation,
            child_result_volume,
            child_result_generation,
            inherited_context,
            summary,
            inherited_through_sequence,
            shared_grants,
            reference_grants,
            history_witness: None,
            attachment_manifests,
        };
        report.validate()?;
        let required_captures_ready =
            report
                .request
                .selections
                .iter()
                .zip(&report.captures)
                .all(|(selection, capture)| {
                    !selection.required || matches!(capture, Capture::Captured(_))
                });
        if required_captures_ready {
            self.verify_task_lease(false).await?;
            let mut seed = report.clone().into_seed()?;
            if let Some(history) = &self.history {
                self.parent.attest_fork_history(&mut seed, history.conversation())?;
                report.history_witness = seed.history_witness;
            }
            self.host.bind_fork_seed_bounded(&seed, self.journal_limit(MAX_REQUEST_BYTES), self.io.as_ref()).await?;
        }
        self.verify_task_lease(false).await?;
        self.commit_report(&journal, &report).await
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Claims exact child-volume identities for a custom parent-authorized
    /// preparation path. The stock preparer claims the same identities from
    /// its immutable request before creating either workspace.
    pub async fn claim_fork_seed(
        &self,
        seed: &ForkSeed,
        parent: &Reducer,
        verifier: &AuthorityVerifier,
        scope: &Scope,
    ) -> Result<()> {
        if seed.summary.is_some() {
            return Err(Error::Unsupported(
                "summary fork allocation requires its owner-bound checkpoint preparer".into(),
            ));
        }
        seed.validate()?;
        if parent.authority() != &seed.parent || parent.revision() != seed.parent_revision {
            return Err(Error::Conflict(
                "fork allocation is not at the selected parent revision".into(),
            ));
        }
        let source_project = seed
            .resources
            .iter()
            .find_map(|resource| {
                if let ResourceRevision::Project { volume, .. } = &resource.source {
                    Some(volume)
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::Invalid("fork has no source project".into()))?;
        let controller =
            ParentProjectController::new(self, parent, verifier, scope, source_project.clone())?;
        controller.require(capability::FORK_PUBLISH, VolumeOperation::Read)?;
        let parent_agent = scope
            .agent()
            .ok_or_else(|| Error::Unauthorized("fork allocation requires a parent agent".into()))?;
        if parent_agent == seed.child_agent {
            return Err(Error::Unauthorized(
                "fork child must have a distinct agent identity".into(),
            ));
        }
        let conversation = parent
            .conversation()
            .ok_or_else(|| Error::Invalid("fork parent has no conversation state".into()))?;
        if let Some(file) = seed.inherited_context.first() {
            let bytes = self
                .read_pinned(file, file.descriptor().byte_length())
                .await?;
            let actual: InheritedConversationPrefix = crate::contract::json_from_slice(&bytes)
                .map_err(|_| Error::Invalid("inherited conversation is malformed".into()))?;
            let selected = if seed.inherited_through_sequence < conversation.resident_after_sequence() {
                if seed.history_witness != Some(verifier.fork_history_proof(seed)?) {
                    return Err(Error::Unauthorized("archived fork activation requires its original witness".into()));
                }
                // The signed seed pins this file's exact descriptor/digest.
                // Earlier retired cuts cannot be recomputed from a resident suffix.
                actual.format_version == 3
                    && actual.parent == seed.parent
                    && actual.parent_revision == seed.parent_revision
                    && actual.parent_agent == parent_agent
                    && actual.through_sequence == seed.inherited_through_sequence
                    && actual.attached_agents == seed.attached_agents
            } else {
                actual == InheritedConversationPrefix::select(
                    seed.parent.clone(), seed.parent_revision, seed.inherited_through_sequence,
                    &seed.attached_agents, conversation,
                )?
            };
            if !selected || actual.canonical_bytes()?.as_slice() != bytes.as_ref() {
                return Err(Error::Conflict(
                    "inherited conversation differs from the selected parent prefix".into(),
                ));
            }
        }
        let digest = seed_binding(seed)?.digest;
        let child_project = seed
            .resources
            .iter()
            .find_map(|resource| {
                if let ResourceRevision::Project { volume, .. } = &resource.revision {
                    Some(volume)
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::Invalid("fork has no child project".into()))?;
        if source_project.provider() != &self.provider
            || child_project.provider() != &self.provider
            || seed.child_private_volume.provider() != &self.provider
        {
            return Err(Error::Invalid(
                "fork allocation belongs to another Filesystem provider".into(),
            ));
        }
        for (volume, role) in seed_creation_volumes(seed)? {
            self.claim_fork_allocation(AllocationClaim {
                operation_id: seed.operation_id,
                preparation_digest: digest,
                task_origin: None,
                parent: seed.parent.clone(),
                child: seed.child.clone(),
                volume: volume.clone(),
                role,
            })
            .await?;
        }
        self.bind_fork_seed(seed).await
    }

    pub(crate) async fn verify_fork_allocation(
        &self,
        seed: &ForkSeed,
        volume: &VolumeRef,
    ) -> Result<()> {
        let journal = allocation_ref(self.provider.clone(), volume)?;
        let claim = read_record::<A, O, AllocationClaim>(self, &journal, "/claim.json", MAX_REQUEST_BYTES)
            .await?
            .ok_or_else(|| Error::Unauthorized("fork child volume was not allocated".into()))?;
        if claim.operation_id != seed.operation_id
            || claim.parent != seed.parent
            || claim.child != seed.child
            || claim.volume != *volume
            || seed_creation_volumes(seed)?.find(|(candidate,_)| *candidate == volume).map(|(_,role)| role) != Some(claim.role)
        {
            return Err(Error::Conflict(
                "fork child volume belongs to another preparation".into(),
            ));
        }
        let binding = read_record::<A, O, SeedBinding>(self, &journal, "/seed.json", MAX_REQUEST_BYTES)
            .await?
            .ok_or_else(|| {
                Error::Unauthorized("fork seed was not bound to its allocation".into())
            })?;
        if binding != seed_binding(seed)? {
            return Err(Error::Conflict(
                "fork seed differs from its allocated publication".into(),
            ));
        }
        Ok(())
    }

    async fn bind_fork_seed(&self, seed: &ForkSeed) -> Result<()> {
        self.bind_fork_seed_bounded(seed, MAX_REQUEST_BYTES, None).await
    }

    async fn bind_fork_seed_bounded(&self, seed: &ForkSeed, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<()> {
        let binding = seed_binding_bounded(seed, io)?;
        for (volume, role) in seed_creation_volumes(seed)? {
            let journal = allocation_ref(self.provider.clone(), volume)?;
            let claim =
                read_record_bounded::<A, O, AllocationClaim>(self, &journal, "/claim.json", maximum_bytes, io)
                    .await?
                    .ok_or_else(|| {
                        Error::Unauthorized("fork child volume was not allocated".into())
                    })?;
            if claim.operation_id != seed.operation_id
                || claim.parent != seed.parent
                || claim.child != seed.child
                || claim.volume != *volume
                || claim.role != role
            {
                return Err(Error::Conflict(
                    "fork child volume belongs to another preparation".into(),
                ));
            }
            match read_record_bounded::<A, O, SeedBinding>(self, &journal, "/seed.json", maximum_bytes, io).await? {
                Some(prior) if prior == binding => continue,
                Some(_) => return Err(Error::Conflict("fork allocation has another seed".into())),
                None => {}
            }
            let observed = bounded_io::resolve(self, &journal, io).await?;
            let key = IdempotencyKey::new(format!("fork:{}:seed", seed.operation_id))?;
            match bounded_io::apply(self, &journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
                path: "/seed.json".into(),
                bytes: encode_bounded(&binding, maximum_bytes, io)?,
            }], &key, io)
                .await
            {
                Ok(_) | Err(Error::Conflict(_)) => {}
                Err(error) => return Err(error),
            }
            match read_record_bounded::<A, O, SeedBinding>(self, &journal, "/seed.json", maximum_bytes, io).await? {
                Some(prior) if prior == binding => {}
                Some(_) => return Err(Error::Conflict("fork allocation has another seed".into())),
                None => return Err(Error::Indeterminate(seed.operation_id)),
            }
        }
        Ok(())
    }

    async fn claim_fork_allocation(&self, claim: AllocationClaim) -> Result<()> {
        self.claim_fork_allocation_bounded(claim, MAX_REQUEST_BYTES, None).await
    }

    async fn claim_fork_allocation_bounded(&self, claim: AllocationClaim, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<()> {
        claim.volume.validate()?;
        if claim.volume.provider() != &self.provider {
            return Err(Error::Invalid(
                "fork allocation belongs to another Filesystem provider".into(),
            ));
        }
        let journal = allocation_ref(self.provider.clone(), &claim.volume)?;
        let name = std::str::from_utf8(journal.as_resource().key())
            .map_err(|_| Error::Invalid("allocation journal name is not UTF-8".into()))?;
        let creation_key = IdempotencyKey::new(format!("fork:{}:allocation-journal", claim.operation_id))?;
        bounded_io::create(self, name, &creation_key, io).await?;
        match read_record_bounded::<A, O, AllocationClaim>(self, &journal, "/claim.json", maximum_bytes, io).await? {
            Some(prior) if same_allocation_claim(&prior, &claim) => return Ok(()),
            Some(_) => {
                return Err(Error::Conflict(
                    "fork child volume is already allocated".into(),
                ));
            }
            None => {}
        }
        let observed = bounded_io::resolve(self, &journal, io).await?;
        let key = IdempotencyKey::new(format!("fork:{}:allocation", claim.operation_id))?;
        match bounded_io::apply(self, &journal, Some(&observed.generation), &[WorkspaceMutation::PutFile {
            path: "/claim.json".into(),
            bytes: encode_bounded(&claim, maximum_bytes, io)?,
        }], &key, io)
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
        match read_record_bounded::<A, O, AllocationClaim>(self, &journal, "/claim.json", maximum_bytes, io).await? {
            Some(prior) if same_allocation_claim(&prior, &claim) => Ok(()),
            Some(_) => Err(Error::Conflict(
                "fork child volume is already allocated".into(),
            )),
            None => Err(Error::Indeterminate(claim.operation_id)),
        }
    }
}

fn same_allocation_claim(retained: &AllocationClaim, requested: &AllocationClaim) -> bool {
    let mut expected = requested.clone();
    if let (Some(prior), Some(current)) = (&retained.task_origin, &mut expected.task_origin) {
        // A legitimate recovery owns a new current reservation. Keep the first
        // creation fence in the protected record; compare all other bindings.
        current.creation_fence = prior.creation_fence.clone();
    }
    retained == &expected
}

fn allocation_ref(provider: ProviderRef, volume: &VolumeRef) -> Result<WorkspaceRef> {
    workspace_ref(
        provider,
        &format!("fork-allocation-{}", volume.storage_name()?),
    )
}

fn seed_binding(seed: &ForkSeed) -> Result<SeedBinding> {
    seed_binding_bounded(seed, None)
}

fn seed_binding_bounded(seed: &ForkSeed, io: Option<&PreparationIo>) -> Result<SeedBinding> {
    seed.validate()?;
    Ok(SeedBinding {
        digest: *blake3::hash(&encode_bounded(seed, MAX_REPORT_BYTES, io)?).as_bytes(),
    })
}

fn encode_bounded<T: Serialize>(value: &T, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<Vec<u8>> {
    match io {
        Some(io) => io.encode(value, maximum_bytes),
        None => encode_record(value, maximum_bytes),
    }
}


async fn read_request_claim<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, request: &ForkRequest, maximum_bytes: u64, io: Option<&PreparationIo>,
) -> Result<bool> {
    match read_record_bounded::<A, O, ForkRequest>(host, journal, "/request.json", maximum_bytes, io).await? {
        Some(claim) if claim == *request => Ok(true),
        Some(_) => Err(Error::Conflict("fork operation is claimed by another request".into())),
        None => Ok(false),
    }
}

async fn read_preparation_report<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, request: &ForkRequest, maximum_bytes: u64, io: Option<&PreparationIo>,
) -> Result<Option<ForkReport>> {
    let report = read_record_bounded::<A, O, ForkReport>(host, journal, "/report.json", maximum_bytes, io).await?;
    if let Some(report) = &report {
        report.validate()?;
        if report.request != *request {
            return Err(Error::Conflict("fork report does not match its claimed request".into()));
        }
    }
    Ok(report)
}

fn validate_capture(selection: &ForkSelection, capture: &Capture) -> Result<()> {
    match capture {
        Capture::Captured(resource) if resource.source == selection.revision => resource.validate(),
        Capture::Captured(_) => Err(Error::Invalid(
            "capture provider substituted another source revision".into(),
        )),
        Capture::Unsupported(reason) if !reason.is_empty() => Ok(()),
        Capture::Unsupported(_) => Err(Error::Invalid("fork capture reason is invalid".into())),
        Capture::InFlight(operation) | Capture::Indeterminate(operation) => {
            Err(Error::Indeterminate(*operation))
        }
    }
}

impl<A, O> ForkPreparer for FilesystemForkPreparer<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn parent_snapshot(&self) -> (&Authority, u64) {
        (self.parent.authority(), self.parent.revision())
    }

    fn prepare<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<ForkReport>> {
        Box::pin(async move { self.attempt()?.prepare_inner(request).await })
    }

    fn reconcile<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<Option<ForkReport>>> {
        Box::pin(async move {
            let this = self.attempt()?;
            this.authorize_request(&request)?;
            this.verify_task_lease(true).await?;
            let journal = this.journal_ref(&request)?;
            match bounded_io::resolve(&this.host, &journal, this.io.as_ref()).await {
                Ok(_) => {}
                Err(Error::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
            if !this.read_claim(&journal, &request).await? {
                return Ok(None);
            }
            let report = this.read_report(&journal, &request).await?;
            if report.is_some() {
                this.verify_task_allocations(&request).await?;
            }
            Ok(report)
        })
    }
}

fn project_creation_key(operation: OperationId, request_digest: blake3::Hash) -> Result<IdempotencyKey> {
    IdempotencyKey::new(format!("fork:{operation}:{request_digest}:project"))
}

fn encode_record<T: Serialize>(value: &T, maximum_bytes: u64) -> Result<Vec<u8>> {
    struct BoundedRecord {
        bytes: Vec<u8>,
        maximum: u64,
    }
    impl std::io::Write for BoundedRecord {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let length = (self.bytes.len() as u64).checked_add(bytes.len() as u64)
                .ok_or_else(|| std::io::Error::other("fork journal byte count overflows"))?;
            if length > self.maximum {
                return Err(std::io::Error::other("fork journal record exceeds byte limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let mut record = BoundedRecord { bytes: Vec::new(), maximum: maximum_bytes };
    serde_json::to_writer(&mut record, value).map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(record.bytes)
}

async fn read_record<A: AsyncAuthorityStore, O: AsyncObjectStore, T: DeserializeOwned>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    path: &str,
    maximum_bytes: u64,
) -> Result<Option<T>> {
    read_record_bounded(host, journal, path, maximum_bytes, None).await
}

async fn read_record_bounded<A: AsyncAuthorityStore, O: AsyncObjectStore, T: DeserializeOwned>(
    host: &FilesystemHost<A, O>, journal: &WorkspaceRef, path: &str, maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<Option<T>> {
    match bounded_io::read(host, journal, path, maximum_bytes, io).await {
        Ok(bytes) => crate::contract::json_from_slice(&bytes)
            .map(Some)
            .map_err(|error| Error::Invalid(format!("fork journal record is invalid: {error}"))),
        Err(Error::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod task_origin_tests {
    use super::*;
    use crate::conversation::{VolumeClass, VolumeOwner};
    use crate::core::AggregateKind;
    use crate::{AgentId, TaskId};

    #[test]
    fn record_encoder_stops_at_the_admitted_byte_boundary() -> Result<()> {
        assert_eq!(encode_record(&"x", 3)?, b"\"x\"");
        assert!(matches!(encode_record(&"x", 2), Err(Error::Invalid(_))));
        assert!(matches!(encode_record(&[0_u8; 64], 8), Err(Error::Invalid(_))));
        Ok(())
    }

    #[tokio::test]
    async fn allocation_retains_creation_fence_and_rejects_foreign_task_origins() -> Result<()> {
        let provider = ProviderRef::new("task-origin-test", "filesystem", "2")?;
        let host = FilesystemHost::new(acyclic_fs::Fs::memory(), provider.clone())?;
        let parent = Authority {
            kind: AggregateKind::Task,
            id: "parent".into(),
        };
        let child = Authority {
            kind: AggregateKind::Task,
            id: "child".into(),
        };
        let volume = VolumeRef::new(
            provider,
            "child",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([71; 16])),
        )?;
        let origin = ResourceCreationOrigin {
            owner: parent.clone(),
            parent_task: TaskId::from_bytes([72; 16]),
            preparation_task: TaskId::from_bytes([73; 16]),
            intended_child: OperationId::from_bytes([74; 16]),
            admission_digest: [75; 32],
            request_digest: [76; 32],
            creation_fence: crate::scheduler::LeaseFence {
                reservation_id: "original".into(),
                placement: "first-worker".into(),
            },
        };
        let claim = AllocationClaim {
            operation_id: OperationId::from_bytes([77; 16]),
            preparation_digest: [78; 32],
            task_origin: Some(origin),
            parent,
            child,
            volume: volume.clone(),
            role: ForkAllocationRole::Scratch,
        };
        host.claim_fork_allocation(claim.clone()).await?;
        let mut recovery = claim.clone();
        if let Some(origin) = &mut recovery.task_origin {
            origin.creation_fence = crate::scheduler::LeaseFence {
                reservation_id: "recovered".into(),
                placement: "later-worker".into(),
            };
        }
        host.claim_fork_allocation(recovery).await?;
        let journal = allocation_ref(host.provider.clone(), &volume)?;
        let retained = read_record::<_, _, AllocationClaim>(&host, &journal, "/claim.json", 65_536)
            .await?
            .ok_or_else(|| Error::NotFound("retained allocation".into()))?;
        assert_eq!(retained, claim);
        for field in 0..6 {
            let mut changed = claim.clone();
            let origin = changed
                .task_origin
                .as_mut()
                .ok_or_else(|| Error::NotFound("test task origin".into()))?;
            match field {
                0 => origin.owner.id = "foreign".into(),
                1 => origin.parent_task = TaskId::from_bytes([79; 16]),
                2 => origin.preparation_task = TaskId::from_bytes([80; 16]),
                3 => origin.intended_child = OperationId::from_bytes([81; 16]),
                4 => origin.admission_digest = [82; 32],
                _ => origin.request_digest = [83; 32],
            }
            assert!(matches!(
                host.claim_fork_allocation(changed).await,
                Err(Error::Conflict(_))
            ));
        }
        let mut raw = claim.clone();
        raw.task_origin = None;
        assert!(matches!(
            host.claim_fork_allocation(raw).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(
            read_record::<_, _, AllocationClaim>(&host, &journal, "/claim.json", 65_536)
                .await?
                .ok_or_else(|| Error::NotFound("retained allocation".into()))?,
            claim
        );
        Ok(())
    }
}
