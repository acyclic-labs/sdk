//! Provider-owned fork preparation journal and exact Filesystem captures.
//!
//! The journal is a separate workspace: putting it in the child private or
//! project volume would leak implementation metadata into the published fork.

use super::{FilesystemHost, ParentProjectController, WorkspaceMutation, map_error, workspace_ref};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_harness::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentGrant, ContentResidencyVerifier, VolumeOperation, VolumeRef},
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkPreparer, ForkReport, ForkRequest,
        ForkSeed, ForkSelection, InheritedConversationPrefix, ResourceRevision, SharedGrant,
    },
    resources::{ProviderRef, WorkspaceRef},
};
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{collections::BTreeSet, sync::Arc};
use uuid::Uuid;

const MAX_REQUEST_BYTES: u64 = 64 * 1_024 * 1_024;
const MAX_REPORT_BYTES: u64 = 128 * 1_024 * 1_024;
const MAX_CAPTURE_BYTES: u64 = 1_024 * 1_024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AllocationClaim {
    operation_id: OperationId,
    preparation_digest: [u8; 32],
    parent: Authority,
    child: Authority,
    volume: VolumeRef,
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
        })
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
    fn authorize_request(&self, request: &ForkRequest) -> Result<()> {
        request.validate()?;
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
                _ => {}
            }
        }
        Ok(())
    }

    fn check_request(&self, request: &ForkRequest) -> Result<()> {
        self.authorize_request(request)?;
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
        match read_record::<A, O, ForkRequest>(
            &self.host,
            journal,
            "/request.json",
            MAX_REQUEST_BYTES,
        )
        .await?
        {
            Some(claim) if claim == *request => Ok(true),
            Some(_) => Err(Error::Conflict(
                "fork operation is claimed by another request".into(),
            )),
            None => Ok(false),
        }
    }

    async fn claim(&self, request: &ForkRequest) -> Result<WorkspaceRef> {
        let journal = self.journal_ref(request)?;
        let name = std::str::from_utf8(journal.as_resource().key())
            .map_err(|_| Error::Invalid("fork journal name is not UTF-8".into()))?;
        self.host
            .filesystem
            .create_workspace(name)
            .await
            .map_err(map_error)?;
        if self.read_claim(&journal, request).await? {
            return Ok(journal);
        }
        let bytes = encode_record(request, MAX_REQUEST_BYTES)?;
        let observed = self.host.resolve(&journal).await?;
        let key = IdempotencyKey::new(format!("fork:{}:claim", request.operation_id))?;
        match self
            .host
            .apply(
                &journal,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/request.json".into(),
                    bytes,
                }],
                &key,
            )
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
        let report =
            read_record::<A, O, ForkReport>(&self.host, journal, "/report.json", MAX_REPORT_BYTES)
                .await?;
        if let Some(report) = &report {
            report.validate()?;
            if report.request != *request {
                return Err(Error::Conflict(
                    "fork report does not match its claimed request".into(),
                ));
            }
        }
        Ok(report)
    }

    async fn commit_report(
        &self,
        journal: &WorkspaceRef,
        report: &ForkReport,
    ) -> Result<ForkReport> {
        let bytes = encode_record(report, MAX_REPORT_BYTES)?;
        let observed = self.host.resolve(journal).await?;
        let key = IdempotencyKey::new(format!("fork:{}:report", report.request.operation_id))?;
        match self
            .host
            .apply(
                journal,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/report.json".into(),
                    bytes,
                }],
                &key,
            )
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
            read_record::<A, O, CaptureResult>(&self.host, journal, &path, MAX_CAPTURE_BYTES)
                .await?;
        match result {
            Some(result) if result.selection == *selection => {
                let attempt_path = format!("/capture-{index}-attempt.json");
                let attempt = read_record::<A, O, CaptureAttempt>(
                    &self.host,
                    journal,
                    &attempt_path,
                    MAX_CAPTURE_BYTES,
                )
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
            read_record::<A, O, CaptureAttempt>(&self.host, journal, &path, MAX_CAPTURE_BYTES)
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
        let observed = self.host.resolve(journal).await?;
        let key = IdempotencyKey::new(format!(
            "fork:{}:capture-attempt:{index}:{}",
            request.operation_id, attempt.attempt,
        ))?;
        match self
            .host
            .apply(
                journal,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path: path.clone(),
                    bytes: encode_record(&attempt, MAX_CAPTURE_BYTES)?,
                }],
                &key,
            )
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
        match read_record::<A, O, CaptureAttempt>(&self.host, journal, &path, MAX_CAPTURE_BYTES)
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
        let observed = self.host.resolve(journal).await?;
        let key = IdempotencyKey::new(format!(
            "fork:{}:capture-result:{index}",
            request.operation_id,
        ))?;
        match self
            .host
            .apply(
                journal,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path,
                    bytes: encode_record(
                        &CaptureResult {
                            selection: selection.clone(),
                            capture: capture.clone(),
                        },
                        MAX_CAPTURE_BYTES,
                    )?,
                }],
                &key,
            )
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
        let journal = self.claim(&request).await?;
        if let Some(report) = self.read_report(&journal, &request).await? {
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
        let request_bytes = encode_record(&request, MAX_REQUEST_BYTES)?;
        let request_digest = blake3::hash(&request_bytes);
        for volume in [
            &request.preparation.child_private_volume,
            &request.preparation.child_project_volume,
        ] {
            self.host
                .claim_fork_allocation(AllocationClaim {
                    operation_id: request.operation_id,
                    preparation_digest: *request_digest.as_bytes(),
                    parent: request.parent.clone(),
                    child: request.child.clone(),
                    volume: volume.clone(),
                })
                .await?;
        }
        let private = request.preparation.child_private_volume.clone();
        let private_observation = self.host.create_volume(&private).await?;
        let controller = ParentProjectController::new(
            &self.host,
            &self.parent,
            &self.verifier,
            &self.scope,
            self.project.clone(),
        )?;
        let project_key = IdempotencyKey::new(format!(
            "fork:{}:{request_digest}:project",
            request.operation_id
        ))?;
        let child_project = controller
            .fork_project(
                source_generation,
                &request.preparation.child_project_volume,
                &project_key,
            )
            .await?;
        let (child_private_generation, inherited_context, reference_grants, attachment_manifests) =
            if request.preparation.inherited_through_sequence == 0 {
                let entries = self
                    .host
                    .list(
                        &private_observation.workspace,
                        Some(&private_observation.generation),
                        "/",
                        1,
                    )
                    .await?;
                if !entries.entries.is_empty() {
                    return Err(Error::Conflict("child private volume is not empty".into()));
                }
                (
                    private_observation.generation,
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                )
            } else {
                let context_key = IdempotencyKey::new(format!(
                    "fork:{}:{request_digest}:context",
                    request.operation_id
                ))?;
                let inherited = controller
                    .materialize_inherited_conversation(
                        &self.parent,
                        &private,
                        request.child_agent,
                        &request.attached_agents,
                        request.preparation.inherited_through_sequence,
                        usize::try_from(request.preparation.maximum_inherited_messages).map_err(
                            |_| Error::Invalid("fork message bound is too large".into()),
                        )?,
                        request.preparation.maximum_inherited_bytes,
                        request.preparation.maximum_inherited_references as usize,
                        self.resolver.as_ref(),
                        &context_key,
                    )
                    .await?;
                (
                    inherited.generation,
                    vec![inherited.file],
                    inherited.reference_grants,
                    inherited.attachment_manifests,
                )
            };
        let mut captures = Vec::with_capacity(request.selections.len());
        let mut shared_grants = Vec::new();
        for (index, selection) in request.selections.iter().enumerate() {
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
                    self.host.resolve(&reference).await?;
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
        let report = ForkReport {
            request,
            captures,
            child_private_volume: private.clone(),
            child_private_generation,
            inherited_context,
            inherited_through_sequence,
            shared_grants,
            reference_grants,
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
            let seed = report.clone().into_seed()?;
            self.host.bind_fork_seed(&seed).await?;
        }
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
        controller.require("fork:publish", VolumeOperation::Read)?;
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
            let bytes = self.read_pinned(file, 64 * 1_024 * 1_024).await?;
            let actual: InheritedConversationPrefix = serde_json::from_slice(&bytes)
                .map_err(|_| Error::Invalid("inherited conversation is malformed".into()))?;
            let expected = InheritedConversationPrefix::select(
                seed.parent.clone(),
                seed.parent_revision,
                parent_agent,
                seed.inherited_through_sequence,
                &seed.attached_agents,
                &conversation.messages,
            )?;
            if actual != expected || actual.canonical_bytes()?.as_slice() != bytes.as_ref() {
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
        for volume in [&seed.child_private_volume, child_project] {
            self.claim_fork_allocation(AllocationClaim {
                operation_id: seed.operation_id,
                preparation_digest: digest,
                parent: seed.parent.clone(),
                child: seed.child.clone(),
                volume: volume.clone(),
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
        let claim = read_record::<A, O, AllocationClaim>(self, &journal, "/claim.json", 4_096)
            .await?
            .ok_or_else(|| Error::Unauthorized("fork child volume was not allocated".into()))?;
        if claim.operation_id != seed.operation_id
            || claim.parent != seed.parent
            || claim.child != seed.child
            || claim.volume != *volume
        {
            return Err(Error::Conflict(
                "fork child volume belongs to another preparation".into(),
            ));
        }
        let binding = read_record::<A, O, SeedBinding>(self, &journal, "/seed.json", 4_096)
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
        let binding = seed_binding(seed)?;
        for volume in [
            &seed.child_private_volume,
            seed.resources
                .iter()
                .find_map(|resource| {
                    if let ResourceRevision::Project { volume, .. } = &resource.revision {
                        Some(volume)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| Error::Invalid("fork has no child project".into()))?,
        ] {
            let journal = allocation_ref(self.provider.clone(), volume)?;
            let claim = read_record::<A, O, AllocationClaim>(self, &journal, "/claim.json", 4_096)
                .await?
                .ok_or_else(|| Error::Unauthorized("fork child volume was not allocated".into()))?;
            if claim.operation_id != seed.operation_id
                || claim.parent != seed.parent
                || claim.child != seed.child
                || claim.volume != *volume
            {
                return Err(Error::Conflict(
                    "fork child volume belongs to another preparation".into(),
                ));
            }
            match read_record::<A, O, SeedBinding>(self, &journal, "/seed.json", 4_096).await? {
                Some(prior) if prior == binding => continue,
                Some(_) => return Err(Error::Conflict("fork allocation has another seed".into())),
                None => {}
            }
            let observed = self.resolve(&journal).await?;
            let key = IdempotencyKey::new(format!("fork:{}:seed", seed.operation_id))?;
            match self
                .apply(
                    &journal,
                    Some(&observed.generation),
                    &[WorkspaceMutation::PutFile {
                        path: "/seed.json".into(),
                        bytes: encode_record(&binding, 4_096)?,
                    }],
                    &key,
                )
                .await
            {
                Ok(_) | Err(Error::Conflict(_)) => {}
                Err(error) => return Err(error),
            }
            match read_record::<A, O, SeedBinding>(self, &journal, "/seed.json", 4_096).await? {
                Some(prior) if prior == binding => {}
                Some(_) => return Err(Error::Conflict("fork allocation has another seed".into())),
                None => return Err(Error::Indeterminate(seed.operation_id)),
            }
        }
        Ok(())
    }

    async fn claim_fork_allocation(&self, claim: AllocationClaim) -> Result<()> {
        claim.volume.validate()?;
        if claim.volume.provider() != &self.provider {
            return Err(Error::Invalid(
                "fork allocation belongs to another Filesystem provider".into(),
            ));
        }
        let journal = allocation_ref(self.provider.clone(), &claim.volume)?;
        let name = std::str::from_utf8(journal.as_resource().key())
            .map_err(|_| Error::Invalid("allocation journal name is not UTF-8".into()))?;
        self.filesystem
            .create_workspace(name)
            .await
            .map_err(map_error)?;
        match read_record::<A, O, AllocationClaim>(self, &journal, "/claim.json", 4_096).await? {
            Some(prior) if prior == claim => return Ok(()),
            Some(_) => {
                return Err(Error::Conflict(
                    "fork child volume is already allocated".into(),
                ));
            }
            None => {}
        }
        let observed = self.resolve(&journal).await?;
        let key = IdempotencyKey::new(format!("fork:{}:allocation", claim.operation_id))?;
        match self
            .apply(
                &journal,
                Some(&observed.generation),
                &[WorkspaceMutation::PutFile {
                    path: "/claim.json".into(),
                    bytes: encode_record(&claim, 4_096)?,
                }],
                &key,
            )
            .await
        {
            Ok(_) | Err(Error::Conflict(_)) => {}
            Err(error) => return Err(error),
        }
        match read_record::<A, O, AllocationClaim>(self, &journal, "/claim.json", 4_096).await? {
            Some(prior) if prior == claim => Ok(()),
            Some(_) => Err(Error::Conflict(
                "fork child volume is already allocated".into(),
            )),
            None => Err(Error::Indeterminate(claim.operation_id)),
        }
    }
}

fn allocation_ref(provider: ProviderRef, volume: &VolumeRef) -> Result<WorkspaceRef> {
    workspace_ref(
        provider,
        &format!("fork-allocation-{}", volume.storage_name()?),
    )
}

fn seed_binding(seed: &ForkSeed) -> Result<SeedBinding> {
    seed.validate()?;
    Ok(SeedBinding {
        digest: *blake3::hash(&encode_record(seed, MAX_REPORT_BYTES)?).as_bytes(),
    })
}

fn validate_capture(selection: &ForkSelection, capture: &Capture) -> Result<()> {
    match capture {
        Capture::Captured(resource) if resource.source == selection.revision => resource.validate(),
        Capture::Captured(_) => Err(Error::Invalid(
            "capture provider substituted another source revision".into(),
        )),
        Capture::Unsupported(reason) if !reason.is_empty() && reason.len() <= 4_096 => Ok(()),
        Capture::Unsupported(_) => Err(Error::Invalid("fork capture reason is invalid".into())),
        Capture::InFlight(operation) | Capture::Indeterminate(operation) => {
            Err(Error::Indeterminate(*operation))
        }
    }
}

impl<A, O> ForkPreparer for FilesystemForkPreparer<A, O>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn parent_snapshot(&self) -> (&Authority, u64) {
        (self.parent.authority(), self.parent.revision())
    }

    fn prepare<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<ForkReport>> {
        Box::pin(async move { self.prepare_inner(request).await })
    }

    fn reconcile<'a>(&'a self, request: ForkRequest) -> BoxFuture<'a, Result<Option<ForkReport>>> {
        Box::pin(async move {
            self.authorize_request(&request)?;
            let journal = self.journal_ref(&request)?;
            match self.host.resolve(&journal).await {
                Ok(_) => {}
                Err(Error::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            }
            if !self.read_claim(&journal, &request).await? {
                return Ok(None);
            }
            self.read_report(&journal, &request).await
        })
    }
}

fn encode_record<T: Serialize>(value: &T, maximum_bytes: u64) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|error| Error::Invalid(error.to_string()))?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(Error::Invalid(
            "fork journal record exceeds byte limit".into(),
        ));
    }
    Ok(bytes)
}

async fn read_record<A: AsyncAuthorityStore, O: AsyncObjectStore, T: DeserializeOwned>(
    host: &FilesystemHost<A, O>,
    journal: &WorkspaceRef,
    path: &str,
    maximum_bytes: u64,
) -> Result<Option<T>> {
    match host.read(journal, None, path, maximum_bytes).await {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| Error::Invalid(format!("fork journal record is invalid: {error}"))),
        Err(Error::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}
