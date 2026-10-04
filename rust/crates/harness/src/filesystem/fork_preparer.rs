//! Provider-owned fork preparation journal and exact Filesystem captures.
//!
//! The journal is a separate workspace: putting it in the child private or
//! project volume would leak implementation metadata into the published fork.

use super::{FilesystemHost, ParentProjectController, WorkspaceMutation, map_error, workspace_ref};
use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{
        ContentGrant, ContentResidencyVerifier, VolumeClass, VolumeOperation, VolumeOwner,
        VolumeRef,
    },
    core::{Authority, AuthorityVerifier, Reducer, Scope},
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkPreparer, ForkReport, ForkRequest,
        ForkSeed, ForkSelection, InheritedConversationPrefix, ReferenceGrant, ResourceRevision,
        SharedGrant,
    },
    resources::{ProviderRef, WorkspaceRef},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
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

    /// Authenticates durable preparation records before allowing a later
    /// publication to rebind its parent history revision.
    pub(crate) async fn authenticate_rebind(
        &self,
        request: &ForkRequest,
    ) -> Result<ForkRebindProof> {
        self.authorize_request(request)?;
        self.authenticate_rebind_records(request).await
    }

    /// Reconstructs the rebound capability from durable preparation records.
    pub(crate) async fn authenticate_rebind_records(
        &self,
        request: &ForkRequest,
    ) -> Result<ForkRebindProof> {
        request.validate()?;
        let journal = self.journal_ref(request)?;
        let stored_request = read_record::<A, O, ForkRequest>(
            &self.host,
            &journal,
            "/request.json",
            MAX_REQUEST_BYTES,
        )
        .await?
        .ok_or_else(|| Error::Conflict("fork preparation request is missing".into()))?;
        if stored_request != *request {
            return Err(Error::Conflict("fork preparation request changed before rebind".into()));
        }
        let report = self
            .read_report(&journal, request)
            .await?
            .ok_or_else(|| Error::Conflict("fork preparation report is missing".into()))?;
        let preparation_digest =
            *blake3::hash(&encode_record(request, MAX_REQUEST_BYTES)?).as_bytes();
        let original_request_digest = crate::contract::canonical_json_digest(request)?;
        let report_digest = crate::contract::canonical_json_digest(&report)?;
        for volume in [
            &request.preparation.child_private_volume,
            &request.preparation.child_project_volume,
        ] {
            let allocation = allocation_ref(self.host.provider.clone(), volume)?;
            let claim = read_record::<A, O, AllocationClaim>(
                &self.host,
                &allocation,
                "/claim.json",
                4_096,
            )
            .await?
            .ok_or_else(|| Error::Conflict("fork allocation claim is missing".into()))?;
            if claim.operation_id != request.operation_id
                || claim.parent != request.parent
                || claim.child != request.child
                || claim.volume != *volume
                || claim.preparation_digest != preparation_digest
            {
                return Err(Error::Conflict(
                    "fork allocation claim is not bound to the preparation request".into(),
                ));
            }
        }
        Ok(ForkRebindProof::from_preparation(
            request.operation_id,
            original_request_digest,
            preparation_digest,
            report_digest,
        ))
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
        if let Some(model_boundary) = &request.model_boundary {
            // Authenticate the exact child identity and model manifest before
            // claiming the durable preparation journal. A forged proof must
            // not leave a retry claim or any later allocation effect behind.
            self.verifier.verify_model_boundary(
                &request.parent,
                &request.child,
                request.child_agent,
                &request.attached_agents,
                model_boundary,
            )?;
        }
        let journal = self.claim(&request).await?;
        if let Some(report) = self.read_report(&journal, &request).await? {
            return Ok(report);
        }
        if let Some(model_boundary) = &request.model_boundary {
            for file in &model_boundary.files {
                // Resolve before claiming or forking child workspaces. The
                // parent exact scope is the admission authority for every
                // model prefix/suffix ref.
                self.resolver.read(file).await?;
            }
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
        let (
            child_private_generation,
            inherited_context,
            mut reference_grants,
            attachment_manifests,
        ) = if request.preparation.inherited_through_sequence == 0 {
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
                    usize::try_from(request.preparation.maximum_inherited_messages)
                        .map_err(|_| Error::Invalid("fork message bound is too large".into()))?,
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
        if let Some(model_boundary) = &request.model_boundary {
            let mut granted = reference_grants
                .iter()
                .map(|grant| {
                    grant
                        .file
                        .read_capability()
                        .map(|capability| (grant.reader, capability))
                })
                .collect::<Result<BTreeSet<_>>>()?;
            let readers = std::iter::once(request.child_agent)
                .chain(request.attached_agents.iter().copied())
                .collect::<Vec<_>>();
            for file in &model_boundary.files {
                let capability = file.read_capability()?;
                for reader in &readers {
                    if file.volume().class() == VolumeClass::AgentPrivate
                        && file.volume().owner() == &VolumeOwner::Agent(*reader)
                    {
                        continue;
                    }
                    if granted.insert((*reader, capability.clone())) {
                        reference_grants.push(ReferenceGrant {
                            file: file.clone(),
                            reader: *reader,
                            attachment_manifest: None,
                        });
                    }
                }
            }
            if reference_grants.len() > request.preparation.maximum_inherited_references as usize {
                return Err(Error::Invalid(
                    "model boundary read grants exceed limit".into(),
                ));
            }
        }
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
            original_request_digest: None,
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

    /// Rebinds an allocated seed after another child has advanced the parent
    /// stream. The rebind intent is durable before either allocation journal
    /// mutates its seed binding. A retry therefore either observes the exact
    /// same intent and finishes the write, or fails closed on a different
    /// transition.
    pub(crate) async fn rebind_fork_seed(
        &self,
        old_seed: &ForkSeed,
        new_seed: &ForkSeed,
    ) -> Result<()> {
        old_seed.validate()?;
        new_seed.validate()?;
        let old_binding = seed_binding(old_seed)?;
        let new_binding = seed_binding(new_seed)?;
        if old_seed.operation_id != new_seed.operation_id
            || old_seed.parent != new_seed.parent
            || old_seed.child != new_seed.child
            || old_seed.child_agent != new_seed.child_agent
            || old_seed.child_private_volume != new_seed.child_private_volume
            || seed_allocation_volumes(old_seed)?[1] != seed_allocation_volumes(new_seed)?[1]
            || old_seed.attached_agents != new_seed.attached_agents
            || old_seed.omissions != new_seed.omissions
            || old_seed.child_private_generation != new_seed.child_private_generation
            || old_seed.inherited_context != new_seed.inherited_context
            || old_seed.inherited_through_sequence != new_seed.inherited_through_sequence
            || old_seed.shared_grants != new_seed.shared_grants
            || old_seed.reference_grants != new_seed.reference_grants
            || old_seed.model_boundary != new_seed.model_boundary
            || old_seed.attachment_manifests != new_seed.attachment_manifests
            || old_seed.boundary != new_seed.boundary
            || old_seed.resources.len() != new_seed.resources.len()
            || old_seed
                .resources
                .iter()
                .zip(&new_seed.resources)
                .any(|(old, new)| !captured_resource_rebind_shape_equal(old, new))
        {
            return Err(Error::Conflict(
                "fork seed rebind changes data outside its parent history boundary".into(),
            ));
        }
        if old_binding == new_binding {
            return Ok(());
        }
        let volumes = seed_allocation_volumes(new_seed)?;
        let mut journals = Vec::with_capacity(volumes.len());
        // Phase one: every allocation records the same authenticated intent.
        // No seed file is changed until all journals have acknowledged it.
        for volume in &volumes {
            let journal = allocation_ref(self.provider.clone(), volume)?;
            let claim = read_record::<A, O, AllocationClaim>(self, &journal, "/claim.json", 4_096)
                .await?
                .ok_or_else(|| Error::Unauthorized("fork child volume was not allocated".into()))?;
            if claim.operation_id != new_seed.operation_id
                || claim.parent != new_seed.parent
                || claim.child != new_seed.child
                || claim.volume != *volume
            {
                return Err(Error::Conflict(
                    "fork child volume belongs to another preparation".into(),
                ));
            }
            let current =
                read_record::<A, O, SeedBinding>(self, &journal, "/seed.json", 4_096).await?;
            // Keep each transition as an immutable receipt. A single mutable
            // intent file would permanently fence a later retry if the
            // parent advanced again before the earlier publication reconciled.
            let path = seed_rebind_path(new_binding.digest);
            let same_identity = |intent: &SeedRebindIntent| {
                intent.operation_id == new_seed.operation_id
                    && intent.parent == new_seed.parent
                    && intent.child == new_seed.child
                    && intent.volume == *volume
            };
            if current
                .as_ref()
                .is_some_and(|binding| binding.digest == new_binding.digest)
            {
                let prior = read_record::<A, O, SeedRebindIntent>(self, &journal, &path, 4_096)
                    .await?
                    .ok_or_else(|| {
                        Error::Conflict(
                            "fork seed reached a rebound value without its durable intent".into(),
                        )
                    })?;
                if !same_identity(&prior)
                    || prior.from != old_binding.digest
                    || prior.to != new_binding.digest
                {
                    return Err(Error::Conflict(
                        "fork allocation has another seed rebind intent".into(),
                    ));
                }
                journals.push((journal, prior));
                continue;
            }
            let from = match current {
                None => old_binding.digest,
                Some(binding) if binding.digest == old_binding.digest => binding.digest,
                Some(_) => {
                    // A different transition already advanced this allocation.
                    // Its receipt may prove that transition, but it cannot
                    // authorize skipping the caller's admitted predecessor.
                    return Err(Error::Conflict(
                        "fork seed is ahead of the admitted rebind predecessor".into(),
                    ));
                }
            };
            let intent = SeedRebindIntent {
                operation_id: new_seed.operation_id,
                parent: new_seed.parent.clone(),
                child: new_seed.child.clone(),
                volume: volume.clone(),
                from,
                to: new_binding.digest,
            };
            match read_record::<A, O, SeedRebindIntent>(self, &journal, &path, 4_096).await? {
                Some(prior) if prior == intent => {}
                Some(_) => {
                    return Err(Error::Conflict(
                        "fork allocation has another seed rebind intent".into(),
                    ));
                }
                None => {
                    let observed = self.resolve(&journal).await?;
                    let key = IdempotencyKey::new(format!(
                        "fork:{}:seed-rebind-intent:{}",
                        new_seed.operation_id,
                        blake3::Hash::from_bytes(new_binding.digest).to_hex(),
                    ))?;
                    match self
                        .apply(
                            &journal,
                            Some(&observed.generation),
                            &[WorkspaceMutation::PutFile {
                                path: path.clone(),
                                bytes: encode_record(&intent, 4_096)?,
                            }],
                            &key,
                        )
                        .await
                    {
                        Ok(_) | Err(Error::Conflict(_)) => {}
                        Err(error) => return Err(error),
                    }
                    match read_record::<A, O, SeedRebindIntent>(self, &journal, &path, 4_096)
                        .await?
                    {
                        Some(prior) if prior == intent => {}
                        Some(_) => {
                            return Err(Error::Conflict(
                                "fork allocation has another seed rebind intent".into(),
                            ));
                        }
                        None => return Err(Error::Indeterminate(new_seed.operation_id)),
                    }
                }
            }
            journals.push((journal, intent));
        }
        // Phase two: the intent makes this mutation replayable. An absent
        // seed is allowed because a crash may have occurred before the first
        // preparer binding; a different binding is never overwritten.
        for (journal, intent) in journals {
            let binding_path = "/seed.json";
            match read_record::<A, O, SeedBinding>(self, &journal, binding_path, 4_096).await? {
                Some(prior) if prior.digest == intent.to => continue,
                Some(prior) if prior.digest != intent.from => {
                    return Err(Error::Conflict("fork allocation has another seed".into()));
                }
                Some(_) | None => {}
            }
            let observed = self.resolve(&journal).await?;
            let key = IdempotencyKey::new(format!(
                "fork:{}:seed-rebind:{}",
                new_seed.operation_id,
                blake3::Hash::from_bytes(intent.to).to_hex(),
            ))?;
            match self
                .apply(
                    &journal,
                    Some(&observed.generation),
                    &[WorkspaceMutation::PutFile {
                        path: binding_path.into(),
                        bytes: encode_record(&SeedBinding { digest: intent.to }, 4_096)?,
                    }],
                    &key,
                )
                .await
            {
                Ok(_) | Err(Error::Conflict(_)) => {}
                Err(error) => return Err(error),
            }
            match read_record::<A, O, SeedBinding>(self, &journal, binding_path, 4_096).await? {
                Some(prior) if prior.digest == intent.to => {}
                Some(_) => return Err(Error::Conflict("fork allocation has another seed".into())),
                None => return Err(Error::Indeterminate(new_seed.operation_id)),
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

fn seed_allocation_volumes(seed: &ForkSeed) -> Result<[VolumeRef; 2]> {
    let child_project = seed
        .resources
        .iter()
        .find_map(|resource| {
            if let ResourceRevision::Project { volume, .. } = &resource.revision {
                Some(volume.clone())
            } else {
                None
            }
        })
        .ok_or_else(|| Error::Invalid("fork has no child project".into()))?;
    Ok([seed.child_private_volume.clone(), child_project])
}

fn seed_rebind_path(digest: [u8; 32]) -> String {
    format!(
        "/seed-rebind-intent-{}.json",
        blake3::Hash::from_bytes(digest).to_hex()
    )
}

fn captured_resource_rebind_shape_equal(old: &CapturedResource, new: &CapturedResource) -> bool {
    resource_revision_rebind_equal(&old.source, &new.source)
        && resource_revision_rebind_equal(&old.revision, &new.revision)
}

fn resource_revision_rebind_equal(old: &ResourceRevision, new: &ResourceRevision) -> bool {
    match (old, new) {
        (ResourceRevision::History(old), ResourceRevision::History(new)) => {
            old.as_resource().provider() == new.as_resource().provider()
                && old.as_resource().key() == new.as_resource().key()
        }
        _ => old == new,
    }
}

#[cfg(test)]
mod rebind_shape_tests {
    use super::*;
    use acyclic_fs::Fs;
    use crate::{core::AggregateKind, resources::{GenerationRef, StreamRef}};

    #[test]
    fn only_history_version_may_advance_during_rebind() -> Result<()> {
        let stream_provider = ProviderRef::new("test", "stream", "1")?;
        let old_history = ResourceRevision::History(StreamRef::new(
            stream_provider.clone(),
            b"parent".to_vec(),
            Some("7".into()),
        )?);
        let new_history = ResourceRevision::History(StreamRef::new(
            stream_provider,
            b"parent".to_vec(),
            Some("8".into()),
        )?);
        assert!(resource_revision_rebind_equal(&old_history, &new_history));

        let fs_provider = ProviderRef::new("test", "filesystem", "1")?;
        let volume = VolumeRef::new(
            fs_provider.clone(),
            "child-project",
            VolumeClass::Project,
            VolumeOwner::Project("test".into()),
        )?;
        let old_project = ResourceRevision::Project {
            volume: volume.clone(),
            generation: GenerationRef::new(fs_provider.clone(), [1; 32], Some("7".into()))?,
        };
        let changed_generation = ResourceRevision::Project {
            volume,
            generation: GenerationRef::new(fs_provider, [2; 32], Some("8".into()))?,
        };
        assert!(!resource_revision_rebind_equal(
            &old_project,
            &changed_generation
        ));
        Ok(())
    }

    #[test]
    fn captured_resource_rebind_rejects_non_history_changes() -> Result<()> {
        let fs_provider = ProviderRef::new("test", "filesystem", "1")?;
        let volume = VolumeRef::new(
            fs_provider.clone(),
            "child-project",
            VolumeClass::Project,
            VolumeOwner::Project("test".into()),
        )?;
        let old = CapturedResource {
            source: ResourceRevision::Project {
                volume: volume.clone(),
                generation: GenerationRef::new(fs_provider.clone(), [1; 32], None)?,
            },
            revision: ResourceRevision::Project {
                volume: volume.clone(),
                generation: GenerationRef::new(fs_provider.clone(), [3; 32], None)?,
            },
        };
        let mut changed_source = old.clone();
        if let ResourceRevision::Project { generation, .. } = &mut changed_source.source {
            *generation = GenerationRef::new(fs_provider.clone(), [9; 32], None)?;
        }
        assert!(!captured_resource_rebind_shape_equal(&old, &changed_source));

        let mut changed_revision = old.clone();
        if let ResourceRevision::Project { generation, .. } = &mut changed_revision.revision {
            *generation = GenerationRef::new(fs_provider, [8; 32], None)?;
        }
        assert!(!captured_resource_rebind_shape_equal(&old, &changed_revision));
        Ok(())
    }

    fn seed(parent_revision: u64) -> Result<ForkSeed> {
        let filesystem = ProviderRef::new("test", "filesystem", "1")?;
        let stream = ProviderRef::new("test", "stream", "1")?;
        let parent = Authority {
            kind: AggregateKind::Conversation,
            id: "rebind-parent".into(),
        };
        let child = Authority {
            kind: AggregateKind::Conversation,
            id: "rebind-child".into(),
        };
        let child_agent = crate::AgentId::from_bytes([2; 16]);
        let parent_project = VolumeRef::new(
            filesystem.clone(),
            "rebind-parent-project",
            VolumeClass::Project,
            VolumeOwner::Project("rebind".into()),
        )?;
        let child_project = VolumeRef::new(
            filesystem.clone(),
            "rebind-child-project",
            VolumeClass::Project,
            VolumeOwner::Project("rebind".into()),
        )?;
        let private = VolumeRef::new(
            filesystem.clone(),
            "rebind-child-private",
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(child_agent),
        )?;
        let history = ResourceRevision::History(StreamRef::new(
            stream,
            parent.stream_path()?.into_bytes(),
            Some(parent_revision.to_string()),
        )?);
        let source_generation = GenerationRef::new(filesystem.clone(), [3; 32], None)?;
        let child_generation = GenerationRef::new(filesystem.clone(), [4; 32], None)?;
        let project = ResourceRevision::Project {
            volume: child_project.clone(),
            generation: child_generation,
        };
        let source_project = ResourceRevision::Project {
            volume: parent_project,
            generation: source_generation,
        };
        Ok(ForkSeed {
            operation_id: OperationId::from_bytes([9; 16]),
            parent,
            parent_revision,
            child,
            child_agent,
            attached_agents: Vec::new(),
            resources: vec![
                CapturedResource {
                    source: history.clone(),
                    revision: history,
                },
                CapturedResource {
                    source: source_project,
                    revision: project,
                },
            ],
            omissions: Vec::new(),
            child_private_volume: private,
            child_private_generation: GenerationRef::new(filesystem, [5; 32], None)?,
            inherited_context: Vec::new(),
            inherited_through_sequence: 0,
            shared_grants: Vec::new(),
            reference_grants: Vec::new(),
            model_boundary: None,
            attachment_manifests: Vec::new(),
            boundary: None,
        })
    }

    #[tokio::test]
    async fn durable_rebind_supports_sequential_history_transitions() -> Result<()> {
        let provider = ProviderRef::new("test", "filesystem", "1")?;
        let host = FilesystemHost::new(Fs::memory(), provider.clone())?;
        let first = seed(1)?;
        let second = seed(2)?;
        let third = seed(3)?;
        let old_binding = seed_binding(&first)?;
        for volume in seed_allocation_volumes(&first)? {
            let journal = allocation_ref(provider.clone(), &volume)?;
            host.filesystem
                .create_workspace(std::str::from_utf8(journal.as_resource().key()).map_err(
                    |error| Error::Invalid(error.to_string()),
                )?)
                .await
                .map_err(map_error)?;
            let claim = AllocationClaim {
                operation_id: first.operation_id,
                preparation_digest: [6; 32],
                parent: first.parent.clone(),
                child: first.child.clone(),
                volume,
            };
            host.apply(
                &journal,
                None,
                &[WorkspaceMutation::PutFile {
                    path: "/claim.json".into(),
                    bytes: encode_record(&claim, 4_096)?,
                }, WorkspaceMutation::PutFile {
                    path: "/seed.json".into(),
                    bytes: encode_record(&old_binding, 4_096)?,
                }],
                &IdempotencyKey::new(format!("rebind-test-claim-{}", claim.volume.storage_name()?))?,
            )
            .await?;
        }

        host.rebind_fork_seed(&first, &second).await?;
        let second_binding = seed_binding(&second)?;
        for volume in seed_allocation_volumes(&second)? {
            let journal = allocation_ref(provider.clone(), &volume)?;
            let intent = read_record::<_, _, SeedRebindIntent>(
                &host,
                &journal,
                &seed_rebind_path(second_binding.digest),
                4_096,
            )
            .await?
            .ok_or_else(|| Error::Storage("first rebind intent was not retained".into()))?;
            assert_eq!(intent.from, old_binding.digest);
            assert_eq!(intent.to, second_binding.digest);
            assert_eq!(
                read_record::<_, _, SeedBinding>(&host, &journal, "/seed.json", 4_096)
                    .await?
                    .ok_or_else(|| Error::Storage("first seed binding missing".into()))?,
                second_binding
            );
        }

        host.rebind_fork_seed(&second, &third).await?;
        let third_binding = seed_binding(&third)?;
        for volume in seed_allocation_volumes(&third)? {
            let journal = allocation_ref(provider.clone(), &volume)?;
            let intent = read_record::<_, _, SeedRebindIntent>(
                &host,
                &journal,
                &seed_rebind_path(third_binding.digest),
                4_096,
            )
            .await?
            .ok_or_else(|| Error::Storage("second rebind intent was not retained".into()))?;
            assert_eq!(intent.from, second_binding.digest);
            assert_eq!(intent.to, third_binding.digest);
            assert_eq!(
                read_record::<_, _, SeedBinding>(&host, &journal, "/seed.json", 4_096)
                    .await?
                    .ok_or_else(|| Error::Storage("second seed binding missing".into()))?,
                third_binding
            );
        }
        assert!(matches!(
            host.rebind_fork_seed(&first, &third).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }
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
