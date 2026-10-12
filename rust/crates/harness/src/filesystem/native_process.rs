//! Approved one-shot process effects, using the task's existing fenced Stream.

use super::{
    NATIVE_PROCESS_EFFECT_KIND, NativeProcessRequest, NativeProcessResult, NativeProcessStop,
    NativeProcessStopKind, NativeViewManifest, NativeVolumeView,
};
use super::{PreparationIo, retain_decoded};
use crate::store::history::HistoryIo as _;
use crate::{
    Error, InteractionId, OperationId, Result,
    conversation::{ContentPublisher, ContentResidencyVerifier},
    core::{AuthorityVerifier, EffectGuarantee, EffectStatus},
    distributed::JournalWrite,
    durable_host::TaskJournalOwner,
    effect_host::{TaskEffectPlan, task_effect_id},
    effects::{EffectDispatch, EffectObservation, EffectProvider},
    store::{HistoryReadLimits, HistoryReader},
    tool::ToolApprovalVerifier,
};
use acyclic_fs::{
    AsyncAuthorityStore, AsyncObjectStore, NativeAllocation, NativeMaterializationWork,
};
use acyclic_stream::{BoxProviderFuture, StreamError, StreamPath, StreamProvider};

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

mod capture;
use capture::capture;
mod journal;
use journal::{LaunchReceipt, NativeObservation, Receipt, append_receipt, read_receipt};
mod records;
pub use records::{NativeProcessPublishedVolume, NativeProcessReceipt};

impl NativeProcessRequest {
    fn validate(&self) -> Result<()> {
        if let Some(exchange) = &self.mcp_stdio {
            exchange.validate()?;
        }
        if !self.executable.is_absolute()
            || !self.cwd.is_absolute()
            || !self.view.options.root.is_absolute()
            || !self.cwd.starts_with(&self.view.options.root)
            || self
                .cwd
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
            || !self.has_valid_capture_allowances()
            || self.environment.iter().any(|(key, value)| {
                key.is_empty() || key.contains(['=', '\0']) || value.contains('\0')
            })
            || self.argv.iter().any(|arg| arg.contains('\0'))
        {
            return Err(Error::Invalid(
                "invalid or changed native process invocation".into(),
            ));
        }
        #[cfg(windows)]
        if self.environment.keys().enumerate().any(|(index, key)| {
            self.environment.keys().take(index).any(|prior| {
                key.chars()
                    .flat_map(char::to_uppercase)
                    .eq(prior.chars().flat_map(char::to_uppercase))
            })
        }) {
            return Err(Error::Invalid(
                "duplicate native environment key under Windows case folding".into(),
            ));
        }
        Ok(())
    }

    fn validate_native_paths(&self, view: &NativeViewManifest) -> Result<()> {
        self.validate()?;
        if &self.view != view
            || !std::fs::canonicalize(&self.cwd)
                .map_err(io_error)?
                .starts_with(&view.options.root)
        {
            return Err(Error::Conflict("native command view or cwd changed".into()));
        }
        if !std::fs::metadata(&self.executable)
            .map_err(io_error)?
            .is_file()
            || !std::fs::metadata(&self.cwd).map_err(io_error)?.is_dir()
        {
            return Err(Error::Invalid(
                "native executable or cwd is unavailable".into(),
            ));
        }
        Ok(())
    }
}

/// Host-only capabilities for checking committed admission and staging results.
pub struct NativeProcessAuthority {
    /// Original conversation authority verifier.
    pub verifier: AuthorityVerifier,
    /// Finite authenticated history bounds.
    pub history_limits: HistoryReadLimits,
    /// Owner-authenticated immutable request/schema reader.
    pub content: Arc<dyn ContentResidencyVerifier>,
    /// Authenticated result reader.
    pub result_content: Arc<dyn ContentResidencyVerifier>,
    /// Owner-bound result publisher; it retains the result before returning.
    pub results: Arc<dyn ContentPublisher>,
    /// Existing resolved-approval ledger verifier.
    pub approvals: Arc<dyn ToolApprovalVerifier>,
    /// Original authority allocation.
    pub authority_custody: Option<NativeAllocation>,
}

/// Optional provider for one retained task command.
pub struct NativeProcessProvider<P, A, O> {
    owner: Arc<TaskJournalOwner<P>>,
    command: OperationId,
    plan: TaskEffectPlan,
    request: NativeProcessRequest,
    view: Option<Arc<NativeVolumeView<A, O>>>,
    approval: InteractionId,
    authority: Arc<NativeProcessAuthority>,
    result_schema: acyclic_stream::OwnedBytes,
    active: Arc<Mutex<Option<acyclic_native_runtime::ProcessTree>>>,
    custody: NativeAllocation,
}

impl<P, A, O> NativeProcessProvider<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    /// Reads the authenticated original launch and final observation.
    pub async fn output(
        &self,
        attempt: crate::EffectAttemptId,
    ) -> Result<Option<NativeProcessReceipt>> {
        let Some(((launch, observed), custody)) = self.receipt(attempt).await? else {
            return Ok(None);
        };
        let settled = observed.is_some();
        let (effect, diagnostic, published) = match observed {
            Some(observed) => (observed.effect, observed.diagnostic, observed.published),
            None => (
                observation(&launch.dispatch, EffectStatus::Indeterminate),
                None,
                Vec::new(),
            ),
        };
        let reference = match &effect.status {
            EffectStatus::Succeeded { result } => Some(result),
            _ => diagnostic.as_ref(),
        };
        if let Some(reference) = reference {
            let io = PreparationIo::from_native_work(self.work())?;
            self.owner.validate_input_file(reference, Some(&io))?;
            if reference.volume() != self.authority.results.volume()
                || reference.descriptor().byte_length()
                    > u64::from(self.request.maximum_result_bytes)
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native output differs from its admitted result owner or bound",
                });
            }
            let bytes = self
                .authority
                .result_content
                .read(
                    reference,
                    u64::from(self.request.maximum_result_bytes),
                    Some(&io),
                )?
                .await?;
            crate::effects::validate_result_bytes(
                &self.plan.result_schema,
                &self.result_schema,
                reference,
                &bytes,
                &mut Some(&mut io.history_io()),
            )?;
        }
        Ok(Some(NativeProcessReceipt {
            task: launch.task,
            command: launch.command,
            original_fence: launch.fence,
            effect,
            observed: settled,
            diagnostic,
            published,
            work: self.work().receipt(),
            custody,
        }))
    }

    /// Binds one retained command, optionally retaining its executable view.
    pub async fn new(
        owner: Arc<TaskJournalOwner<P>>,
        command: OperationId,
        plan: TaskEffectPlan,
        view: Option<Arc<NativeVolumeView<A, O>>>,
        approval: InteractionId,
        authority: NativeProcessAuthority,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        if !owner.native_shared_work_matches(work)
            || view
                .as_ref()
                .is_some_and(|view| !view.work().shares_ledger(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native provider requires the original task work",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        let mut history = io.history_io();
        let mut observer = Some(&mut history as &mut dyn crate::store::history::HistoryIo);
        owner.verify(true, &mut observer).await?;
        owner.require_effect_grants(&plan.provider, false, &mut observer)?;
        owner.validate_input_file(&plan.request, Some(&io))?;
        owner.validate_input_file(&plan.result_schema, Some(&io))?;
        authority.results.supports_stage(Some(&io))?;
        if !authority
            .result_content
            .supports_read(authority.results.volume(), Some(&io))?
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "native result storage requires bounded reads",
            });
        }

        if plan.effect_kind != NATIVE_PROCESS_EFFECT_KIND
            || plan.guarantee != EffectGuarantee::AtMostOnce
        {
            return Err(Error::Invalid(
                "native process requires its one-shot at-most-once contract".into(),
            ));
        }
        if plan.request.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("native request must be JSON content".into()));
        }
        let bytes = authority
            .content
            .read(&plan.request, owner.input_limits().file_bytes, Some(&io))?
            .await?;
        plan.request.descriptor().verify(&bytes, &mut observer)?;
        let request = retain_decoded(io.decode(
            std::marker::PhantomData::<NativeProcessRequest>,
            &bytes,
            crate::wire::ErrorCode::Invalid,
        )?);
        request.validate()?;
        if request
            .view
            .volumes
            .iter()
            .any(|mapping| &mapping.volume == authority.results.volume())
        {
            return Err(Error::Unsupported(
                "native result storage must be separate from materialized volumes".into(),
            ));
        }
        if u64::from(request.maximum_result_bytes) > owner.input_limits().file_bytes {
            return Err(Error::Unauthorized(
                "native result bound exceeds retained task file allowance".into(),
            ));
        }
        if let Some(view) = &view {
            request.validate_native_paths(view.manifest())?;
        }
        let schema = authority
            .content
            .read(
                &plan.result_schema,
                owner.input_limits().file_bytes,
                Some(&io),
            )?
            .await?;
        crate::effects::validate_schema_bytes(&plan.result_schema, &schema, &mut observer)?;
        let result_schema = schema;
        for operation in [
            crate::conversation::VolumeOperation::Write,
            crate::conversation::VolumeOperation::Read,
        ] {
            owner.require_volume_grant(authority.results.volume(), operation, &mut observer)?;
        }
        authority
            .approvals
            .verify(
                approval,
                command,
                request.approval_digest_with_native_work(owner.task_binding().0, command, work)?,
            )
            .await?;
        drop(observer);
        drop(history);
        io.retain_owned(
            (std::mem::size_of::<Self>()
                + std::mem::size_of::<NativeProcessAuthority>()
                + std::mem::size_of::<Mutex<Option<acyclic_native_runtime::ProcessTree>>>())
                as u64,
        )?;
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 2,
            ..Default::default()
        })?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok(Self {
            owner,
            command,
            plan,
            request,
            view,
            approval,
            authority: Arc::new(authority),
            result_schema,
            active: Arc::new(Mutex::new(None)),
            custody,
        })
    }

    async fn receipt(
        &self,
        attempt: crate::EffectAttemptId,
    ) -> Result<Option<((LaunchReceipt, Option<NativeObservation>), NativeAllocation)>> {
        let Some(((launch, observed), custody)) = read_receipt(
            &self.owner,
            &receipt_path(attempt, self.work())?,
            self.work(),
        )
        .await?
        else {
            return Ok(None);
        };
        if launch.task != self.owner.task_binding().0
            || launch.command != self.command
            || launch.plan != self.plan
            || &launch.conversation != self.authority.verifier.audience()
            || launch.dispatch.attempt_id != attempt
            || launch.approval != self.approval
            || launch.approval_digest
                != self.request.approval_digest_with_native_work(
                    launch.task,
                    self.command,
                    self.work(),
                )?
        {
            return Err(Error::Conflict(
                "native receipt belongs to a different admission".into(),
            ));
        }
        let (_state, _state_custody) = self.require_effect(&launch.dispatch).await?;
        Ok(Some(((launch, observed), custody)))
    }

    fn work(&self) -> &NativeMaterializationWork {
        self.custody.work()
    }

    async fn require_effect(
        &self,
        dispatch: &EffectDispatch,
    ) -> Result<(crate::core::EffectState, NativeAllocation)> {
        let effect = task_effect_id(
            self.owner.task_binding().0,
            self.command,
            &mut Some(&mut crate::store::history::native_work_io(self.work())?),
        )?;
        let io = PreparationIo::from_native_work(self.work())?;
        let reader = HistoryReader::new(
            &self.owner.stream(),
            self.authority.verifier.audience(),
            &self.authority.verifier,
            &mut Some(&mut io.history_io()),
        )?;
        let (cursor, _cursor) = reader.pin_with_native_work(0, self.work()).await?;
        let (state, custody) = reader
            .effect_with_native_work(&cursor, effect, self.authority.history_limits, self.work())
            .await?;
        let state = state.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native command has no committed effect",
        })?;
        let requested = (
            &dispatch.provider,
            &dispatch.effect_kind,
            &dispatch.request,
            dispatch.guarantee,
        );
        let retained = (
            &state.provider,
            &state.effect_kind,
            &state.request,
            state.guarantee,
        );
        let planned = (
            &self.plan.provider,
            &self.plan.effect_kind,
            &self.plan.request,
            self.plan.guarantee,
        );
        if dispatch.effect_id != effect
            || requested != retained
            || retained != planned
            || state.result_schema != self.plan.result_schema
            || state.request_digest != dispatch.request_digest
            || state.attempts.last() != Some(&dispatch.attempt_id)
        {
            return Err(Error::Conflict(
                "native dispatch differs from retained command".into(),
            ));
        }
        Ok((state, custody))
    }

    async fn require_dispatch(&self, dispatch: &EffectDispatch) -> Result<()> {
        self.owner
            .verify(
                false,
                &mut Some(&mut crate::store::history::native_work_io(self.work())?),
            )
            .await?;
        let (state, _custody) = self.require_effect(dispatch).await?;
        if state.status != EffectStatus::Dispatched {
            return Err(Error::Conflict("effect is not awaiting dispatch".into()));
        }
        let view = self.view.as_ref().ok_or_else(|| {
            Error::Unsupported("recovered native provider can only reconcile receipts".into())
        })?;
        self.request.validate_native_paths(view.manifest())?;
        self.authority
            .approvals
            .verify(
                self.approval,
                self.command,
                self.request.approval_digest_with_native_work(
                    self.owner.task_binding().0,
                    self.command,
                    self.work(),
                )?,
            )
            .await?;
        view.validate_for_dispatch().await
    }

    async fn launch(&self, dispatch: EffectDispatch) -> Result<EffectObservation> {
        self.require_dispatch(&dispatch).await?;
        let io = PreparationIo::from_native_work(self.work())?;
        let (task, fence) = self.owner.task_binding();
        let launch = LaunchReceipt {
            dispatch: journal::copy(&dispatch, &io)?,
            plan: journal::copy(&self.plan, &io)?,
            conversation: io
                .history_io()
                .clone_authority(self.authority.verifier.audience())?,
            task,
            command: self.command,
            fence: fence.clone_with_work(&mut Some(&mut crate::store::history::native_work_io(
                self.work(),
            )?))?,
            approval: self.approval,
            approval_digest: self.request.approval_digest_with_native_work(
                task,
                self.command,
                self.work(),
            )?,
        };
        let path = receipt_path(dispatch.attempt_id, self.work())?;
        let existing = read_receipt(&self.owner, &path, self.work()).await?;
        if let Some(((retained, observed), _receipt_custody)) = existing {
            require_same_launch(&retained, &launch)?;
            return Ok(observed.map_or_else(
                || observation(&dispatch, EffectStatus::Indeterminate),
                |observed| observed.effect,
            ));
        }
        if !append_receipt(
            &self.owner,
            path.clone(),
            0,
            &Receipt::<&LaunchReceipt, &NativeObservation>::Launch(&launch),
            JournalWrite::Fresh,
            self.work(),
        )
        .await?
        {
            let ((retained, observed), _receipt_custody) =
                read_receipt(&self.owner, &path, self.work())
                    .await?
                    .ok_or_else(|| Error::Conflict("native launch receipt changed".into()))?;
            require_same_launch(&retained, &launch)?;
            return Ok(observed.map_or_else(
                || observation(&dispatch, EffectStatus::Indeterminate),
                |observed| observed.effect,
            ));
        }
        let owner = Arc::clone(&self.owner);
        let view = Arc::clone(self.view.as_ref().ok_or_else(|| {
            Error::Unsupported("native dispatch requires a prepared view".into())
        })?);
        let authority = Arc::clone(&self.authority);
        let request = journal::copy(&self.request, &io)?;
        let schema = self.result_schema.clone();
        let active = Arc::clone(&self.active);
        let work = self.work().clone();
        let schema_reference = self
            .plan
            .result_schema
            .copy(&mut Some(&mut io.history_io()))?;
        let retained = io.owned_bytes()?;
        let launch_custody = io.into_native_custody(retained)?;
        tokio::spawn(async move {
            let _launch_custody = launch_custody;
            let (status, diagnostic, published, _publication_custody) = execute(
                &owner,
                &view,
                &authority,
                &request,
                &dispatch,
                &schema_reference,
                &schema,
                &work,
                &active,
            )
            .await?;
            let observed = NativeObservation {
                effect: observation(&dispatch, status),
                diagnostic,
                published,
            };
            if !append_receipt(
                &owner,
                path.clone(),
                1,
                &Receipt::<&LaunchReceipt, &NativeObservation>::Observed(&observed),
                JournalWrite::Settlement,
                &work,
            )
            .await?
                && read_receipt(&owner, &path, &work)
                    .await?
                    .and_then(|((_, observed), _custody)| observed)
                    .as_ref()
                    != Some(&observed)
            {
                return Err(Error::Conflict(
                    "native observed outcome was not retained".into(),
                ));
            }
            Ok(observed.effect)
        })
        .await
        .map_err(|source| Error::NativeSupervisor {
            source,
            custody: None,
        })?
    }
}

impl<P, A, O> EffectProvider for NativeProcessProvider<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    fn id(&self) -> &str {
        &self.plan.provider
    }
    fn guarantees(&self, kind: &str) -> BTreeSet<EffectGuarantee> {
        if kind == NATIVE_PROCESS_EFFECT_KIND {
            BTreeSet::from([EffectGuarantee::AtMostOnce])
        } else {
            BTreeSet::new()
        }
    }
    fn linearizable_reconciliation(&self) -> bool {
        false
    }
    fn dispatch<'a>(
        &'a self,
        request: EffectDispatch,
    ) -> BoxProviderFuture<'a, Result<EffectObservation>> {
        Box::pin(self.launch(request))
    }
    fn reconcile<'a>(
        &'a self,
        attempt: crate::EffectAttemptId,
    ) -> BoxProviderFuture<'a, Result<Option<EffectObservation>>> {
        Box::pin(async move {
            Ok(self
                .receipt(attempt)
                .await?
                .map(|((launch, observed), _custody)| {
                    observed.map_or_else(
                        || observation(&launch.dispatch, EffectStatus::Indeterminate),
                        |observed| observed.effect,
                    )
                }))
        })
    }
}

async fn execute<P, A, O>(
    owner: &Arc<TaskJournalOwner<P>>,
    view: &NativeVolumeView<A, O>,
    authority: &NativeProcessAuthority,
    request: &NativeProcessRequest,
    dispatch: &EffectDispatch,
    schema_reference: &crate::conversation::FileRef,
    schema: &[u8],
    work: &NativeMaterializationWork,
    active: &Arc<Mutex<Option<acyclic_native_runtime::ProcessTree>>>,
) -> Result<(
    EffectStatus,
    Option<crate::conversation::FileRef>,
    Vec<NativeProcessPublishedVolume>,
    NativeAllocation,
)>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let (result, _capture_custody) = capture(owner, request, work, active).await?;
    let io = PreparationIo::from_native_work(work)?;
    let bytes = io.encode(&result, u64::from(request.maximum_result_bytes), true)?;
    let publish = result.stop.is_none();
    let cleanup_completed = result
        .stop
        .as_ref()
        .is_none_or(|stop| stop.cleanup_completed);
    let result = authority
        .results
        .stage_once(
            OperationId::from_bytes(dispatch.attempt_id.into_bytes()),
            "native-process/result.json",
            &bytes,
            "application/json",
            "Native process result",
            Some(&io),
        )?
        .await?;
    let (result, _result_hold) = result;
    if result.volume() != authority.results.volume() {
        return Err(Error::Unauthorized(
            "native result publisher returned another owner volume".into(),
        ));
    }
    crate::effects::validate_result_bytes(
        schema_reference,
        schema,
        &result,
        &bytes,
        &mut Some(&mut io.history_io()),
    )?;
    let (published, custody) = if publish {
        match view.publish(owner).await {
            Ok(result) => result,
            Err(Error::NativePublication {
                mut failure,
                custody,
            }) => {
                failure.diagnostic = Some(result);
                return Err(Error::NativePublication { failure, custody });
            }
            Err(cause) => return Err(cause),
        }
    } else {
        (
            Vec::new(),
            work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)?,
        )
    };
    Ok(if cleanup_completed {
        (EffectStatus::Succeeded { result }, None, published, custody)
    } else {
        (
            EffectStatus::Indeterminate,
            Some(result),
            published,
            custody,
        )
    })
}

fn receipt_path(
    attempt: crate::EffectAttemptId,
    work: &NativeMaterializationWork,
) -> Result<StreamPath> {
    let (path, _custody) = work.display(&format_args!("harness/v2/native-process/{attempt}"))?;
    StreamPath::new_native(&path, &work.runtime_meter())
        .map_err(|error| Error::from(work.runtime_error(error)))?
        .map_err(Error::from)
}

fn observation(dispatch: &EffectDispatch, status: EffectStatus) -> EffectObservation {
    EffectObservation {
        provider: dispatch.provider.clone(),
        effect_id: dispatch.effect_id,
        attempt_id: dispatch.attempt_id,
        request_digest: dispatch.request_digest,
        guarantee: dispatch.guarantee,
        status,
    }
}

fn require_same_launch(retained: &LaunchReceipt, requested: &LaunchReceipt) -> Result<()> {
    if retained.dispatch != requested.dispatch
        || retained.plan != requested.plan
        || retained.conversation != requested.conversation
        || retained.task != requested.task
        || retained.command != requested.command
        || retained.approval != requested.approval
        || retained.approval_digest != requested.approval_digest
    {
        return Err(Error::Conflict("native launch identity was reused".into()));
    }
    Ok(())
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "owned I/O error callback used by map_err"
)]
fn io_error(error: std::io::Error) -> Error {
    Error::Storage(error.to_string())
}
