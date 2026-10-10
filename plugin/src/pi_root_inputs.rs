//! Immutable root-preparation inputs in the existing owner volume.

use super::{
    PiScratchAllocation, PiTaskBinding, pi_owner::PiOwnerConfig, pi_root::PiRootIntent,
    pi_root_allocation::PiRootAllocation,
};
use acyclic_harness::{
    IdempotencyKey, OperationId, TaskId,
    conversation::{ContentGrant, FileRef, VolumeOperation},
    core::EffectGuarantee,
    effect_host::TaskEffectPlan,
    filesystem::{EFFECT_TASK_COMMAND_KIND, FilesystemHost},
    workflow::WorkflowCommand,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::path::Path;

/// Only this public input directory is readable by the root task. Runtime
/// payloads, execution journals and provider receipts retain their own grants.
pub(crate) const ROOT_INPUT_DIRECTORY: &str = "pi-inputs";
pub(crate) const ROOT_ALLOCATION_PROVIDER: &str = "acyclic.pi.root";
pub(crate) const ROOT_ALLOCATION_EFFECT: &str = "acyclic.pi.root.allocate@1";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiRootRequest {
    pub(crate) binding: PiTaskBinding,
    pub(crate) root: PiRootIntent,
}

/// Read-only preparation authenticated against the original task admission.
/// This contains no execution permit or provider creation evidence.
pub(crate) struct PiRootPreparation {
    command: WorkflowCommand,
    plan: TaskEffectPlan,
    request: PiRootRequest,
}

impl PiRootPreparation {
    /// Authenticates the original preparation and binds its current execution
    /// owner before a provider may inspect selected native roots. This only
    /// checks dispatch freshness; allocation and publication retain their own
    /// resource grants, finite work budgets and destination permits.
    pub(crate) async fn load_for_dispatch<P, A, O>(
        session: &Path,
        runtime: &super::pi_runtime::PiTaskRuntime<P, A, O>,
        filesystem: &FilesystemHost<A, O>,
        retained: &PiRootRequest,
        fence: acyclic_harness::scheduler::LeaseFence,
    ) -> Result<(Self, acyclic_harness::durable_host::TaskJournalOwner<P>), String>
    where
        P: acyclic_stream::StreamProvider + 'static,
        A: acyclic_fs::AsyncAuthorityStore + 'static,
        O: acyclic_fs::AsyncObjectStore + 'static,
    {
        let preparation = Self::load(session, runtime, filesystem, retained).await?;
        let owner = runtime
            .runtime
            .task_host()
            .journal_owner(preparation.request.binding.task_id, fence)
            .await
            .map_err(super::display)?;
        owner.verify_dispatch().await.map_err(super::display)?;
        Ok((preparation, owner))
    }

    pub(crate) fn command(&self) -> &WorkflowCommand {
        &self.command
    }

    pub(crate) fn plan(&self) -> &TaskEffectPlan {
        &self.plan
    }

    pub(crate) fn request(&self) -> &PiRootRequest {
        &self.request
    }

    /// Reopens original immutable artifacts without staging, claiming work or
    /// touching a selected native path. Retained allocation progress may have
    /// advanced; request bytes must still match the original admitted intent.
    pub(crate) async fn load<P, A, O>(
        session: &Path,
        runtime: &super::pi_runtime::PiTaskRuntime<P, A, O>,
        filesystem: &FilesystemHost<A, O>,
        retained: &PiRootRequest,
    ) -> Result<Self, String>
    where
        P: acyclic_stream::StreamProvider + 'static,
        A: acyclic_fs::AsyncAuthorityStore + 'static,
        O: acyclic_fs::AsyncObjectStore + 'static,
    {
        let owner = PiOwnerConfig::load(session)?;
        runtime.verify_owner(&owner)?;
        owner.validate_root(&retained.binding)?;
        let admitted = super::pi_recovery::authenticate_pi_task(
            &runtime.runtime,
            &retained.binding,
            super::pi_recovery::PiTaskOrigin::Root(&retained.root),
        )
        .await?;
        let super::pi_task::PiTaskExecution::Host { root, preparation } = admitted.execution else {
            return Err("Pi root preparation requires its original host admission".to_owned());
        };
        preparation.validate().map_err(super::display)?;
        if preparation.kind != EFFECT_TASK_COMMAND_KIND
            || preparation.operation_id != preparation_operation(owner.policy().root_task)
        {
            return Err("Pi root preparation command identity differs".to_owned());
        }
        let authority = input_authority(&owner).map_err(super::display)?;
        let read = ContentGrant::verify_directory_read(
            &authority.verifier,
            &authority.signed,
            &owner.policy().journal,
            ROOT_INPUT_DIRECTORY,
        )
        .map_err(super::display)?;
        let plan: TaskEffectPlan = read_input(
            filesystem,
            &authority,
            &read,
            &preparation.payload,
            "preparation",
            "application/json",
        )
        .await?;
        if plan.provider != ROOT_ALLOCATION_PROVIDER
            || plan.effect_kind != ROOT_ALLOCATION_EFFECT
            || plan.guarantee != EffectGuarantee::AtMostOnce
        {
            return Err("Pi root allocation provider contract differs".to_owned());
        }
        let request: PiRootRequest = read_input(
            filesystem,
            &authority,
            &read,
            &plan.request,
            "root",
            "application/json",
        )
        .await?;
        validate_request(&owner, &request).map_err(super::display)?;
        if request
            != (PiRootRequest {
                binding: admitted.binding,
                root,
            })
        {
            return Err("Pi root preparation differs from its original admission".to_owned());
        }
        let schema: serde_json::Value = read_input(
            filesystem,
            &authority,
            &read,
            &plan.result_schema,
            "allocation-schema",
            "application/schema+json",
        )
        .await?;
        if schema != PiRootAllocation::schema(&request.binding)? {
            return Err("Pi root allocation result schema differs".to_owned());
        }
        Ok(Self {
            command: preparation,
            plan,
            request,
        })
    }
}

async fn read_input<A, O, T: DeserializeOwned + Serialize>(
    filesystem: &FilesystemHost<A, O>,
    authority: &super::pi_runtime::PiRuntimeAuthority,
    grant: &ContentGrant,
    reference: &FileRef,
    name: &str,
    media_type: &str,
) -> Result<T, String>
where
    A: acyclic_fs::AsyncAuthorityStore,
    O: acyclic_fs::AsyncObjectStore,
{
    authority
        .scope
        .limits()
        .validate_file(reference)
        .map_err(super::display)?;
    grant.require_file_read(reference).map_err(super::display)?;
    if reference.path() != format!("{ROOT_INPUT_DIRECTORY}/{name}.json")
        || reference.descriptor().media_type() != media_type
    {
        return Err("Pi root preparation artifact path or media type differs".to_owned());
    }
    let bytes = filesystem
        .read_content(
            reference,
            grant,
            authority
                .maximum_payload_bytes
                .min(super::MAXIMUM_ADAPTER_STATE_BYTES),
        )
        .await
        .map_err(super::display)?;
    let value: T = serde_json::from_slice(&bytes).map_err(super::display)?;
    let canonical =
        encode_input(&value, authority.maximum_payload_bytes).map_err(super::display)?;
    if canonical.as_slice() != bytes.as_ref() {
        return Err("Pi preparation artifact is not in its first-party encoding".to_owned());
    }
    Ok(value)
}

/// Pins the first-party allocation schema, actual stock effect plan and
/// original command identity. Callers cannot substitute a weaker result schema.
/// The service supplies its already loaded original owner snapshot.
pub(crate) async fn stage_root_command<A, O>(
    owner: &PiOwnerConfig,
    filesystem: &FilesystemHost<A, O>,
    request: &PiRootRequest,
) -> acyclic_harness::Result<WorkflowCommand>
where
    A: acyclic_fs::AsyncAuthorityStore,
    O: acyclic_fs::AsyncObjectStore,
{
    let request_reference = stage_root_request(owner, filesystem, request).await?;
    let schema =
        PiRootAllocation::schema(&request.binding).map_err(acyclic_harness::Error::Invalid)?;
    let result_schema = stage_input(
        owner,
        filesystem,
        "allocation-schema",
        &schema,
        "application/schema+json",
    )
    .await?;
    let plan = TaskEffectPlan {
        provider: ROOT_ALLOCATION_PROVIDER.to_owned(),
        guarantee: EffectGuarantee::AtMostOnce,
        effect_kind: ROOT_ALLOCATION_EFFECT.to_owned(),
        request: request_reference,
        result_schema,
    };
    Ok(WorkflowCommand {
        operation_id: preparation_operation(owner.policy().root_task),
        kind: EFFECT_TASK_COMMAND_KIND.to_owned(),
        payload: stage_input(owner, filesystem, "preparation", &plan, "application/json").await?,
    })
}

fn preparation_operation(task: TaskId) -> OperationId {
    let mut digest = blake3::Hasher::new();
    digest.update(b"acyclic.pi/root-preparation/v1");
    digest.update(&task.into_bytes());
    let mut operation = [0; 16];
    operation.copy_from_slice(&digest.finalize().as_bytes()[..16]);
    OperationId::from_bytes(operation)
}

fn validate_request(owner: &PiOwnerConfig, request: &PiRootRequest) -> acyclic_harness::Result<()> {
    owner
        .validate_root(&request.binding)
        .map_err(acyclic_harness::Error::Invalid)?;
    request
        .root
        .validate(&request.binding)
        .map_err(acyclic_harness::Error::Invalid)?;
    if !matches!(
        request.binding.scratch.allocation,
        PiScratchAllocation::Preparing
    ) {
        return Err(acyclic_harness::Error::Invalid(
            "Pi root preparation requires its original unallocated scratch intent".to_owned(),
        ));
    }
    Ok(())
}

fn input_authority(
    owner: &PiOwnerConfig,
) -> acyclic_harness::Result<super::pi_runtime::PiRuntimeAuthority> {
    let authority = owner.authority().map_err(acyclic_harness::Error::Invalid)?;
    let volume = &owner.policy().journal;
    if !authority
        .scope
        .grants()
        .contains(&volume.directory_read_capability(ROOT_INPUT_DIRECTORY)?)
    {
        return Err(acyclic_harness::Error::Unauthorized(
            "Pi root lacks its explicit preparation-input read grant".to_owned(),
        ));
    }
    Ok(authority)
}

/// Stages a bounded immutable request using the service's original owner
/// snapshot. Filesystem's atomic upload rejects changed retry bytes or metadata
/// under the same operation. Selected native paths remain unopened.
pub(crate) async fn stage_root_request<A, O>(
    owner: &PiOwnerConfig,
    filesystem: &FilesystemHost<A, O>,
    request: &PiRootRequest,
) -> acyclic_harness::Result<FileRef>
where
    A: acyclic_fs::AsyncAuthorityStore,
    O: acyclic_fs::AsyncObjectStore,
{
    validate_request(owner, request)?;
    stage_input(owner, filesystem, "root", request, "application/json").await
}

async fn stage_input<A, O>(
    owner: &PiOwnerConfig,
    filesystem: &FilesystemHost<A, O>,
    name: &'static str,
    value: &impl Serialize,
    media_type: &'static str,
) -> acyclic_harness::Result<FileRef>
where
    A: acyclic_fs::AsyncAuthorityStore,
    O: acyclic_fs::AsyncObjectStore,
{
    let authority = input_authority(owner)?;
    let volume = &owner.policy().journal;
    let grant = ContentGrant::verify(
        &authority.verifier,
        &authority.signed,
        volume,
        VolumeOperation::Write,
    )?;
    let maximum = owner.policy().maximum_payload_bytes;
    let path = format!("{ROOT_INPUT_DIRECTORY}/{name}.json");
    if path.len() > authority.scope.limits().path_bytes {
        return Err(acyclic_harness::Error::Invalid(
            "Pi root preparation input exceeds its admitted path bound".to_owned(),
        ));
    }
    let encoded = encode_input(value, maximum)?;
    filesystem
        .put_content(
            volume,
            &grant,
            &path,
            &encoded,
            media_type,
            &format!("{name}.json"),
            maximum,
            &IdempotencyKey::new(format!(
                "acyclic.pi/{name}-input/{}",
                owner.policy().root_task
            ))?,
        )
        .await
}

fn encode_input(value: &impl Serialize, maximum: u64) -> acyclic_harness::Result<Vec<u8>> {
    let mut encoded = super::BoundedJsonBuffer::with_limit(
        usize::try_from(maximum.min(super::MAXIMUM_ADAPTER_STATE_BYTES))
            .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?,
    );
    serde_json::to_writer(&mut encoded, value)
        .map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
    Ok(encoded.bytes)
}
