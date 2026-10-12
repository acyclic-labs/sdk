//! Immutable, private host credentials for reopening one Pi root runtime.

use super::pi_runtime::PiRuntimeAuthority;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_harness::{
    AgentId, Capabilities, Error, Result, TaskId,
    conversation::{Limits, VolumeOperation, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer, SchemaRegistry},
    effect_host::ConversationEffectHost,
    effects::{EffectProvider, EffectRegistry},
    runtime::{RuntimeScope, TaskRunLimits},
    scheduler::SessionLimits,
};
use acyclic_native_runtime::{Durability, sync_file, sync_parent};
use acyclic_stream::{StreamProvider};
use serde::{Deserialize, Serialize};
use std::{
    io::{self, Read},
    path::Path,
    sync::Arc,
};

#[path = "pi_owner_protection.rs"]
mod protection;

const OWNER_FILE: &str = "pi-owner.v1";

const OWNER_DIAGNOSTIC_LABEL_BYTES: u64 = super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
    "PiOwnerConfig", "version", "key", "policy", "native_runtime", "PiOwnerPolicy",
    "root_task", "agent", "journal", "grants", "limits", "run_limits", "session_limits",
    "concurrency", "maximum_payload_bytes", "Limits", "file_bytes", "path_bytes",
    "attachments", "render_bytes", "model_steps", "model_events_per_step", "tool_calls_per_step",
    "context_messages", "TaskRunLimits", "max_steps", "deadline_epoch_ms", "SessionLimits",
    "active_tasks", "total_tasks", "depth", "PiNativeHostRuntime", "executable",
    "executable_prefix", "extension", "provider_extension", "environment", "VolumeRefWire",
    "provider", "id", "class", "owner", "kind", "project", "agent_private", "session_shared",
    "session", "ProviderRefWire", "namespace", "family",
]) + acyclic_harness::contract::preparation_serde_diagnostic_label_bytes();
const OWNER_MAGIC: &[u8; 8] = b"ACYPIOW1";
const MAXIMUM_OWNER_BYTES: u64 = super::MAXIMUM_ADAPTER_STATE_BYTES;
const ISSUER: &str = "acyclic.pi";

/// Host-selected root policy.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiOwnerPolicy {
    pub(crate) root_task: TaskId,
    pub(crate) agent: AgentId,
    pub(crate) journal: VolumeRef,
    pub(crate) grants: Capabilities,
    pub(crate) limits: Limits,
    pub(crate) run_limits: TaskRunLimits,
    pub(crate) session_limits: SessionLimits,
    pub(crate) concurrency: usize,
    pub(crate) maximum_payload_bytes: u64,
}

impl PiOwnerPolicy {
    /// The original journal and grant producers fund their own field copies.
    fn clone_with_native_work(&self, work: &NativeMaterializationWork) -> Result<(Self, NativeAllocation)> {
        let slot = work.claim(std::mem::size_of::<Self>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(native_work_error)?;
        let journal = self.journal.clone_with_native_work(work)?;
        let grants = self.grants.clone_with_native_work(work)?;
        combine_native_metadata((Self {
            root_task: self.root_task, agent: self.agent, journal: journal.0, grants: grants.0,
            limits: self.limits, run_limits: self.run_limits, session_limits: self.session_limits,
            concurrency: self.concurrency, maximum_payload_bytes: self.maximum_payload_bytes,
        }, slot, [journal.1, grants.1]))
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiOwnerConfig {
    version: u32,
    key: [u8; 32],
    policy: PiOwnerPolicy,
    native_runtime: Option<super::pi_native::PiNativeHostRuntime>,
    #[serde(skip)]
    _native_custody: Option<NativeAllocation>,
}

impl PartialEq for PiOwnerConfig {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version && self.key == other.key
            && self.policy == other.policy && self.native_runtime == other.native_runtime
    }
}
impl Eq for PiOwnerConfig {}

impl Drop for PiOwnerConfig {
    fn drop(&mut self) { self.key.fill(0); }
}

impl PiOwnerConfig {
    pub(crate) fn policy(&self) -> &PiOwnerPolicy {
        &self.policy
    }


    /// Initial owner creation borrows admitted configuration sources and copies only through their genuine original-ledger producers.
    pub(crate) fn new(
        policy: &PiOwnerPolicy, runtime: Option<&super::pi_native::PiNativeHostRuntime>,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        let custody = work.claim(std::mem::size_of::<Self>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED).map_err(native_work_error)?;
        let policy = policy.clone_with_native_work(work)?;
        let runtime = runtime.map(|runtime| runtime.clone_with_native_work(work)).transpose()?;
        let mut key = protection::new_key().map_err(native_owner_io)?;
        let (runtime, runtime_custody) = match runtime {
            Some((runtime, custody)) => (Some(runtime), Some(custody)),
            None => (None, None),
        };
        let owner = Self {
            version: 1, key,
            policy: policy.0, native_runtime: runtime, _native_custody: Some(custody),
        };
        key.fill(0);
        // Complete data precedes every source guard on a failed transfer.
        let mut owned = (owner, policy.1, runtime_custody);
        owned.0._native_custody.as_mut().unwrap().absorb(&mut owned.1).map_err(native_work_error)?;
        if let Some(runtime_custody) = &mut owned.2 {
            owned.0._native_custody.as_mut().unwrap().absorb(runtime_custody).map_err(native_work_error)?;
        }
        if let Some(runtime) = owned.0.native_runtime.take() {
            owned.0.native_runtime = Some(runtime.sanitize(work)?);
        }
        let authority = owned.0.authority_with_native_work(work)?;
        drop(authority.0);
        drop(authority.1);
        Ok(owned.0)
    }

    pub(crate) fn native_runtime(&self) -> Result<&super::pi_native::PiNativeHostRuntime> {
        self.native_runtime.as_ref()
            .ok_or_else(|| native_invalid("Pi original bootstrap has no pinned native runtime"))
    }


    /// Moves a genuinely native owner into shared host custody without copying credentials.
    pub(crate) fn into_native_shared(
        self, work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_work_error)?;
        if !self._native_custody.as_ref().is_some_and(|custody|
            custody.matches_work(work) && custody.bytes() >= std::mem::size_of::<Self>() as u64)
        {
            return Err(native_invalid("Pi owner sharing requires its original native metadata custody"));
        }
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1, ..acyclic_fs::WorkCounters::UNCHARGED
        }).map_err(native_work_error)?;
        Ok(Arc::new(self))
    }

    pub(crate) fn authority(&self) -> Result<PiRuntimeAuthority> {
        if self.version != 1 || self.key == [0; 32] || self.policy.agent.into_bytes() == [0; 16] {
            return Err(invalid("invalid Pi owner configuration"));
        }
        let journal_grants = [
            self.policy.journal.capability(VolumeOperation::Read)?,
            self.policy.journal.capability(VolumeOperation::Write)?,
        ];
        let issuer = AuthorityIssuer::new(
            ISSUER,
            self.key,
            Authority {
                kind: AggregateKind::Task,
                id: self.policy.root_task.to_string(),
            },
        );
        let storage_grants = Capabilities::new(
            self.policy
                .grants
                .iter()
                .map(str::to_owned)
                .chain(journal_grants),
        );
        let authority = PiRuntimeAuthority {
            verifier: issuer.verifier(),
            signed: issuer.issue(&self.policy.root_task.to_string(), &storage_grants, Some(self.policy.agent), None, &mut None).expect("scope proof"),
            scope: RuntimeScope::new(self.policy.grants.clone(), self.policy.limits)?
                .with_run_limits(self.policy.run_limits, &mut None)?,
            session_limits: self.policy.session_limits,
            concurrency: self.policy.concurrency,
            maximum_payload_bytes: self.policy.maximum_payload_bytes,
            issuer,
        };
        authority.validate(&self.policy.journal)?;
        Ok(authority)
    }

    /// Constructs the same protected authority using the original native ledger.
    pub(crate) fn authority_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<(PiRuntimeAuthority, NativeAllocation)> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_work_error)?;
        if self.version != 1 || self.key == [0; 32] || self.policy.agent.into_bytes() == [0; 16] {
            return Err(native_invalid("invalid Pi owner configuration"));
        }
        RuntimeScope::validate_session_limits_with_native_work(&self.policy.session_limits, work)?;
        let journal_grants = [
            self.policy.journal.capability_with_native_work(VolumeOperation::Read, work)?,
            self.policy.journal.capability_with_native_work(VolumeOperation::Write, work)?,
        ];
        let storage = Capabilities::new_with_native_work(
            self.policy.grants.iter().chain(journal_grants.iter().map(|grant| grant.0.as_str())), work,
        )?;
        let issuer = self.issuer_with_native_work(AggregateKind::Task, self.policy.root_task, work)?;
        let verifier = issuer.0.verifier_with_native_work(work)?;
        let signed = issuer.0.root_for_agent_with_native_work(
            self.policy.agent, &verifier.0.audience().id, &storage.0, work,
        )?;
        drop(storage.0);
        drop(storage.1);
        drop(journal_grants);
        let mut scope = RuntimeScope::new_with_native_work(
            self.policy.grants.clone_with_native_work(work)?, self.policy.limits, work,
        )?;
        scope.0 = scope.0.with_run_limits(self.policy.run_limits, &mut Some(&mut acyclic_harness::store::native_work_io(work).map_err(|cause| acyclic_harness::Error::NativeWork { cause, custody: None })?))?;
        let owned = (PiRuntimeAuthority {
            issuer: issuer.0, verifier: verifier.0, signed: signed.0, scope: scope.0,
            session_limits: self.policy.session_limits, concurrency: self.policy.concurrency,
            maximum_payload_bytes: self.policy.maximum_payload_bytes,
        }, issuer.1, [verifier.1, signed.1, scope.1]);
        owned.0.validate_with_native_work(&self.policy.journal, work)?;
        combine_native_metadata(owned)
    }

    /// Authorizes through the SDK's original journal owner and protected-key verifier.
    #[allow(clippy::too_many_arguments, reason = "original source selection and owner remain explicit")]
    pub(crate) async fn authorize_project_source_with_native_work<P: StreamProvider + 'static>(
        &self,
        caller_owner: &Arc<acyclic_harness::durable_host::TaskJournalOwner<P>>,
        context: &acyclic_harness::runtime::TaskContext,
        provider: &dyn acyclic_harness::source_authorization::RootProjectSourceProvider,
        source_task: TaskId, operation: acyclic_harness::OperationId,
        snapshot: &acyclic_harness::source_authorization::ProjectSourceSnapshot,
        maximum_work: u64, maximum_bytes: u64, work: &NativeMaterializationWork,
    ) -> Result<(acyclic_harness::source_authorization::ProjectSourceReadHandle, NativeAllocation)> {
        let authority = self.authority_with_native_work(work)?;
        let authorization = caller_owner.authorize_project_source_with_native_work(
            context, &authority.0.issuer, provider, source_task, operation,
            snapshot, maximum_work, maximum_bytes, work,
        )?.await;
        drop(authority.0);
        drop(authority.1);
        authorization
    }

    fn issuer_with_native_work(
        &self, kind: AggregateKind, task: TaskId, work: &NativeMaterializationWork,
    ) -> Result<(AuthorityIssuer, NativeAllocation)> {
        let audience = work.display(&task).map_err(native_work_error)?;
        let id = work.display(ISSUER).map_err(native_work_error)?;
        work.charge(acyclic_fs::WorkCounters {
            bytes_copied: self.key.len() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
        }).map_err(native_work_error)?;
        combine_native_metadata((AuthorityIssuer::new(id.0, self.key,
            Authority { kind, id: audience.0 }), audience.1, [id.1]))
    }

    pub(crate) fn effect_verifier_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<(acyclic_harness::core::AuthorityVerifier, NativeAllocation)> {
        let original = self.authority_with_native_work(work)?;
        drop(original.0);
        drop(original.1);
        let issuer = self.issuer_with_native_work(
            AggregateKind::Conversation, self.policy.root_task, work,
        )?;
        let verifier = issuer.0.verifier_with_native_work(work)?;
        drop(issuer.0);
        drop(issuer.1);
        Ok(verifier)
    }

    /// Uses the existing conversation scope ID, audience, owner key and grants.
    pub(crate) fn conversation_authority_with_native_work(
        &self, task: TaskId, agent: AgentId, grants: &Capabilities,
        work: &NativeMaterializationWork,
    ) -> Result<((Authority, AuthorityIssuer, acyclic_harness::core::Scope), NativeAllocation)> {
        self.conversation_authority_parts_with_native_work(
            task, agent, grants, format_args!("pi:conversation:{task}"), work,
        )
    }

    /// The Root effect grant retains its original task-ID scope name, not the conversation binding prefix.
    pub(crate) fn effect_authority_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<((Authority, AuthorityIssuer, acyclic_harness::core::Scope), NativeAllocation)> {
        self.conversation_authority_parts_with_native_work(
            self.policy.root_task, self.policy.agent, &self.policy.grants,
            format_args!("{}", self.policy.root_task), work,
        )
    }

    fn conversation_authority_parts_with_native_work(
        &self, task: TaskId, agent: AgentId, grants: &Capabilities, scope_id: std::fmt::Arguments<'_>,
        work: &NativeMaterializationWork,
    ) -> Result<((Authority, AuthorityIssuer, acyclic_harness::core::Scope), NativeAllocation)> {
        let original = self.authority_with_native_work(work)?;
        drop(original.0);
        drop(original.1);
        let audience_id = work.display(&task).map_err(native_work_error)?;
        let audience = (Authority { kind: AggregateKind::Conversation, id: audience_id.0 }, audience_id.1);
        let issuer = self.issuer_with_native_work(AggregateKind::Conversation, task, work)?;
        let scope_id = work.display(&scope_id).map_err(native_work_error)?;
        let scope = issuer.0.root_for_agent_with_native_work(agent, &scope_id.0, grants, work)?;
        drop(scope_id.0);
        drop(scope_id.1);
        combine_native_metadata(((audience.0, issuer.0, scope.0), audience.1, [issuer.1, scope.1]))
    }




    pub(crate) fn validate_root(
        &self, binding: &super::PiTaskBinding, work: &NativeMaterializationWork,
    ) -> Result<()> {
        binding.validate_with_native_work(None, binding.project_volumes.keys().map(String::as_str), true, work)?;
        if binding.task_id != self.policy.root_task
            || binding.agent_id != self.policy.agent
            || binding.scratch.volume.provider() != self.policy.journal.provider()
        {
            return Err(Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi root binding differs from its original service owner" });
        }
        Ok(())
    }

    /// Atomically publishes once.
    pub(crate) fn save_new_with_native_work(
        &self, root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path, work: &NativeMaterializationWork,
    ) -> Result<()> {
        let authority = self.authority_with_native_work(work)?;
        drop(authority.0);
        drop(authority.1);
        work.validate_private_state_directory(root, session).map_err(native_work_error)?;
        let plain = super::pi_root_provider::guarded_pi_bytes(self, MAXIMUM_OWNER_BYTES, work)?;
        let plain = NativeOwnerPlaintext { bytes: plain.0, _custody: plain.1 };
        let protected = protection::protect_with_copy(&plain.bytes, |bytes| work.copy_bytes(bytes));
        drop(plain);
        let protected = protected.map_err(native_work_error)?;
        let protected = NativeOwnerPlaintext { bytes: protected.0, _custody: protected.1 };
        if protected.bytes.len() as u64 > MAXIMUM_OWNER_BYTES.saturating_sub(40) {
            return Err(native_invalid("protected Pi owner configuration exceeds its byte bound"));
        }
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: protected.bytes.len() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
        }).map_err(native_work_error)?;
        let digest = blake3::hash(&protected.bytes);
        let leaf = work.display(&format_args!(".pi-owner-{}.next", uuid::Uuid::new_v4()))
            .map_err(native_work_error)?;
        let temporary = work.join_path(session, Path::new(&leaf.0)).map_err(native_work_error)?;
        let destination = work.join_path(session, Path::new(OWNER_FILE)).map_err(native_work_error)?;
        work.publish_private_state_file_with_native_work(
            root, &temporary.0, &destination.0,
            &[OWNER_MAGIC.as_slice(), digest.as_bytes().as_slice(), protected.bytes.as_slice()],
        ).map_err(native_work_error)
    }


    /// Reopens the original protected credential record with the request's borrowed native work.
    pub(crate) fn load_with_native_work(
        root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path, work: &NativeMaterializationWork,
    ) -> Result<Self> {
        Self::load_native(root, session, work, false)
    }

    /// Observes the same immutable protected record without a durability write.
    pub(crate) fn load_readonly_with_native_work(
        root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path, work: &NativeMaterializationWork,
    ) -> Result<Self> {
        Self::load_native(root, session, work, true)
    }

    fn load_native(
        root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path, work: &NativeMaterializationWork, readonly: bool,
    ) -> Result<Self> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_work_error)?;
        work.validate_private_state_directory(root, session).map_err(native_work_error)?;
        let path = work.join_path(session, Path::new(OWNER_FILE)).map_err(native_work_error)?;
        let mode = if readonly { acyclic_fs::NativeStateFileMode::Readonly }
            else { acyclic_fs::NativeStateFileMode::ReadWriteExisting };
        let mut file = work.open_state_file_with_native_work(root, &path.0, mode).map_err(native_work_error)?;
        file.validate_private_owner().map_err(native_work_error)?;
        let bytes = file.read_owned(MAXIMUM_OWNER_BYTES as usize).map_err(native_work_error)?;
        let bytes = NativeOwnerPlaintext { bytes: bytes.0, _custody: bytes.1 };
        let (magic, rest) = bytes.bytes.split_first_chunk::<8>()
            .ok_or_else(|| native_invalid("Pi owner configuration is truncated"))?;
        let (digest, protected) = rest.split_first_chunk::<32>()
            .ok_or_else(|| native_invalid("Pi owner configuration is truncated"))?;
        if magic != OWNER_MAGIC {
            return Err(native_invalid("Pi owner configuration digest differs"));
        }
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: protected.len() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
        }).map_err(native_work_error)?;
        if digest != blake3::hash(protected).as_bytes() {
            return Err(native_invalid("Pi owner configuration digest differs"));
        }
        let plain = protection::unprotect_with_copy(protected, |bytes| work.copy_bytes(bytes))
            .map_err(native_work_error)?;
        let plain = NativeOwnerPlaintext { bytes: plain.0, _custody: plain.1 };
        let owner = super::pi_root_provider::guarded_pi_decode::<Self>(
            &plain.bytes, MAXIMUM_OWNER_BYTES, std::mem::size_of::<Self>() as u64,
            acyclic_harness::wire::ErrorCode::Invalid, acyclic_harness::JsonValueSchema::None,
            OWNER_DIAGNOSTIC_LABEL_BYTES, work,
        );
        drop(plain);
        let (mut owner, custody) = owner?;
        owner._native_custody = Some(custody);
        let authority = owner.authority_with_native_work(work)?;
        drop(authority.0);
        drop(authority.1);
        if !readonly {
            file.sync_file().map_err(native_work_error)?;
            file.sync_parent().map_err(native_work_error)?;
        }
        Ok(owner)
    }

    pub(crate) fn load(session: &Path) -> Result<Self> {
        protection::validate_directory(session).map_err(storage)?;
        let file = protection::open_private(&session.join(OWNER_FILE)).map_err(storage)?;
        let mut bytes = Vec::new();
        (&file)
            .take(MAXIMUM_OWNER_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(storage)?;
        if bytes.len() as u64 > MAXIMUM_OWNER_BYTES {
            return Err(invalid("Pi owner file exceeds its byte bound"));
        }
        let (magic, rest) = bytes
            .split_first_chunk::<8>()
            .ok_or_else(|| invalid("truncated Pi owner file"))?;
        let (digest, protected) = rest
            .split_first_chunk::<32>()
            .ok_or_else(|| invalid("truncated Pi owner file"))?;
        if magic != OWNER_MAGIC || digest != blake3::hash(protected).as_bytes() {
            return Err(invalid("Pi owner file integrity check failed"));
        }
        let mut plain = protection::unprotect(protected).map_err(storage)?;
        let decoded = if plain.len() as u64 > MAXIMUM_OWNER_BYTES {
            Err(invalid("Pi owner configuration exceeds its byte bound"))
        } else {
            serde_json::from_slice::<Self>(&plain).map_err(|_| invalid("Pi owner configuration is not valid JSON"))
        };
        plain.fill(0);
        let owner = decoded?;
        owner.authority()?;
        sync_file(&file, Durability::Full).map_err(storage)?;
        sync_parent(session, Durability::Full).map_err(storage)?;
        Ok(owner)
    }
}

/// Managed plaintext is wiped before its bytes and original allocation drop, including cancellation, malformed JSON and native authority failures.
struct NativeOwnerPlaintext {
    bytes: Vec<u8>,
    _custody: NativeAllocation,
}
impl Drop for NativeOwnerPlaintext {
    fn drop(&mut self) { self.bytes.fill(0); }
}

fn native_owner_io(source: io::Error) -> Error {
    native_work_error(acyclic_fs::NativeTreeMaterializationError::Io(source))
}

fn invalid(message: &str) -> Error { Error::Invalid(message.to_owned()) }

// Only ordinary credential producers use this explicit host-storage mapping.
fn storage(error: io::Error) -> Error { Error::Storage(error.to_string()) }

#[cfg(test)]
#[path = "pi_owner_tests.rs"]
pub(crate) mod tests;

/// Consumes genuine original effect metadata before its protected owner moves into the Root provider.
pub(crate) fn root_effects_with_native_work<P: StreamProvider + 'static>(
    stream: std::sync::Arc<P>,
    authority: (Authority, AuthorityIssuer, acyclic_harness::core::Scope),
    custody: NativeAllocation,
    content: Arc<dyn ContentResidencyVerifier>,
    provider: Arc<dyn EffectProvider>,
    work: &NativeMaterializationWork,
) -> Result<Arc<ConversationEffectHost<P>>> {
    // On any registration failure the complete payload drops before its guard.
    let authorization = (authority, custody);
    work.admit(acyclic_fs::WorkCounters::UNCHARGED)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    if provider.id() != super::pi_root_inputs::ROOT_ALLOCATION_PROVIDER {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi root allocation provider identity differs",
        });
    }
    let mut providers = EffectRegistry::new(Some(work))?
        .with_result_resolver_with_native_work(content.clone(), work)?;
    providers.register_with_native_work(provider, work)?;
    let ((audience, issuer, scope), custody) = authorization;
    ConversationEffectHost::new(
        stream, audience, issuer, scope, SchemaRegistry::new(),
        content, providers, Some(custody), Some(work),
    )?.into_arc_with_native_work(work)
}

// Drop tuple payloads before their allocation guards.
fn combine_native_metadata<T, const N: usize>(
    mut owned: (T, NativeAllocation, [NativeAllocation; N]),
) -> Result<(T, NativeAllocation)> {
    for additional in &mut owned.2 {
        owned.1.absorb(additional).map_err(native_work_error)?;
    }
    let (value, custody, _retired) = owned;
    Ok((value, custody))
}

fn native_work_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

fn native_invalid(reason: &'static str) -> Error {
    Error::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason }
}
