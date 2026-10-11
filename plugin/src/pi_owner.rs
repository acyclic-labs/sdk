//! Immutable, private host credentials for reopening one Pi root runtime.
//! Native decoding uses the SDK's original source-shape admission for this
//! closed, Value/Raw-free DTO. Retained configuration drops before its custody;
//! plaintext buffers and the fixed signing-key array are cleared on release.
//! Native private-file access requires the service's genuine held namespace
//! owner plus relative coordinates; accounting is never namespace authority.
//! Readonly observers share the decoder but perform no durability barriers.

use super::pi_runtime::PiRuntimeAuthority;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_harness::{
    AgentId, Capabilities, Error, Result, TaskId,
    conversation::{ContentResidencyVerifier, Limits, VolumeOperation, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer, SchemaRegistry},
    effect_host::ConversationEffectHost,
    effects::{EffectProvider, EffectRegistry},
    runtime::{RuntimeScope, TaskRunLimits},
    scheduler::SessionLimits,
};
use acyclic_native_runtime::{Durability, RenameMode, durable_rename, sync_file, sync_parent};
use acyclic_stream::{StreamClient, StreamProvider};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{self, Read, Write},
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

/// Host-selected root policy. This is immutable service configuration, not a
/// route selection or a task/effect ledger. Task admissions remain authoritative.
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
        let slot = work.retain_owned(std::mem::size_of::<Self>() as u64).map_err(native_work_error)?;
        let journal = self.journal.clone_with_native_work(work)?;
        let grants = self.grants.clone_with_native_work(work)?;
        combine_native_metadata((Self {
            root_task: self.root_task, agent: self.agent, journal: journal.0, grants: grants.0,
            limits: self.limits, run_limits: self.run_limits, session_limits: self.session_limits,
            concurrency: self.concurrency, maximum_payload_bytes: self.maximum_payload_bytes,
        }, slot, [journal.1, grants.1]))
    }
}



// Deliberately no Debug: configuration contains the signing key. It is never
// returned to the extension, serialized into adapter state or included in logs.
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
    /// Called only for an explicitly new root, before admission or selected
    /// allocation. Reopening must load the original file, never call this.
    pub(crate) fn new(policy: PiOwnerPolicy) -> Result<Self> {
        Self::new_with_native_runtime(policy, None)
    }

    pub(crate) fn new_with_native_runtime(
        policy: PiOwnerPolicy, native_runtime: Option<super::pi_native::PiNativeHostRuntime>,
    ) -> Result<Self> {
        let native_runtime = native_runtime.map(|runtime| runtime.sanitize()).transpose()?;
        let owner = Self {
            version: 1,
            key: protection::new_key().map_err(storage)?,
            policy,
            native_runtime,
            _native_custody: None,
        };
        owner.authority()?;
        Ok(owner)
    }

    /// Initial owner creation borrows admitted configuration sources and copies
    /// only through their genuine original-ledger producers. This is called
    /// solely for a new root, never as a recovery credential replacement.
    pub(crate) fn new_with_native_runtime_with_native_work(
        policy: &PiOwnerPolicy, runtime: Option<&super::pi_native::PiNativeHostRuntime>,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        let custody = work.retain_owned(std::mem::size_of::<Self>() as u64).map_err(native_work_error)?;
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
            owned.0.native_runtime = Some(runtime.sanitize()?);
        }
        let authority = owned.0.authority_with_native_work(work)?;
        drop(authority.0);
        drop(authority.1);
        Ok(owned.0)
    }

    pub(crate) fn native_runtime(&self) -> Result<&super::pi_native::PiNativeHostRuntime> {
        self.native_runtime.as_ref()
            .ok_or_else(|| invalid("Pi original bootstrap has no pinned native runtime"))
    }

    /// Borrows the original protected runtime; no credential, scope, String or
    /// guard is copied, and absence retains its genuine Invalid classification.
    pub(crate) fn native_runtime_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<&super::pi_native::PiNativeHostRuntime> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_work_error)?;
        self.native_runtime.as_ref()
            .ok_or_else(|| native_invalid("Pi original bootstrap has no pinned native runtime"))
    }

    /// Moves a genuinely native owner into shared host custody without copying
    /// credentials. Ordinary preexisting DTOs cannot be adopted retrospectively.
    pub(crate) fn into_native_shared(
        self, work: &NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_work_error)?;
        if !self._native_custody.as_ref().is_some_and(|custody|
            custody.matches_work(work) && custody.bytes() >= std::mem::size_of::<Self>() as u64)
        {
            return Err(native_invalid("Pi owner sharing requires its original native metadata custody"));
        }
        // The existing guard already owns the complete payload. This is the
        // actual Arc allocation attempt, not a second Self claim or a key copy.
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
            signed: issuer.root_for_agent(
                self.policy.agent,
                self.policy.root_task.to_string(),
                storage_grants,
            ),
            scope: RuntimeScope::new(self.policy.grants.clone(), self.policy.limits)?
                .with_run_limits(self.policy.run_limits)?,
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
        scope.0 = scope.0.with_run_limits_with_native_work(self.policy.run_limits, work)?;
        let owned = (PiRuntimeAuthority {
            issuer: issuer.0, verifier: verifier.0, signed: signed.0, scope: scope.0,
            session_limits: self.policy.session_limits, concurrency: self.policy.concurrency,
            maximum_payload_bytes: self.policy.maximum_payload_bytes,
        }, issuer.1, [verifier.1, signed.1, scope.1]);
        owned.0.validate_with_native_work(&self.policy.journal, work)?;
        combine_native_metadata(owned)
    }

    /// Authorizes through the SDK's original journal owner and protected-key
    /// verifier. The issuer and its genuine custody remain private and live
    /// until the original native authorization future settles.
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
            task, agent, grants, &format_args!("pi:conversation:{task}"), work,
        )
    }

    /// The Root effect grant retains its original task-ID scope name, not the
    /// conversation binding prefix. No owner configuration or scope is cloned.
    pub(crate) fn effect_authority_with_native_work(
        &self, work: &NativeMaterializationWork,
    ) -> Result<((Authority, AuthorityIssuer, acyclic_harness::core::Scope), NativeAllocation)> {
        self.conversation_authority_parts_with_native_work(
            self.policy.root_task, self.policy.agent, &self.policy.grants,
            &self.policy.root_task, work,
        )
    }

    fn conversation_authority_parts_with_native_work<D: std::fmt::Display + ?Sized>(
        &self, task: TaskId, agent: AgentId, grants: &Capabilities, scope_id: &D,
        work: &NativeMaterializationWork,
    ) -> Result<((Authority, AuthorityIssuer, acyclic_harness::core::Scope), NativeAllocation)> {
        let original = self.authority_with_native_work(work)?;
        drop(original.0);
        drop(original.1);
        let audience_id = work.display(&task).map_err(native_work_error)?;
        let audience = (Authority { kind: AggregateKind::Conversation, id: audience_id.0 }, audience_id.1);
        let issuer_audience_id = work.display(&audience.0.id).map_err(native_work_error)?;
        let issuer_audience = (Authority {
            kind: AggregateKind::Conversation, id: issuer_audience_id.0,
        }, issuer_audience_id.1);
        let issuer_id = work.display(ISSUER).map_err(native_work_error)?;
        work.charge(acyclic_fs::WorkCounters {
            bytes_copied: self.key.len() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
        }).map_err(native_work_error)?;
        let issuer = combine_native_metadata((AuthorityIssuer::new(
            issuer_id.0, self.key, issuer_audience.0,
        ), issuer_id.1, [issuer_audience.1]))?;
        let scope_id = work.display(scope_id).map_err(native_work_error)?;
        let scope = issuer.0.root_for_agent_with_native_work(agent, &scope_id.0, grants, work)?;
        drop(scope_id.0);
        drop(scope_id.1);
        combine_native_metadata(((audience.0, issuer.0, scope.0), audience.1, [issuer.1, scope.1]))
    }
    /// Shares the original effect audience and protected owner key. References
    /// and caller-provided task IDs never select another issuer.
    fn effect_issuer(&self) -> AuthorityIssuer {
        AuthorityIssuer::new(ISSUER, self.key, Authority {
            kind: AggregateKind::Conversation,
            id: self.policy.root_task.to_string(),
        })
    }

    pub(crate) fn effect_verifier(&self) -> Result<acyclic_harness::core::AuthorityVerifier> {
        self.authority()?;
        Ok(self.effect_issuer().verifier())
    }

    /// Called only after authenticating the actual task and its reconstructed
    /// scope. This signs that conversation's identity with the same original
    /// protected key; it does not create or admit a task or resource.
    pub(crate) fn conversation_authority(
        &self, task: TaskId, agent: AgentId, grants: Capabilities,
    ) -> Result<(Authority, AuthorityIssuer, acyclic_harness::core::Scope)> {
        self.authority()?;
        let audience = Authority { kind: AggregateKind::Conversation, id: task.to_string() };
        let issuer = AuthorityIssuer::new(ISSUER, self.key, audience.clone());
        let signed = issuer.root_for_agent(agent, format!("pi:conversation:{task}"), grants);
        Ok((audience, issuer, signed))
    }


    /// Composes the stock effect lifecycle with the original root credentials.
    /// The caller supplies the original task-bound content source; this factory
    /// never turns protected owner storage credentials into private input reads.
    pub(crate) fn root_effects<P: StreamProvider + 'static>(
        &self,
        stream: StreamClient<P>,
        content: Arc<dyn ContentResidencyVerifier>,
        provider: Arc<dyn EffectProvider>,
    ) -> Result<ConversationEffectHost<P>>
    {
        if provider.id() != super::pi_root_inputs::ROOT_ALLOCATION_PROVIDER {
            return Err(invalid("Pi root allocation provider identity differs"));
        }
        self.authority()?;
        let mut providers = EffectRegistry::default().with_result_resolver(content.clone());
        providers.register(provider)?;
        // Aggregate kind is part of the signed audience, keeping effect history
        // separate from task admission without another key or identity source.
        let issuer = self.effect_issuer();
        let audience = issuer.verifier().audience().clone();
        let scope = issuer.root_for_agent(
            self.policy.agent,
            self.policy.root_task.to_string(),
            self.policy.grants.clone(),
        );
        ConversationEffectHost::new(
            stream,
            audience,
            issuer,
            scope,
            SchemaRegistry::new(),
            content,
            providers,
        )
    }

    /// Check the retained references before opening the owner's journals. The
    /// journal still authenticates the complete original admission afterward.
    pub(crate) fn validate_root(&self, binding: &super::PiTaskBinding) -> Result<()> {
        self.validate_root_inner(binding, None)
    }

    pub(crate) fn validate_root_with_native_work(
        &self, binding: &super::PiTaskBinding, work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.validate_root_inner(binding, Some(work))
    }

    fn validate_root_inner(
        &self, binding: &super::PiTaskBinding, work: Option<&NativeMaterializationWork>,
    ) -> Result<()> {
        if let Some(work) = work {
            binding.validate_with_native_work(
                None, binding.project_volumes.keys().map(String::as_str), true, work,
            )?;
        } else {
            binding.validate(None, binding.project_volumes.keys().map(String::as_str), true)?;
        }
        if binding.task_id != self.policy.root_task
            || binding.agent_id != self.policy.agent
            || binding.scratch.volume.provider() != self.policy.journal.provider()
        {
            return Err(match work {
                Some(_) => Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "Pi root binding differs from its original service owner",
                },
                None => Error::Unauthorized("Pi root binding differs from its original service owner".to_owned()),
            });
        }
        Ok(())
    }

    /// Atomically publishes once. An existing, missing-on-reopen or damaged
    /// owner is never replaced, regenerated or repaired from route references.
    /// Publishes the same protected owner format once, with original-ledger
    /// canonical encoding and actual held-file writes. No success is inferred
    /// from a path, and NoReplace preserves the original immutable credential.
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
        // Unix protection is private-file custody rather than encryption, so
        // clear managed protected buffers on every platform as well.
        let protected = NativeOwnerPlaintext { bytes: protected.0, _custody: protected.1 };
        if protected.bytes.len() as u64 > MAXIMUM_OWNER_BYTES.saturating_sub(40) {
            return Err(native_invalid("protected Pi owner configuration exceeds its byte bound"));
        }
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: protected.bytes.len() as u64, ..acyclic_fs::WorkCounters::UNCHARGED
        }).map_err(native_work_error)?;
        let digest = blake3::hash(&protected.bytes);
        // UUID remains the original unguessable temporary name, never a Task,
        // invocation operation, journal identity or admission replacement.
        let leaf = work.display(&format_args!(".pi-owner-{}.next", uuid::Uuid::new_v4()))
            .map_err(native_work_error)?;
        let temporary = work.join_path(session, Path::new(&leaf.0)).map_err(native_work_error)?;
        let destination = work.join_path(session, Path::new(OWNER_FILE)).map_err(native_work_error)?;
        work.publish_private_state_file_with_native_work(
            root, &temporary.0, &destination.0,
            &[OWNER_MAGIC.as_slice(), digest.as_bytes().as_slice(), protected.bytes.as_slice()],
        ).map_err(native_work_error)
    }

    pub(crate) fn save_new(&self, session: &Path) -> Result<()> {
        self.authority()?;
        protection::validate_directory(session).map_err(storage)?;
        let mut plain = serde_json::to_vec(self).map_err(|_| invalid("Pi owner encoding failed"))?;
        if plain.len() as u64 > MAXIMUM_OWNER_BYTES {
            plain.fill(0);
            return Err(invalid("Pi owner configuration exceeds its byte bound"));
        }
        let protected = protection::protect(&plain);
        plain.fill(0);
        let protected = protected.map_err(storage)?;
        if protected.len() as u64 + 40 > MAXIMUM_OWNER_BYTES {
            return Err(invalid("protected Pi owner configuration exceeds its byte bound"));
        }
        let temporary = session.join(format!(".pi-owner-{}.next", uuid::Uuid::new_v4()));
        let mut file = protection::create_private(&temporary).map_err(storage)?;
        let publish = (|| -> io::Result<()> {
            file.write_all(OWNER_MAGIC)?;
            file.write_all(blake3::hash(&protected).as_bytes())?;
            file.write_all(&protected)?;
            sync_file(&file, Durability::Full)?;
            drop(file);
            durable_rename(&temporary, &session.join(OWNER_FILE), RenameMode::NoReplace)
        })();
        let _ = fs::remove_file(&temporary);
        publish.map_err(storage)
    }

    /// Reopens the original protected credential record with the request's
    /// borrowed native work. Private-file/ACL checks and OS crypto remain the
    /// same host authority boundary; all managed bytes and JSON ownership use
    /// actual SDK producers, never a restaged input or a fresh owner key.
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
        // The callback runs while DPAPI's foreign buffer is still live. The
        // same engine clears/frees that buffer on native copy denial as well.
        let plain = protection::unprotect_with_copy(protected, |bytes| work.copy_bytes(bytes))
            .map_err(native_work_error)?;
        let plain = NativeOwnerPlaintext { bytes: plain.0, _custody: plain.1 };
        let owner = Self::decode_with_native_work(&plain.bytes, work);
        drop(plain);
        let owner = owner?;
        let authority = owner.authority_with_native_work(work)?;
        drop(authority.0);
        drop(authority.1);
        if !readonly {
            // These exact held-file barriers preserve the existing interrupted
            // publication recovery rule; readonly observers never flush.
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
        // Publication may have reached the final name before its caller lost
        // the durability acknowledgement. Make these original credentials
        // durable before allowing any later admission to rely on them.
        sync_file(&file, Durability::Full).map_err(storage)?;
        sync_parent(session, Durability::Full).map_err(storage)?;
        Ok(owner)
    }
    /// The existing SDK source pass issues this closed DTO's admission shape.
    /// Fixed fields (including the exact [u8; 32] key) are in Self. Dynamic slots
    /// are String, PathBuf and (String, String); their collection/serde overlap
    /// fits the SDK's Value slot bound. Its four source-byte string allowances
    /// also cover UTF-16 paths and serde scratch. Opaque serde/map allocation
    /// metadata is not reported as observed allocation operations.
    fn decode_with_native_work(bytes: &[u8], work: &NativeMaterializationWork) -> Result<Self> {
        use acyclic_harness::contract::{
            JsonBounds, JsonCodecCharge, JsonValueSchema,
            json_from_slice_bounded_native_trusted, json_value_decode_owned_bytes,
            json_typed_diagnostic_owned_bytes,
        };
        let remaining = work.remaining().map_err(native_work_error)?;
        let mut custody = work.retain_owned(0).map_err(native_work_error)?;
        let mut retained = 0;
        let bounds = JsonBounds {
            maximum_bytes: MAXIMUM_OWNER_BYTES,
            maximum_items: remaining.items_examined,
            maximum_depth: 64,
        };
        let decoded = json_from_slice_bounded_native_trusted::<Self, _>(
            bytes, bounds, 0, acyclic_harness::wire::ErrorCode::Invalid,
            JsonValueSchema::None, &mut |charge| {
                if let JsonCodecCharge::Release { bytes } = charge {
                    return custody.release(bytes).map_err(native_work_error);
                }
                work.admit(acyclic_fs::WorkCounters::UNCHARGED).map_err(native_work_error)?;
                match charge {
                    JsonCodecCharge::Admission { maximum_owned_bytes, value_shape } => {
                        retained = json_value_decode_owned_bytes(value_shape)?
                            .checked_add(std::mem::size_of::<Self>() as u64)
                            .ok_or_else(|| native_invalid("Pi owner ownership allowance overflow"))?;
                        let diagnostic = json_typed_diagnostic_owned_bytes(
                            value_shape, OWNER_DIAGNOSTIC_LABEL_BYTES,
                            Some(acyclic_harness::wire::ErrorCode::Invalid),
                        )?;
                        let admitted = retained.checked_add(maximum_owned_bytes)
                            .and_then(|bytes| bytes.checked_add(diagnostic))
                            .ok_or_else(|| native_invalid("Pi owner decoder allowance overflow"))?;
                        custody.resize_retained(admitted).map_err(native_work_error)
                    }
                    JsonCodecCharge::Work { bytes, items } => work.charge(acyclic_fs::WorkCounters {
                        bytes_encoded: bytes, items_examined: items, ..acyclic_fs::WorkCounters::UNCHARGED
                    }).map_err(native_work_error),
                    JsonCodecCharge::Inspect { items } => work.charge(acyclic_fs::WorkCounters {
                        items_examined: items, ..acyclic_fs::WorkCounters::UNCHARGED
                    }).map_err(native_work_error),
                    JsonCodecCharge::Read { bytes } => work.charge(acyclic_fs::WorkCounters {
                        source_bytes_read: bytes, ..acyclic_fs::WorkCounters::UNCHARGED
                    }).map_err(native_work_error),
                    JsonCodecCharge::CopyAdmission { bytes } => work.admit(acyclic_fs::WorkCounters {
                        bytes_copied: bytes, ..acyclic_fs::WorkCounters::UNCHARGED
                    }).map_err(native_work_error),
                    JsonCodecCharge::Copy { bytes } => work.charge(acyclic_fs::WorkCounters {
                        bytes_copied: bytes, ..acyclic_fs::WorkCounters::UNCHARGED
                    }).map_err(native_work_error),
                    JsonCodecCharge::AllocationOperation { .. } => work.charge(acyclic_fs::WorkCounters {
                        allocation_operations: 1, ..acyclic_fs::WorkCounters::UNCHARGED
                    }).map_err(native_work_error),
                    JsonCodecCharge::Retain { bytes } => custody.grow(bytes).map_err(native_work_error),
                    JsonCodecCharge::Release { .. } => unreachable!(),
                }
            },
        );
        match decoded {
            Ok(mut owner) => {
                // Decoder scratch has already dropped; the original admitted
                // configuration ownership survives intrinsically, after data.
                owner._native_custody = Some(custody);
                owner._native_custody.as_mut().unwrap().substitute(retained)
                    .map_err(native_work_error)?;
                Ok(owner)
            }
            Err(Error::NativeDecode { code, source, custody: None }) => {
                Err(Error::NativeDecode { code, source, custody: Some(custody) })
            }
            Err(error) => Err(error),
        }
    }
}

/// Managed plaintext is wiped before its bytes and original allocation drop,
/// including cancellation, malformed JSON and native authority failures.
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
mod tests;

/// Consumes genuine original effect metadata before its protected owner moves
/// into the Root provider. The SDK host retains authorization custody itself.
pub(crate) fn root_effects_with_native_work<P: StreamProvider + 'static>(
    stream: StreamClient<P>,
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
    let mut providers = EffectRegistry::new_with_native_work(work)?
        .with_result_resolver_with_native_work(content.clone(), work)?;
    providers.register_with_native_work(provider, work)?;
    let ((audience, issuer, scope), custody) = authorization;
    ConversationEffectHost::new_with_native_work(
        stream, audience, issuer, scope, SchemaRegistry::new(),
        content, providers, custody, work,
    )?.into_arc_with_native_work(work)
}

// Tuple layout keeps every original payload ahead of all of its guards on
// cancellation or a failed merge. Retired guards are empty only after absorb.
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
