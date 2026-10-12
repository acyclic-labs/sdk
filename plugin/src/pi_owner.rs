//! Immutable, private host credentials for reopening one Pi root runtime.

use super::pi_runtime::PiRuntimeAuthority;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_harness::conversation::ContentResidencyVerifier;
use acyclic_harness::{
    AgentId, Capabilities, Error, Result, TaskId,
    conversation::{Limits, VolumeOperation, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer, SchemaRegistry},
    effect_host::ConversationEffectHost,
    effects::{EffectProvider, EffectRegistry},
    runtime::{RuntimeScope, TaskRunLimits},
    scheduler::SessionLimits,
};
use acyclic_stream::StreamProvider;
use serde::{Deserialize, Serialize};
use std::{io, path::Path, sync::Arc};

#[path = "pi_owner_protection.rs"]
mod protection;

const OWNER_FILE: &str = "pi-owner.v1";

const OWNER_DIAGNOSTIC_LABEL_BYTES: u64 =
    super::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "PiOwnerConfig",
        "version",
        "key",
        "policy",
        "native_runtime",
        "PiOwnerPolicy",
        "root_task",
        "agent",
        "journal",
        "grants",
        "limits",
        "run_limits",
        "session_limits",
        "concurrency",
        "maximum_payload_bytes",
        "Limits",
        "file_bytes",
        "path_bytes",
        "attachments",
        "render_bytes",
        "model_steps",
        "model_events_per_step",
        "tool_calls_per_step",
        "context_messages",
        "TaskRunLimits",
        "max_steps",
        "deadline_epoch_ms",
        "SessionLimits",
        "active_tasks",
        "total_tasks",
        "depth",
        "PiNativeHostRuntime",
        "executable",
        "executable_prefix",
        "extension",
        "provider_extension",
        "environment",
        "VolumeRefWire",
        "provider",
        "id",
        "class",
        "owner",
        "kind",
        "project",
        "agent_private",
        "session_shared",
        "session",
        "ProviderRefWire",
        "namespace",
        "family",
    ]) + acyclic_harness::preparation_serde_diagnostic_label_bytes();
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
    fn clone_with_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(Self, NativeAllocation)> {
        let slot = work.claim(
            std::mem::size_of::<Self>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let mut io = acyclic_harness::store::native_work_io(work)?;
        let journal = self.journal.copy(&mut Some(&mut io))?;
        let grants = self.grants.clone_with_native_work(work)?;
        combine_native_metadata((
            Self {
                root_task: self.root_task,
                agent: self.agent,
                journal,
                grants: grants.0,
                limits: self.limits,
                run_limits: self.run_limits,
                session_limits: self.session_limits,
                concurrency: self.concurrency,
                maximum_payload_bytes: self.maximum_payload_bytes,
            },
            slot,
            [io.into_custody(), grants.1],
        ))
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
        self.version == other.version
            && self.key == other.key
            && self.policy == other.policy
            && self.native_runtime == other.native_runtime
    }
}
impl Eq for PiOwnerConfig {}

impl Drop for PiOwnerConfig {
    fn drop(&mut self) {
        self.key.fill(0);
    }
}

impl PiOwnerConfig {
    pub(crate) fn policy(&self) -> &PiOwnerPolicy {
        &self.policy
    }

    /// Initial owner creation borrows admitted configuration sources and copies only through their genuine original-ledger producers.
    pub(crate) fn new(
        policy: &PiOwnerPolicy,
        runtime: Option<&super::pi_native::PiNativeHostRuntime>,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        let custody = work.claim(
            std::mem::size_of::<Self>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        let policy = policy.clone_with_native_work(work)?;
        let runtime = runtime
            .map(|runtime| runtime.clone_with_native_work(work))
            .transpose()?;
        let mut key = protection::new_key().map_err(native_owner_io)?;
        let (runtime, runtime_custody) = match runtime {
            Some((runtime, custody)) => (Some(runtime), Some(custody)),
            None => (None, None),
        };
        let owner = Self {
            version: 1,
            key,
            policy: policy.0,
            native_runtime: runtime,
            _native_custody: Some(custody),
        };
        key.fill(0);
        // Complete data precedes every source guard on a failed transfer.
        let mut owned = (owner, policy.1, runtime_custody);
        owned
            .0
            ._native_custody
            .as_mut()
            .unwrap()
            .absorb(&mut owned.1)?;
        if let Some(runtime_custody) = &mut owned.2 {
            owned
                .0
                ._native_custody
                .as_mut()
                .unwrap()
                .absorb(runtime_custody)?;
        }
        if let Some(runtime) = owned.0.native_runtime.take() {
            owned.0.native_runtime = Some(runtime.sanitize(work)?);
        }
        let authority = owned.0.authority(work)?;
        drop(authority);
        Ok(owned.0)
    }

    pub(crate) fn native_runtime(&self) -> Result<&super::pi_native::PiNativeHostRuntime> {
        self.native_runtime
            .as_ref()
            .ok_or_else(|| native_invalid("Pi original bootstrap has no pinned native runtime"))
    }

    /// Moves a genuinely native owner into shared host custody without copying credentials.
    pub(crate) fn into_native_shared(self, work: &NativeMaterializationWork) -> Result<Arc<Self>> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
        if !self._native_custody.as_ref().is_some_and(|custody| {
            custody.work().shares_allocation_context(work)
                && custody.bytes() >= std::mem::size_of::<Self>() as u64
        }) {
            return Err(native_invalid(
                "Pi owner sharing requires its original native metadata custody",
            ));
        }
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1,
            ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
        Ok(Arc::new(self))
    }

    /// Issues authority on the caller's original ledger.
    pub(crate) fn authority(&self, work: &NativeMaterializationWork) -> Result<PiRuntimeAuthority> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
        if self.version != 1 || self.key == [0; 32] || self.policy.agent.into_bytes() == [0; 16] {
            return Err(native_invalid("invalid Pi owner configuration"));
        }
        let storage = {
            let mut io = acyclic_harness::store::native_work_io(work)?;
            self.policy.session_limits.validate(&mut Some(&mut io))?;
            let grants = [
                self.policy
                    .journal
                    .capability(VolumeOperation::Read, &mut Some(&mut io))?,
                self.policy
                    .journal
                    .capability(VolumeOperation::Write, &mut Some(&mut io))?,
            ];
            Capabilities::new_with_native_work(
                self.policy
                    .grants
                    .iter()
                    .chain(grants.iter().map(String::as_str)),
                work,
            )?
        };
        let issuer =
            self.issuer_with_native_work(AggregateKind::Task, self.policy.root_task, work)?;
        let mut io = acyclic_harness::store::native_work_io(work)?;
        let verifier = issuer.0.verifier(&mut Some(&mut io))?;
        let signed = issuer.0.issue(
            &verifier.audience().id,
            &storage.0,
            Some(self.policy.agent),
            None,
            &mut Some(&mut io),
        )?;
        drop(storage);
        let mut scope = RuntimeScope::new_with_native_work(
            self.policy.grants.clone_with_native_work(work)?,
            self.policy.limits,
            work,
        )?;
        scope.0 = scope
            .0
            .with_run_limits(self.policy.run_limits, &mut Some(&mut io))?;
        let ((issuer, verifier, signed, scope), custody) = combine_native_metadata((
            (issuer.0, verifier, signed, scope.0),
            issuer.1,
            [io.into_custody(), scope.1],
        ))?;
        let authority = PiRuntimeAuthority {
            issuer,
            verifier,
            signed,
            scope,
            session_limits: self.policy.session_limits,
            concurrency: self.policy.concurrency,
            maximum_payload_bytes: self.policy.maximum_payload_bytes,
            custody,
        };
        authority.validate(
            &self.policy.journal,
            &mut Some(&mut acyclic_harness::store::native_work_io(work)?),
        )?;
        Ok(authority)
    }

    fn issuer_with_native_work(
        &self,
        kind: AggregateKind,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<(AuthorityIssuer, NativeAllocation)> {
        let audience = work.display(&task)?;
        let id = work.display(ISSUER)?;
        work.charge(acyclic_fs::WorkCounters {
            bytes_copied: self.key.len() as u64,
            ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
        combine_native_metadata((
            AuthorityIssuer::new(
                id.0,
                self.key,
                Authority {
                    kind,
                    id: audience.0,
                },
            ),
            audience.1,
            [id.1],
        ))
    }

    pub(crate) fn effect_verifier_with_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(acyclic_harness::core::AuthorityVerifier, NativeAllocation)> {
        let original = self.authority(work)?;
        drop(original);
        let issuer =
            self.issuer_with_native_work(AggregateKind::Conversation, self.policy.root_task, work)?;
        let verifier = {
            let mut io = acyclic_harness::store::history::native_work_io(work)?;
            let value = issuer.0.verifier(&mut Some(&mut io))?;
            (value, io.into_custody())
        };
        drop(issuer.0);
        drop(issuer.1);
        Ok(verifier)
    }

    /// Uses the existing conversation scope ID, audience, owner key and grants.
    pub(crate) fn conversation_authority_with_native_work(
        &self,
        task: TaskId,
        agent: AgentId,
        grants: &Capabilities,
        work: &NativeMaterializationWork,
    ) -> Result<(
        (Authority, AuthorityIssuer, acyclic_harness::core::Scope),
        NativeAllocation,
    )> {
        self.conversation_authority_parts_with_native_work(
            task,
            agent,
            grants,
            format_args!("pi:conversation:{task}"),
            work,
        )
    }

    /// The Root effect grant retains its original task-ID scope name, not the conversation binding prefix.
    pub(crate) fn effect_authority_with_native_work(
        &self,
        work: &NativeMaterializationWork,
    ) -> Result<(
        (Authority, AuthorityIssuer, acyclic_harness::core::Scope),
        NativeAllocation,
    )> {
        self.conversation_authority_parts_with_native_work(
            self.policy.root_task,
            self.policy.agent,
            &self.policy.grants,
            format_args!("{}", self.policy.root_task),
            work,
        )
    }

    fn conversation_authority_parts_with_native_work(
        &self,
        task: TaskId,
        agent: AgentId,
        grants: &Capabilities,
        scope_id: std::fmt::Arguments<'_>,
        work: &NativeMaterializationWork,
    ) -> Result<(
        (Authority, AuthorityIssuer, acyclic_harness::core::Scope),
        NativeAllocation,
    )> {
        let original = self.authority(work)?;
        drop(original);
        let audience_id = work.display(&task)?;
        let audience = (
            Authority {
                kind: AggregateKind::Conversation,
                id: audience_id.0,
            },
            audience_id.1,
        );
        let issuer = self.issuer_with_native_work(AggregateKind::Conversation, task, work)?;
        let scope_id = work.display(&scope_id)?;
        let mut io = acyclic_harness::store::native_work_io(work)?;
        let scope = issuer
            .0
            .issue(&scope_id.0, grants, Some(agent), None, &mut Some(&mut io))?;
        drop(scope_id.0);
        drop(scope_id.1);
        combine_native_metadata((
            (audience.0, issuer.0, scope),
            audience.1,
            [issuer.1, io.into_custody()],
        ))
    }

    pub(crate) fn validate_root(
        &self,
        binding: &super::PiTaskBinding,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        binding.validate_with_native_work(
            None,
            binding.project_volumes.keys().map(String::as_str),
            true,
            work,
        )?;
        if binding.task_id != self.policy.root_task
            || binding.agent_id != self.policy.agent
            || binding.scratch.volume.provider() != self.policy.journal.provider()
        {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi root binding differs from its original service owner",
            });
        }
        Ok(())
    }

    /// Atomically publishes once.
    pub(crate) fn save_new_with_native_work(
        &self,
        root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let authority = self.authority(work)?;
        drop(authority);
        work.validate_private_state_directory(root, session)?;
        let plain = super::pi_root_provider::guarded_pi_bytes(self, MAXIMUM_OWNER_BYTES, work)?;
        let plain = NativeOwnerPlaintext {
            bytes: plain.0,
            _custody: plain.1,
        };
        let protected = protection::protect_with_copy(&plain.bytes, |bytes| work.copy_bytes(bytes));
        drop(plain);
        let protected = protected?;
        let protected = NativeOwnerPlaintext {
            bytes: protected.0,
            _custody: protected.1,
        };
        if protected.bytes.len() as u64 > MAXIMUM_OWNER_BYTES.saturating_sub(40) {
            return Err(native_invalid(
                "protected Pi owner configuration exceeds its byte bound",
            ));
        }
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: protected.bytes.len() as u64,
            ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
        let digest = blake3::hash(&protected.bytes);
        let leaf = work.display(&format_args!(".pi-owner-{}.next", uuid::Uuid::new_v4()))?;
        let temporary = work.join_path(session, Path::new(&leaf.0))?;
        let destination = work.join_path(session, Path::new(OWNER_FILE))?;
        work.publish_private_state_file_with_native_work(
            root,
            &temporary.0,
            &destination.0,
            &[
                OWNER_MAGIC.as_slice(),
                digest.as_bytes().as_slice(),
                protected.bytes.as_slice(),
            ],
        )
        .map_err(acyclic_harness::Error::from)
    }

    /// Reads the original protected credential record.
    pub(crate) fn load(
        root: &acyclic_fs::NativeHostPrivateStateRoot,
        session: &Path,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
        work.validate_private_state_directory(root, session)?;
        let path = work.join_path(session, Path::new(OWNER_FILE))?;
        let mut file = work.open_state_file_with_native_work(
            root,
            &path.0,
            acyclic_fs::NativeStateFileMode::Readonly,
        )?;
        file.validate_private_owner()?;
        let bytes = file.read_owned(MAXIMUM_OWNER_BYTES as usize)?;
        let bytes = NativeOwnerPlaintext {
            bytes: bytes.0,
            _custody: bytes.1,
        };
        let (magic, rest) = bytes
            .bytes
            .split_first_chunk::<8>()
            .ok_or_else(|| native_invalid("Pi owner configuration is truncated"))?;
        let (digest, protected) = rest
            .split_first_chunk::<32>()
            .ok_or_else(|| native_invalid("Pi owner configuration is truncated"))?;
        if magic != OWNER_MAGIC {
            return Err(native_invalid("Pi owner configuration digest differs"));
        }
        work.charge(acyclic_fs::WorkCounters {
            bytes_hashed: protected.len() as u64,
            ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
        if digest != blake3::hash(protected).as_bytes() {
            return Err(native_invalid("Pi owner configuration digest differs"));
        }
        let plain = protection::unprotect_with_copy(protected, |bytes| work.copy_bytes(bytes))?;
        let plain = NativeOwnerPlaintext {
            bytes: plain.0,
            _custody: plain.1,
        };
        let owner = super::pi_root_provider::guarded_pi_decode::<Self>(
            &plain.bytes,
            MAXIMUM_OWNER_BYTES,
            std::mem::size_of::<Self>() as u64,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            OWNER_DIAGNOSTIC_LABEL_BYTES,
            work,
        );
        drop(plain);
        let (mut owner, custody) = owner?;
        owner._native_custody = Some(custody);
        let authority = owner.authority(work)?;
        drop(authority);
        Ok(owner)
    }
}

/// Managed plaintext is wiped before its bytes and original allocation drop, including cancellation, malformed JSON and native authority failures.
struct NativeOwnerPlaintext {
    bytes: Vec<u8>,
    _custody: NativeAllocation,
}
impl Drop for NativeOwnerPlaintext {
    fn drop(&mut self) {
        self.bytes.fill(0);
    }
}

fn native_owner_io(source: io::Error) -> Error {
    acyclic_harness::Error::from(acyclic_fs::NativeTreeMaterializationError::Io(source))
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.to_owned())
}

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
    work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
    if provider.id() != super::pi_root_inputs::ROOT_ALLOCATION_PROVIDER {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Invalid,
            reason: "Pi root allocation provider identity differs",
        });
    }
    let mut providers = EffectRegistry::new(Some(work))?.with_result_resolver(content.clone());
    providers.register(provider, Some(work))?;
    let ((audience, issuer, scope), custody) = authorization;
    ConversationEffectHost::new(
        stream,
        audience,
        issuer,
        scope,
        SchemaRegistry::new(),
        content,
        providers,
        Some(custody),
        Some(work),
    )?
    .into_arc_with_native_work(work)
}

// Drop tuple payloads before their allocation guards.
fn combine_native_metadata<T, const N: usize>(
    mut owned: (T, NativeAllocation, [NativeAllocation; N]),
) -> Result<(T, NativeAllocation)> {
    for additional in &mut owned.2 {
        owned.1.absorb(additional)?;
    }
    let (value, custody, _retired) = owned;
    Ok((value, custody))
}

fn native_invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}
