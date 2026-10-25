//! Immutable, private host credentials for reopening one Pi root runtime.

use super::pi_runtime::PiRuntimeAuthority;
use acyclic_fs::{NativeAllocation, NativeMaterializationWork};
use acyclic_harness::conversation::ContentResidencyVerifier;
use acyclic_harness::filesystem::PreparationIo;
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
use std::{
    io::{self, Read, Write},
    path::Path,
    sync::Arc,
};

use acyclic_native_runtime::{owner_record, private_file};

// Keep native denial custody intact while the shared codec supplies I/O failures.
enum OwnerIo {
    Native(Error),
    Io(io::Error),
}

impl From<Error> for OwnerIo {
    fn from(error: Error) -> Self {
        Self::Native(error)
    }
}

impl From<acyclic_fs::NativeTreeMaterializationError> for OwnerIo {
    fn from(error: acyclic_fs::NativeTreeMaterializationError) -> Self {
        Self::Native(Error::from(error))
    }
}

impl From<io::Error> for OwnerIo {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

impl OwnerIo {
    fn into_native(self, work: &NativeMaterializationWork) -> Error {
        match self {
            Self::Native(error) => error,
            Self::Io(error) => Error::from(work.runtime_error(error)),
        }
    }
}

const OWNER_FILE: &str = "pi-owner.v1";

const OWNER_DIAGNOSTIC_LABEL_BYTES: u64 =
    733 + acyclic_harness::preparation_serde_diagnostic_label_bytes();
const MAXIMUM_OWNER_BYTES: u64 = super::MAXIMUM_ADAPTER_STATE_BYTES;
const ISSUER: &str = "acyclic.pi";

/// Host-selected root policy.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
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
        let mut io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let journal = self.journal.copy(&mut Some(&mut io))?;
        let grants = Capabilities::copy(self.grants.iter(), work)?;
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
            [io.into_custody()?, grants.1],
        ))
    }
}

#[derive(Serialize, Deserialize, acyclic_harness::HeapLayout)]
#[heap_layout(crate = acyclic_harness)]
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
        acyclic_native_runtime::owner_record::clear_secret(&mut self.key);
    }
}

impl PiOwnerConfig {
    pub(crate) fn policy(&self) -> &PiOwnerPolicy {
        &self.policy
    }

    /// The record of one successful fork plan validation: a MAC under this
    /// owner's key over the published plan bytes' digest, the caller and the
    /// policy the validation read. Records never cross owners, callers or
    /// policies, and the key itself never leaves the owner.
    pub(crate) fn validated_plan_key(
        &self,
        published: &blake3::Hash,
        caller: TaskId,
    ) -> super::pi_fork::ValidatedPlanKey {
        let mut mac = blake3::Hasher::new_keyed(&self.key);
        mac.update(b"acyclic.pi-fork-plan.validated.v1");
        mac.update(published.as_bytes());
        mac.update(&caller.into_bytes());
        mac.update(&self.policy.root_task.into_bytes());
        mac.update(&self.policy.maximum_payload_bytes.to_le_bytes());
        super::pi_fork::ValidatedPlanKey(*mac.finalize().as_bytes())
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
        let mut key = owner_record::new_key().map_err(native_owner_io)?;
        let (runtime, runtime_custody) = match runtime {
            Some((runtime, custody)) => (Some(runtime), Some(custody)),
            None => (None, None),
        };
        let owner = Self {
            version: 1,
            key,
            policy: policy.0,
            native_runtime: runtime,
            _native_custody: None,
        };
        acyclic_native_runtime::owner_record::clear_secret(&mut key);
        // Data drops before its original custody on a failed transfer.
        let mut owned = (owner, custody, policy.1, runtime_custody);
        owned.1.absorb(&mut owned.2)?;
        if let Some(runtime_custody) = &mut owned.3 {
            owned.1.absorb(runtime_custody)?;
        }
        owned.0._native_custody = Some(owned.1);
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
            let mut io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            self.policy.session_limits.validate(&mut Some(&mut io))?;
            let grants = [
                self.policy
                    .journal
                    .capability(VolumeOperation::Read, &mut Some(&mut io))?,
                self.policy
                    .journal
                    .capability(VolumeOperation::Write, &mut Some(&mut io))?,
            ];
            Capabilities::copy(
                self.policy
                    .grants
                    .iter()
                    .chain(grants.iter().map(String::as_str)),
                work,
            )?
        };
        let issuer =
            self.issuer_with_native_work(AggregateKind::Task, self.policy.root_task, work)?;
        let mut io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
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
            Capabilities::copy(self.policy.grants.iter(), work)?,
            self.policy.limits,
            work,
        )?;
        scope.0 = scope
            .0
            .with_run_limits(self.policy.run_limits, &mut Some(&mut io))?;
        let ((issuer, verifier, signed, scope), custody) = combine_native_metadata((
            (issuer.0, verifier, signed, scope.0),
            issuer.1,
            [io.into_custody()?, scope.1],
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
            &mut Some(&mut &PreparationIo::from_native_work(work)?),
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
        let issuer =
            self.issuer_with_native_work(AggregateKind::Conversation, self.policy.root_task, work)?;
        let verifier = {
            let mut io =
                acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
            let value = issuer.0.verifier(&mut Some(&mut io))?;
            (value, io.into_custody()?)
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
        let mut io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let scope = issuer
            .0
            .issue(&scope_id.0, grants, Some(agent), None, &mut Some(&mut io))?;
        drop(scope_id.0);
        drop(scope_id.1);
        combine_native_metadata((
            (audience.0, issuer.0, scope),
            audience.1,
            [issuer.1, io.into_custody()?],
        ))
    }

    pub(crate) fn validate_root(&self, binding: &super::PiTaskBinding) -> Result<()> {
        binding.validate(
            None,
            binding.project_volumes.keys().map(|key| key.as_str()),
            true,
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
    pub(crate) fn save_new(
        &self,
        root: &Path,
        session: &Path,
        work: &NativeMaterializationWork,
    ) -> Result<bool> {
        let session = work.path(root, Some(session))?;
        let plain = super::pi_root_provider::guarded_pi_bytes(self, MAXIMUM_OWNER_BYTES, work)?;
        let plain = NativeOwnerPlaintext {
            bytes: plain.0,
            _custody: plain.1,
        };
        let meter = work.runtime_meter();
        let protected =
            owner_record::encode_with(&plain.bytes, MAXIMUM_OWNER_BYTES, Some(&meter), |parts| {
                let length = parts.iter().map(|part| part.len() as u64).sum::<u64>();
                let custody = work.claim(
                    length,
                    1,
                    acyclic_fs::WorkCounters {
                        bytes_copied: length,
                        ..acyclic_fs::WorkCounters::UNCHARGED
                    },
                )?;
                let mut bytes =
                    Vec::with_capacity(usize::try_from(length).map_err(io::Error::other)?);
                for part in parts {
                    bytes.extend_from_slice(part);
                }
                Ok::<_, OwnerIo>((bytes, custody))
            })
            .map_err(|error| error.into_native(work))?;
        drop(plain);
        let protected = NativeOwnerPlaintext {
            bytes: protected.0,
            _custody: protected.1,
        };
        let leaf = work.display(&format_args!(
            ".pi-owner-{}.next",
            uuid::Builder::from_random_bytes({
                let mut bytes = [0; 16];
                owner_record::fill_random(&mut bytes)
                    .map_err(|error| Error::from(work.runtime_error(io::Error::other(error))))?;
                bytes
            })
            .into_uuid()
        ))?;
        let temporary = work.path(&session.0, Some(Path::new(&leaf.0)))?;
        let destination = work.path(&session.0, Some(Path::new(OWNER_FILE)))?;
        private_file::publish_new(
            &destination.0,
            &temporary.0,
            |file| file.write_all(&protected.bytes),
            |_| Ok(()),
        )
        .map_err(|error| Error::from(work.runtime_error(error)))
    }

    /// Reads the original protected credential record.
    pub(crate) fn load(
        root: &Path,
        session: &Path,
        work: &NativeMaterializationWork,
    ) -> Result<Self> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
        let session = work.path(root, Some(session))?;
        let path = work.path(&session.0, Some(Path::new(OWNER_FILE)))?;
        let bytes = private_file::read_with(&path.0, MAXIMUM_OWNER_BYTES, |file, length| {
            let custody = work.claim(length + 1, 1, acyclic_fs::WorkCounters::UNCHARGED)?;
            let mut bytes =
                Vec::with_capacity(usize::try_from(length + 1).map_err(io::Error::other)?);
            file.read_to_end(&mut bytes)
                .map_err(|error| Error::from(work.runtime_error(error)))?;
            Ok::<_, OwnerIo>(NativeOwnerPlaintext {
                bytes,
                _custody: custody,
            })
        })
        .map_err(|error| error.into_native(work))?;
        let meter = work.runtime_meter();
        let plain =
            owner_record::decode_with(&bytes.bytes, MAXIMUM_OWNER_BYTES, Some(&meter), |plain| {
                work.copy_bytes(plain).map_err(OwnerIo::from)
            })
            .map_err(|error| error.into_native(work))?;
        let plain = NativeOwnerPlaintext {
            bytes: plain.0,
            _custody: plain.1,
        };
        let owner = super::pi_root_provider::guarded_pi_decode::<Self>(
            &plain.bytes,
            MAXIMUM_OWNER_BYTES,
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

impl super::pi_runtime::PiTaskRuntime {
    pub(crate) fn verify_owner_with_native_work(
        &self,
        owner: &PiOwnerConfig,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        work.admit(acyclic_fs::WorkCounters::UNCHARGED)?;
        let current = &self.authority;
        let policy = &owner.policy;
        let invalid = || Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi owner differs from the original runtime credentials or policy",
        };
        if owner.version != 1
            || owner.key == [0; 32]
            || policy.agent.into_bytes() == [0; 16]
            || &policy.grants != current.scope.grants()
            || policy.limits != current.scope.limits()
            || policy.run_limits != current.scope.run_limits()
            || policy.session_limits != current.session_limits
            || policy.concurrency != current.concurrency
            || policy.maximum_payload_bytes != current.maximum_payload_bytes
        {
            return Err(invalid());
        }
        let mut io =
            acyclic_harness::store::HistoryWork::new(PreparationIo::from_native_work(work)?);
        let grants = [
            policy
                .journal
                .capability(VolumeOperation::Read, &mut Some(&mut io))?,
            policy
                .journal
                .capability(VolumeOperation::Write, &mut Some(&mut io))?,
        ];
        let storage = Capabilities::copy(
            policy
                .grants
                .iter()
                .chain(grants.iter().map(String::as_str)),
            work,
        )?;
        let issuer = owner.issuer_with_native_work(AggregateKind::Task, policy.root_task, work)?;
        let task = work.display(&policy.root_task)?;
        issuer
            .0
            .verifier(&mut Some(&mut io))?
            .verify(&current.signed, &mut Some(&mut io))?;
        if current.signed.id() != task.0
            || current.signed.issuer() != ISSUER
            || current.signed.capabilities() != &storage.0
            || current.signed.agent() != Some(policy.agent)
        {
            return Err(invalid());
        }
        Ok(())
    }
}

/// Managed plaintext is wiped before its bytes and original allocation drop, including cancellation, malformed JSON and native authority failures.
struct NativeOwnerPlaintext {
    bytes: Vec<u8>,
    _custody: NativeAllocation,
}
impl Drop for NativeOwnerPlaintext {
    fn drop(&mut self) {
        acyclic_native_runtime::owner_record::clear_secret(&mut self.bytes);
    }
}

fn native_owner_io(source: io::Error) -> Error {
    acyclic_harness::Error::from(acyclic_fs::NativeTreeMaterializationError::Io(source))
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
    let mut providers = EffectRegistry::new(Some(work), Some(content.clone()))?;
    providers.register(provider, Some(work))?;
    let ((audience, issuer, scope), custody) = authorization;
    ConversationEffectHost::new(
        stream,
        (audience, issuer, scope),
        SchemaRegistry::new(),
        content,
        providers,
        Some(custody),
        Some(work),
    )
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
