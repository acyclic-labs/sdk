//! Immutable, private host credentials for reopening one Pi root runtime.

use super::pi_runtime::PiRuntimeAuthority;
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_harness::{
    AgentId, Capabilities, TaskId,
    conversation::{Limits, VolumeOperation, VolumeRef},
    core::{AggregateKind, Authority, AuthorityIssuer, SchemaRegistry},
    effect_host::ConversationEffectHost,
    effects::{EffectProvider, EffectRegistry},
    filesystem::{FilesystemContentVerifier, FilesystemHost},
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

// Deliberately no Debug: configuration contains the signing key. It is never
// returned to the extension, serialized into adapter state or included in logs.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiOwnerConfig {
    version: u32,
    key: [u8; 32],
    policy: PiOwnerPolicy,
}

impl PiOwnerConfig {
    pub(crate) fn policy(&self) -> &PiOwnerPolicy {
        &self.policy
    }
    /// Called only for an explicitly new root, before admission or selected
    /// allocation. Reopening must load the original file, never call this.
    pub(crate) fn new(policy: PiOwnerPolicy) -> Result<Self, String> {
        let owner = Self {
            version: 1,
            key: protection::new_key().map_err(super::display)?,
            policy,
        };
        owner.authority()?;
        Ok(owner)
    }

    pub(crate) fn authority(&self) -> Result<PiRuntimeAuthority, String> {
        if self.version != 1 || self.key == [0; 32] || self.policy.agent.into_bytes() == [0; 16] {
            return Err("invalid Pi owner configuration".to_owned());
        }
        let journal_grants = [
            self.policy
                .journal
                .capability(VolumeOperation::Read)
                .map_err(super::display)?,
            self.policy
                .journal
                .capability(VolumeOperation::Write)
                .map_err(super::display)?,
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
            scope: RuntimeScope::new(self.policy.grants.clone(), self.policy.limits)
                .and_then(|scope| scope.with_run_limits(self.policy.run_limits))
                .map_err(super::display)?,
            session_limits: self.policy.session_limits,
            concurrency: self.policy.concurrency,
            maximum_payload_bytes: self.policy.maximum_payload_bytes,
            issuer,
        };
        authority
            .validate(&self.policy.journal)
            .map_err(super::display)?;
        Ok(authority)
    }

    /// Composes the stock effect lifecycle with the original root credentials.
    /// The provider owns allocation evidence; this only selects its exact
    /// identity and binds content reads to the owner's original storage grants.
    pub(crate) fn root_effects<P, A, O>(
        &self,
        stream: StreamClient<P>,
        filesystem: Arc<FilesystemHost<A, O>>,
        provider: Arc<dyn EffectProvider>,
    ) -> Result<ConversationEffectHost<P>, String>
    where
        P: StreamProvider + 'static,
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        if provider.id() != super::pi_root_inputs::ROOT_ALLOCATION_PROVIDER {
            return Err("Pi root allocation provider identity differs".to_owned());
        }
        let authority = self.authority()?;
        let content = Arc::new(
            FilesystemContentVerifier::new(
                filesystem,
                authority.verifier,
                authority.signed,
                self.policy.maximum_payload_bytes,
            )
            .map_err(super::display)?,
        );
        let mut providers = EffectRegistry::default().with_result_resolver(content.clone());
        providers.register(provider).map_err(super::display)?;
        // Aggregate kind is part of the signed audience, keeping effect history
        // separate from task admission without another key or identity source.
        let audience = Authority {
            kind: AggregateKind::Conversation,
            id: self.policy.root_task.to_string(),
        };
        let issuer = AuthorityIssuer::new(ISSUER, self.key, audience.clone());
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
        .map_err(super::display)
    }

    /// Check the retained references before opening the owner's journals. The
    /// journal still authenticates the complete original admission afterward.
    pub(crate) fn validate_root(&self, binding: &super::PiTaskBinding) -> Result<(), String> {
        binding.validate(
            None,
            binding.project_volumes.keys().map(String::as_str).collect(),
            true,
        )?;
        if binding.task_id != self.policy.root_task
            || binding.agent_id != self.policy.agent
            || binding.scratch.volume.provider() != self.policy.journal.provider()
        {
            return Err("Pi root binding differs from its original service owner".to_owned());
        }
        Ok(())
    }

    /// Atomically publishes once. An existing, missing-on-reopen or damaged
    /// owner is never replaced, regenerated or repaired from route references.
    pub(crate) fn save_new(&self, session: &Path) -> Result<(), String> {
        self.authority()?;
        protection::validate_directory(session).map_err(super::display)?;
        let mut plain = serde_json::to_vec(self).map_err(super::display)?;
        if plain.len() as u64 > MAXIMUM_OWNER_BYTES {
            plain.fill(0);
            return Err("Pi owner configuration exceeds its byte bound".to_owned());
        }
        let protected = protection::protect(&plain);
        plain.fill(0);
        let protected = protected.map_err(super::display)?;
        if protected.len() as u64 + 40 > MAXIMUM_OWNER_BYTES {
            return Err("protected Pi owner configuration exceeds its byte bound".to_owned());
        }
        let temporary = session.join(format!(".pi-owner-{}.next", uuid::Uuid::new_v4()));
        let mut file = protection::create_private(&temporary).map_err(super::display)?;
        let publish = (|| -> io::Result<()> {
            file.write_all(OWNER_MAGIC)?;
            file.write_all(blake3::hash(&protected).as_bytes())?;
            file.write_all(&protected)?;
            sync_file(&file, Durability::Full)?;
            drop(file);
            durable_rename(&temporary, &session.join(OWNER_FILE), RenameMode::NoReplace)
        })();
        let _ = fs::remove_file(&temporary);
        publish.map_err(super::display)
    }

    pub(crate) fn load(session: &Path) -> Result<Self, String> {
        protection::validate_directory(session).map_err(super::display)?;
        let file = protection::open_private(&session.join(OWNER_FILE)).map_err(super::display)?;
        let mut bytes = Vec::new();
        (&file)
            .take(MAXIMUM_OWNER_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(super::display)?;
        if bytes.len() as u64 > MAXIMUM_OWNER_BYTES {
            return Err("Pi owner file exceeds its byte bound".to_owned());
        }
        let (magic, rest) = bytes
            .split_first_chunk::<8>()
            .ok_or_else(|| "truncated Pi owner file".to_owned())?;
        let (digest, protected) = rest
            .split_first_chunk::<32>()
            .ok_or_else(|| "truncated Pi owner file".to_owned())?;
        if magic != OWNER_MAGIC || digest != blake3::hash(protected).as_bytes() {
            return Err("Pi owner file integrity check failed".to_owned());
        }
        let mut plain = protection::unprotect(protected).map_err(super::display)?;
        let decoded = if plain.len() as u64 > MAXIMUM_OWNER_BYTES {
            Err("Pi owner configuration exceeds its byte bound".to_owned())
        } else {
            serde_json::from_slice::<Self>(&plain).map_err(super::display)
        };
        plain.fill(0);
        let owner = decoded?;
        owner.authority()?;
        // Publication may have reached the final name before its caller lost
        // the durability acknowledgement. Make these original credentials
        // durable before allowing any later admission to rely on them.
        sync_file(&file, Durability::Full).map_err(super::display)?;
        sync_parent(session, Durability::Full).map_err(super::display)?;
        Ok(owner)
    }
}

#[cfg(test)]
#[path = "pi_owner_tests.rs"]
mod tests;
