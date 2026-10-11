//! Canonical, payload-free conversation and file references.
//!
//! A reference identifies an immutable version of a file. A volume head may be
//! edited, but edits create a new version and never change a recorded message.

use crate::{
    AgentId, Error, OperationId, Result,
    core::{AuthorityVerifier, Scope},
    resources::{GenerationRef, ProviderRef},
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use uuid::Uuid;

/// Borrowed asynchronous result returned by content-provider extension hooks.
pub type ContentFuture<'a, T> = BoxFuture<'a, T>;
/// A freshly authorized content grant paired with its residency verifier.
pub type ContentMount = (ContentGrant, Arc<dyn ContentResidencyVerifier>);

/// Maximum normalized UTF-8 path length admitted by the Harness protocol.
pub const MAX_PATH_BYTES: usize = MAX_PORTABLE_COUNT;
/// Maximum UTF-8 bytes in a protocol label or display name.
pub const MAX_LABEL_BYTES: usize = crate::COMPONENT_LABEL_MAX_BYTES;
/// Largest integer that can be represented exactly by a JavaScript number.
pub const MAX_EXACT_JS_INTEGER: u64 = (1_u64 << 53) - 1;
#[allow(clippy::cast_precision_loss, reason = "2^53 - 1 is exact in f64")]
const MAX_EXACT_JS_FLOAT: f64 = MAX_EXACT_JS_INTEGER as f64;

/// Whether a JavaScript Number round-trips this value exactly: finite, not
/// negative zero, and not an integral value beyond the safe-integer range.
pub(crate) fn is_exact_js_number(value: f64) -> bool {
    value.is_finite()
        && !(value == 0.0 && value.is_sign_negative())
        && (value.fract() != 0.0 || value.abs() <= MAX_EXACT_JS_FLOAT)
}

/// Whether a JSON integer, if `number` is one, is within the safe-integer range.
pub(crate) fn is_exact_js_integer(number: &serde_json::Number) -> bool {
    number.as_i64().map_or_else(
        || {
            number
                .as_u64()
                .is_none_or(|value| value <= MAX_EXACT_JS_INTEGER)
        },
        |value| value.unsigned_abs() <= MAX_EXACT_JS_INTEGER,
    )
}
/// Maximum file byte length accepted by the limits validator.
pub const MAX_LIMIT_FILE_BYTES: u64 = MAX_EXACT_JS_INTEGER;
/// Maximum rendered byte length representable by the numeric wire format.
pub const MAX_LIMIT_RENDER_BYTES: u64 = MAX_EXACT_JS_INTEGER;
/// Largest count representable by both this platform and the numeric wire format.
pub const MAX_PORTABLE_COUNT: usize =
    usize::MAX >> usize::BITS.saturating_sub(f64::MANTISSA_DIGITS);
/// Representable attachment count; admission selects the actual budget.
pub const MAX_LIMIT_ATTACHMENTS: usize = MAX_PORTABLE_COUNT;
/// Representable model step count; admission selects the actual budget.
pub const MAX_LIMIT_MODEL_STEPS: usize = MAX_PORTABLE_COUNT;
/// Representable event count; admission selects the actual budget.
pub const MAX_LIMIT_MODEL_EVENTS_PER_STEP: usize = MAX_PORTABLE_COUNT;
/// Representable tool call count; admission selects the actual budget.
pub const MAX_LIMIT_TOOL_CALLS_PER_STEP: usize = MAX_PORTABLE_COUNT;
/// Representable message count; admission selects the actual budget.
pub const MAX_LIMIT_CONTEXT_MESSAGES: usize = MAX_PORTABLE_COUNT;
/// Representable conversation page allowance; the caller chooses its budget.
pub const MAX_CONVERSATION_PAGE_MESSAGES: usize = u32::MAX as usize;
/// Default conversation read batch, replaceable by the caller.
pub const DEFAULT_CONVERSATION_PAGE_MESSAGES: u32 = 1_024;

/// Admission and rendering bounds. Each value may narrow the protocol ceiling;
/// provider adapters may impose a still lower physical limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// Maximum bytes in one referenced file admitted to a conversation.
    pub file_bytes: u64,
    /// Maximum normalized UTF-8 path bytes.
    pub path_bytes: usize,
    /// Maximum attachments in one message, inline or manifest-backed.
    pub attachments: usize,
    /// Maximum bytes rendered directly into one model context part.
    pub render_bytes: u64,
    /// Maximum model round trips in the stock agent loop.
    pub model_steps: usize,
    /// Maximum streamed observations admitted in one model round trip.
    pub model_events_per_step: usize,
    /// Maximum distinct tool calls admitted in one model round trip.
    pub tool_calls_per_step: usize,
    /// Maximum canonical messages selected into one model request.
    pub context_messages: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            file_bytes: MAX_LIMIT_FILE_BYTES,
            // Defaults must also fit the 32-bit WASM runtime. Native callers
            // can explicitly select the wider platform-representable counts.
            path_bytes: u32::MAX as usize,
            attachments: u32::MAX as usize,
            render_bytes: MAX_LIMIT_RENDER_BYTES,
            model_steps: u32::MAX as usize,
            model_events_per_step: u32::MAX as usize,
            tool_calls_per_step: u32::MAX as usize,
            context_messages: u32::MAX as usize,
        }
    }
}

impl Limits {
    /// Checks each independent budget against its numeric wire representation.
    pub fn validate(&self) -> Result<()> {
        if self.invalid_numeric_limits() {
            return Err(Error::Invalid("harness limits are invalid".into()));
        }
        Ok(())
    }

    /// Checks the same numeric limits without allocating a native denial message.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining_budget().map_err(Error::from)?;
        if self.invalid_numeric_limits() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "harness limits are invalid",
            });
        }
        Ok(())
    }

    fn invalid_numeric_limits(&self) -> bool {
        self.file_bytes == 0
            || self.file_bytes > MAX_LIMIT_FILE_BYTES
            || self.path_bytes == 0
            || self.path_bytes as u64 > MAX_PATH_BYTES as u64
            || self.attachments == 0
            || self.attachments as u64 > MAX_LIMIT_ATTACHMENTS as u64
            || self.render_bytes == 0
            || self.render_bytes > MAX_LIMIT_RENDER_BYTES
            || self.model_steps == 0
            || self.model_steps as u64 > MAX_LIMIT_MODEL_STEPS as u64
            || self.model_events_per_step == 0
            || self.model_events_per_step as u64 > MAX_LIMIT_MODEL_EVENTS_PER_STEP as u64
            || self.tool_calls_per_step == 0
            || self.tool_calls_per_step as u64 > MAX_LIMIT_TOOL_CALLS_PER_STEP as u64
            || self.context_messages == 0
            || self.context_messages as u64 > MAX_LIMIT_CONTEXT_MESSAGES as u64

    }

    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if self.invalid_numeric_limits() {
            return Err(conversation_boundary(work, "harness limits are invalid"));
        }
        Ok(())
    }

    /// Checks one immutable version against this runtime's tighter limits.
    pub fn validate_file(&self, file: &FileRef) -> Result<()> {
        self.validate_file_with_work(file, &mut None)
    }

    pub(crate) fn validate_file_with_work(
        &self, file: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate_with_work(work)?;
        file.validate_with_work(work)?;
        if file.path().len() > self.path_bytes || file.descriptor().byte_length() > self.file_bytes
        {
            return Err(conversation_boundary(work, "file exceeds harness limits"));
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn validate_file_with_native_work(
        &self, file: &FileRef, native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        let mut io = crate::store::history::native_work_io(native)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.validate_file_with_work(file, &mut Some(&mut io))
    }

    /// Checks every directly named file and attachment count before admission.
    pub fn validate_message(&self, message: &ConversationMessage) -> Result<()> {
        self.validate_message_with_work(message, &mut None)
    }

    pub(crate) fn validate_message_with_work(
        &self, message: &ConversationMessage,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        message.validate_with_work(work)?;
        self.validate_file_with_work(&message.content, work)?;
        match &message.attachments {
            ReferencedAttachments::Inline { items } => {
                if items.len() > self.attachments {
                    return Err(conversation_boundary(work, "attachments exceed harness limits"));
                }
                for item in items { self.validate_file_with_work(&item.file, work)?; }
            }
            ReferencedAttachments::Manifest { manifest, item_count } => {
                if *item_count as usize > self.attachments {
                    return Err(conversation_boundary(work, "attachments exceed harness limits"));
                }
                self.validate_file_with_work(manifest, work)?;
            }
        }
        for file in message.extensions.values() { self.validate_file_with_work(file, work)?; }
        Ok(())
    }
}

/// The three independently governed file namespaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VolumeClass {
    /// Consumer's project role. Lineage and merge semantics belong to Filesystem.
    Project,
    /// Scratch files owned by exactly one agent.
    AgentPrivate,
    /// Files shared through explicit session grants.
    SessionShared,
}

/// Logical owner used for routing; access still requires an authenticated grant.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum VolumeOwner {
    /// Project workspace owner.
    Project(String),
    /// Agent private-volume owner.
    Agent(#[schemars(with = "String")] AgentId),
    /// Shared session-volume owner.
    Session(String),
}

/// Globally addressable volume identity with no embedded capability or endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct VolumeRef {
    provider: ProviderRef,
    id: String,
    class: VolumeClass,
    owner: VolumeOwner,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VolumeRefWire {
    provider: ProviderRef,
    id: String,
    class: VolumeClass,
    owner: VolumeOwner,
}

impl<'de> Deserialize<'de> for VolumeRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = VolumeRefWire::deserialize(deserializer)?;
        Self::new(wire.provider, wire.id, wire.class, wire.owner).map_err(serde::de::Error::custom)
    }
}

impl VolumeRef {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let owner = match &self.owner {
            VolumeOwner::Project(owner) | VolumeOwner::Session(owner) => owner.capacity() as u64,
            VolumeOwner::Agent(_) => 0,
        };
        self.provider.logical_owned_bytes()?.checked_add(self.id.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(owner))
            .ok_or_else(|| Error::Invalid("volume identity ownership overflow".into()))
    }

    /// Copies only this closed identity's audited String fields. Borrowed
    /// source capacity is an admission ceiling, not bytes actually copied.
    #[cfg(feature = "filesystem")]
    pub(crate) fn clone_with_io(
        &self, io: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        let allowance = self.logical_owned_bytes()?;
        io.retain_owned(allowance)?;
        let held = io.guard_owned(allowance)?;
        let value = Self {
            provider: self.provider.copy_fields_with_io(io)?,
            id: crate::resources::copy_metadata_string(&self.id, io)?,
            class: self.class,
            owner: match &self.owner {
                VolumeOwner::Project(id) => VolumeOwner::Project(crate::resources::copy_metadata_string(id, io)?),
                VolumeOwner::Session(id) => VolumeOwner::Session(crate::resources::copy_metadata_string(id, io)?),
                VolumeOwner::Agent(id) => VolumeOwner::Agent(*id),
            },
        };
        let actual = value.logical_owned_bytes()?;
        let unused = allowance.checked_sub(actual)
            .ok_or_else(|| io.invalid("volume clone exceeded its source capacity"))?;
        io.release(unused)?;
        held.into_retained();
        Ok(value)
    }

    /// Validates independent consumer role and owner identities.
    pub fn new(
        provider: ProviderRef,
        id: impl Into<String>,
        class: VolumeClass,
        owner: VolumeOwner,
    ) -> Result<Self> {
        Self::new_with_work(provider, id.into(), class, owner, &mut None)
    }

    pub(crate) fn new_with_work(
        provider: ProviderRef,
        id: String,
        class: VolumeClass,
        owner: VolumeOwner,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let value = Self { provider, id, class, owner };
        value.validate_with_work(work)?;
        Ok(value)
    }

    /// Copies a volume identity under the original native work ledger.
    ///
    /// The returned custody covers owned identity text, not authority or grants.
    /// Keep it alive until after the returned identity is physically dropped.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        provider: &ProviderRef,
        id: &str,
        class: VolumeClass,
        owner: &VolumeOwner,
        native: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut observer = crate::store::history::native_work_io(native)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut work = Some(&mut observer as &mut dyn crate::store::history::HistoryIo);
        Self::validate_fields_with_work(provider, id, owner, &mut work)?;
        let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
        let volume = (|| {
            let provider = crate::core::copy_provider(provider, &mut quota)?;
            let id = crate::core::copy_text(id, &mut quota)?;
            let owner = match owner {
                VolumeOwner::Project(text) =>
                    VolumeOwner::Project(crate::core::copy_text(text, &mut quota)?),
                VolumeOwner::Session(text) =>
                    VolumeOwner::Session(crate::core::copy_text(text, &mut quota)?),
                VolumeOwner::Agent(agent) => VolumeOwner::Agent(*agent),
            };
            Self::new_with_work(provider, id, class, owner, quota.work())
        })()?;
        quota.transfer();
        drop(quota);
        Ok((volume, observer.into_custody()))
    }

    /// Rejects malformed provider and owner identities.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        Self::validate_fields_with_work(&self.provider, &self.id, &self.owner, work)
    }

    fn validate_fields_with_work(
        provider: &ProviderRef, id: &str, owner: &VolumeOwner,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(reason) = provider.validation_error() {
            return Err(conversation_boundary(work, reason));
        }
        validate_label_with_work(id, MAX_LABEL_BYTES, work)?;
        match owner {
            VolumeOwner::Project(id) | VolumeOwner::Session(id) => {
                validate_label_with_work(id, MAX_LABEL_BYTES, work)?;
            }
            VolumeOwner::Agent(_) => {}
        }
        Ok(())
    }

    /// Validates source metadata on the original native ledger; conveys no grant.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        inspect_native_metadata(work, 0)?;
        self.provider.validate_with_native_work(work)?;
        inspect_native_metadata(work, self.id.len())?;
        validate_label_native(&self.id, MAX_LABEL_BYTES)?;
        if let VolumeOwner::Project(id) | VolumeOwner::Session(id) = &self.owner {
            inspect_native_metadata(work, id.len())?;
            validate_label_native(id, MAX_LABEL_BYTES)?;
        }
        Ok(())
    }

    /// Storage implementation identity.
    #[must_use]
    pub const fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    /// Provider-owned volume identity.
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Consumer role of this namespace; grants determine access.
    #[must_use]
    pub const fn class(&self) -> VolumeClass {
        self.class
    }

    /// Logical owner, used to route authorized reads.
    #[must_use]
    pub const fn owner(&self) -> &VolumeOwner {
        &self.owner
    }

    /// Enforces owner-only agent mutations independently of the consumer role.
    pub(crate) fn require_writer(&self, agent: Option<AgentId>) -> Result<()> {
        self.require_writer_inner(agent, false)
    }

    fn require_writer_inner(&self, agent: Option<AgentId>, native: bool) -> Result<()> {
        if let VolumeOwner::Agent(owner) = self.owner()
            && agent != Some(*owner)
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if native {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "only the owning agent may write this volume",
                });
            }
            #[cfg(any(not(feature = "filesystem"), target_arch = "wasm32"))]
            let _ = native;
            return Err(Error::Unauthorized(
                "only the owning agent may write this volume".into(),
            ));
        }
        Ok(())
    }

    /// Exact capability for one operation on this volume identity.
    pub fn capability(&self, operation: VolumeOperation) -> Result<String> {
        Ok(format!(
            "volume:{}:{}",
            operation.as_str(),
            blake3::Hash::from_bytes(crate::contract::canonical_json_digest(self)?).to_hex()
        ))
    }

    /// Computes the original capability identity through the finite codec.
    #[cfg(feature = "filesystem")]
    pub fn capability_measured(
        &self, operation: VolumeOperation, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<String>, Error> {
        let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = io.capability(self, match operation {
            VolumeOperation::Read => "volume:read:", VolumeOperation::Write => "volume:write:",
        });
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    /// Computes the same grant on the original native operation ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn capability_with_native_work(
        &self,
        operation: VolumeOperation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(String, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let value = io.capability(self, match operation {
            VolumeOperation::Read => "volume:read:",
            VolumeOperation::Write => "volume:write:",
        })?;
        let retained = io.owned_bytes()?;
        let guard = io.into_native_custody(retained)?;
        Ok((value, guard))
    }

    /// Deep-copies the known volume fields using the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut observer = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let volume = self.clone_with_work(&mut Some(&mut observer))?;
        Ok((volume, observer.into_custody()))
    }

    pub(crate) fn clone_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if work.is_none() { return Ok(self.clone()); }
        self.validate_with_work(work)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let volume = self.clone_in_quota(&mut quota)?;
        quota.transfer();
        Ok(volume)
    }

    pub(crate) fn clone_in_quota(
        &self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        let namespace = clone_owned_file_text(self.provider.namespace(), quota)?;
        let family = clone_owned_file_text(self.provider.family(), quota)?;
        let version = clone_owned_file_text(self.provider.version(), quota)?;
        let provider = ProviderRef::new_with_work(namespace, family, version, quota.work())?;
        let volume = Self {
            provider,
            id: clone_owned_file_text(&self.id, quota)?,
            class: self.class,
            owner: match &self.owner {
                VolumeOwner::Project(text) => VolumeOwner::Project(clone_owned_file_text(text, quota)?),
                VolumeOwner::Session(text) => VolumeOwner::Session(clone_owned_file_text(text, quota)?),
                VolumeOwner::Agent(id) => VolumeOwner::Agent(*id),
            },
        };
        volume.validate_with_work(quota.work())?;
        Ok(volume)
    }

    /// Owner-issued read capability for a directory and its descendants.
    /// Empty prefix names the whole volume. It never implies a write.
    pub fn directory_read_capability(&self, prefix: &str) -> Result<String> {
        if !prefix.is_empty() {
            validate_content_path(prefix)?;
        }
        if is_internal_path(prefix) {
            return Err(Error::Invalid(
                "internal storage paths cannot be delegated".into(),
            ));
        }
        Ok(format!(
            "directory:read:{}",
            blake3::Hash::from_bytes(crate::contract::canonical_json_digest(&(self, prefix))?)
                .to_hex()
        ))
    }

    /// Computes the original directory grant without changing its read scope.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn directory_read_capability_with_native_work(
        &self,
        prefix: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(String, acyclic_fs::NativeAllocation)> {
        if !prefix.is_empty() {
            let mut observer = crate::store::history::native_work_io(work)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            validate_content_path_with_work(prefix, &mut Some(&mut observer))?;
        }
        if is_internal_path(prefix) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "internal storage paths cannot be delegated",
            });
        }
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let value = io.capability(&(self, prefix), "directory:read:")?;
        let retained = io.owned_bytes()?;
        let guard = io.into_native_custody(retained)?;
        Ok((value, guard))
    }

    /// Collision-resistant physical namespace key for Filesystem adapters.
    ///
    /// Including the owner prevents equal private paths and logical IDs from
    /// resolving to another agent's workspace.
    pub fn storage_name(&self) -> Result<String> {
        Ok(format!(
            "harness-{}",
            blake3::Hash::from_bytes(crate::contract::canonical_json_digest(self)?).to_hex()
        ))
    }

    /// Produces the same physical namespace on the original native ledger.
    /// The returned name grants no resource access.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn storage_name_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(String, acyclic_fs::NativeAllocation)> {
        crate::filesystem::native_volume_storage_name(self, work)
    }
}

/// Operations that can be granted independently on a volume.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeOperation {
    /// Resolve exact file bytes.
    Read,
    /// Create a new file version.
    Write,
}

impl VolumeOperation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
        }
    }
}

/// Verified, exact-volume authority for a single storage operation.
///
/// The private fields prevent callers from using an unverified scope as a grant.
#[derive(Clone, Debug)]
pub struct ContentGrant {
    volume: VolumeRef,
    operation: VolumeOperation,
    exact_file: Option<FileRef>,
    directory_prefix: Option<String>,
}

impl ContentGrant {
    /// Consumes only the kernel's sealed operation proof, never serialized
    /// project-source metadata or an opaque attestation used as a read scope.
    pub(crate) fn from_verified_project_source(
        proof: &crate::source_authorization::VerifiedProjectSourceRead<'_>,
    ) -> Self {
        Self {
            volume: proof.project().clone(),
            operation: VolumeOperation::Read,
            exact_file: None,
            directory_prefix: Some(String::new()),
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn from_verified_project_source_with_work(
        proof: &crate::source_authorization::VerifiedProjectSourceRead<'_>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        Ok(Self {
            volume: proof.project().clone_with_work(work)?,
            operation: VolumeOperation::Read,
            exact_file: None,
            directory_prefix: Some(String::new()),
        })
    }

    /// Resolves a pinned read from an authenticated whole-volume, exact-file,
    /// or segment-bounded private-directory grant. A reference itself carries
    /// no authority; the owning provider verifies this scope before reading.
    pub fn verify_read(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        file: &FileRef,
    ) -> Result<Self> {
        file.validate()?;
        if is_internal_path(file.path()) && !is_inherited_context_path(file.path()) {
            return Err(Error::Unauthorized(
                "internal storage file is not public content".into(),
            ));
        }
        verifier.verify(scope)?;
        if scope
            .capabilities()
            .contains(&file.volume().capability(VolumeOperation::Read)?)
        {
            return Self::verify(verifier, scope, file.volume(), VolumeOperation::Read);
        }
        if scope.capabilities().contains(&file.read_capability()?) {
            return Self::verify_file_read(verifier, scope, file);
        }
        if file.volume().class() == VolumeClass::AgentPrivate {
            let mut prefixes = vec![String::new()];
            let mut prefix = String::new();
            for segment in file
                .path()
                .split('/')
                .take(file.path().split('/').count().saturating_sub(1))
            {
                if !prefix.is_empty() {
                    prefix.push('/');
                }
                prefix.push_str(segment);
                prefixes.push(prefix.clone());
            }
            for prefix in prefixes {
                if file
                    .volume()
                    .directory_read_capability(&prefix)
                    .is_ok_and(|capability| scope.capabilities().contains(&capability))
                {
                    let grant =
                        Self::verify_directory_read(verifier, scope, file.volume(), &prefix)?;
                    grant.require_file_read(file)?;
                    return Ok(grant);
                }
            }
        }
        Err(Error::Unauthorized(
            "file read is not granted by its owner".into(),
        ))
    }

    /// Verifies the host-issued scope and its exact volume capability.
    pub fn verify(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        volume: &VolumeRef,
        operation: VolumeOperation,
    ) -> Result<Self> {
        Self::verify_with_work(verifier, scope, volume, operation, u64::MAX, &mut None)
    }

    pub(crate) fn verify_with_work(
        verifier: &AuthorityVerifier, scope: &Scope, volume: &VolumeRef,
        operation: VolumeOperation, maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        verifier.verify_with_work(scope, work)?;
        if operation == VolumeOperation::Write {
            volume.require_writer(scope.agent())?;
        }
        let digest = crate::contract::canonical_json_digest_with_work(volume, maximum_bytes, work)?;
        let hash = blake3::Hash::from_bytes(digest).to_hex();
        let prefix = match operation { VolumeOperation::Read => "volume:read:", VolumeOperation::Write => "volume:write:" };
        // The capability is a fixed-size ASCII label, not an unobserved format!
        // allocation before the original observer gets to admit its ownership.
        let mut label = [0_u8; 77];
        let length = prefix.len() + hash.len();
        label[..prefix.len()].copy_from_slice(prefix.as_bytes());
        label[prefix.len()..length].copy_from_slice(hash.as_bytes());
        let text = std::str::from_utf8(&label[..length])
            .map_err(|_| Error::Invalid("volume capability encoding failed".into()))?;
        let capability = if let Some(io) = work.as_deref_mut() {
            io.clone_text(text)?
        } else { text.to_owned() };
        let owned = capability.capacity() as u64;
        let allowed = scope.capabilities().contains(&capability);
        drop(capability);
        if let Some(io) = work.as_deref_mut() { io.release_owned(owned)?; }
        if !allowed { return Err(Error::Unauthorized("volume operation is not granted".into())); }
        #[cfg(feature = "filesystem")]
        if let Some(io) = work.as_deref_mut() {
            io.retain_owned(volume.logical_owned_bytes()?)?;
        }
        Ok(Self {
            volume: volume.clone(),
            operation,
            exact_file: None,
            directory_prefix: None,
        })
    }

    /// The same whole-volume grant, with its sole owned identity copy funded
    /// by the original preparation ledger before construction.
    #[cfg(feature = "filesystem")]
    pub(crate) fn verify_with_io(
        verifier: &AuthorityVerifier, scope: &Scope, volume: &VolumeRef,
        operation: VolumeOperation, io: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        Self::verify_original_volume_with_io(verifier, scope, volume, operation, io)?;
        Ok(Self {
            volume: volume.clone_with_io(io)?,
            operation,
            exact_file: None,
            directory_prefix: None,
        })
    }

    /// Verifies the original signed volume grant through the canonical observer.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn verify_with_native_work(
        verifier: &AuthorityVerifier, scope: &Scope, volume: &VolumeRef,
        operation: VolumeOperation, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let grant = Self::verify_with_io(verifier, scope, volume, operation, &io)?;
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok((grant, custody))
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = self.volume.logical_owned_bytes()?;
        if let Some(file) = &self.exact_file {
            bytes = bytes.checked_add(file.logical_owned_bytes()?)
                .ok_or_else(|| Error::Invalid("grant field ownership overflow".into()))?;
        }
        bytes.checked_add(self.directory_prefix.as_ref().map_or(0, |prefix| prefix.capacity() as u64))
            .ok_or_else(|| Error::Invalid("grant prefix ownership overflow".into()))
    }

    /// Checks the original signed whole-volume authority without cloning a
    /// grant or minting any new proof. The caller retains its original scope.
    #[cfg(feature = "filesystem")]
    pub fn verify_original_volume_measured(
        verifier: &AuthorityVerifier, scope: &Scope, volume: &VolumeRef,
        operation: VolumeOperation, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error> {
        let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = Self::verify_original_volume_with_io(verifier, scope, volume, operation, &io);
        match result {
            Ok(()) => Ok(acyclic_fs::OperationReceipt { value: (), work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn verify_original_volume_with_io(
        verifier: &AuthorityVerifier, scope: &Scope, volume: &VolumeRef,
        operation: VolumeOperation, io: &crate::filesystem::PreparationIo,
    ) -> Result<()> {
        let mut borrowed = io.history_io();
        verifier.verify_with_work(scope, &mut Some(&mut borrowed))?;
        let native;
        #[cfg(not(target_arch = "wasm32"))]
        { native = io.original_native_work().is_some(); }
        #[cfg(target_arch = "wasm32")]
        { native = false; }
        if operation == VolumeOperation::Write {
            volume.require_writer_inner(scope.agent(), native)?;
        }
        let prefix = match operation { VolumeOperation::Read => "volume:read:", VolumeOperation::Write => "volume:write:" };
        if !io.contains_capability(scope.capabilities(), volume, prefix)? {
            #[cfg(not(target_arch = "wasm32"))]
            if native {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "volume operation is not granted",
                });
            }
            return Err(Error::Unauthorized("volume operation is not granted".into()));
        }
        Ok(())
    }


    /// Verifies a signed private-directory or whole-volume read grant. A
    /// whole-volume reader may discover subdirectories lazily; a delegated
    /// directory reader remains segment-bounded. Neither grants writes.
    pub fn verify_directory_read(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        volume: &VolumeRef,
        prefix: &str,
    ) -> Result<Self> {
        verifier.verify(scope)?;
        let directory_capability = volume.directory_read_capability(prefix)?;
        let volume_capability = volume.capability(VolumeOperation::Read)?;
        if scope.agent().is_none()
            || (!scope.capabilities().contains(&directory_capability)
                && !scope.capabilities().contains(&volume_capability))
        {
            return Err(Error::Unauthorized(
                "private directory read is not granted".into(),
            ));
        }
        Ok(Self {
            volume: volume.clone(),
            operation: VolumeOperation::Read,
            exact_file: None,
            directory_prefix: Some(prefix.to_owned()),
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn verify_directory_read_with_native_work(
        verifier: &AuthorityVerifier, scope: &Scope, volume: &VolumeRef,
        prefix: &str, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let mut borrowed = io.history_io();
        verifier.verify_with_work(scope, &mut Some(&mut borrowed))?;
        if !prefix.is_empty() { validate_content_path_with_native_work(prefix, work)?; }
        if is_internal_path(prefix) {
            return Err(io.invalid("internal storage paths cannot be delegated"));
        }
        let directory = io.contains_capability(
            scope.capabilities(), &(volume, prefix), "directory:read:",
        )?;
        let entire_volume = io.contains_capability(
            scope.capabilities(), volume, "volume:read:",
        )?;
        if scope.agent().is_none() || (!directory && !entire_volume) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "private directory read is not granted",
            });
        }
        io.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let volume = volume.clone_with_io(&io)?;
        io.retain_owned(prefix.len() as u64)?;
        let directory_prefix = crate::resources::copy_metadata_string(prefix, &io)?;
        let grant = Self {
            volume, operation: VolumeOperation::Read,
            exact_file: None, directory_prefix: Some(directory_prefix),
        };
        let retained = io.owned_bytes()?;
        let custody = io.into_native_custody(retained)?;
        Ok((grant, custody))
    }

    /// Authorizes only one pinned file version, including across an owner
    /// boundary. The owner must explicitly issue this exact capability.
    pub fn verify_file_read(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        file: &FileRef,
    ) -> Result<Self> {
        file.validate()?;
        if is_internal_path(file.path()) && !is_inherited_context_path(file.path()) {
            return Err(Error::Unauthorized(
                "internal storage file is not public content".into(),
            ));
        }
        verifier.verify(scope)?;
        if !scope.capabilities().contains(&file.read_capability()?) {
            return Err(Error::Unauthorized("exact file read is not granted".into()));
        }
        Ok(Self {
            volume: file.volume().clone(),
            operation: VolumeOperation::Read,
            exact_file: Some(file.clone()),
            directory_prefix: None,
        })
    }

    /// Rejects a confused-deputy attempt to use the grant on a different volume.
    pub fn require(&self, volume: &VolumeRef, operation: VolumeOperation) -> Result<()> {
        #[cfg(feature = "filesystem")]
        { self.require_with_io(volume, operation, None) }
        #[cfg(not(feature = "filesystem"))]
        { self.require_native(volume, operation, false) }
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn require_with_io(
        &self, volume: &VolumeRef, operation: VolumeOperation,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io { io.check_cancellation()?; }
        self.require_native(volume, operation, io.is_some_and(crate::filesystem::PreparationIo::uses_native_work))
    }

    fn require_native(&self, volume: &VolumeRef, operation: VolumeOperation, native: bool) -> Result<()> {
        let _ = native;
        if self.volume != *volume
            || self.operation != operation
            || self.exact_file.is_some()
            || self.directory_prefix.is_some()
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if native {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "content grant does not match the volume",
                });
            }
            return Err(Error::Unauthorized(
                "content grant does not match the volume".into(),
            ));
        }
        Ok(())
    }

    /// Checks a read against either a whole-volume or exact-version grant.
    pub fn require_file_read(&self, file: &FileRef) -> Result<()> {
        #[cfg(feature = "filesystem")]
        { self.require_file_read_with_io(file, None) }
        #[cfg(not(feature = "filesystem"))]
        { self.require_file_read_native(file, false) }
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn require_file_read_with_io(
        &self, file: &FileRef, io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io { io.check_cancellation()?; }
        self.require_file_read_native(file, io.is_some_and(crate::filesystem::PreparationIo::uses_native_work))
    }

    fn require_file_read_native(&self, file: &FileRef, native: bool) -> Result<()> {
        let _ = native;
        if self.volume != *file.volume()
            || self.operation != VolumeOperation::Read
            || self.exact_file.as_ref().is_some_and(|exact| exact != file)
            || self.directory_prefix.as_deref().is_some_and(|prefix| {
                is_internal_path(file.path()) || !path_below(file.path(), prefix)
            })
        {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if native {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "content grant does not match the file",
                });
            }
            return Err(Error::Unauthorized(
                "content grant does not match the file".into(),
            ));
        }
        Ok(())
    }

    /// Checks any lazy directory path against the owner-issued read boundary.
    pub fn require_directory_path(&self, volume: &VolumeRef, prefix: &str) -> Result<()> {
        if !self.directory_path_allowed(volume, prefix) {
            return Err(Error::Unauthorized(
                "directory listing is not granted".into(),
            ));
        }
        Ok(())
    }

    fn directory_path_allowed(&self, volume: &VolumeRef, prefix: &str) -> bool {
        self.volume == *volume
            && self.operation == VolumeOperation::Read
            && !is_internal_path(prefix)
            && self.directory_prefix.as_deref()
                .is_some_and(|granted| path_below(prefix, granted))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn require_directory_path_with_native_work(
        &self, volume: &VolumeRef, path: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        if !self.directory_path_allowed(volume, path) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "directory listing is not granted",
            });
        }
        Ok(())
    }
}

pub(crate) fn is_internal_path(path: &str) -> bool {
    path == ".system" || path.starts_with(".system/")
}

pub(crate) fn is_inherited_context_path(path: &str) -> bool {
    path.starts_with(".system/inherited-conversation/")
}

fn path_below(path: &str, prefix: &str) -> bool {
    prefix.is_empty()
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|tail| tail.starts_with('/'))
}

/// Immutable byte descriptor checked before a file is admitted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct FileDescriptor {
    sha256: [u8; 32],
    #[schemars(range(max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    byte_length: u64,
    media_type: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileDescriptorWire {
    sha256: Vec<u8>,
    byte_length: u64,
    media_type: String,
}

impl<'de> Deserialize<'de> for FileDescriptor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = FileDescriptorWire::deserialize(deserializer)?;
        let digest = wire.sha256.try_into().map_err(|_| {
            serde::de::Error::custom("SHA-256 digest must contain exactly 32 bytes")
        })?;
        Self::new(digest, wire.byte_length, wire.media_type).map_err(serde::de::Error::custom)
    }
}

impl FileDescriptor {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.media_type.capacity() as u64
    }

    /// Constructs a descriptor after validating its media type.
    pub fn new(sha256: [u8; 32], byte_length: u64, media_type: impl Into<String>) -> Result<Self> {
        let value = Self {
            sha256,
            byte_length,
            media_type: media_type.into(),
        };
        value.validate()?;
        Ok(value)
    }

    /// Computes a descriptor for bytes staged by a content provider.
    pub fn from_bytes(bytes: &[u8], media_type: impl Into<String>) -> Result<Self> {
        Self::new(Sha256::digest(bytes).into(), bytes.len() as u64, media_type)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn from_bytes_with_io(
        bytes: &[u8], media_type: &str, io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Self> {
        let Some(io) = io else { return Self::from_bytes(bytes, media_type); };
        let inspection = acyclic_fs::WorkCounters {
            items_examined: media_type.len() as u64 + 1, ..Default::default()
        };
        io.preflight(inspection)?;
        let reason = Self::validation_fields(bytes.len() as u64, media_type);
        io.charge(inspection)?;
        if let Some(reason) = reason { return Err(io.invalid(reason)); }
        let hashing = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
        io.preflight(hashing)?;
        let sha256 = Sha256::digest(bytes).into();
        io.charge(hashing)?;
        let mut observer = io.history_io();
        let work: &mut dyn crate::store::history::HistoryIo = &mut observer;
        let media_type = work.clone_text(media_type)?;
        Ok(Self { sha256, byte_length: bytes.len() as u64, media_type })
    }

    /// Verifies both digest and exact length, including empty files.
    pub fn verify(&self, bytes: &[u8]) -> Result<()> {
        self.verify_inner(bytes, &mut || Ok(()), false)
    }

    /// Verifies through the existing borrowed cumulative history observer.
    pub(crate) fn verify_with_work(
        &self, bytes: &[u8], work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native = false;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = work.as_deref().and_then(|observer| observer.original_native_work()).is_some();
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        self.verify_inner(bytes, &mut || {
            if let Some(io) = work.as_deref_mut() { io.hashed_bytes(bytes.len() as u64)?; }
            Ok(())
        }, native)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn verify_with_io(
        &self, bytes: &[u8], io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let mut observer = io.map(crate::executor::PreparationIo::history_io);
        let mut work = observer.as_mut().map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        self.verify_with_work(bytes, &mut work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    /// Verifies the same descriptor with the original native ledger.
    pub fn verify_with_native_work(
        &self, bytes: &[u8], work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.admit(acyclic_fs::WorkCounters::default())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.verify_inner(bytes, &mut || {
            let next = acyclic_fs::WorkCounters { bytes_hashed: bytes.len() as u64, ..Default::default() };
            work.admit(next).map_err(|cause| Error::NativeWork { cause, custody: None })?;
            work.charge(next).map_err(|cause| Error::NativeWork { cause, custody: None })
        }, true)
    }

    fn verify_inner(
        &self, bytes: &[u8], before_hash: &mut impl FnMut() -> Result<()>, native: bool,
    ) -> Result<()> {
        let matches = if bytes.len() as u64 != self.byte_length {
            false
        } else {
            before_hash()?;
            Sha256::digest(bytes).as_slice() == self.sha256
        };
        if !matches {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if native {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "file content does not match its descriptor" });
            }
            let _ = native;
            return Err(Error::Invalid("file content does not match its descriptor".into()));
        }
        Ok(())
    }


    /// Checks metadata without fetching bytes.
    pub fn validate(&self) -> Result<()> {
        self.validation_reason().map_or(Ok(()), |reason| Err(Error::Invalid(reason.into())))
    }
    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        self.validation_reason().map_or(Ok(()),
            |reason| Err(conversation_boundary(work, reason)))
    }


    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        inspect_native_metadata(work, self.media_type.len())?;
        self.validation_reason().map_or(Ok(()), |reason| Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason,
        }))
    }

    fn validation_reason(&self) -> Option<&'static str> {
        Self::validation_fields(self.byte_length, &self.media_type)
    }

    fn validation_fields(byte_length: u64, media_type: &str) -> Option<&'static str> {
        if byte_length > MAX_EXACT_JS_INTEGER {
            return Some("file byte length exceeds cross-language precision");
        }
        let Some((kind, subtype)) = media_type.split_once('/') else {
            return Some("media type is invalid");
        };
        if kind.is_empty()
            || subtype.is_empty()
            || media_type.chars().any(|character| {
                !character.is_ascii_alphanumeric()
                    && !matches!(character, '/' | '-' | '+' | '.' | '_')
            })
        {
            return Some("media type is invalid");
        }
        None
    }

    /// SHA-256 digest of the exact bytes.
    #[must_use]
    pub const fn sha256(&self) -> &[u8; 32] {
        &self.sha256
    }

    /// Exact file length.
    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }

    /// Declared MIME type.
    #[must_use]
    pub fn media_type(&self) -> &str {
        &self.media_type
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn inspect_native_metadata(
    work: &acyclic_fs::NativeMaterializationWork, bytes: usize,
) -> Result<()> {
    let items_examined = (bytes as u64).checked_add(1).ok_or(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid, reason: "metadata inspection count overflow",
    })?;
    let next = acyclic_fs::WorkCounters { items_examined, ..Default::default() };
    work.admit(next).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    work.charge(next).map_err(|cause| Error::NativeWork { cause, custody: None })
}

/// An immutable version of a file within a scoped volume.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct FileRef {
    volume: VolumeRef,
    path: String,
    version: String,
    descriptor: FileDescriptor,
    display_name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileRefWire {
    volume: VolumeRef,
    path: String,
    version: String,
    descriptor: FileDescriptor,
    display_name: String,
}

impl<'de> Deserialize<'de> for FileRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = FileRefWire::deserialize(deserializer)?;
        Self::new(
            wire.volume,
            wire.path,
            wire.version,
            wire.descriptor,
            wire.display_name,
        )
        .map_err(serde::de::Error::custom)
    }
}

impl FileRef {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        [self.path.capacity(), self.version.capacity(), self.display_name.capacity()]
            .into_iter().try_fold(self.volume.logical_owned_bytes()?, |bytes, capacity|
                bytes.checked_add(capacity as u64).ok_or_else(|| Error::Invalid("file identity ownership overflow".into())))?
            .checked_add(self.descriptor.logical_owned_bytes())
            .ok_or_else(|| Error::Invalid("file descriptor ownership overflow".into()))
    }

    /// Copies the audited identity graph directly, without a serialization clone.
    /// The returned metadata is deep-only; its enclosing DTO owns its root slot.
    #[cfg(feature = "filesystem")]
    pub(crate) fn clone_with_io(
        &self, io: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        let mut observer = io.history_io();
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
        self.clone_with_work(&mut work)
    }

    /// Variable byte content copied by the canonical closed-DTO clone.
    /// Retained custody still uses `logical_owned_bytes` after cloning.
    pub(crate) fn logical_clone_owned_bytes(&self) -> Result<u64> {
        let owner = match &self.volume.owner {
            VolumeOwner::Project(owner) | VolumeOwner::Session(owner) => owner.len() as u64,
            VolumeOwner::Agent(_) => 0,
        };
        let provider = &self.volume.provider;
        [provider.namespace().len(), provider.family().len(), provider.version().len(),
            self.volume.id.len(), self.path.len(), self.version.len(), self.display_name.len(),
            self.descriptor.media_type.len()]
            .into_iter().try_fold(owner, |bytes, length| bytes.checked_add(length as u64)
                .ok_or_else(|| Error::Invalid("file clone ownership overflow".into())))
    }

    /// Creates a reference after checking paths, version and descriptor.
    pub fn new(
        volume: VolumeRef,
        path: impl Into<String>,
        version: impl Into<String>,
        descriptor: FileDescriptor,
        display_name: impl Into<String>,
    ) -> Result<Self> {
        let value = Self {
            volume,
            path: path.into(),
            version: version.into(),
            descriptor,
            display_name: display_name.into(),
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn from_owned_value_with_work(
        value: serde_json::Value,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
            return native_file_from_owned_value(value, work);
        }
        let file: Self = serde_json::from_value(value)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        file.validate_with_work(work)?;
        Ok(file)
    }

    /// Creates the same reference from borrowed metadata, using the canonical
    /// guarded field-copy engine when a finite observer is present.
    #[cfg(feature = "filesystem")]
    pub(crate) fn new_with_io(
        volume: &VolumeRef, path: &str, version: &str, descriptor: &FileDescriptor,
        display_name: &str, io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Self> {
        let Some(io) = io else {
            return Self::new(volume.clone(), path, version, descriptor.clone(), display_name);
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            volume.validate_with_native_work(work)?;
            validate_content_path_with_native_work(path, work)?;
            for label in [version, display_name] {
                inspect_native_metadata(work, label.len())?;
                validate_label_native(label, MAX_LABEL_BYTES)?;
            }
            descriptor.validate_with_native_work(work)?;
        } else {
            volume.validate()?;
            validate_content_path(path)?;
            validate_label(version, MAX_LABEL_BYTES)?;
            validate_label(display_name, MAX_LABEL_BYTES)?;
            descriptor.validate()?;
        }
        #[cfg(target_arch = "wasm32")]
        {
            volume.validate()?;
            validate_content_path(path)?;
            validate_label(version, MAX_LABEL_BYTES)?;
            validate_label(display_name, MAX_LABEL_BYTES)?;
            descriptor.validate()?;
        }
        let mut observer = io.history_io();
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut observer);
        Self::copy_fields_with_work(volume, path, version, descriptor, display_name, &mut work)
    }

    /// Validates a ref decoded at any external boundary.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.volume.validate_with_work(work)?;
        validate_content_path_with_work(&self.path, work)?;
        validate_label_with_work(&self.version, MAX_LABEL_BYTES, work)?;
        validate_label_with_work(&self.display_name, MAX_LABEL_BYTES, work)?;
        self.descriptor.validate_with_work(work)
    }


    /// Checks the same immutable metadata using the caller's original ledger.
    /// Validation does not grant authority to read the referenced bytes.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        inspect_native_metadata(work, 0)?;
        self.volume.validate_with_native_work(work)?;
        validate_content_path_with_native_work(&self.path, work)?;
        for label in [&self.version, &self.display_name] {
            inspect_native_metadata(work, label.len())?;
            validate_label_native(label, MAX_LABEL_BYTES)?;
        }
        self.descriptor.validate_with_native_work(work)
    }

    /// Owning volume.
    #[must_use]
    pub const fn volume(&self) -> &VolumeRef {
        &self.volume
    }

    /// Normalized volume-relative path.
    #[must_use]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Immutable provider-owned revision.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Integrity and rendering metadata.
    #[must_use]
    pub const fn descriptor(&self) -> &FileDescriptor {
        &self.descriptor
    }

    /// Human-facing file name.
    #[must_use]
    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// Owner-issued capability for reading precisely this immutable version.
    pub fn read_capability(&self) -> Result<String> {
        self.validate()?;
        Ok(format!(
            "file:read:{}",
            blake3::Hash::from_bytes(crate::contract::canonical_json_digest(self)?).to_hex()
        ))
    }
}

/// Primary message content uses exactly the same file contract as attachments.
pub type ContentRef = FileRef;

/// Durable terminal task outcome. Successful bodies stay in owner-controlled
/// files; transport and scheduler records carry only the pinned reference.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "keep the public outcome descriptor by value"
)]
pub enum TaskOutcomeRecord {
    /// Schema-checked successful result artifact.
    Succeeded {
        /// Pinned owner-controlled result artifact.
        result: FileRef,
    },
    /// Stable non-secret failure description.
    Failed {
        /// Bounded, non-secret failure description.
        message: String,
    },
    /// Cancellation was acknowledged.
    Cancelled,
    /// Provider must reconcile this exact operation.
    Indeterminate {
        /// Operation requiring owner-mediated reconciliation.
        operation_id: crate::OperationId,
    },
}

impl TaskOutcomeRecord {
    /// Validates bounded metadata and all carried references.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Succeeded { result } => result.validate(),
            Self::Failed { message } if message.is_empty() => {
                Err(Error::Invalid("task failure description is invalid".into()))
            }
            _ => Ok(()),
        }
    }
}

/// Admission barrier for every version-pinned file in a conversation message.
///
/// Implementations must verify exact byte residency and access before Stream
/// publishes references. The provider retains admitted versions independently.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum PrivateDirectoryEntryKind {
    /// A named regular file; resolve its current immutable ref by path.
    File,
    /// A named child directory.
    Directory,
}

/// One lazily discovered name, without eager byte or ref transfer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct PrivateDirectoryEntry {
    /// One normalized child name relative to the listed directory.
    pub name: String,
    /// Whether the name resolves to a regular file or another directory.
    pub kind: PrivateDirectoryEntryKind,
}

/// A bounded page from one exact owner-private generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct PrivateDirectoryPage {
    /// Exact immutable generation from which this page was read.
    #[cfg_attr(
        feature = "wasm",
        tsify(type = "WasmResourceRefWire & { kind: 'generation' }")
    )]
    pub generation: GenerationRef,
    /// Ordered entries returned for the requested directory segment.
    pub entries: Vec<PrivateDirectoryEntry>,
    /// Whether another page may be requested with the returned cursor.
    pub has_more: bool,
}

/// Maximum number of entries admitted in one private-directory page.
pub const MAX_PRIVATE_DIRECTORY_PAGE: usize = u32::MAX as usize;

impl PrivateDirectoryPage {
    /// Rejects malformed, duplicate, or out-of-order names independently of
    /// the concrete Filesystem page implementation.
    pub fn validate(&self) -> Result<()> {
        self.generation.validate()?;
        if self.entries.len() as u64 > MAX_PRIVATE_DIRECTORY_PAGE as u64 {
            return Err(Error::Invalid(
                "private directory page exceeds protocol limit".into(),
            ));
        }
        let mut previous: Option<&[u8]> = None;
        for entry in &self.entries {
            validate_content_path(&entry.name)?;
            if entry.name.contains('/')
                || previous.is_some_and(|name| entry.name.as_bytes() <= name)
            {
                return Err(Error::Invalid(
                    "private directory page names are not ordered".into(),
                ));
            }
            previous = Some(entry.name.as_bytes());
        }
        Ok(())
    }
}

/// Provider boundary that authenticates and reads immutable content refs.
pub trait ContentResidencyVerifier: acyclic_stream::ProviderPlatform {
    /// Reads an authenticated bounded prefix without fetching the remaining body.
    /// The supplied generation pins subsequent ordinary path reads. Providers
    /// must authenticate the range and return at most the supplied byte bound.
    fn read_private_prefix<'a>(
        &'a self,
        _volume: &'a VolumeRef,
        _granted_prefix: &'a str,
        _path: &'a str,
        _generation: &'a GenerationRef,
        _maximum_bytes: u64,
    ) -> ContentFuture<'a, Result<Vec<u8>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "bounded prefix reads are unavailable".into(),
            ))
        })
    }

    /// Resolves and checks the exact referenced file version.
    fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>>;

    /// Reads exact verified bytes for schema validation at an admission boundary.
    fn read<'a>(&'a self, _reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "content byte resolution is unavailable".into(),
            ))
        })
    }

    /// Capability availability only, never an authorization decision. Allows
    /// a multi-operation finite publisher to reject missing readers before IO.
    #[cfg(feature = "filesystem")]
    fn supports_measured_read(
        &self, _volume: &VolumeRef, _budget: acyclic_fs::WorkBudget,
        _cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<bool>, Error> {
        Err(acyclic_fs::OperationFailure::before_work(Error::Unsupported(
            "finite content reader capability is unavailable".into())))
    }

    /// Optional finite-work read. Unsupported providers reject before any IO.
    #[cfg(feature = "filesystem")]
    fn read_measured<'a>(
        &'a self, _reference: &'a FileRef, _maximum_bytes: u64,
        _budget: acyclic_fs::WorkBudget, _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> ContentFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<u8>>, Error>> {
        Box::pin(async {
            Err(acyclic_fs::OperationFailure::before_work(Error::Unsupported(
                "finite content resolution is unavailable".into(),
            )))
        })
    }

    /// Reads on the caller's original native ledger and transfers live custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn read_with_native_work<'a>(
        &'a self, _reference: &'a FileRef, _maximum_bytes: u64,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, (Vec<u8>, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "original native-ledger content resolution is unavailable" })
    }

    /// Availability only; it conveys no read authority.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_read(&self, _volume: &VolumeRef, _work: &acyclic_fs::NativeMaterializationWork) -> crate::Result<bool> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "original native-ledger content reader is unavailable" })
    }

    /// Verifies through the actual native engine without creating a new meter.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self, _reference: &'a FileRef, _maximum_bytes: u64,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, ()>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "original native-ledger content verification is unavailable" })
    }

    /// Resolves the complete canonical pinned manifest on the original ledger.
    /// Custom readers without that capability reject before constructing a
    /// future or invoking their ordinary provider implementation.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn load_manifest_with_native_work<'a>(
        &'a self, reference: &'a FileRef, item_count: u32,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, (Vec<Attachment>, acyclic_fs::NativeAllocation)>> {
        if !self.supports_native_read(reference.volume(), work)? {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
                reason: "original native-ledger manifest reader is unavailable" });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let input = self.read_with_native_work(
                reference, reference.descriptor().byte_length(), work,
            )?.await?;
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            reference.descriptor().verify_with_io(&input.0, Some(&io))?;
            let items = decode_manifest_bounded(reference, &input.0, item_count as usize, &io)?;
            let retained = io.owned_bytes()?;
            let output = (items, io.into_native_custody(retained)?);
            if output.0.len() != item_count as usize {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "attachment manifest count is incomplete" });
            }
            Ok(output)
        })
    }

    /// Verifies bytes through the provider's actual finite read capability.
    #[cfg(feature = "filesystem")]
    fn verify_measured<'a>(
        &'a self, reference: &'a FileRef, maximum_bytes: u64,
        budget: acyclic_fs::WorkBudget, cancellation: &'a acyclic_fs::CancellationToken,
    ) -> ContentFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = async {
                if reference.descriptor().byte_length() > maximum_bytes {
                    return Err(Error::Invalid("content exceeds selected byte allowance".into()));
                }
                let bytes = io.receive_content(self.read_measured(reference, maximum_bytes,
                    io.remaining()?, cancellation).await)?;
                io.retain_owned(bytes.capacity() as u64)?;
                reference.descriptor().verify_with_io(&bytes, Some(&io))
            }.await;
            match result {
                Ok(()) => Ok(acyclic_fs::OperationReceipt { value: (), work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    /// Decodes a complete canonical manifest under a selected member allowance.
    #[cfg(feature = "filesystem")]
    fn load_complete_manifest_measured<'a>(
        &'a self, reference: &'a FileRef, maximum_members: usize,
        budget: acyclic_fs::WorkBudget, cancellation: &'a acyclic_fs::CancellationToken,
    ) -> ContentFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<Attachment>>, Error>> {
        Box::pin(async move {
            let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = async {
                let bytes = io.receive_content(self.read_measured(reference,
                    reference.descriptor().byte_length(), io.remaining()?, cancellation).await)?;
                io.retain_owned(bytes.capacity() as u64)?;
                reference.descriptor().verify_with_io(&bytes, Some(&io))?;
                decode_manifest_bounded(reference, &bytes, maximum_members, &io)
            }.await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    /// Preserves the exact declared count while resolving bounded manifest bytes.
    #[cfg(feature = "filesystem")]
    fn load_manifest_measured<'a>(
        &'a self, reference: &'a FileRef, item_count: u32,
        budget: acyclic_fs::WorkBudget, cancellation: &'a acyclic_fs::CancellationToken,
    ) -> ContentFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<Attachment>>, Error>> {
        Box::pin(async move {
            let receipt = self.load_complete_manifest_measured(reference, item_count as usize, budget, cancellation).await?;
            if receipt.value.len() != item_count as usize {
                return Err(acyclic_fs::OperationFailure::new(Error::Invalid(
                    "attachment manifest count is incomplete".into()), receipt.work));
            }
            Ok(receipt)
        })
    }

    /// Resolves the canonical list and verifies every member with the same
    /// effective policy, using only genuine finite provider capabilities.
    #[cfg(feature = "filesystem")]
    fn verify_manifest_measured<'a>(
        &'a self, reference: &'a FileRef, item_count: u32, limits: &'a Limits,
        budget: acyclic_fs::WorkBudget, cancellation: &'a acyclic_fs::CancellationToken,
    ) -> ContentFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>> {
        Box::pin(async move {
            let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = async {
                limits.validate_file(reference)?;
                if item_count as usize > limits.attachments {
                    return Err(Error::Invalid("attachment manifest count exceeds limits".into()));
                }
                let items = io.receive_content(self.load_manifest_measured(reference, item_count, io.remaining()?, cancellation).await)?;
                io.retain_owned(manifest_owned_bytes(&items, items.capacity())?)?;
                for item in &items {
                    let work = acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() };
                    io.preflight(work)?;
                    io.charge(work)?;
                    limits.validate_file(&item.file)?;
                    io.receive_content(self.verify_measured(&item.file, limits.file_bytes, io.remaining()?, cancellation).await)?;
                }
                Ok(())
            }.await;
            match result {
                Ok(()) => Ok(acyclic_fs::OperationReceipt { value: (), work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    /// Lazily discovers an owner-private directory under an authenticated
    /// subtree boundary; the generation must remain stable across pages.
    fn list_private_directory<'a>(
        &'a self,
        _volume: &'a VolumeRef,
        _granted_prefix: &'a str,
        _path: &'a str,
        _expected_generation: Option<&'a GenerationRef>,
        _after: Option<&'a str>,
        _maximum_entries: u32,
    ) -> ContentFuture<'a, Result<PrivateDirectoryPage>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "private directory discovery is unavailable".into(),
            ))
        })
    }

    /// Resolves the current named private file to verified immutable bytes.
    fn read_private_path<'a>(
        &'a self,
        _volume: &'a VolumeRef,
        _granted_prefix: &'a str,
        _path: &'a str,
        _expected_generation: Option<&'a GenerationRef>,
    ) -> ContentFuture<'a, Result<(FileRef, Vec<u8>)>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "private path resolution is unavailable".into(),
            ))
        })
    }

    /// Resolves a complete referenced list and enforces configured limits on every member.
    fn verify_manifest<'a>(
        &'a self,
        _reference: &'a FileRef,
        _item_count: u32,
        _limits: &'a Limits,
    ) -> ContentFuture<'a, Result<()>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "attachment manifest verification is unavailable".into(),
            ))
        })
    }

    /// Returns the canonical complete manifest from its owning provider.
    fn load_manifest<'a>(
        &'a self,
        _reference: &'a FileRef,
        _item_count: u32,
    ) -> ContentFuture<'a, Result<Vec<Attachment>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "attachment manifest resolution is unavailable".into(),
            ))
        })
    }
}

/// Owner-bound publication. Implementations retain the staged immutable version
/// before returning its ref and authenticate the original writer internally.
/// The runtime checks the returned ref and digest again before using it.
pub trait ContentPublisher: acyclic_stream::ProviderPlatform {
    /// Exact writable volume represented by this bound provider handle.
    fn volume(&self) -> &VolumeRef;

    /// Stages immutable bytes under one stable operation identity.
    fn stage<'a>(
        &'a self,
        operation_id: OperationId,
        path: &'a str,
        bytes: &'a [u8],
        media_type: &'a str,
        display_name: &'a str,
    ) -> BoxFuture<'a, Result<FileRef>>;
    /// Publishes one complete file contract per operation, including its path.
    /// Unsupported publishers reject without invoking ordinary staging.
    fn stage_once<'a>(
        &'a self,
        _operation_id: OperationId,
        _path: &'a str,
        _bytes: &'a [u8],
        _media_type: &'a str,
        _display_name: &'a str,
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "operation-bound file publication is unavailable".into(),
            ))
        })
    }

    /// Observes only this writer's original once-publication. Knowing its
    /// reference grants no read authority; failed lookups never mean absence.
    fn observe_staged_once<'a>(
        &'a self, _operation_id: OperationId, _maximum_bytes: u64,
    ) -> BoxFuture<'a, Result<Option<FileRef>>> {
        Box::pin(async { Err(Error::Unsupported("original publication observation is unavailable".into())) })
    }

    /// Optional genuine finite once-publication; never calls unmeasured stage.
    #[cfg(feature = "filesystem")]
    #[allow(clippy::too_many_arguments, reason = "complete once-publication identity and finite caller budget")]
    fn stage_once_measured<'a>(
        &'a self, _operation_id: OperationId, _path: &'a str, _bytes: &'a [u8],
        _media_type: &'a str, _display_name: &'a str,
        _budget: acyclic_fs::WorkBudget, _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> ContentFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<FileRef>, Error>> {
        Box::pin(async { Err(acyclic_fs::OperationFailure::before_work(
            Error::Unsupported("finite once-publication is unavailable".into()))) })
    }

    /// Rejects unavailable original-ledger publication before a caller starts
    /// process capture or another external side effect. It grants no authority.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_stage(&self, _work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "original native-ledger publication is unavailable" })
    }

    /// Publishes once on the original native ledger; the caller retains custody.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[allow(clippy::too_many_arguments, reason = "exact once-publication identity and original native ledger")]
    fn stage_once_with_native_work<'a>(
        &'a self, _operation_id: OperationId, _path: &'a str, _bytes: &'a [u8],
        _media_type: &'a str, _display_name: &'a str,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "original native-ledger once-publication is unavailable" })
    }

    /// Replaces only the original source generation on the caller's native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn stage_at_with_native_work<'a>(
        &'a self, _operation_id: OperationId, _source: &'a FileRef,
        _bytes: &'a [u8], _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "original native-ledger replacement is unavailable" })
    }

    /// Observes the original publication without substituting a mutable head.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn observe_staged_once_with_native_work<'a>(
        &'a self, _operation_id: OperationId, _maximum_bytes: u64,
        _work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, (Option<FileRef>, acyclic_fs::NativeAllocation)>> {
        Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unsupported,
            reason: "original native-ledger publication observation is unavailable" })
    }

    /// Resolves the original operation, metadata and generation without head.
    #[cfg(feature = "filesystem")]
    fn observe_staged_once_measured<'a>(
        &'a self, _operation_id: OperationId, _maximum_bytes: u64,
        _budget: acyclic_fs::WorkBudget, _cancellation: &'a acyclic_fs::CancellationToken,
    ) -> ContentFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Option<FileRef>>, Error>> {
        Box::pin(async { Err(acyclic_fs::OperationFailure::before_work(
            Error::Unsupported("finite publication observation is unavailable".into()))) })
    }

    /// Publishes replacement bytes only at the immutable source generation.
    /// Unsupported publishers must reject rather than fall back to `stage`.
    fn stage_at<'a>(
        &'a self,
        _operation_id: OperationId,
        _source: &'a FileRef,
        _bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "generation-checked publication is unavailable".into(),
            ))
        })
    }
}

/// Resolves an owner-mediated reader when a volume has not been explicitly
/// bound. Implementations authenticate the caller's current read grant at
/// resolution time; knowing a volume or file ref is not authorization.
pub trait ContentMountResolver: acyclic_stream::ProviderPlatform {
    /// Lazily opens the owner for one exact ref and returns its independently
    /// verified caller grant. Each call must reauthenticate the current scope.
    fn mount<'a>(&'a self, reference: &'a FileRef) -> ContentFuture<'a, Result<ContentMount>>;

    /// Lazily opens an unseen owner's directory without requiring a `FileRef`.
    fn mount_directory<'a>(
        &'a self,
        _volume: &'a VolumeRef,
        _granted_prefix: &'a str,
        _path: &'a str,
    ) -> ContentFuture<'a, Result<ContentMount>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "private directory mount is unavailable".into(),
            ))
        })
    }
}

/// Routes admission reads by exact owner volume, including several agents on
/// one provider and members stored separately from their manifests. An
/// optional mount resolves previously unseen volumes lazily under fresh grants.
pub struct CompositeContentVerifier {
    volumes: BTreeMap<String, Arc<dyn ContentResidencyVerifier>>,
    mount: Option<Arc<dyn ContentMountResolver>>,
}

impl CompositeContentVerifier {
    #[cfg(feature = "filesystem")]
    fn finite_bound_reader(&self, volume: &VolumeRef, io: &crate::filesystem::PreparationIo) -> Result<&dyn ContentResidencyVerifier> {
        let key_hold;
        let key = io.encode_json(volume, io.remaining()?.bytes_encoded, false)?;
        key_hold = io.guard_owned(key.len() as u64)?;
        volume.validate()?;
        let text = std::str::from_utf8(&key).map_err(|error| Error::Invalid(error.to_string()))?;
        let reader = self.volumes.get(text).ok_or_else(|| Error::Unsupported(
            "finite content routing requires an explicitly bound owner; lazy mount lacks finite IO".into()))?;
        drop(key);
        drop(key_hold);
        Ok(reader.as_ref())
    }

    /// Rejects duplicate exact-volume bindings instead of silently replacing one.
    pub fn new(bindings: Vec<(VolumeRef, Arc<dyn ContentResidencyVerifier>)>) -> Result<Self> {
        let mut volumes = BTreeMap::new();
        for (volume, resolver) in bindings {
            volume.validate()?;
            let key = Self::key(&volume)?;
            if volumes.insert(key, resolver).is_some() {
                return Err(Error::Invalid("content volume is registered twice".into()));
            }
        }
        Ok(Self {
            volumes,
            mount: None,
        })
    }

    /// Adds lazy owner routing without eagerly enumerating agent directories.
    #[must_use]
    pub fn with_mount_resolver(mut self, mount: Arc<dyn ContentMountResolver>) -> Self {
        self.mount = Some(mount);
        self
    }

    fn key(volume: &VolumeRef) -> Result<String> {
        volume.validate()?;
        serde_json::to_string(volume).map_err(|error| Error::Invalid(error.to_string()))
    }

    async fn owner(&self, reference: &FileRef) -> Result<Arc<dyn ContentResidencyVerifier>> {
        reference.validate()?;
        if let Some(reader) = self.volumes.get(&Self::key(reference.volume())?) {
            return Ok(reader.clone());
        }
        let (grant, reader) = self
            .mount
            .as_ref()
            .ok_or_else(|| Error::Unsupported("content volume is not registered".into()))?
            .mount(reference)
            .await?;
        grant.require_file_read(reference)?;
        Ok(reader)
    }

    async fn directory_owner(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
    ) -> Result<Arc<dyn ContentResidencyVerifier>> {
        if let Some(reader) = self.volumes.get(&Self::key(volume)?) {
            return Ok(reader.clone());
        }
        let (grant, reader) = self
            .mount
            .as_ref()
            .ok_or_else(|| Error::Unsupported("content volume is not registered".into()))?
            .mount_directory(volume, granted_prefix, path)
            .await?;
        grant.require_directory_path(volume, path)?;
        Ok(reader)
    }
}

impl ContentResidencyVerifier for CompositeContentVerifier {
    #[cfg(feature = "filesystem")]
    fn supports_measured_read(
        &self, volume: &VolumeRef, budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<bool>, Error> {
        let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
            .map_err(acyclic_fs::OperationFailure::before_work)?;
        let result = (|| {
            let reader = self.finite_bound_reader(volume, &io)?;
            io.receive_content(reader.supports_measured_read(volume, io.remaining()?, cancellation))
        })();
        match result {
            Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
            Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
        }
    }

    #[cfg(feature = "filesystem")]
    fn read_measured<'a>(
        &'a self, reference: &'a FileRef, maximum_bytes: u64,
        budget: acyclic_fs::WorkBudget, cancellation: &'a acyclic_fs::CancellationToken,
    ) -> ContentFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<Vec<u8>>, Error>> {
        Box::pin(async move {
            let io = crate::filesystem::PreparationIo::from_budget(budget, cancellation)
                .map_err(acyclic_fs::OperationFailure::before_work)?;
            let result = async {
                reference.validate()?;
                let reader = self.finite_bound_reader(reference.volume(), &io)?;
                io.receive_content(reader.read_measured(reference, maximum_bytes, io.remaining()?, cancellation).await)
            }.await;
            match result {
                Ok(value) => Ok(acyclic_fs::OperationReceipt { value, work: io.receipt() }),
                Err(error) => Err(acyclic_fs::OperationFailure::new(error, io.receipt())),
            }
        })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn read_with_native_work<'a>(
        &'a self, reference: &'a FileRef, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, (Vec<u8>, acyclic_fs::NativeAllocation)>> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let reader = self.finite_bound_reader(reference.volume(), &io)?;
        reader.read_with_native_work(reference, maximum_bytes, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn supports_native_read(&self, volume: &VolumeRef, work: &acyclic_fs::NativeMaterializationWork) -> crate::Result<bool> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let reader = self.finite_bound_reader(volume, &io)?;
        reader.supports_native_read(volume, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn verify_with_native_work<'a>(
        &'a self, reference: &'a FileRef, maximum_bytes: u64,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, ()>> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let reader = self.finite_bound_reader(reference.volume(), &io)?;
        reader.verify_with_native_work(reference, maximum_bytes, work)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn load_manifest_with_native_work<'a>(
        &'a self, reference: &'a FileRef, item_count: u32,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<crate::NativeFutureBundle<'a, (Vec<Attachment>, acyclic_fs::NativeAllocation)>> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        self.finite_bound_reader(reference.volume(), &io)?
            .load_manifest_with_native_work(reference, item_count, work)
    }

    fn read_private_prefix<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        generation: &'a GenerationRef,
        maximum_bytes: u64,
    ) -> ContentFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.directory_owner(volume, granted_prefix, path)
                .await?
                .read_private_prefix(volume, granted_prefix, path, generation, maximum_bytes)
                .await
        })
    }

    fn list_private_directory<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
        after: Option<&'a str>,
        maximum_entries: u32,
    ) -> BoxFuture<'a, Result<PrivateDirectoryPage>> {
        Box::pin(async move {
            self.directory_owner(volume, granted_prefix, path)
                .await?
                .list_private_directory(
                    volume,
                    granted_prefix,
                    path,
                    expected_generation,
                    after,
                    maximum_entries,
                )
                .await
        })
    }

    fn read_private_path<'a>(
        &'a self,
        volume: &'a VolumeRef,
        granted_prefix: &'a str,
        path: &'a str,
        expected_generation: Option<&'a GenerationRef>,
    ) -> BoxFuture<'a, Result<(FileRef, Vec<u8>)>> {
        Box::pin(async move {
            self.directory_owner(volume, granted_prefix, path)
                .await?
                .read_private_path(volume, granted_prefix, path, expected_generation)
                .await
        })
    }

    fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.owner(reference).await?.verify(reference).await })
    }

    fn read<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.owner(reference).await?.read(reference).await })
    }

    fn load_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
    ) -> BoxFuture<'a, Result<Vec<Attachment>>> {
        Box::pin(async move {
            self.owner(reference)
                .await?
                .load_manifest(reference, item_count)
                .await
        })
    }

    fn verify_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
        limits: &'a Limits,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            verified_attachment_manifest(self, reference, item_count, limits).await?;
            Ok(())
        })
    }
}

/// Immutable file plus optional user-visible label.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Attachment {
    /// Version-pinned file.
    pub file: FileRef,
    /// Optional caption; never interpreted as a path.
    pub label: Option<String>,
}

impl Attachment {
    /// Validates referenced bytes and bounded metadata.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.file.validate_with_work(work)?;
        if let Some(label) = &self.label {
            validate_label_with_work(label, MAX_LABEL_BYTES, work)?;
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn validate_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        self.file.validate_with_native_work(work)?;
        if let Some(label) = &self.label {
            inspect_native_metadata(work, label.len())?;
            validate_label_native(label, MAX_LABEL_BYTES)?;
        }
        Ok(())
    }
}

/// Encodes a validated, ordered attachment list in its single durable wire form.
/// Unlike generic canonical JSON, this preserves the typed serde field order
/// required by manifest admission across Rust and WASM producers.
pub fn encode_attachment_manifest(items: &[Attachment]) -> Result<Vec<u8>> {
    if items.len() as u64 > MAX_LIMIT_ATTACHMENTS as u64 {
        return Err(Error::Invalid("attachment count exceeds limit".into()));
    }
    for item in items {
        item.validate()?;
    }
    serde_json::to_vec(items).map_err(|error| Error::Invalid(error.to_string()))
}

/// Decodes one complete canonical manifest; providers may only publish the
/// ordered list after its pinned bytes and every member ref are validated.
pub fn decode_attachment_manifest(
    reference: &FileRef,
    bytes: &[u8],
    item_count: u32,
) -> Result<Vec<Attachment>> {
    let items = decode_complete_attachment_manifest(reference, bytes)?;
    if items.len() != item_count as usize {
        return Err(Error::Invalid(
            "attachment manifest count is incomplete".into(),
        ));
    }
    Ok(items)
}

/// Parses a complete pinned manifest when the item count is supplied by the
/// bytes themselves, as at a cross-provider fork admission boundary.
pub fn decode_complete_attachment_manifest(
    reference: &FileRef,
    bytes: &[u8],
) -> Result<Vec<Attachment>> {
    reference.validate()?;
    if reference.descriptor().media_type() != "application/vnd.acyclic.harness.attachments+json" {
        return Err(Error::Invalid(
            "attachment manifest metadata is invalid".into(),
        ));
    }
    reference.descriptor().verify(bytes)?;
    let items: Vec<Attachment> = crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Invalid(format!("attachment manifest is invalid: {error}")))?;
    if encode_attachment_manifest(&items)? != bytes {
        return Err(Error::Invalid(
            "attachment manifest is not canonical or complete".into(),
        ));
    }
    Ok(items)
}

#[cfg(feature = "filesystem")]
pub(crate) fn manifest_owned_bytes(items: &[Attachment], capacity: usize) -> Result<u64> {
    let mut bytes = capacity.checked_mul(std::mem::size_of::<Attachment>())
        .ok_or_else(|| Error::Invalid("manifest vector ownership overflow".into()))? as u64;
    for item in items {
        bytes = bytes.checked_add(crate::filesystem::UploadRecord::logical_owned_bytes(&item.file)?)
            .and_then(|bytes| bytes.checked_add(item.label.as_ref().map_or(0, |label| label.capacity() as u64)))
            .ok_or_else(|| Error::Invalid("manifest field ownership overflow".into()))?;
    }
    Ok(bytes)
}

#[cfg(feature = "filesystem")]
pub(crate) fn decode_manifest_bounded(
    reference: &FileRef, bytes: &[u8], maximum_members: usize,
    io: &crate::filesystem::PreparationIo,
) -> Result<Vec<Attachment>> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.original_native_work() {
        reference.validate_with_native_work(work)?;
    } else {
        reference.validate()?;
    }
    #[cfg(target_arch = "wasm32")]
    reference.validate()?;
    if reference.descriptor().media_type() != "application/vnd.acyclic.harness.attachments+json"
        || maximum_members > MAX_LIMIT_ATTACHMENTS {
        return Err(io.invalid("attachment manifest metadata or allowance is invalid"));
    }
    reference.descriptor().verify_with_io(bytes, Some(io))?;
    fn guard<T, E: serde::de::Error>(result: Result<T>, failure: &mut Option<Error>) -> std::result::Result<T, E> {
        result.map_err(|error| {
            *failure = Some(error);
            E::custom("manifest admission interrupted")
        })
    }
    struct ManifestSeed<'a> {
        maximum: usize, io: &'a crate::filesystem::PreparationIo, failure: &'a mut Option<Error>,
    }
    impl<'de> serde::de::DeserializeSeed<'de> for ManifestSeed<'_> {
        type Value = Vec<Attachment>;
        fn deserialize<D: Deserializer<'de>>(self, decoder: D) -> std::result::Result<Self::Value, D::Error> {
            decoder.deserialize_seq(self)
        }
    }
    impl<'de> serde::de::Visitor<'de> for ManifestSeed<'_> {
        type Value = Vec<Attachment>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded complete attachment array")
        }
        fn visit_seq<S: serde::de::SeqAccess<'de>>(self, mut sequence: S) -> std::result::Result<Self::Value, S::Error> {
            let mut items: Vec<Attachment> = Vec::new();
            loop {
                guard::<_, S::Error>(self.io.remaining(), self.failure)?;
                if items.len() == self.maximum {
                    if sequence.next_element::<serde::de::IgnoredAny>()?.is_some() {
                        return guard::<_, S::Error>(
                            Err(self.io.invalid("attachment manifest exceeds member allowance")), self.failure,
                        );
                    }
                    break;
                }
                let Some(item) = sequence.next_element::<Attachment>()? else { break; };
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = self.io.original_native_work() {
                    guard::<_, S::Error>(item.validate_with_native_work(work), self.failure)?;
                } else {
                    guard::<_, S::Error>(item.validate(), self.failure)?;
                }
                #[cfg(target_arch = "wasm32")]
                guard::<_, S::Error>(item.validate(), self.failure)?;
                if items.len() == items.capacity() {
                    let capacity = items.capacity().saturating_mul(2).max(4).min(self.maximum);
                    let bytes = guard::<_, S::Error>(capacity.checked_mul(std::mem::size_of::<Attachment>())
                        .map(|bytes| bytes as u64)
                        .ok_or_else(|| self.io.invalid("manifest vector capacity overflow")), self.failure)?;
                    // The audited pre-Serde reservation already covers these
                    // known slots. Charge the actual SDK grow, not twice-owned
                    // buffers or inferred opaque Serde allocator operations.
                    guard::<_, S::Error>(self.io.allocation_attempt(bytes), self.failure)?;
                    items.reserve_exact(capacity - items.len());
                }
                items.push(item);
            }
            Ok(items)
        }
    }
    let mut failure = None;
    let result = io.decode_preparation_seed_with_code(
        ManifestSeed { maximum: maximum_members, io, failure: &mut failure },
        bytes, crate::wire::ErrorCode::Invalid,
    );
    let mut decoded = match result {
        Ok(decoded) => decoded,
        Err(transport) => {
            if let Some(primary) = failure.take() {
                // Retire the foreign static Serde marker and its real paid
                // diagnostic custody before returning the original gate cause.
                drop(transport);
                return Err(primary);
            }
            return Err(transport);
        }
    };
    let actual = (std::mem::size_of::<Vec<Attachment>>() as u64)
        .checked_add(manifest_owned_bytes(&decoded, decoded.capacity())?)
        .ok_or_else(|| io.invalid("manifest ownership overflow"))?;
    decoded.substitute_owned(actual)?;
    let encoded_hold;
    let encoded = io.encode_json(&decoded.value, io.remaining()?.bytes_encoded, false)?;
    encoded_hold = io.guard_owned(encoded.capacity() as u64)?;
    if encoded != bytes {
        return Err(io.invalid("attachment manifest is not canonical or complete"));
    }
    drop(encoded);
    drop(encoded_hold);
    Ok(decoded.into_retained())
}

/// The core admission boundary checks actual owner-resolved bytes, rather
/// than trusting a provider's metadata-only residency assertion.
pub async fn verified_content_bytes(
    verifier: &dyn ContentResidencyVerifier,
    reference: &FileRef,
) -> Result<Vec<u8>> {
    let bytes = verifier.read(reference).await?;
    reference.descriptor().verify(&bytes)?;
    Ok(bytes)
}

/// Resolves one pinned manifest and all of its members through their owning
/// providers. The manifest's canonical bytes, not a provider-supplied list,
/// determine the attachment identities admitted to history.
pub async fn verified_attachment_manifest(
    verifier: &dyn ContentResidencyVerifier,
    reference: &FileRef,
    item_count: u32,
    limits: &Limits,
) -> Result<Vec<Attachment>> {
    verified_attachment_manifest_with_work(verifier, reference, item_count, limits, &mut None).await
}

pub(crate) async fn verified_attachment_manifest_with_work(
    verifier: &dyn ContentResidencyVerifier, reference: &FileRef,
    item_count: u32, limits: &Limits,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Vec<Attachment>> {
    limits.validate_file_with_work(reference, work)?;
    if item_count as usize > limits.attachments {
        return Err(conversation_boundary(work, "attachment manifest count exceeds limits"));
    }
    let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
    let items = if let Some(observer) = quota.work().as_deref_mut() {
        #[cfg(feature = "filesystem")]
        {
            let (items, bytes) = load_manifest_for_history(verifier, reference, item_count, observer).await?;
            quota.adopt(bytes)?;
            items
        }
        #[cfg(not(feature = "filesystem"))]
        {
            observer.check_cancellation()?;
            return Err(Error::Unsupported("finite manifest resolution is unavailable".into()));
        }
    } else {
        let bytes = verified_content_bytes(verifier, reference).await?;
        decode_attachment_manifest(reference, &bytes, item_count)?
    };
    if items.len() != item_count as usize {
        return Err(conversation_boundary(quota.work(), "attachment manifest count is incomplete"));
    }
    for item in &items {
        limits.validate_file_with_work(&item.file, quota.work())?;
        if let Some(observer) = quota.work().as_deref_mut() {
            crate::store::history::verify_content_with_work(observer, verifier, &item.file).await?;
        } else {
            verified_content_bytes(verifier, &item.file).await?;
        }
    }
    quota.transfer();
    Ok(items)
}

#[cfg(feature = "filesystem")]
async fn load_manifest_for_history(
    verifier: &dyn ContentResidencyVerifier, reference: &FileRef, item_count: u32,
    observer: &mut dyn crate::store::history::HistoryIo,
) -> Result<(Vec<Attachment>, u64)> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(native) = observer.original_native_work() {
        let (items, custody) = verifier
            .load_manifest_with_native_work(reference, item_count, native)?.await?;
        let bytes = custody.bytes();
        if let Err(error) = observer.absorb_native_custody(custody) {
            drop(items);
            return Err(error);
        }
        return Ok((items, bytes));
    }
    let result = verifier.load_manifest_measured(
        reference, item_count, observer.remaining()?, observer.cancellation(),
    ).await;
    let receipt = match result {
        Ok(receipt) => receipt,
        Err(failure) => {
            observer.receive_work(failure.work)?;
            return Err(failure.cause);
        }
    };
    if let Err(error) = observer.receive_work(receipt.work) {
        drop(receipt.value);
        return Err(error);
    }
    let bytes = manifest_owned_bytes(&receipt.value, receipt.value.capacity())?;
    observer.retain_owned(bytes)?;
    Ok((receipt.value, bytes))
}

/// A complete ordered attachment list, inline or in one version-pinned file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "manifest refs retain the public tagged enum contract"
)]
pub enum ReferencedAttachments {
    /// Bounded small list retained directly in the event.
    Inline {
        /// Complete ordered attachments.
        items: Vec<Attachment>,
    },
    /// Complete canonical JSON list retained as a file, never a partial page.
    Manifest {
        /// Version-pinned file containing a canonical JSON array.
        manifest: FileRef,
        /// Exact number of list members.
        item_count: u32,
    },
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "wire shape mirrors the canonical public attachment contract"
)]
enum ReferencedAttachmentsWire {
    Inline { items: Vec<Attachment> },
    Manifest { manifest: FileRef, item_count: u32 },
}

impl<'de> Deserialize<'de> for ReferencedAttachments {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let value = match ReferencedAttachmentsWire::deserialize(deserializer)? {
            ReferencedAttachmentsWire::Inline { items } => Self::Inline { items },
            ReferencedAttachmentsWire::Manifest {
                manifest,
                item_count,
            } => Self::Manifest {
                manifest,
                item_count,
            },
        };
        value.validate().map_err(serde::de::Error::custom)?;
        Ok(value)
    }
}

impl ReferencedAttachments {
    /// Rejects malformed attachment records and manifest metadata.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        match self {
            Self::Inline { items } => {
                for item in items { item.validate_with_work(work)?; }
            }
            Self::Manifest { manifest, .. } => {
                manifest.validate_with_work(work)?;
                if manifest.descriptor().media_type()
                    != "application/vnd.acyclic.harness.attachments+json"
                {
                    return Err(conversation_boundary(work, "attachment manifest metadata is invalid"));
                }
            }
        }
        Ok(())
    }
}

impl From<Vec<Attachment>> for ReferencedAttachments {
    fn from(items: Vec<Attachment>) -> Self {
        Self::Inline { items }
    }
}

/// Semantic event type retained in a conversation history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// Human input.
    User,
    /// Agent output.
    Assistant,
    /// Instruction supplied by the host.
    System,
    /// Agent request to invoke a tool.
    ToolCall,
    /// Complete result of a tool call.
    ToolResult,
    /// Question, answer, or other human interaction.
    Interaction,
    /// Permission request or resolution.
    Permission,
    /// Fork announcement or inherited-context reference.
    Fork,
    /// Project merge notice.
    Merge,
}

/// One canonical, payload-free conversation record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConversationMessage {
    /// Caller-assigned stable identity.
    pub id: Uuid,
    /// Ordered position within this conversation, starting at one.
    pub sequence: u64,
    /// Message type.
    pub kind: MessageKind,
    /// Primary text or structured body, stored as a file.
    pub content: FileRef,
    /// Ordered file attachments.
    pub attachments: ReferencedAttachments,
    /// Causal parent message within this conversation.
    pub reply_to: Option<Uuid>,
    /// Tool call identity shared by a call/result pair.
    pub tool_call_id: Option<String>,
    /// Namespaced extension revisions; values are refs rather than arbitrary JSON.
    pub extensions: BTreeMap<String, FileRef>,
}

impl ConversationMessage {
    /// Validates bounded metadata and role-specific linkage.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if self.id.is_nil() || self.reply_to.is_some_and(|id| id.is_nil()) {
            return Err(conversation_boundary(work, "message identity is nil"));
        }
        if self.sequence == 0 {
            return Err(conversation_boundary(work, "message sequence must start at one"));
        }
        self.content.validate_with_work(work)?;
        self.attachments.validate_with_work(work)?;
        for (name, reference) in &self.extensions {
            validate_label_with_work(name, MAX_LABEL_BYTES, work)?;
            if !name.contains('.') {
                return Err(conversation_boundary(work, "extension key must be namespaced"));
            }
            reference.validate_with_work(work)?;
        }
        let has_call = self.tool_call_id.is_some();
        if has_call != matches!(self.kind, MessageKind::ToolCall | MessageKind::ToolResult) {
            return Err(conversation_boundary(work, "tool linkage does not match message kind"));
        }
        if let Some(call) = &self.tool_call_id {
            validate_label_with_work(call, MAX_LABEL_BYTES, work)?;
        }
        Ok(())
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[derive(Default)]
struct NativeReferenceClone {
    allocations: u64,
    copied: u64,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl NativeReferenceClone {
    fn allocation(&mut self) -> Result<()> {
        self.allocations = self.allocations.checked_add(1).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason: "reference clone operations overflow",
        })?;
        Ok(())
    }
    fn text(&mut self, text: &str) -> Result<()> {
        if !text.is_empty() { self.allocation()?; }
        self.copied = self.copied.checked_add(text.len() as u64).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason: "reference clone bytes overflow",
        })?;
        Ok(())
    }
    fn file(&mut self, file: &FileRef) -> Result<()> {
        self.text(file.volume.provider.namespace())?;
        self.text(file.volume.provider.family())?;
        self.text(file.volume.provider.version())?;
        self.text(&file.volume.id)?;
        match &file.volume.owner {
            VolumeOwner::Project(text) | VolumeOwner::Session(text) => self.text(text)?,
            VolumeOwner::Agent(_) => {}
        }
        self.text(&file.path)?;
        self.text(&file.version)?;
        self.text(&file.descriptor.media_type)?;
        self.text(&file.display_name)
    }
    fn message(&mut self, message: &ConversationMessage) -> Result<()> {
        self.file(&message.content)?;
        match &message.attachments {
            ReferencedAttachments::Inline { items } => {
                // Vec<Attachment> clones its elements fieldwise, not by memcpy.
                if !items.is_empty() { self.allocation()?; }
                for attachment in items {
                    self.file(&attachment.file)?;
                    if let Some(label) = &attachment.label { self.text(label)?; }
                }
            }
            ReferencedAttachments::Manifest { manifest, .. } => self.file(manifest)?,
        }
        if let Some(call) = &message.tool_call_id { self.text(call)?; }
        // Opaque BTree allocator metadata is not a claimed source observation.
        for (key, file) in &message.extensions {
            self.text(key)?;
            self.file(file)?;
        }
        Ok(())
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn clone_native_reference_text(
    source: &str, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    let mut allocation = work.retain_owned(0)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let text = clone_native_reference_text_into(source, work, &mut allocation)?;
    Ok((text, allocation))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn clone_native_reference_text_into(
    source: &str, work: &acyclic_fs::NativeMaterializationWork,
    allocation: &mut acyclic_fs::NativeAllocation,
) -> Result<String> {
    if source.is_empty() { return Ok(String::new()); }
    work.admit(acyclic_fs::WorkCounters {
        bytes_copied: source.len() as u64, ..acyclic_fs::WorkCounters::default()
    }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    // The backing guard precedes the physical String on every failure path.
    let mut component = work.retain_owned(source.len() as u64)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    let mut text = String::new();
    work.charge(acyclic_fs::WorkCounters {
        allocation_operations: 1, ..acyclic_fs::WorkCounters::default()
    }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    text.try_reserve_exact(source.len()).map_err(|cause| Error::NativeWork {
        cause: acyclic_fs::NativeTreeMaterializationError::AllocationFailed(cause), custody: None,
    })?;
    component.reconcile_capacity(text.capacity() as u64)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    work.charge(acyclic_fs::WorkCounters {
        bytes_copied: source.len() as u64, ..acyclic_fs::WorkCounters::default()
    }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
    text.push_str(source);
    allocation.absorb(&mut component)
        .map_err(|cause| Error::NativeWork { cause, custody: None })?;
    Ok(text)
}

impl FileRef {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let owner = match &self.volume.owner {
            VolumeOwner::Project(text) | VolumeOwner::Session(text) => text.capacity() as u64,
            VolumeOwner::Agent(_) => 0,
        };
        [self.volume.id.capacity(), self.path.capacity(), self.version.capacity(),
            self.descriptor.media_type.capacity(), self.display_name.capacity()]
            .into_iter().try_fold(self.volume.provider.logical_owned_bytes()?,
                |bytes, capacity| bytes.checked_add(capacity as u64))
            .and_then(|bytes| bytes.checked_add(owner))
            .ok_or_else(|| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "file capacity inventory overflows" } }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                { Error::Invalid("file capacity inventory overflows".into()) }
            })
    }

    pub(crate) fn logical_clone_owned_bytes(&self) -> Result<u64> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        { self.clone_work_recipe().map(|(_, copied)| copied) }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        {
            let owner = match &self.volume.owner {
                VolumeOwner::Project(text) | VolumeOwner::Session(text) => text.len() as u64,
                VolumeOwner::Agent(_) => 0,
            };
            let provider = &self.volume.provider;
            [provider.namespace().len(), provider.family().len(), provider.version().len(),
                self.volume.id.len(), self.path.len(), self.version.len(),
                self.descriptor.media_type.len(), self.display_name.len()]
                .into_iter().try_fold(owner, |bytes, length| bytes.checked_add(length as u64)
                    .ok_or_else(|| Error::Invalid("file clone ownership overflow".into())))
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_work_recipe(&self) -> Result<(u64, u64)> {
        let mut recipe = NativeReferenceClone::default();
        recipe.file(self)?;
        Ok((recipe.allocations, recipe.copied))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let file = self.clone_with_work(&mut Some(&mut io))?;
        Ok((file, io.into_custody()))
    }

    pub(crate) fn clone_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if work.is_none() { return Ok(self.clone()); }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let file = self.clone_in_quota(&mut quota)?;
        quota.transfer();
        Ok(file)
    }

    pub(crate) fn clone_in_quota(
        &self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        if quota.work().is_none() { return Ok(self.clone()); }
        self.validate_with_work(quota.work())?;
        let volume = self.volume.clone_in_quota(quota)?;
        let path = clone_owned_file_text(&self.path, quota)?;
        let version = clone_owned_file_text(&self.version, quota)?;
        let descriptor = FileDescriptor {
            sha256: self.descriptor.sha256, byte_length: self.descriptor.byte_length,
            media_type: clone_owned_file_text(&self.descriptor.media_type, quota)?,
        };
        descriptor.validate_with_work(quota.work())?;
        let file = Self {
            volume, path, version, descriptor,
            display_name: clone_owned_file_text(&self.display_name, quota)?,
        };
        file.validate_with_work(quota.work())?;
        Ok(file)
    }
}

fn clone_owned_file_text(
    source: &str, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
) -> Result<String> {
    let text = quota.work().as_deref_mut().expect("observed file clone").clone_text(source)?;
    quota.adopt(text.capacity() as u64)?;
    Ok(text)
}

fn copy_preclaimed_conversation_vector<T: Copy>(
    source: &[T], target: &mut Vec<T>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    let bytes = conversation_buffer_bytes::<T>(source.len(), work)?;
    if let Some(io) = work.as_deref_mut() {
        io.admit_copy(bytes)?;
        io.copy_bytes(bytes)?;
    }
    target.extend_from_slice(source);
    Ok(())
}

impl ConversationMessage {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_work_recipe(&self) -> Result<(u64, u64)> {
        let mut recipe = NativeReferenceClone::default();
        recipe.message(self)?;
        Ok((recipe.allocations, recipe.copied))
    }

    pub(crate) fn clone_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        self.clone_with_work_and_custody(work).map(|(message, _)| message)
    }

    pub(crate) fn clone_with_work_and_custody(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let message = self.clone_in_quota(&mut quota)?;
        let retained = quota.retained();
        quota.transfer();
        Ok((message, retained))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let message = self.clone_with_work(&mut Some(&mut io))?;
        Ok((message, io.into_custody()))
    }

    pub(crate) fn clone_in_quota(
        &self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        if quota.work().is_none() { return Ok(self.clone()); }
        self.validate_with_work(quota.work())?;
        let content = self.content.clone_in_quota(quota)?;
        let attachments = match &self.attachments {
            ReferencedAttachments::Inline { items } => {
                let items = crate::core::copy_iter(items.len(), items.iter(), quota, |item, quota| {
                    let file = item.file.clone_in_quota(quota)?;
                    let label = item.label.as_deref().map(|label|
                        clone_owned_file_text(label, quota)).transpose()?;
                    Ok(Attachment { file, label })
                })?;
                ReferencedAttachments::Inline { items }
            }
            ReferencedAttachments::Manifest { manifest, item_count } =>
                ReferencedAttachments::Manifest {
                    manifest: manifest.clone_in_quota(quota)?, item_count: *item_count,
                },
        };
        let tool_call_id = self.tool_call_id.as_deref()
            .map(|id| clone_owned_file_text(id, quota)).transpose()?;
        let mut extensions = BTreeMap::new();
        for (key, file) in &self.extensions {
            let bytes = std::mem::size_of::<(String, FileRef)>() as u64;
            let io = quota.work().as_deref_mut().expect("observed message clone");
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            quota.adopt(bytes)?;
            let key = clone_owned_file_text(key, quota)?;
            let file = file.clone_in_quota(quota)?;
            extensions.insert(key, file);
        }
        Ok(Self {
            id: self.id, sequence: self.sequence, kind: self.kind.clone(), content,
            attachments, reply_to: self.reply_to, tool_call_id, extensions,
        })
    }
}


/// Projection of one agent-owned conversation.
#[derive(Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "ConversationStateWire")]
pub struct ConversationState {
    /// Set once at admission.
    pub agent: Option<AgentId>,
    /// Loaded canonical suffix; older records remain in the authoritative Stream.
    pub(crate) messages: Vec<ConversationMessage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    archived_prefix: Option<ArchivedConversationPrefix>,
    #[serde(skip)]
    prefix_digests: Vec<[u8; 32]>,
    #[serde(skip)]
    by_id: BTreeMap<Uuid, usize>,
    #[serde(skip)]
    pending_user: Option<Uuid>,
    #[serde(skip)]
    outcomes: BTreeMap<Uuid, usize>,
    #[serde(skip)]
    model_positions: Vec<usize>,
    #[serde(skip)]
    history_digest: [u8; 32],
    // LAST: resident buffers and source indexes physically drop before custody.
    #[serde(skip)]
    native_custody: NativeConversationCustody,
}

/// Only an issuer-authenticated reducer snapshot makes this a trusted prefix.
/// The checkpoint pin is a projection boundary, never replacement history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchivedConversationPrefix {
    through_sequence: u64,
    history_digest: [u8; 32],
    checkpoint: FileRef,
}


#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConversationStateWire {
    agent: Option<AgentId>,
    messages: Vec<ConversationMessage>,
    #[serde(default)]
    archived_prefix: Option<ArchivedConversationPrefix>,
}

#[derive(Default)]
struct NativeConversationCustody {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    messages: Option<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    digests: Option<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    positions: Option<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    archive: Option<acyclic_fs::NativeAllocation>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    indexes: BTreeMap<(bool, Uuid), NativeConversationIndex>,
    // A failed cold transfer retains every not-yet-assigned source claim here.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    cache_carrier: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
struct NativeConversationIndex {
    _source: acyclic_fs::NativeAllocation,
    _metadata: acyclic_fs::NativeAllocation,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl NativeConversationIndex {
    #[cfg(test)]
    fn work(&self) -> &acyclic_fs::NativeMaterializationWork { self._source.work() }
    fn metadata_bytes() -> u64 {
        (11 * (std::mem::size_of::<(bool, Uuid)>() + std::mem::size_of::<Self>())
            + 12 * std::mem::size_of::<usize>()) as u64
    }
    fn prepare_metadata(
        io: &mut dyn crate::store::history::HistoryIo,
    ) -> Result<acyclic_fs::NativeAllocation> {
        let bytes = Self::metadata_bytes();
        io.admit_allocation(bytes)?;
        io.retain_owned(bytes)?;
        io.take_native_custody(bytes)
    }
}

impl std::fmt::Debug for NativeConversationCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeConversationCustody").finish_non_exhaustive()
    }
}
impl PartialEq for NativeConversationCustody {
    fn eq(&self, _: &Self) -> bool { true }
}
impl Eq for NativeConversationCustody {}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
impl Drop for NativeConversationCustody {
    fn drop(&mut self) {
        // Source containers have already dropped. Extract the bookkeeping
        // entries before releasing the claims protecting their nodes.
        while let Some((_, index)) = self.indexes.pop_first() { drop(index); }
    }
}

fn conversation_boundary(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
    }
    let _ = work;
    Error::Invalid(reason.into())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_owned_wire_inspect(
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() {
        io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_owned_wire_object(
    value: serde_json::Value, fields: Option<&[&str]>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<serde_json::Map<String, serde_json::Value>> {
    native_owned_wire_inspect(work)?;
    let serde_json::Value::Object(object) = value else {
        return Err(conversation_boundary(work, "wire value must be an object"));
    };
    for key in object.keys() {
        native_owned_wire_inspect(work)?;
        if fields.is_some_and(|fields| !fields.contains(&key.as_str())) {
            return Err(conversation_boundary(work, "wire object has an unknown field"));
        }
    }
    Ok(object)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_owned_wire_field(
    object: &mut serde_json::Map<String, serde_json::Value>, name: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<serde_json::Value> {
    object.remove(name).ok_or_else(|| conversation_boundary(work, "wire object is missing a field"))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_owned_wire_text(
    value: serde_json::Value,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    native_owned_wire_inspect(work)?;
    match value {
        serde_json::Value::String(text) => Ok(text),
        _ => Err(conversation_boundary(work, "wire value must be a string")),
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_owned_volume_class(
    value: serde_json::Value,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<VolumeClass> {
    native_owned_wire_inspect(work)?;
    let name = match value {
        serde_json::Value::String(name) => name,
        serde_json::Value::Object(variants) if variants.len() == 1 => {
            let (name, value) = variants.into_iter().next().expect("one enum variant");
            native_owned_wire_inspect(work)?;
            native_owned_wire_inspect(work)?;
            if !value.is_null() {
                return Err(conversation_boundary(work, "volume class is invalid"));
            }
            name
        }
        _ => return Err(conversation_boundary(work, "volume class is invalid")),
    };
    match name.as_str() {
        "project" => Ok(VolumeClass::Project),
        "agent_private" => Ok(VolumeClass::AgentPrivate),
        "session_shared" => Ok(VolumeClass::SessionShared),
        _ => Err(conversation_boundary(work, "volume class is invalid")),
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_file_from_owned_value(
    value: serde_json::Value,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<FileRef> {
    // The producer's incoming Value guard remains LAST over all moved Strings.
    let mut file = native_owned_wire_object(value,
        Some(&["volume", "path", "version", "descriptor", "display_name"]), work)?;
    let mut volume = native_owned_wire_object(native_owned_wire_field(&mut file, "volume", work)?,
        Some(&["provider", "id", "class", "owner"]), work)?;
    let provider = ProviderRef::from_owned_value_with_work(
        native_owned_wire_field(&mut volume, "provider", work)?, work)?;
    let id = native_owned_wire_text(native_owned_wire_field(&mut volume, "id", work)?, work)?;
    let class = native_owned_volume_class(native_owned_wire_field(&mut volume, "class", work)?, work)?;
    // VolumeOwner's original adjacent-tag visitor ignores extra object fields.
    let mut owner = native_owned_wire_object(native_owned_wire_field(&mut volume, "owner", work)?,
        None, work)?;
    let kind = native_owned_wire_text(native_owned_wire_field(&mut owner, "kind", work)?, work)?;
    let owner_id = native_owned_wire_text(native_owned_wire_field(&mut owner, "id", work)?, work)?;
    let owner = match kind.as_str() {
        "project" => VolumeOwner::Project(owner_id),
        "session" => VolumeOwner::Session(owner_id),
        "agent" => {
            let id = Uuid::parse_str(&owner_id)
                .map_err(|_| conversation_boundary(work, "volume agent identity is invalid"))?;
            VolumeOwner::Agent(AgentId::from_bytes(*id.as_bytes()))
        }
        _ => return Err(conversation_boundary(work, "volume owner kind is invalid")),
    };
    let volume = VolumeRef { provider, id, class, owner };
    volume.validate_with_work(work)?;
    let path = native_owned_wire_text(native_owned_wire_field(&mut file, "path", work)?, work)?;
    let version = native_owned_wire_text(native_owned_wire_field(&mut file, "version", work)?, work)?;
    let display_name = native_owned_wire_text(
        native_owned_wire_field(&mut file, "display_name", work)?, work)?;
    let mut descriptor = native_owned_wire_object(
        native_owned_wire_field(&mut file, "descriptor", work)?,
        Some(&["sha256", "byte_length", "media_type"]), work)?;
    let digest = native_owned_wire_field(&mut descriptor, "sha256", work)?;
    native_owned_wire_inspect(work)?;
    let serde_json::Value::Array(digest) = digest else {
        return Err(conversation_boundary(work, "SHA-256 digest must contain exactly 32 bytes"));
    };
    if digest.len() != 32 {
        return Err(conversation_boundary(work, "SHA-256 digest must contain exactly 32 bytes"));
    }
    let mut sha256 = [0_u8; 32];
    for (slot, value) in sha256.iter_mut().zip(digest) {
        native_owned_wire_inspect(work)?;
        *slot = value.as_u64().and_then(|value| u8::try_from(value).ok())
            .ok_or_else(|| conversation_boundary(work, "SHA-256 digest byte is invalid"))?;
    }
    let length = native_owned_wire_field(&mut descriptor, "byte_length", work)?;
    native_owned_wire_inspect(work)?;
    let byte_length = length.as_u64()
        .ok_or_else(|| conversation_boundary(work, "file byte length is invalid"))?;
    let media_type = native_owned_wire_text(
        native_owned_wire_field(&mut descriptor, "media_type", work)?, work)?;
    let descriptor = FileDescriptor { sha256, byte_length, media_type };
    descriptor.validate_with_work(work)?;
    let file = FileRef { volume, path, version, display_name, descriptor };
    file.validate_with_work(work)?;
    Ok(file)
}

fn conversation_conflict(
    work: &Option<&mut dyn crate::store::history::HistoryIo>, reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Conflict, reason };
    }
    let _ = work;
    Error::Conflict(reason.into())
}

fn conversation_buffer_bytes<T>(
    capacity: usize, work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    capacity.checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| conversation_boundary(work, "conversation buffer capacity overflows"))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_conversation_buffer_bytes<T>(capacity: usize) -> Result<u64> {
    capacity.checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
            reason: "conversation buffer capacity overflows" })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn reserve_native_conversation_buffer<T>(
    values: &mut Vec<T>, owner: &mut Option<acyclic_fs::NativeAllocation>,
    target: usize, io: &mut dyn crate::store::history::HistoryIo,
) -> Result<()> {
    if target <= values.capacity() { return Ok(()); }
    let bytes = conversation_buffer_bytes::<T>(target, &Some(&mut *io))?;
    io.admit_allocation(bytes)?;
    io.retain_owned(bytes)?;
    let incoming = io.take_native_custody(bytes)?;
    // Install the new owner before the allocator may replace the old backing.
    // Each replacement is independently paid, even across query ledgers.
    let retired = owner.replace(incoming);
    if let Err(cause) = values.try_reserve_exact(target - values.len()) {
        let rejected = std::mem::replace(owner, retired);
        drop(rejected);
        return Err(io.allocation_failure(cause));
    }
    let actual = conversation_buffer_bytes::<T>(values.capacity(), &Some(&mut *io))?;
    let result = owner.as_mut().expect("installed conversation buffer custody")
        .reconcile_capacity(actual)
        .map_err(|cause| Error::NativeWork { cause, custody: None });
    // try_reserve_exact has already deallocated the old physical buffer.
    drop(retired);
    result
}

#[derive(Clone, Copy)]
enum ConversationBuffer { Messages, Digests, Positions }

impl NativeConversationCustody {
    fn reserve<T>(
        &mut self, kind: ConversationBuffer, values: &mut Vec<T>, target: usize,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        if target <= values.capacity() { return Ok(0); }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work.as_deref_mut().filter(|io| io.original_native_work().is_some()) {
            let owner = match kind {
                ConversationBuffer::Messages => &mut self.messages,
                ConversationBuffer::Digests => &mut self.digests,
                ConversationBuffer::Positions => &mut self.positions,
            };
            reserve_native_conversation_buffer(values, owner, target, io)?;
            return Ok(0);
        }
        let _ = kind;
        let old = values.capacity();
        if let Some(io) = work.as_deref_mut() {
            let requested = conversation_buffer_bytes::<T>(target - old, &Some(&mut *io))?;
            io.admit_allocation(requested)?;
            values.try_reserve_exact(target - values.len())
                .map_err(|cause| io.allocation_failure(cause))?;
            let actual = conversation_buffer_bytes::<T>(values.capacity() - old, &Some(&mut *io))?;
            io.retain_owned(actual)?;
            Ok(actual)
        } else {
            values.reserve_exact(target - values.len());
            Ok(0)
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn adopt_existing_buffer<T>(
        &mut self, kind: ConversationBuffer, capacity: usize,
        io: &mut dyn crate::store::history::HistoryIo,
    ) -> Result<()> {
        let owner = match kind {
            ConversationBuffer::Messages => &mut self.messages,
            ConversationBuffer::Digests => &mut self.digests,
            ConversationBuffer::Positions => &mut self.positions,
        };
        if owner.is_none() && capacity != 0 {
            let bytes = conversation_buffer_bytes::<T>(capacity, &Some(&mut *io))?;
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            *owner = Some(io.take_native_custody(bytes)?);
        }
        Ok(())
    }


    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn has_native(&self) -> bool {
        self.messages.is_some() || self.digests.is_some() || self.positions.is_some()
            || self.archive.is_some() || self.cache_carrier.is_some() || !self.indexes.is_empty()
    }

    fn retain_index(
        &mut self, outcome: bool, id: Uuid,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work.as_deref_mut().filter(|io| io.original_native_work().is_some()) {
            if self.indexes.contains_key(&(outcome, id)) { return Ok(0); }
            // The source witness is the logical (Uuid, usize) slot, not opaque
            // BTree allocator nodes. The private guard index funds its own node.
            let metadata = NativeConversationIndex::metadata_bytes();
            let bytes = std::mem::size_of::<(Uuid, usize)>() as u64 + metadata;
            io.admit_allocation(bytes)?;
            io.retain_owned(bytes)?;
            let mut allocation = io.take_native_custody(bytes)?;
            let source = allocation.split_off(std::mem::size_of::<(Uuid, usize)>() as u64)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            self.indexes.insert((outcome, id), NativeConversationIndex {
                _source: source, _metadata: allocation,
            });
            return Ok(0);
        }
        let _ = (outcome, id);
        if let Some(io) = work.as_deref_mut() {
            let bytes = std::mem::size_of::<(Uuid, usize)>() as u64;
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            return Ok(bytes);
        }
        Ok(0)
    }
}

impl TryFrom<ConversationStateWire> for ConversationState {
    type Error = Error;

    fn try_from(wire: ConversationStateWire) -> Result<Self> {
        Self::from_wire_with_work(wire, 1, &mut None).map(|(state, _)| state)
    }
}

impl ConversationState {
    pub(crate) fn from_wire_with_work(
        wire: ConversationStateWire,
        maximum_encoded_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        if let Some(prefix) = &wire.archived_prefix {
            prefix.checkpoint.validate_with_work(cache.work())?;
            if wire.agent.is_none() || prefix.through_sequence == 0 {
                return Err(conversation_boundary(cache.work(),
                    "archived conversation prefix is unbound"));
            }
        }
        let mut previous = wire.archived_prefix.as_ref()
            .map_or(0, |prefix| prefix.through_sequence);
        let mut state = Self {
            agent: wire.agent,
            messages: wire.messages,
            history_digest: wire.archived_prefix.as_ref()
                .map_or([0; 32], |prefix| prefix.history_digest),
            archived_prefix: wire.archived_prefix,
            ..Self::default()
        };
        // The decoder's original payload guard already owns messages' backing;
        // Snapshot transfers that shell separately without a second claim.
        let retained = state.native_custody.reserve(
            ConversationBuffer::Digests, &mut state.prefix_digests,
            state.messages.len(), cache.work())?;
        cache.adopt(retained)?;
        for (position, message) in state.messages.iter().enumerate() {
            if let Some(io) = cache.work().as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            message.validate_with_work(cache.work())?;
            if message.sequence <= previous
                || (state.archived_prefix.is_some()
                    && previous.checked_add(1) != Some(message.sequence))
                || state.by_id.contains_key(&message.id)
            {
                return Err(conversation_boundary(cache.work(),
                    "conversation order or identity is invalid"));
            }
            let retained = state.native_custody.retain_index(false, message.id, cache.work())?;
            cache.adopt(retained)?;
            state.by_id.insert(message.id, position);
            previous = message.sequence;
            state.history_digest = advance_history_digest_with_work(
                state.history_digest, message, maximum_encoded_bytes, cache.work())?;
            state.prefix_digests.push(state.history_digest);
        }
        for message in state.messages.iter().rev() {
            if let Some(io) = cache.work().as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            if message.kind == MessageKind::User {
                state.pending_user = Some(message.id);
                break;
            }
        }
        for position in 0..state.messages.len() {
            if let Some(io) = cache.work().as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            let retained = state.index_turn_with_work(position, cache.work())?;
            cache.adopt(retained)?;
        }
        let retained = cache.retained();
        cache.transfer();
        Ok((state, retained))
    }
}

/// Exact history revision and ordered subset selected for one model request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelContextSelection {
    /// Number of canonical messages observed when selecting.
    pub conversation_revision: u64,
    /// Ordered, unique message identities; omitted history is deliberate.
    pub message_ids: Vec<Uuid>,
    /// Immutable canonical checkpoint covering history before this selection's delta.
    /// Its typed payload and publication must be resolved by the owning journal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<FileRef>,
}

impl ModelContextSelection {
    /// Rejects stale, missing, duplicated, or out-of-order source identities.
    pub fn validate(&self, conversation: &ConversationState) -> Result<()> {
        if self.conversation_revision != conversation.logical_revision() {
            return Err(Error::Conflict(
                "model context selection has a stale conversation revision".into(),
            ));
        }
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.validate()?;
        }
        let mut previous_sequence = 0;
        for id in &self.message_ids {
            let sequence = conversation
                .message(*id)
                .map(|message| message.sequence)
                .ok_or_else(|| Error::Invalid("selected conversation message is missing".into()))?;
            if sequence <= previous_sequence {
                return Err(Error::Invalid(
                    "model context selection is not ordered and unique".into(),
                ));
            }
            previous_sequence = sequence;
        }
        Ok(())
    }
}

impl ConversationState {
    fn require_native_work(
        &self, work: &Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if self.native_custody.has_native()
            && !work.as_deref().is_some_and(|io| io.original_native_work().is_some())
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "native conversation ownership requires explicit native work",
            });
        }
        let _ = work;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn native_unclaimed_message_buffer_bytes(&self) -> Result<u64> {
        if self.native_custody.messages.is_some() { return Ok(0); }
        native_conversation_buffer_bytes::<ConversationMessage>(self.messages.capacity())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn native_unclaimed_archived_prefix_bytes(&self) -> Result<u64> {
        if self.native_custody.archive.is_some() { return Ok(0); }
        self.archived_prefix.as_ref().map_or(Ok(0), |prefix| prefix.checkpoint.logical_owned_bytes())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn install_native_message_buffer(&mut self, allocation: acyclic_fs::NativeAllocation) {
        assert!(self.native_custody.messages.is_none(), "message buffer already has custody");
        self.native_custody.messages = Some(allocation);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn native_archived_prefix_bytes(&self) -> Result<u64> {
        self.archived_prefix.as_ref()
            .map_or(Ok(0), crate::core::source_inventory)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn install_native_archived_prefix(&mut self, allocation: acyclic_fs::NativeAllocation) {
        assert!(self.native_custody.archive.is_none(), "archived prefix already has custody");
        self.native_custody.archive = Some(allocation);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn native_unclaimed_cache_bytes(&self) -> Result<u64> {
        if self.native_custody.cache_carrier.is_some() { return Ok(0); }
        let mut bytes = 0_u64;
        if self.native_custody.digests.is_none() {
            bytes += native_conversation_buffer_bytes::<[u8; 32]>(self.prefix_digests.capacity())?;
        }
        if self.native_custody.positions.is_none() {
            bytes = bytes.checked_add(native_conversation_buffer_bytes::<usize>(
                self.model_positions.capacity())?).ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid, reason: "conversation cache transfer overflows",
                })?;
        }
        for (outcome, map) in [(false, &self.by_id), (true, &self.outcomes)] {
            for id in map.keys() {
                if !self.native_custody.indexes.contains_key(&(outcome, *id)) {
                    bytes = bytes.checked_add(std::mem::size_of::<(Uuid, usize)>() as u64)
                        .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                            reason: "conversation cache transfer overflows" })?;
                }
            }
        }
        Ok(bytes)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn install_native_preclaimed_caches(
        &mut self, allocation: acyclic_fs::NativeAllocation,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        // Original data custody is installed LAST before any metadata denial.
        // It is only split, never resized, rebound or absorbed by another ledger.
        if self.native_custody.cache_carrier.is_none() {
            self.native_custody.cache_carrier = Some(allocation);
        } else {
            assert_eq!(allocation.bytes(), 0, "cache source already has custody");
        }
        let carrier = self.native_custody.cache_carrier.as_mut().expect("installed cache source custody");
        if self.native_custody.digests.is_none() {
            let bytes = conversation_buffer_bytes::<[u8; 32]>(self.prefix_digests.capacity(), work)?;
            self.native_custody.digests = Some(carrier.split_off(bytes)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?);
        }
        if self.native_custody.positions.is_none() {
            let bytes = conversation_buffer_bytes::<usize>(self.model_positions.capacity(), work)?;
            self.native_custody.positions = Some(carrier.split_off(bytes)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?);
        }
        for (outcome, map) in [(false, &self.by_id), (true, &self.outcomes)] {
            for id in map.keys() {
                if self.native_custody.indexes.contains_key(&(outcome, *id)) { continue; }
                let io = work.as_deref_mut().filter(|io| io.original_native_work().is_some())
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                        reason: "cache metadata requires original native work" })?;
                let metadata = NativeConversationIndex::prepare_metadata(io)?;
                let source = carrier.split_off(std::mem::size_of::<(Uuid, usize)>() as u64)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                self.native_custody.indexes.insert((outcome, *id), NativeConversationIndex {
                    _source: source, _metadata: metadata,
                });
            }
        }
        assert_eq!(carrier.bytes(), 0, "cache source transfer inventory must match");
        Ok(())
    }

    pub(crate) fn clone_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        self.clone_with_work_and_custody(work).map(|(state, _)| state)
    }

    pub(crate) fn clone_with_work_and_custody(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        self.clone_with_work_engine(work, None)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_with_work_and_message_custody<'work>(
        &self, work: &mut Option<&'work mut dyn crate::store::history::HistoryIo>,
        retain: &mut dyn FnMut(u64, u64, &mut Option<&'work mut dyn crate::store::history::HistoryIo>) -> Result<()>,
    ) -> Result<(Self, u64)> {
        if !work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "message component custody requires original native work" });
        }
        self.clone_with_work_engine(work, Some(retain))
    }

    fn clone_with_work_engine<'work>(
        &self, work: &mut Option<&'work mut dyn crate::store::history::HistoryIo>,
        mut retain: Option<&mut dyn FnMut(u64, u64, &mut Option<&'work mut dyn crate::store::history::HistoryIo>) -> Result<()>>,
    ) -> Result<(Self, u64)> {
        self.require_native_work(work)?;
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut state = Self {
            agent: self.agent, pending_user: self.pending_user,
            history_digest: self.history_digest, ..Self::default()
        };
        let retained = state.native_custody.reserve(
            ConversationBuffer::Messages, &mut state.messages, self.messages.len(), cache.work())?;
        cache.adopt(retained)?;
        let retained = state.native_custody.reserve(
            ConversationBuffer::Digests, &mut state.prefix_digests,
            self.prefix_digests.len(), cache.work())?;
        cache.adopt(retained)?;
        let retained = state.native_custody.reserve(
            ConversationBuffer::Positions, &mut state.model_positions,
            self.model_positions.len(), cache.work())?;
        cache.adopt(retained)?;
        for id in self.by_id.keys() {
            let retained = state.native_custody.retain_index(false, *id, cache.work())?;
            cache.adopt(retained)?;
        }
        for id in self.outcomes.keys() {
            let retained = state.native_custody.retain_index(true, *id, cache.work())?;
            cache.adopt(retained)?;
        }
        for message in &self.messages {
            let before = cache.retained();
            let message = message.clone_in_quota(&mut cache)?;
            if let Some(retain) = retain.as_mut() {
                let produced = cache.retained() - before;
                retain(message.sequence, produced, cache.work())?;
                cache.transfer();
            }
            state.messages.push(message);
        }
        copy_preclaimed_conversation_vector(
            &self.prefix_digests, &mut state.prefix_digests, cache.work())?;
        copy_preclaimed_conversation_vector(
            &self.model_positions, &mut state.model_positions, cache.work())?;
        for (id, position) in &self.by_id { state.by_id.insert(*id, *position); }
        for (id, position) in &self.outcomes { state.outcomes.insert(*id, *position); }
        let retained = state.clone_archived_prefix_with_work(self.archived_prefix.as_ref(), cache.work())?;
        cache.adopt(retained)?;
        let retained = cache.retained();
        cache.transfer();
        Ok((state, retained))
    }

    fn clone_archived_prefix_with_work(
        &mut self, prefix: Option<&ArchivedConversationPrefix>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        let Some(prefix) = prefix else { return Ok(0); };
        self.clone_archive_checkpoint_with_work(
            prefix.through_sequence, prefix.history_digest, &prefix.checkpoint, work)
    }

    fn clone_archive_checkpoint_with_work(
        &mut self, through_sequence: u64, history_digest: [u8; 32], checkpoint: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let checkpoint = checkpoint.clone_in_quota(&mut quota)?;
        let bytes = quota.retained();
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let retained = if let Some(io) = quota.work().as_deref_mut().filter(|io| io.original_native_work().is_some()) {
            self.native_custody.archive = Some(io.take_native_custody(bytes)?);
            0
        } else { bytes };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let retained = bytes;
        quota.transfer();
        self.archived_prefix = Some(ArchivedConversationPrefix {
            through_sequence, history_digest, checkpoint,
        });
        Ok(retained)
    }

    /// Logical payload cloned from caches omitted by the wire projection.
    /// Opaque BTreeMap allocator nodes are deliberately outside this witness.
    pub(crate) fn clone_cache_logical_bytes(&self) -> Result<u64> {
        let digests = self.prefix_digests.len().checked_mul(std::mem::size_of::<[u8; 32]>());
        let positions = self.model_positions.len().checked_mul(std::mem::size_of::<usize>());
        let maps = self.by_id.len().checked_add(self.outcomes.len())
            .and_then(|count| count.checked_mul(std::mem::size_of::<(Uuid, usize)>()));
        digests.and_then(|bytes| positions.and_then(|positions| bytes.checked_add(positions)))
            .and_then(|bytes| maps.and_then(|maps| bytes.checked_add(maps)))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or_else(|| Error::Invalid("conversation clone cache inventory overflows".into()))
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_clone_cache_copy_bytes(&self) -> Result<u64> {
        native_conversation_buffer_bytes::<[u8; 32]>(self.prefix_digests.len())?
            .checked_add(native_conversation_buffer_bytes::<usize>(self.model_positions.len())?)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "conversation clone bytes overflow" })
    }

    /// Typed heap clone and CopyVec source work, not logical BTree slots.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn native_clone_copy_recipe(&self) -> Result<(u64, u64)> {
        let mut recipe = NativeReferenceClone::default();
        for message in &self.messages { recipe.message(message)?; }
        if let Some(prefix) = &self.archived_prefix { recipe.file(&prefix.checkpoint)?; }
        let copied = recipe.copied.checked_add(self.native_clone_cache_copy_bytes()?)
            .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "conversation clone bytes overflow" })?;
        Ok((recipe.allocations, copied))
    }

    /// Logical history head, independent of the number of resident records.
    /// A sparse read-only selection may end before its separately pinned source cut.
    #[must_use]
    pub fn logical_revision(&self) -> u64 {
        self.messages
            .last()
            .map_or(self.archived_through(), |message| message.sequence)
    }

    /// Canonical records through this cut are archived, not resident.
    /// Use the authoritative history reader for requests beginning before it.
    #[must_use]
    pub fn resident_after_sequence(&self) -> u64 {
        self.archived_through()
    }

    fn archived_through(&self) -> u64 {
        self.archived_prefix
            .as_ref()
            .map_or(0, |prefix| prefix.through_sequence)
    }

    /// Fingerprints an exact loaded logical prefix without revisiting older records.
    /// An archived cut is authenticated by the containing issuer snapshot; older
    /// cuts require an explicit read from the canonical history reader.
    pub(crate) fn history_prefix_digest(&self, through_sequence: u64) -> Result<[u8; 32]> {
        self.history_prefix_digest_with_work(through_sequence, &mut None)
    }

    pub(crate) fn history_prefix_digest_with_work(
        &self, through_sequence: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        let base = self.archived_through();
        if self.logical_revision() != base.saturating_add(self.messages.len() as u64)
            || through_sequence < base
            || through_sequence > self.logical_revision()
        {
            return Err(conversation_boundary(work,
                "history fingerprint requires a loaded dense logical prefix"));
        }
        if through_sequence == base {
            return Ok(self
                .archived_prefix
                .as_ref()
                .map_or([0; 32], |prefix| prefix.history_digest));
        }
        let position = usize::try_from(through_sequence - base - 1)
            .map_err(|_| conversation_boundary(work, "history prefix exceeds platform bounds"))?;
        self.prefix_digests
            .get(position)
            .copied()
            .ok_or_else(|| conversation_boundary(work, "history prefix digest is missing"))
    }

    pub(crate) fn checkpoint_covers_archived_prefix(
        &self,
        reference: &FileRef,
        through: u64,
    ) -> bool {
        self.archived_prefix.as_ref().is_some_and(|prefix| {
            prefix.checkpoint == *reference && prefix.through_sequence == through
        })
    }

    /// Builds the snapshot suffix only after a committed checkpoint and closed
    /// user/tool exchange. The canonical Stream and identity indexes are untouched.
    pub(crate) fn checkpointed_suffix_with_work(
        &self, selection: &ModelContextSelection,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        self.checkpointed_suffix_with_work_and_custody(selection, work).map(|(state, _)| state)
    }

    pub(crate) fn checkpointed_suffix_with_work_and_custody(
        &self, selection: &ModelContextSelection,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        self.checkpointed_suffix_with_work_engine(selection, work, None)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn checkpointed_suffix_with_work_and_message_custody<'work>(
        &self, selection: &ModelContextSelection,
        work: &mut Option<&'work mut dyn crate::store::history::HistoryIo>,
        retain: &mut dyn FnMut(u64, u64, &mut Option<&'work mut dyn crate::store::history::HistoryIo>) -> Result<()>,
    ) -> Result<(Self, u64)> {
        if !work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "message component custody requires original native work" });
        }
        self.checkpointed_suffix_with_work_engine(selection, work, Some(retain))
    }

    fn checkpointed_suffix_with_work_engine<'work>(
        &self, selection: &ModelContextSelection,
        work: &mut Option<&'work mut dyn crate::store::history::HistoryIo>,
        mut retain: Option<&mut dyn FnMut(u64, u64, &mut Option<&'work mut dyn crate::store::history::HistoryIo>) -> Result<()>>,
    ) -> Result<(Self, u64)> {
        self.require_native_work(work)?;
        let reference = selection.checkpoint.as_ref()
            .ok_or_else(|| conversation_boundary(work, "history retirement requires a checkpoint pin"))?;
        let cut = selection.conversation_revision;
        if cut <= self.archived_through() || self.pending_user.is_some() {
            return self.clone_with_work_engine(work, retain);
        }
        crate::projection::validate_model_context_selection_at_revision_with_work(
            self, selection, cut, work)?;
        let mut temporary = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut calls = BTreeSet::new();
        for message in self.messages.iter().take_while(|message| message.sequence <= cut) {
            if let Some(io) = temporary.work().as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            match message.kind {
                MessageKind::ToolCall => {
                    if !calls.contains(&message.id) {
                        let bytes = std::mem::size_of::<Uuid>() as u64;
                        if let Some(io) = temporary.work().as_deref_mut() {
                            io.admit_owned(bytes)?;
                            io.retain_owned(bytes)?;
                        }
                        temporary.adopt(bytes)?;
                        calls.insert(message.id);
                    }
                }
                MessageKind::ToolResult => {
                    if let Some(parent) = message.reply_to && calls.remove(&parent) {
                        temporary.relinquish(std::mem::size_of::<Uuid>() as u64)?;
                    }
                }
                _ => {}
            }
        }
        if !calls.is_empty() {
            drop(calls);
            drop(temporary);
            return self.clone_with_work_engine(work, retain);
        }
        drop(calls);
        drop(temporary);
        let digest = self.history_prefix_digest_with_work(cut, work)?;
        let start = self.messages.partition_point(|message| message.sequence <= cut);
        let suffix = self.messages.get(start..)
            .ok_or_else(|| conversation_boundary(work, "history retirement cut is invalid"))?;
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut carrier = Self::default();
        let retained = carrier.native_custody.reserve(
            ConversationBuffer::Messages, &mut carrier.messages, suffix.len(), cache.work())?;
        cache.adopt(retained)?;
        for message in suffix {
            let before = cache.retained();
            let message = message.clone_in_quota(&mut cache)?;
            if let Some(retain) = retain.as_mut() {
                let produced = cache.retained() - before;
                retain(message.sequence, produced, cache.work())?;
                cache.transfer();
            }
            carrier.messages.push(message);
        }
        let retained = carrier.clone_archive_checkpoint_with_work(cut, digest, reference, cache.work())?;
        cache.adopt(retained)?;
        let wire = ConversationStateWire {
            agent: self.agent, messages: std::mem::take(&mut carrier.messages),
            archived_prefix: carrier.archived_prefix.take(),
        };
        let (state, retained) = Self::from_wire_with_work(
            wire, acyclic_stream::MAX_RECORD_BYTES as u64, cache.work())?;
        cache.adopt(retained)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let state = {
            let mut state = state;
            state.native_custody.messages = carrier.native_custody.messages.take();
            state.native_custody.archive = carrier.native_custody.archive.take();
            state
        };
        let retained = cache.retained();
        cache.transfer();
        Ok((state, retained))
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn history_prefix_digest_measured(
        &self, through_sequence: u64, io: &crate::filesystem::PreparationIo,
    ) -> Result<[u8; 32]> {
        if self.messages.last().map_or(0, |message| message.sequence) != self.messages.len() as u64
            || through_sequence > self.messages.len() as u64 {
            return Err(Error::Invalid("history fingerprint requires a dense logical prefix".into()));
        }
        if through_sequence == self.messages.len() as u64 { return Ok(self.history_digest); }
        let mut previous = [0; 32];
        for message in self.messages.iter().take(through_sequence as usize) {
            let encoded = io.encode_json(message, io.remaining()?.bytes_encoded, true)?;
            let hash = acyclic_fs::WorkCounters {
                bytes_hashed: encoded.len() as u64 + b"harness:conversation-prefix:v3\0".len() as u64 + 64,
                items_examined: 1, ..Default::default()
            };
            io.preflight(hash)?;
            io.charge(hash)?;
            let digest = blake3::hash(&encoded);
            let mut hash = blake3::Hasher::new();
            hash.update(b"harness:conversation-prefix:v3\0");
            hash.update(&previous);
            hash.update(digest.as_bytes());
            previous = *hash.finalize().as_bytes();
        }
        Ok(previous)
    }

    /// Rebuilds only a bounded, already attested selection for byte projection.
    /// This unbound sparse view cannot authorize append or stand in for history.
    pub(crate) fn selected_view(messages: Vec<ConversationMessage>) -> Result<Self> {
        Self::selected_view_with_work(messages, &mut None).map(|(state, _)| state)
    }

    pub(crate) fn selected_view_with_work(
        messages: Vec<ConversationMessage>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        // A query-scoped producer already owns the message payload and shell.
        // This same sparse rebuild owns its native caches intrinsically.
        Self::from_wire_with_work(ConversationStateWire {
            agent: None, messages, archived_prefix: None,
        }, acyclic_stream::MAX_RECORD_BYTES as u64, work)
    }

    fn prepare_turn_index(
        &mut self, visible: bool, outcome_parent: Option<Uuid>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        if visible && self.model_positions.len() == self.model_positions.capacity() {
            let target = self.model_positions.capacity().checked_mul(2)
                .map(|count| count.max(4))
                .ok_or_else(|| conversation_boundary(cache.work(),
                    "conversation position cache overflows"))?;
            let retained = self.native_custody.reserve(
                ConversationBuffer::Positions, &mut self.model_positions, target, cache.work())?;
            cache.adopt(retained)?;
        }
        if let Some(parent) = outcome_parent.filter(|parent| !self.outcomes.contains_key(parent)) {
            let retained = self.native_custody.retain_index(true, parent, cache.work())?;
            cache.adopt(retained)?;
        }
        let retained = cache.retained();
        cache.transfer();
        Ok(retained)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "only append-after-push and decode's in-bounds enumeration call this private indexer"
    )]
    fn index_turn_with_work(
        &mut self, position: usize,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        let message = &self.messages[position];
        let visible = model_visible(&message.kind);
        let outcome_parent = turn_outcome_parent(message);
        let retained = self.prepare_turn_index(visible, outcome_parent, work)?;
        self.index_turn(position);
        Ok(retained)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the caller pre-admits every index and passes a validated in-bounds message"
    )]
    fn index_turn(&mut self, position: usize) {
        let message = &self.messages[position];
        if model_visible(&message.kind) {
            self.model_positions.push(position);
        }
        let outcome = message.kind == MessageKind::System
            && message.extensions.contains_key("acyclic.turn.outcome");
        if let Some(parent) = message.reply_to {
            if (message.kind == MessageKind::Assistant || outcome)
                && self.pending_user == Some(parent)
            {
                self.pending_user = None;
            }
            if outcome {
                self.outcomes.entry(parent).or_insert(position);
            }
        }
    }

    pub(crate) fn unresolved_user(&self) -> Option<Uuid> {
        self.pending_user
    }

    pub(crate) fn turn_outcome(&self, user: Uuid) -> Option<&ConversationMessage> {
        self.outcomes
            .get(&user)
            .and_then(|position| self.messages.get(*position))
    }

    /// Loaded authoritative records, in sequence order. Mutation goes through `append`.
    #[must_use]
    pub fn messages(&self) -> &[ConversationMessage] {
        &self.messages
    }

    /// Reads a bounded archive page at a caller-pinned logical tail. Later appends
    /// are excluded; continue with the last returned sequence, preserving `through`.
    pub fn page(&self, after: u64, through: u64, maximum: usize) -> Result<&[ConversationMessage]> {
        let tail = self.logical_revision();
        if maximum == 0 || after < self.archived_through() || after > through || through > tail {
            return Err(Error::Invalid(
                "conversation page cursor or bound is invalid".into(),
            ));
        }
        let start = self
            .messages
            .partition_point(|message| message.sequence <= after);
        let end = self
            .messages
            .partition_point(|message| message.sequence <= through)
            .min(start.saturating_add(maximum));
        self.messages
            .get(start..end)
            .ok_or_else(|| Error::Invalid("conversation page range is invalid".into()))
    }

    /// Reads one identity using the incrementally maintained, rebuildable index.
    #[must_use]
    pub fn message(&self, id: Uuid) -> Option<&ConversationMessage> {
        self.by_id
            .get(&id)
            .and_then(|position| self.messages.get(*position))
    }

    pub(crate) fn message_position(&self, id: Uuid) -> Option<usize> {
        self.by_id.get(&id).copied()
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "partition bounds the position slice; entries are inserted only from validated message positions"
    )]
    pub(crate) fn recent_model_messages(
        &self,
        before_position: usize,
        maximum: usize,
    ) -> impl DoubleEndedIterator<Item = &ConversationMessage> {
        let end = self
            .model_positions
            .partition_point(|position| *position < before_position);
        self.model_positions[end.saturating_sub(maximum)..end]
            .iter()
            .map(|position| &self.messages[*position])
    }

    /// Returns one exact model-visible delta using the existing position index.
    /// Oversized deltas reject before iteration; no older history is silently dropped.
    #[expect(
        clippy::indexing_slicing,
        reason = "the position index is maintained by append/decode; partition bounds its slice"
    )]
    pub(crate) fn model_messages_between(
        &self,
        after: u64,
        through: u64,
        maximum: usize,
    ) -> Result<impl Iterator<Item = &ConversationMessage>> {
        if after < self.archived_through() || after > through || through > self.logical_revision() {
            return Err(Error::Invalid(
                "model history delta bounds are invalid".into(),
            ));
        }
        let start = self
            .model_positions
            .partition_point(|position| self.messages[*position].sequence <= after);
        let end = self
            .model_positions
            .partition_point(|position| self.messages[*position].sequence <= through);
        if end - start > maximum {
            return Err(Error::Invalid(
                "canonical history delta exceeds context limit".into(),
            ));
        }
        Ok(self.model_positions[start..end]
            .iter()
            .map(|position| &self.messages[*position]))
    }

    /// Binds an empty conversation to exactly one agent.
    pub fn bind(&mut self, agent: AgentId) -> Result<()> {
        if self.agent.is_some() || !self.messages.is_empty() {
            return Err(Error::Conflict("conversation is already bound".into()));
        }
        self.agent = Some(agent);
        Ok(())
    }

    /// Appends one ordered, validated message.
    pub fn append(&mut self, message: ConversationMessage) -> Result<()> {
        self.append_with_work(message, &mut None)
    }

    fn adopt_existing_metadata(
        &mut self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work.as_deref_mut().filter(|io| io.original_native_work().is_some()) {
            if self.native_custody.cache_carrier.as_ref().is_some_and(|carrier| carrier.bytes() != 0) {
                let no_data = io.take_native_custody(0)?;
                self.install_native_preclaimed_caches(no_data, &mut Some(&mut *io))?;
            }
            self.native_custody.adopt_existing_buffer::<ConversationMessage>(
                ConversationBuffer::Messages, self.messages.capacity(), io)?;
            self.native_custody.adopt_existing_buffer::<[u8; 32]>(
                ConversationBuffer::Digests, self.prefix_digests.capacity(), io)?;
            self.native_custody.adopt_existing_buffer::<usize>(
                ConversationBuffer::Positions, self.model_positions.capacity(), io)?;
            if self.native_custody.archive.is_none()
                && let Some(prefix) = &self.archived_prefix
            {
                let bytes = crate::core::source_inventory(prefix)?;
                io.admit_owned(bytes)?;
                io.retain_owned(bytes)?;
                self.native_custody.archive = Some(io.take_native_custody(bytes)?);
            }
            let count = self.by_id.len().checked_add(self.outcomes.len()).ok_or(
                Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "conversation source index count overflows" })?;
            if self.native_custody.indexes.len() != count {
                // Ordinary input promotion inventories its live keys once.
                // Native append checks counts only, never revisits old history.
                let mut native: Option<&mut dyn crate::store::history::HistoryIo> = Some(io);
                for id in self.by_id.keys() {
                    self.native_custody.retain_index(false, *id, &mut native)?;
                }
                for id in self.outcomes.keys() {
                    self.native_custody.retain_index(true, *id, &mut native)?;
                }
            }
        }
        let _ = work;
        Ok(())
    }

    pub(crate) fn append_with_work(
        &mut self, message: ConversationMessage,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.require_native_work(work)?;
        self.validate_append_with_work(&message, work)?;
        let history_digest = advance_history_digest_with_work(
            self.history_digest, &message, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
        self.adopt_existing_metadata(work)?;
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        let outcome_parent = turn_outcome_parent(&message)
            .filter(|parent| !self.outcomes.contains_key(parent));
        let preparation = (|| {
            if self.messages.len() == self.messages.capacity() {
                let target = self.messages.capacity().checked_mul(2).map(|count| count.max(4))
                    .ok_or_else(|| conversation_boundary(cache.work(),
                        "conversation message capacity overflows"))?;
                let retained = self.native_custody.reserve(
                    ConversationBuffer::Messages, &mut self.messages, target, cache.work())?;
                cache.adopt(retained)?;
            }
            if self.prefix_digests.len() == self.prefix_digests.capacity() {
                let target = self.prefix_digests.capacity().checked_mul(2).map(|count| count.max(4))
                    .ok_or_else(|| conversation_boundary(cache.work(),
                        "conversation digest cache overflows"))?;
                let retained = self.native_custody.reserve(
                    ConversationBuffer::Digests, &mut self.prefix_digests, target, cache.work())?;
                cache.adopt(retained)?;
            }
            let retained = self.prepare_turn_index(
                model_visible(&message.kind), outcome_parent, cache.work())?;
            cache.adopt(retained)?;
            let retained = self.native_custody.retain_index(false, message.id, cache.work())?;
            cache.adopt(retained)?;
            Ok(())
        })();
        if let Err(error) = preparation {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            {
                self.native_custody.indexes.remove(&(false, message.id));
                if let Some(parent) = outcome_parent {
                    self.native_custody.indexes.remove(&(true, parent));
                }
            }
            return Err(error);
        }
        // Foreign-owned refs may be carried globally. Their bytes are gated by
        // the provider's owner-mediated read grant at event admission/resolution.
        self.by_id.insert(message.id, self.messages.len());
        if message.kind == MessageKind::User {
            self.pending_user = Some(message.id);
        }
        self.messages.push(message);
        self.history_digest = history_digest;
        self.prefix_digests.push(history_digest);
        self.index_turn(self.messages.len() - 1);
        cache.transfer();
        Ok(())
    }

    /// Admits the next append through rebuildable identity indexes without
    /// copying or scanning the complete history.
    pub(crate) fn validate_append(&self, message: &ConversationMessage) -> Result<()> {
        self.validate_append_with_work(message, &mut None)
    }

    pub(crate) fn validate_append_with_work(
        &self, message: &ConversationMessage,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.agent
            .ok_or_else(|| conversation_conflict(work, "conversation is unbound"))?;
        message.validate_with_work(work)?;
        if self.messages.last().is_some_and(|last| {
            last.sequence
                != self
                    .archived_through()
                    .saturating_add(self.messages.len() as u64)
        }) {
            return Err(conversation_boundary(work, "sparse conversation history is read-only"));
        }
        let expected = self
            .logical_revision()
            .checked_add(1)
            .ok_or_else(|| conversation_boundary(work, "conversation sequence exhausted"))?;
        if message.sequence != expected || self.by_id.contains_key(&message.id) {
            return Err(conversation_conflict(work, "message sequence or identity is invalid"));
        }
        if let Some(parent) = message.reply_to
            && !self.by_id.contains_key(&parent)
        {
            return Err(conversation_boundary(work, "message reply target is missing"));
        }
        if message.kind == MessageKind::ToolResult
            && !message
                .reply_to
                .and_then(|id| self.message(id))
                .is_some_and(|prior| {
                    prior.kind == MessageKind::ToolCall
                        && Some(prior.id) == message.reply_to
                        && prior.tool_call_id.as_deref() == message.tool_call_id.as_deref()
                })
        {
            return Err(conversation_boundary(work, "tool result has no preceding call"));
        }
        // Foreign-owned refs may be carried globally. Their bytes are gated by
        // the provider's owner-mediated read grant at event admission/resolution.

        Ok(())
    }
}

#[cfg(test)]
std::thread_local! {
    static HISTORY_HASH_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn model_visible(kind: &MessageKind) -> bool {
    matches!(kind, MessageKind::System | MessageKind::User | MessageKind::Assistant
        | MessageKind::ToolCall | MessageKind::ToolResult)
}

fn turn_outcome_parent(message: &ConversationMessage) -> Option<Uuid> {
    (message.kind == MessageKind::System
        && message.extensions.contains_key("acyclic.turn.outcome"))
        .then_some(message.reply_to).flatten()
}

fn advance_history_digest_with_work(
    previous: [u8; 32], message: &ConversationMessage, maximum_encoded_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
    #[cfg(test)]
    HISTORY_HASH_CALLS.with(|calls| calls.set(calls.get() + 1));
    let digest = crate::contract::canonical_json_digest_with_work(
        message, maximum_encoded_bytes, work)?;
    if let Some(work) = work.as_deref_mut() {
        work.hashed_bytes(b"harness:conversation-prefix:v3\0".len() as u64 + 64)?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness:conversation-prefix:v3\0");
    hash.update(&previous);
    hash.update(&digest);
    Ok(*hash.finalize().as_bytes())
}

fn validate_label(value: &str, limit: usize) -> Result<()> {
    if label_is_invalid(value, limit) {
        return Err(Error::Invalid("file or volume label is invalid".into()));
    }
    Ok(())
}

fn label_is_invalid(value: &str, limit: usize) -> bool {
    value.is_empty()
        || value.len() > limit
        || value.chars().any(char::is_control)
        || value.contains('/')
        || value.contains('\\')
        || value == "."
        || value == ".."
}

fn validate_label_with_work(
    value: &str, limit: usize, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
    if label_is_invalid(value, limit) {
        return Err(conversation_boundary(work, "file or volume label is invalid"));
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn validate_label_native(value: &str, limit: usize) -> Result<()> {
    if label_is_invalid(value, limit) {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason: "file or volume label is invalid",
        });
    }
    Ok(())
}

/// Checks a normalized volume-relative content path at every provider boundary.
/// Backslashes, traversal segments, control characters, and oversized paths are rejected.
pub fn validate_content_path(path: &str) -> Result<()> {
    if !content_path_is_valid(path) {
        return Err(Error::Invalid("volume-relative path is invalid".into()));
    }
    Ok(())
}

fn content_path_is_valid(path: &str) -> bool {
    !(path.is_empty()
        || path.len() as u64 > MAX_PATH_BYTES as u64
        || path.starts_with('/')
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path.split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == ".."))
}

pub(crate) fn validate_content_path_with_work(
    path: &str, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
    if !content_path_is_valid(path) {
        return Err(conversation_boundary(work, "volume-relative path is invalid"));
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn validate_content_path_with_native_work(
    path: &str, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    work.remaining()?;
    if !content_path_is_valid(path) {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "volume-relative path is invalid",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_file_validation_and_descriptor_hash_use_original_static_denials() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
        let agent = AgentId::from_bytes([24; 16]);
        let mut source = file(agent, "native-validation.txt")?;
        let cancellation = CancellationToken::new();
        let native = NativeMaterializationWork::new(native_conversation_budget(), &cancellation)
            .expect("original validation ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("original validation observer");
        source.path = "../escape".into();
        let before = native.receipt();
        assert!(matches!(source.validate_with_work(&mut Some(&mut io)),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "volume-relative path is invalid" })));
        assert_eq!(native.receipt(), before, "validation denial allocates no diagnostic");
        source.path = "native-validation.txt".into();
        source.validate_with_work(&mut Some(&mut io))?;
        assert!(matches!(source.descriptor().verify_with_work(b"too long", &mut Some(&mut io)),
            Err(Error::NativeBoundary { reason: "file content does not match its descriptor", .. })));
        assert_eq!(native.receipt().bytes_hashed, 0, "original length short-circuit remains intact");
        assert!(matches!(source.descriptor().verify_with_work(b"other", &mut Some(&mut io)),
            Err(Error::NativeBoundary { reason: "file content does not match its descriptor", .. })));
        assert_eq!(native.receipt().bytes_hashed, 5);
        source.descriptor().verify_with_work(b"hello", &mut Some(&mut io))?;
        assert_eq!(native.receipt().bytes_hashed, 10);
        let mut allowance = native_conversation_budget();
        allowance.bytes_hashed = 0;
        let denied = NativeMaterializationWork::new(allowance, &cancellation).expect("hash denial ledger");
        let mut denied_io = crate::store::history::native_work_io(&denied).expect("hash denial observer");
        assert!(matches!(source.descriptor().verify_with_work(b"hello", &mut Some(&mut denied_io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "bytes_hashed", .. }), .. })));
        assert_eq!(denied.receipt().bytes_hashed, 0);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_file_clone_partial_failure_retires_only_its_actual_strings() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
        use crate::store::history::HistoryIo as _;
        let source = file(AgentId::from_bytes([29; 16]), "native-partial-clone.txt")?;
        let cancellation = CancellationToken::new();
        let mut budget = native_conversation_budget();
        budget.bytes_copied = 4 + source.volume().provider().namespace().len() as u64;
        let native = NativeMaterializationWork::new(budget, &cancellation).expect("partial clone ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("partial clone observer");
        let held = io.clone_text("held")?;
        let before = native.receipt();
        assert!(matches!(source.clone_with_work(&mut Some(&mut io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "bytes_copied", .. }), .. })));
        assert_eq!(native.receipt().bytes_copied - before.bytes_copied,
            source.volume().provider().namespace().len() as u64);
        let guard = io.into_custody();
        assert_eq!(guard.bytes(), held.capacity() as u64, "failed file fields are physically retired");
        drop(held);
        drop(guard);
        let available = budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        native.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_owned_file_wire_moves_original_strings_without_new_claims() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        use crate::store::history::HistoryIo as _;
        let source = file(AgentId::from_bytes([28; 16]), "native-owned-value.txt")?;
        let encoded = serde_json::to_vec(&source).map_err(|error| Error::Invalid(error.to_string()))?;
        let source_token = CancellationToken::new();
        let source_work = NativeMaterializationWork::new(native_conversation_budget(), &source_token)
            .expect("original input Value ledger");
        let mut source_io = crate::store::history::native_work_io(&source_work).expect("input decoder");
        let (value, retained) = source_io.decode_json_value(&encoded)?;
        let path = value.get("path").and_then(serde_json::Value::as_str)
            .expect("original path String").as_ptr();
        let namespace = value.get("volume").and_then(|volume| volume.get("provider"))
            .and_then(|provider| provider.get("namespace")).and_then(serde_json::Value::as_str)
            .expect("original provider String").as_ptr();
        let incoming = source_io.take_native_custody(retained)?;
        let source_receipt = source_work.receipt();
        source_token.cancel();
        let observer_token = CancellationToken::new();
        let observer = NativeMaterializationWork::new(native_conversation_budget(), &observer_token)
            .expect("independent consuming visitor ledger");
        let mut io = crate::store::history::native_work_io(&observer).expect("owned visitor observer");
        let before = observer.receipt();
        let file = FileRef::from_owned_value_with_work(value, &mut Some(&mut io))?;
        assert_eq!(file, source);
        assert_eq!(file.path().as_ptr(), path);
        assert_eq!(file.volume().provider().namespace().as_ptr(), namespace);
        assert_eq!(observer.receipt().allocation_operations, before.allocation_operations);
        assert_eq!(observer.receipt().bytes_copied, before.bytes_copied);
        assert_eq!(observer.receipt().bytes_encoded + observer.receipt().bytes_hashed, 0);
        assert!(observer.receipt().items_examined > before.items_examined);
        assert_eq!(source_work.receipt(), source_receipt);
        assert_eq!(io.into_custody().bytes(), 0, "the incoming guard, not a duplicate claim, owns Strings");
        drop(file);
        drop(incoming);
        assert_eq!(source_io.into_custody().bytes(), 0);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_file_field_clone_owns_actual_capacity_on_an_independent_ledger() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let mut source = file(AgentId::from_bytes([27; 16]), "native-field-clone.txt")?;
        let mut path = String::with_capacity(1_024);
        path.push_str(source.path());
        source.path = path;
        let mut media = String::with_capacity(512);
        media.push_str(source.descriptor().media_type());
        source.descriptor.media_type = media;
        assert!(source.logical_owned_bytes()? > source.logical_clone_owned_bytes()?);
        let budget = native_conversation_budget();
        let source_token = CancellationToken::new();
        let source_work = NativeMaterializationWork::new(budget, &source_token).expect("source ledger");
        let source_guard = source_work.retain_owned(source.logical_owned_bytes()?)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let source_receipt = source_work.receipt();
        source_token.cancel();
        let copy_token = CancellationToken::new();
        let copy_work = NativeMaterializationWork::new(budget, &copy_token).expect("independent copy ledger");
        let before = copy_work.receipt();
        let recipe = source.clone_work_recipe()?;
        let (copied, guard) = source.clone_with_native_work(&copy_work)?;
        assert_eq!(copied, source);
        assert_eq!(guard.bytes(), copied.logical_owned_bytes()?);
        assert!(guard.work().shares_ledger(&copy_work));
        assert_eq!(source_work.receipt(), source_receipt);
        assert_eq!(copy_work.receipt().allocation_operations - before.allocation_operations, recipe.0);
        assert_eq!(copy_work.receipt().bytes_copied - before.bytes_copied, recipe.1);
        assert_eq!(copy_work.receipt().bytes_hashed + copy_work.receipt().bytes_encoded, 0);
        drop(copied);
        drop(guard);
        let available = budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        copy_work.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        drop(source);
        drop(source_guard);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_reference_clones_deny_before_actual_payload_copy() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
        let content = file(AgentId::from_bytes([23; 16]), "native-clone-source.txt")?;
        let mut message = native_message(&content, 1);
        message.attachments = ReferencedAttachments::Inline { items: vec![Attachment {
            file: content.clone(), label: Some("caption".into()),
        }] };
        message.extensions.insert("acyclic.turn.outcome".into(), content.clone());
        for denied in ["allocation_operations", "bytes_copied"] {
            let mut budget = native_conversation_budget();
            if denied == "allocation_operations" { budget.allocation_operations = 1; }
            else { budget.bytes_copied = 0; }
            let cancellation = CancellationToken::new();
            let native = NativeMaterializationWork::new(budget, &cancellation).expect("original clone ledger");
            let prefix = native.receipt();
            let mut io = crate::store::history::native_work_io(&native).expect("original clone observer");
            assert!(matches!(content.clone_with_work(&mut Some(&mut io)),
                Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                    WorkError::BudgetExceeded { counter, .. }), .. }) if counter == denied));
            assert!(matches!(message.clone_with_work(&mut Some(&mut io)),
                Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                    WorkError::BudgetExceeded { counter, .. }), .. }) if counter == denied));
            assert_eq!(native.receipt().bytes_copied, prefix.bytes_copied);
            assert_eq!(native.receipt().allocation_operations, prefix.allocation_operations);
            assert!(matches!(content.clone_with_native_work(&native),
                Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                    WorkError::BudgetExceeded { counter, .. }), .. }) if counter == denied));
            assert_eq!(native.receipt().bytes_copied, prefix.bytes_copied);
            assert_eq!(native.receipt().allocation_operations, prefix.allocation_operations);
        }
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_buffer_try_reserve_failure_preserves_the_actual_old_owner() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError};
        let old_token = CancellationToken::new();
        let old = NativeMaterializationWork::new(native_conversation_budget(), &old_token)
            .expect("actual old backing ledger");
        let mut values = vec![[7_u8; 32]];
        let capacity = values.capacity();
        let mut owner = Some(old.retain_owned(native_conversation_buffer_bytes::<[u8; 32]>(capacity)?)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?);
        let before = old.receipt();
        old_token.cancel();
        let new_token = CancellationToken::new();
        let mut allowance = native_conversation_budget();
        allowance.peak_allocation_bytes = u64::MAX;
        let new = NativeMaterializationWork::new(allowance, &new_token)
            .expect("independent requested backing ledger");
        let mut io = crate::store::history::native_work_io(&new).expect("actual growth observer");
        // Rust's real Vec capacity check rejects a backing larger than isize::MAX.
        let target = isize::MAX as usize / std::mem::size_of::<[u8; 32]>() + 1;
        assert!(matches!(reserve_native_conversation_buffer(&mut values, &mut owner, target, &mut io),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::AllocationFailed(_),
                custody: None })));
        assert_eq!(values, [[7_u8; 32]]);
        assert_eq!(values.capacity(), capacity);
        assert!(owner.as_ref().expect("restored original backing").work().shares_ledger(&old));
        assert_eq!(old.receipt(), before);
        assert_eq!(io.into_custody().bytes(), 0);
        let available = allowance.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        new.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        drop(values);
        drop(owner);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_volume_constructor_copies_only_identity_fields_and_retires_failure() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let provider = ProviderRef::new("acyclic", "filesystem", "2")?;
        let agent = AgentId::from_bytes([33; 16]);
        for owner in [VolumeOwner::Project("project-root".into()), VolumeOwner::Session("session-root".into()),
            VolumeOwner::Agent(agent)] {
            let token = CancellationToken::new();
            let budget = native_conversation_budget();
            let native = NativeMaterializationWork::new(budget, &token).expect("volume constructor ledger");
            let before = native.receipt();
            let (volume, custody) = VolumeRef::new_with_native_work(
                &provider, "scratch", VolumeClass::AgentPrivate, &owner, &native)?;
            assert_eq!(volume, VolumeRef::new(provider.clone(), "scratch", VolumeClass::AgentPrivate, owner.clone())?);
            assert_eq!(custody.bytes(), volume.logical_owned_bytes()?);
            let owner_bytes = match &owner {
                VolumeOwner::Project(text) | VolumeOwner::Session(text) => text.len() as u64,
                VolumeOwner::Agent(_) => 0,
            };
            assert_eq!(native.receipt().bytes_copied - before.bytes_copied,
                (provider.namespace().len() + provider.family().len() + provider.version().len() + "scratch".len()) as u64
                    + owner_bytes);
            assert_eq!(native.receipt().bytes_encoded + native.receipt().bytes_hashed, 0);
            drop(volume);
            drop(custody);
            native.admit_retained(budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        }
        let owner = VolumeOwner::Agent(agent);
        let token = CancellationToken::new();
        let budget = native_conversation_budget();
        let native = NativeMaterializationWork::new(budget, &token).expect("invalid volume ledger");
        let before = native.receipt();
        assert!(matches!(VolumeRef::new_with_native_work(
            &provider, "/bad", VolumeClass::AgentPrivate, &owner, &native), Err(Error::NativeBoundary { .. })));
        assert_eq!(native.receipt(), before, "original validation rejects before any payload allocation or copy");
        let token = CancellationToken::new();
        let mut budget = native_conversation_budget();
        budget.bytes_copied = provider.namespace().len() as u64;
        let native = NativeMaterializationWork::new(budget, &token).expect("partial volume ledger");
        assert!(matches!(VolumeRef::new_with_native_work(
            &provider, "scratch", VolumeClass::AgentPrivate, &owner, &native), Err(Error::NativeWork { .. })));
        assert_eq!(native.receipt().bytes_copied, provider.namespace().len() as u64);
        native.admit_retained(budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_message_partial_field_copy_retires_actual_content_and_attachment_buffer() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        use crate::store::history::HistoryIo;
        let content = file(AgentId::from_bytes([32; 16]), "native-partial-message.txt")?;
        let mut source = native_message(&content, 1);
        source.attachments = ReferencedAttachments::Inline {
            items: vec![Attachment { file: content.clone(), label: None }],
        };
        let content_bytes = content.clone_work_recipe()?.1;
        let token = CancellationToken::new();
        let mut budget = native_conversation_budget();
        budget.bytes_copied = 4 + content_bytes;
        let native = NativeMaterializationWork::new(budget, &token).expect("partial message ledger");
        let mut io = crate::store::history::native_work_io(&native).expect("partial message observer");
        let held = io.clone_text("held")?;
        assert!(matches!(source.clone_with_work(&mut Some(&mut io)), Err(Error::NativeWork { .. })));
        assert_eq!(native.receipt().bytes_copied, 4 + content_bytes);
        let guard = io.into_custody();
        assert_eq!(guard.bytes(), held.capacity() as u64, "failed message releases only its new fields and Vec backing");
        drop(held);
        drop(guard);
        native.admit_retained(budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_message_field_clone_returns_actual_deep_payload_custody() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let content = file(AgentId::from_bytes([30; 16]), "native-message-copy.txt")?;
        let mut source = native_message(&content, 1);
        source.attachments = ReferencedAttachments::Inline { items: vec![Attachment {
            file: content.clone(), label: Some("actual label".into()),
        }] };
        source.extensions.insert("example.owned".into(), content);
        let cancellation = CancellationToken::new();
        let budget = native_conversation_budget();
        let native = NativeMaterializationWork::new(budget, &cancellation).expect("message copy ledger");
        let before = native.receipt();
        let recipe = source.clone_work_recipe()?;
        let (copied, guard) = source.clone_with_native_work(&native)?;
        assert_eq!(copied, source);
        let ReferencedAttachments::Inline { items } = &copied.attachments else { unreachable!() };
        let mut actual = copied.content.logical_owned_bytes()?
            + (items.capacity() * std::mem::size_of::<Attachment>()) as u64;
        for item in items {
            actual += item.file.logical_owned_bytes()?
                + item.label.as_ref().map_or(0, |label| label.capacity() as u64);
        }
        for (key, file) in &copied.extensions {
            actual += std::mem::size_of::<(String, FileRef)>() as u64
                + key.capacity() as u64 + file.logical_owned_bytes()?;
        }
        assert_eq!(guard.bytes(), actual, "message Self and outer messageVec slots are not claimed");
        assert_eq!(native.receipt().allocation_operations - before.allocation_operations, recipe.0);
        assert_eq!(native.receipt().bytes_copied - before.bytes_copied, recipe.1);
        assert_eq!(native.receipt().bytes_encoded + native.receipt().bytes_hashed, 0);
        drop(copied);
        drop(guard);
        native.admit_retained(budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_state_clone_and_suffix_transfer_only_actual_message_components() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        use crate::store::history::HistoryIo;
        let agent = AgentId::from_bytes([31; 16]);
        let content = file(agent, "native-component-copy.txt")?;
        let mut source = ConversationState::default();
        source.bind(agent)?;
        for sequence in 1..=4 { source.append(native_message(&content, sequence))?; }
        let first_token = CancellationToken::new();
        let budget = native_conversation_budget();
        let first = NativeMaterializationWork::new(budget, &first_token).expect("snapshot copy ledger");
        let mut io = crate::store::history::native_work_io(&first).expect("snapshot copy observer");
        let recipe = source.native_clone_copy_recipe()?;
        let before = first.receipt().bytes_copied;
        let (held, produced) = source.clone_with_work_and_custody(&mut Some(&mut io))?;
        assert_eq!(held, source);
        assert_eq!(held.native_unclaimed_message_buffer_bytes()?, 0);
        assert_eq!(held.native_unclaimed_cache_bytes()?, 0);
        assert_eq!(first.receipt().bytes_copied - before, recipe.1);
        let deep = io.into_custody();
        assert_eq!(deep.bytes(), produced, "only observer-owned deep payload is reported");
        let original_receipt = first.receipt();
        first_token.cancel();
        let selection = ModelContextSelection {
            conversation_revision: 2, message_ids: vec![Uuid::from_u128(1), Uuid::from_u128(2)],
            checkpoint: Some(file(agent, "native-component-checkpoint.json")?),
        };
        let second_token = CancellationToken::new();
        let second = NativeMaterializationWork::new(budget, &second_token).expect("independent suffix ledger");
        let mut io = crate::store::history::native_work_io(&second).expect("suffix observer");
        let mut components = Vec::with_capacity(2);
        let mut retain = |sequence, bytes, work: &mut Option<&mut dyn HistoryIo>| -> Result<()> {
            assert!(components.len() < components.capacity());
            let guard = work.as_deref_mut().expect("same suffix observer").take_native_custody(bytes)?;
            components.push((sequence, guard));
            Ok(())
        };
        let (suffix, produced) = held.checkpointed_suffix_with_work_and_message_custody(
            &selection, &mut Some(&mut io), &mut retain)?;
        drop(retain);
        assert_eq!(produced, 0);
        assert_eq!(io.into_custody().bytes(), 0);
        assert_eq!(components.iter().map(|(sequence, _)| *sequence).collect::<Vec<_>>(), [3, 4]);
        assert!(components.iter().all(|(_, guard)| guard.work().shares_ledger(&second)));
        assert_eq!(first.receipt(), original_receipt, "cancelled source ledger is neither rebound nor absorbed");
        assert_eq!(suffix.history_digest, source.history_digest);
        assert_eq!(suffix.native_unclaimed_message_buffer_bytes()?, 0);
        assert_eq!(suffix.native_unclaimed_archived_prefix_bytes()?, 0);
        assert_eq!(suffix.native_unclaimed_cache_bytes()?, 0);
        drop(suffix);
        drop(components);
        second.admit_retained(budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes())
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        drop(held);
        drop(deep);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_conversation_budget() -> acyclic_fs::WorkBudget {
        acyclic_fs::WorkBudget {
            allocation_operations: 100_000, peak_allocation_bytes: 16_777_216,
            bytes_copied: 16_777_216, bytes_encoded: 16_777_216,
            bytes_hashed: 16_777_216, items_examined: 16_777_216,
            ..acyclic_fs::WorkBudget::default()
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_message(content: &FileRef, sequence: u64) -> ConversationMessage {
        ConversationMessage {
            id: Uuid::from_u128(u128::from(sequence)), sequence,
            kind: if sequence % 2 == 1 { MessageKind::User } else { MessageKind::Assistant },
            content: content.clone(),
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: (sequence % 2 == 0).then(|| Uuid::from_u128(u128::from(sequence - 1))),
            tool_call_id: None, extensions: BTreeMap::new(),
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_cache_transfer_keeps_source_and_metadata_ledgers_independent() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
        let agent = AgentId::from_bytes([26; 16]);
        let content = file(agent, "native-cache-transfer.txt")?;
        let mut state = ConversationState::default();
        state.bind(agent)?;
        for sequence in 1..=3 { state.append(native_message(&content, sequence))?; }
        let source_token = CancellationToken::new();
        let metadata_token = CancellationToken::new();
        let budget = native_conversation_budget();
        let source = NativeMaterializationWork::new(budget, &source_token).expect("source data ledger");
        let metadata = NativeMaterializationWork::new(budget, &metadata_token).expect("metadata ledger");
        let bytes = state.native_unclaimed_cache_bytes()?;
        let allocation = source.retain_owned(bytes).expect("original source cache preclaim");
        let prefix = source.receipt();
        source_token.cancel();
        let mut io = crate::store::history::native_work_io(&metadata).expect("independent metadata observer");
        state.install_native_preclaimed_caches(allocation, &mut Some(&mut io))?;
        assert_eq!(source.receipt(), prefix, "source transfer is pure despite source cancellation");
        assert_eq!(metadata.receipt().bytes_hashed + metadata.receipt().bytes_copied, 0);
        assert_eq!(state.native_unclaimed_cache_bytes()?, 0);
        let slot = state.native_custody.indexes.get(&(false, Uuid::from_u128(1))).expect("source slot");
        assert!(slot.work().shares_ledger(&source));
        assert!(slot._metadata.work().shares_ledger(&metadata));
        assert_eq!(io.into_custody().bytes(), 0);
        drop(state);
        let available = budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        metadata.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;

        let mut state = ConversationState::default();
        state.bind(agent)?;
        for sequence in 1..=3 { state.append(native_message(&content, sequence))?; }
        let source_token = CancellationToken::new();
        let source = NativeMaterializationWork::new(budget, &source_token).expect("partial source ledger");
        let bytes = state.native_unclaimed_cache_bytes()?;
        let allocation = source.retain_owned(bytes).expect("partial source preclaim");
        let mut denied_budget = budget;
        denied_budget.allocation_operations = 1;
        let denied = NativeMaterializationWork::new(denied_budget, &metadata_token).expect("metadata denial ledger");
        let mut denied_io = crate::store::history::native_work_io(&denied).expect("metadata denial observer");
        assert!(matches!(state.install_native_preclaimed_caches(allocation, &mut Some(&mut denied_io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "allocation_operations", .. }), .. })));
        assert!(state.native_custody.cache_carrier.as_ref().expect("remaining source custody").bytes() > 0);
        assert!(source.admit_retained(available).is_err(), "partial failure still owns actual source caches");
        let mut io = crate::store::history::native_work_io(&metadata).expect("independent retry metadata observer");
        use crate::store::history::HistoryIo as _;
        let no_data = io.take_native_custody(0)?;
        state.install_native_preclaimed_caches(no_data, &mut Some(&mut io))?;
        assert_eq!(state.native_unclaimed_cache_bytes()?, 0);
        assert_eq!(io.into_custody().bytes(), 0);
        drop(state);
        source.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        metadata.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_append_adopts_existing_spare_buffers_once_then_replaces_their_owner() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let agent = AgentId::from_bytes([25; 16]);
        let content = file(agent, "native-spare-backing.txt")?;
        let mut state = ConversationState::default();
        state.bind(agent)?;
        for sequence in 1..=3 { state.append(native_message(&content, sequence))?; }
        let cancellation = CancellationToken::new();
        let budget = native_conversation_budget();
        let first = NativeMaterializationWork::new(budget, &cancellation).expect("adoption ledger");
        let second = NativeMaterializationWork::new(budget, &cancellation).expect("replacement ledger");
        let mut io = crate::store::history::native_work_io(&first).expect("original adoption observer");
        state.append_with_work(native_message(&content, 4), &mut Some(&mut io))?;
        assert!(state.native_custody.messages.as_ref().expect("existing spare backing guard")
            .work().shares_ledger(&first));
        assert_eq!(state.native_custody.indexes.len(), state.by_id.len());
        assert_eq!(io.into_custody().bytes(), 0);
        let mut io = crate::store::history::native_work_io(&second).expect("independent growth observer");
        state.append_with_work(native_message(&content, 5), &mut Some(&mut io))?;
        assert!(state.native_custody.messages.as_ref().expect("replacement backing guard")
            .work().shares_ledger(&second));
        assert!(state.native_custody.indexes.get(&(false, Uuid::from_u128(1)))
            .expect("original logical source slot").work().shares_ledger(&first));
        assert_eq!(io.into_custody().bytes(), 0);
        drop(state);
        let available = budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        first.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        second.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_append_and_retirement_keep_only_live_independent_components() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let cancellation = CancellationToken::new();
        let budget = native_conversation_budget();
        let first = NativeMaterializationWork::new(budget, &cancellation).expect("first original ledger");
        let second = NativeMaterializationWork::new(budget, &cancellation).expect("independent original ledger");
        let agent = AgentId::from_bytes([21; 16]);
        let content = file(agent, "native-history.txt")?;
        let checkpoint = file(agent, "native-checkpoint.json")?;
        let mut state = ConversationState::default();
        state.bind(agent)?;
        HISTORY_HASH_CALLS.with(|calls| calls.set(0));
        for sequence in 1..=64_u64 {
            let ledger = if (sequence - 1) / 4 % 2 == 0 { &first } else { &second };
            let mut message = native_message(&content, sequence);
            if sequence % 8 == 0 {
                message.kind = MessageKind::System;
                message.extensions.insert("acyclic.turn.outcome".into(), content.clone());
            }
            let expected = crate::contract::canonical_json_bytes(&message)?.len() as u64
                + b"harness:conversation-prefix:v3\0".len() as u64 + 64;
            let before = ledger.receipt().bytes_hashed;
            let mut io = crate::store::history::native_work_io(ledger).expect("original observer");
            state.append_with_work(message, &mut Some(&mut io))?;
            assert_eq!(ledger.receipt().bytes_hashed - before, expected,
                "append hashes only its original message, never the resident prefix");
            assert!(state.native_custody.indexes
                .get(&(false, Uuid::from_u128(u128::from(sequence)))).expect("actual source identity slot")
                .work().shares_ledger(ledger));
            assert_eq!(io.into_custody().bytes(), 0, "state, not the query, owns cache custody");
            if sequence % 8 == 0 {
                assert!(state.native_custody.indexes
                    .get(&(true, Uuid::from_u128(u128::from(sequence - 1))))
                    .expect("actual outcome source slot").work().shares_ledger(ledger));
                let selection = ModelContextSelection {
                    conversation_revision: sequence,
                    message_ids: vec![Uuid::from_u128(u128::from(sequence - 1)),
                        Uuid::from_u128(u128::from(sequence))],
                    checkpoint: Some(checkpoint.clone()),
                };
                let digest = state.history_prefix_digest(sequence)?;
                let mut io = crate::store::history::native_work_io(ledger).expect("retirement observer");
                let suffix = state.checkpointed_suffix_with_work(&selection, &mut Some(&mut io))?;
                state = suffix;
                assert!(state.messages.is_empty());
                assert!(state.by_id.is_empty() && state.outcomes.is_empty());
                assert!(state.native_custody.indexes.is_empty());
                assert_eq!(state.history_prefix_digest(sequence)?, digest);
                assert_eq!(io.into_custody().bytes(), 0);
            }
        }
        assert_eq!(HISTORY_HASH_CALLS.with(std::cell::Cell::get), 64);
        assert!(matches!(state.clone_with_work(&mut None), Err(Error::NativeBoundary { .. })));
        let available = budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        assert!(second.admit_retained(available).is_err(), "the live archived checkpoint keeps custody");
        drop(state);
        first.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        second.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_append_failed_hash_and_growth_preserve_actual_state() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork, NativeTreeMaterializationError, WorkError};
        let cancellation = CancellationToken::new();
        let budget = native_conversation_budget();
        let funded = NativeMaterializationWork::new(budget, &cancellation).expect("state ledger");
        let agent = AgentId::from_bytes([22; 16]);
        let content = file(agent, "native-failed-growth.txt")?;
        let mut state = ConversationState::default();
        state.bind(agent)?;
        for sequence in 1..=4 {
            let mut io = crate::store::history::native_work_io(&funded).expect("original observer");
            state.append_with_work(native_message(&content, sequence), &mut Some(&mut io))?;
        }
        let head = state.history_digest;
        let capacities = (state.messages.capacity(), state.prefix_digests.capacity(),
            state.model_positions.capacity());
        let mut denied_budget = budget;
        denied_budget.bytes_hashed = 0;
        let denied = NativeMaterializationWork::new(denied_budget, &cancellation).expect("hash denial ledger");
        let mut io = crate::store::history::native_work_io(&denied).expect("hash denial observer");
        assert!(matches!(state.append_with_work(native_message(&content, 5), &mut Some(&mut io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "bytes_hashed", .. }), .. })));
        assert_eq!(denied.receipt().bytes_hashed, 0);
        drop(io);
        assert_eq!(state.history_digest, head);
        assert_eq!(state.messages.len(), 4);
        assert_eq!(capacities, (state.messages.capacity(), state.prefix_digests.capacity(),
            state.model_positions.capacity()));
        // Pay the exact original hash first; the next allocation is the original
        // messages Vec growth, not a substitute encoder or synthetic charge.
        let probe = NativeMaterializationWork::new(budget, &cancellation).expect("original hash control");
        let mut probe_io = crate::store::history::native_work_io(&probe).expect("original hash observer");
        advance_history_digest_with_work(head, &native_message(&content, 5),
            acyclic_stream::MAX_RECORD_BYTES as u64, &mut Some(&mut probe_io))?;
        denied_budget = budget;
        denied_budget.allocation_operations = probe.receipt().allocation_operations;
        drop(probe_io);
        let denied = NativeMaterializationWork::new(denied_budget, &cancellation).expect("growth denial ledger");
        let mut io = crate::store::history::native_work_io(&denied).expect("growth denial observer");
        assert!(matches!(state.append_with_work(native_message(&content, 5), &mut Some(&mut io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "allocation_operations", .. }), .. })));
        assert!(denied.receipt().bytes_hashed > 0, "the original message hash completed before growth");
        drop(io);
        assert_eq!(state.history_digest, head);
        assert_eq!(state.messages.len(), 4);
        assert!(!state.by_id.contains_key(&Uuid::from_u128(5)));
        assert_eq!(capacities, (state.messages.capacity(), state.prefix_digests.capacity(),
            state.model_positions.capacity()));
        assert_eq!(state.native_custody.messages.as_ref().expect("live backing").bytes(),
            state.messages.capacity() as u64 * std::mem::size_of::<ConversationMessage>() as u64);
        drop(state);
        let available = budget.peak_allocation_bytes - NativeMaterializationWork::accounting_state_bytes();
        funded.admit_retained(available).map_err(|cause| Error::NativeWork { cause, custody: None })?;
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    #[test]
    fn admitted_conversation_rebuild_observes_original_digests_and_negative_work() -> Result<()> {
        use acyclic_fs::{CancellationToken, WorkBudget};
        use crate::store::history::HistoryWork;
        let agent = AgentId::from_bytes([8; 16]);
        let content = file(agent, "observed-rebuild.txt")?;
        let mut original = ConversationState::default();
        original.bind(agent)?;
        for (sequence, kind) in [(1_u64, MessageKind::User),
            (2, MessageKind::Assistant), (3, MessageKind::System)] {
            let mut extensions = BTreeMap::new();
            if sequence == 3 {
                extensions.insert("acyclic.turn.outcome".into(), content.clone());
            }
            original.append(ConversationMessage {
                id: Uuid::from_u128(u128::from(sequence)), sequence, kind,
                content: content.clone(),
                attachments: ReferencedAttachments::Inline { items: Vec::new() },
                reply_to: (sequence > 1).then(|| Uuid::from_u128(1)),
                tool_call_id: None, extensions,
            })?;
        }
        let bytes = crate::contract::canonical_json_bytes(&original)?;
        let expected_hash_bytes = original.messages().iter().try_fold(0_u64, |total, message| {
            total.checked_add(crate::contract::canonical_json_bytes(message)?.len() as u64)
                .and_then(|total| total.checked_add(b"harness:conversation-prefix:v3\0".len() as u64 + 64))
                .ok_or_else(|| Error::Invalid("test digest input overflow".into()))
        })?;
        let budget = WorkBudget {
            bytes_copied: 1_048_576, bytes_encoded: 1_048_576,
            bytes_hashed: expected_hash_bytes, items_examined: 100_000,
            allocation_operations: 100_000, peak_allocation_bytes: 1_048_576,
            ..WorkBudget::default()
        };
        let cancellation = CancellationToken::new();
        let wire: ConversationStateWire = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let mut work = HistoryWork::new(budget, &cancellation);
        HISTORY_HASH_CALLS.with(|calls| calls.set(0));
        let rebuilt = ConversationState::from_wire_with_work(
            wire, bytes.len() as u64, &mut Some(&mut work));
        let receipt = work.finish(rebuilt).map_err(|failure| failure.error)?;
        assert_eq!(receipt.value.0, original);
        assert_eq!(receipt.work.bytes_hashed, expected_hash_bytes);
        assert_eq!(HISTORY_HASH_CALLS.with(std::cell::Cell::get), 3);
        assert!(receipt.value.1 >= 3 * std::mem::size_of::<[u8; 32]>() as u64);
        assert!(receipt.work.bytes_encoded > 0);
        let mut denied = budget;
        denied.bytes_hashed = 0;
        let mut work = HistoryWork::new(denied, &cancellation);
        let wire = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let result = ConversationState::from_wire_with_work(
            wire, bytes.len() as u64, &mut Some(&mut work));
        let failed = work.finish(result).err().expect("first original message hash must reject");
        assert_eq!(failed.work.bytes_hashed, 0);
        assert!(failed.work.bytes_encoded > 0);
        assert_eq!(failed.work.output_bytes, 0);
        cancellation.cancel();
        let mut work = HistoryWork::new(budget, &cancellation);
        let wire = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let result = ConversationState::from_wire_with_work(
            wire, bytes.len() as u64, &mut Some(&mut work));
        let failed = work.finish(result).err().expect("cancelled reconstruction must not start");
        assert_eq!(failed.work.bytes_encoded + failed.work.bytes_hashed, 0);
        Ok(())
    }

    #[test]
    fn checkpoint_suffix_preserves_logical_head_digest_and_next_turn() -> Result<()> {
        let agent = AgentId::new();
        let content = file(agent, "history.txt")?;
        let checkpoint = file(agent, "checkpoint.json")?;
        let mut state = ConversationState::default();
        state.bind(agent)?;
        for sequence in 1..=1_000_u64 {
            state.append(ConversationMessage {
                id: Uuid::from_u128(u128::from(sequence)),
                sequence,
                kind: if sequence % 2 == 1 {
                    MessageKind::User
                } else {
                    MessageKind::Assistant
                },
                content: content.clone(),
                attachments: ReferencedAttachments::Inline { items: Vec::new() },
                reply_to: (sequence % 2 == 0).then(|| Uuid::from_u128(u128::from(sequence - 1))),
                tool_call_id: None,
                extensions: BTreeMap::new(),
            })?;
        }
        let digest = state.history_prefix_digest(1_000)?;
        let selection = ModelContextSelection {
            conversation_revision: 998,
            message_ids: vec![Uuid::from_u128(997), Uuid::from_u128(998)],
            checkpoint: Some(checkpoint.clone()),
        };
        HISTORY_HASH_CALLS.with(|calls| calls.set(0));
        let suffix = state.checkpointed_suffix_with_work(&selection, &mut None)?;
        assert_eq!(suffix.messages().len(), 2);
        assert_eq!(suffix.logical_revision(), 1_000);
        assert_eq!(suffix.resident_after_sequence(), 998);
        assert_eq!(suffix.history_prefix_digest(1_000)?, digest);
        assert_eq!(HISTORY_HASH_CALLS.with(std::cell::Cell::get), 2);
        assert!(suffix.history_prefix_digest(997).is_err());
        assert!(suffix.page(0, 1_000, 10).is_err());
        assert!(suffix.model_messages_between(0, 1_000, 10).is_err());
        assert_eq!(suffix.page(998, 1_000, 10)?.len(), 2);
        assert!(suffix.checkpoint_covers_archived_prefix(&checkpoint, 998));
        assert!(!suffix.checkpoint_covers_archived_prefix(&content, 998));
        let bytes = crate::contract::canonical_json_bytes(&suffix)?;
        let mut restored: ConversationState =
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(restored, suffix);
        assert_eq!(restored.history_prefix_digest(1_000)?, digest);
        let next = ConversationMessage {
            id: Uuid::from_u128(1_001),
            sequence: 1_001,
            kind: MessageKind::User,
            content,
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: None,
            extensions: BTreeMap::new(),
        };
        restored.append(next)?;
        assert_eq!(restored.logical_revision(), 1_001);
        assert_eq!(restored.messages().len(), 3);
        assert_eq!(restored.unresolved_user(), Some(Uuid::from_u128(1_001)));
        assert_eq!(restored.checkpointed_suffix_with_work(&selection, &mut None)?, restored);
        Ok(())
    }

    #[test]
    fn logical_file_ownership_counts_retained_metadata_capacity() -> Result<()> {
        let padded = |text: &str, capacity| {
            let mut value = String::with_capacity(capacity);
            value.push_str(text);
            value
        };
        let provider = ProviderRef::new("harness", "filesystem", "v1")?;
        let volume = VolumeRef::new(provider, padded("volume", 128),
            VolumeClass::AgentPrivate, VolumeOwner::Agent(AgentId::new()))?;
        let file = FileRef::new(volume, padded("file.txt", 256), padded("generation", 512),
            FileDescriptor::new([0; 32], 0, padded("text/plain", 1024))?,
            padded("file.txt", 2048))?;
        let expected = file.volume.logical_owned_bytes()? + file.path.capacity() as u64
            + file.version.capacity() as u64 + file.display_name.capacity() as u64
            + file.descriptor.media_type.capacity() as u64;
        assert_eq!(file.logical_owned_bytes()?, expected);
        let copied = file.volume.provider.namespace().len() + file.volume.provider.family().len()
            + file.volume.provider.version().len() + file.volume.id.len() + file.path.len()
            + file.version.len() + file.display_name.len() + file.descriptor.media_type.len();
        assert_eq!(file.logical_clone_owned_bytes()?, copied as u64);
        assert!(file.logical_clone_owned_bytes()? < file.logical_owned_bytes()?);
        let clone = file.clone();
        assert_eq!(clone.logical_clone_owned_bytes()?, file.logical_clone_owned_bytes()?);
        assert!(file.logical_owned_bytes()? > file.path.len() as u64 + file.version.len() as u64
            + file.display_name.len() as u64 + file.descriptor.media_type.len() as u64);
        Ok(())
    }

    #[test]
    fn full_history_fork_fingerprint_is_incremental_and_rebuilds_after_decode() -> Result<()> {
        let agent = AgentId::new();
        let content = file(agent, "fingerprint.txt")?;
        let mut state = ConversationState::default();
        state.bind(agent)?;
        HISTORY_HASH_CALLS.with(|calls| calls.set(0));
        for sequence in 1..=10_000_u64 {
            state.append(ConversationMessage {
                id: Uuid::from_u128(u128::from(sequence)),
                sequence,
                kind: if sequence % 2 == 0 {
                    MessageKind::Interaction
                } else {
                    MessageKind::User
                },
                content: content.clone(),
                attachments: ReferencedAttachments::Inline { items: Vec::new() },
                reply_to: None,
                tool_call_id: None,
                extensions: BTreeMap::new(),
            })?;
        }
        assert_eq!(HISTORY_HASH_CALLS.with(std::cell::Cell::get), 10_000);
        let authority = crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: "fingerprint-parent".into(),
        };
        HISTORY_HASH_CALLS.with(|calls| calls.set(0));
        let pinned = crate::fork::InheritedConversationPrefix::select(
            authority.clone(),
            10_001,
            10_000,
            &[],
            &state,
        )?;
        for _ in 0..32 {
            assert_eq!(
                crate::fork::InheritedConversationPrefix::select(
                    authority.clone(),
                    10_001,
                    10_000,
                    &[],
                    &state
                )?,
                pinned
            );
        }
        assert_eq!(HISTORY_HASH_CALLS.with(std::cell::Cell::get), 0);
        assert_eq!(pinned.format_version, 3);
        let old = state.history_prefix_digest(2)?;
        assert_eq!(HISTORY_HASH_CALLS.with(std::cell::Cell::get), 0);
        let encoded = crate::contract::canonical_json_bytes(&state)?;
        let mut reopened: ConversationState =
            serde_json::from_slice(&encoded).map_err(|error| Error::Invalid(error.to_string()))?;
        HISTORY_HASH_CALLS.with(|calls| calls.set(0));
        assert_eq!(
            reopened.history_prefix_digest(10_000)?,
            pinned.message_digest
        );
        assert_eq!(HISTORY_HASH_CALLS.with(std::cell::Cell::get), 0);
        let mut next = reopened
            .messages()
            .last()
            .ok_or_else(|| Error::Invalid("fixture history is empty".into()))?
            .clone();
        next.sequence += 1;
        next.id = Uuid::from_u128(10_001);
        reopened.append(next.clone())?;
        assert_ne!(
            reopened.history_prefix_digest(10_001)?,
            pinned.message_digest
        );
        assert_eq!(
            reopened.history_prefix_digest(10_000)?,
            pinned.message_digest
        );
        assert_eq!(reopened.history_prefix_digest(2)?, old);
        let before = reopened.history_prefix_digest(10_001)?;
        assert!(reopened.append(next).is_err());
        assert_eq!(reopened.history_prefix_digest(10_001)?, before);
        let sparse = ConversationState::selected_view(vec![
            reopened
                .messages()
                .last()
                .ok_or_else(|| Error::Invalid("fixture history is empty".into()))?
                .clone(),
        ])?;
        assert!(sparse.history_prefix_digest(1).is_err());
        assert!(state.history_prefix_digest(10_001).is_err());
        Ok(())
    }

    #[test]
    fn pending_turn_rebuilds_without_retaining_settled_turn_identities() -> Result<()> {
        fn append(
            state: &mut ConversationState,
            content: &FileRef,
            kind: MessageKind,
            reply_to: Option<Uuid>,
            terminal: bool,
        ) -> Result<Uuid> {
            let sequence = state.messages().len() as u64 + 1;
            let id = Uuid::from_u128(u128::from(sequence));
            let mut extensions = BTreeMap::new();
            if terminal {
                extensions.insert("acyclic.turn.outcome".into(), content.clone());
            }
            state.append(ConversationMessage {
                id,
                sequence,
                kind,
                content: content.clone(),
                attachments: ReferencedAttachments::Inline { items: Vec::new() },
                reply_to,
                tool_call_id: None,
                extensions,
            })?;
            Ok(id)
        }

        fn reopen(state: &ConversationState) -> Result<ConversationState> {
            let bytes = crate::contract::canonical_json_bytes(state)?;
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))
        }

        let agent = AgentId::new();
        let content = file(agent, "pending.txt")?;
        let mut state = ConversationState::default();
        state.bind(agent)?;
        let mut previous = None;
        for _ in 0..256 {
            let user = append(&mut state, &content, MessageKind::User, None, false)?;
            assert_eq!(state.unresolved_user(), Some(user));
            if let Some(previous) = previous {
                // A delayed older response must not settle the current user.
                append(
                    &mut state,
                    &content,
                    MessageKind::Assistant,
                    Some(previous),
                    false,
                )?;
                assert_eq!(state.unresolved_user(), Some(user));
            }
            append(
                &mut state,
                &content,
                MessageKind::Assistant,
                Some(user),
                false,
            )?;
            assert_eq!(state.unresolved_user(), None);
            previous = Some(user);
        }
        assert_eq!(reopen(&state)?, state);
        let user = append(&mut state, &content, MessageKind::User, None, false)?;
        append(&mut state, &content, MessageKind::System, Some(user), false)?;
        assert_eq!(state.unresolved_user(), Some(user));
        assert_eq!(reopen(&state)?, state);
        let outcome = append(&mut state, &content, MessageKind::System, Some(user), true)?;
        assert_eq!(state.unresolved_user(), None);
        assert_eq!(
            state.turn_outcome(user).map(|message| message.id),
            Some(outcome)
        );
        assert_eq!(reopen(&state)?, state);
        let next = append(&mut state, &content, MessageKind::User, None, false)?;
        assert_eq!(state.unresolved_user(), Some(next));
        assert_eq!(reopen(&state)?, state);
        Ok(())
    }

    #[test]
    fn indexed_history_rebuilds_and_pinned_pages_exclude_later_appends() -> Result<()> {
        let agent = AgentId::new();
        let mut state = ConversationState::default();
        state.bind(agent)?;
        let content = file(agent, "history.txt")?;
        for sequence in 1..=65 {
            state.append(ConversationMessage {
                id: Uuid::new_v4(),
                sequence,
                kind: MessageKind::User,
                content: content.clone(),
                attachments: ReferencedAttachments::Inline { items: Vec::new() },
                reply_to: None,
                tool_call_id: None,
                extensions: Default::default(),
            })?;
        }
        let pinned = 64;
        let bytes =
            serde_json::to_vec(&state).map_err(|error| Error::Invalid(error.to_string()))?;
        let reopened: ConversationState =
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(reopened, state);
        let mut after = 0;
        let mut visited = Vec::new();
        loop {
            let page = reopened.page(after, pinned, 7)?;
            if page.is_empty() {
                break;
            }
            assert!(page.len() <= 7);
            for message in page {
                assert_eq!(reopened.message(message.id), Some(message));
                visited.push(message.sequence);
            }
            after = page
                .last()
                .ok_or_else(|| Error::Invalid("empty page".into()))?
                .sequence;
        }
        assert_eq!(visited, (1..=pinned).collect::<Vec<_>>());
        assert!(reopened.page(0, 66, 7).is_err());
        assert!(reopened.page(0, pinned, 0).is_err());
        let mut corrupt =
            serde_json::to_value(&state).map_err(|error| Error::Invalid(error.to_string()))?;
        corrupt["messages"][1]["id"] = corrupt["messages"][0]["id"].clone();
        assert!(serde_json::from_value::<ConversationState>(corrupt).is_err());
        Ok(())
    }

    #[test]
    fn failed_append_preserves_indexes_and_sparse_decode_cannot_append() -> Result<()> {
        let agent = AgentId::new();
        let mut state = ConversationState::default();
        state.bind(agent)?;
        let mut candidate = ConversationMessage {
            id: Uuid::new_v4(),
            sequence: 1,
            kind: MessageKind::User,
            content: file(agent, "history.txt")?,
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: Some(Uuid::new_v4()),
            tool_call_id: None,
            extensions: Default::default(),
        };
        let before = state.clone_with_work(&mut None)?;
        assert!(state.append(candidate.clone()).is_err());
        assert_eq!(state, before);
        assert!(state.message(candidate.id).is_none());
        candidate.reply_to = None;
        state.append(candidate.clone())?;
        assert_eq!(state.message(candidate.id), Some(&candidate));
        let mut wire =
            serde_json::to_value(&state).map_err(|error| Error::Invalid(error.to_string()))?;
        wire["messages"][0]["sequence"] = 3.into();
        let mut sparse: ConversationState =
            serde_json::from_value(wire).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(sparse.page(0, 3, 1)?.len(), 1);
        candidate.id = Uuid::new_v4();
        candidate.sequence = 2;
        let before = sparse.clone_with_work(&mut None)?;
        assert!(sparse.append(candidate).is_err());
        assert_eq!(sparse, before);
        Ok(())
    }

    #[test]
    fn owner_bound_private_write_rejects_an_attached_agent_with_a_copied_capability() -> Result<()>
    {
        let owner = AgentId::from_bytes([1; 16]);
        let attached = AgentId::from_bytes([2; 16]);
        let private = file(owner, "scratch/note.txt")?.volume().clone();
        let write = private.capability(VolumeOperation::Write)?;
        let issuer = crate::core::AuthorityIssuer::new(
            "owner",
            [7; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Agent,
                id: owner.to_string(),
            },
        );
        let read = private.capability(VolumeOperation::Read)?;
        let scope = issuer.root_for_agent(
            owner,
            "write",
            crate::Capabilities::new([write.clone(), read.clone()]),
        );
        ContentGrant::verify(&issuer.verifier(), &scope, &private, VolumeOperation::Write)?;
        let unsigned = issuer.root(
            "copied-capability",
            crate::Capabilities::new([write.clone()]),
        );
        assert!(
            ContentGrant::verify(
                &issuer.verifier(),
                &unsigned,
                &private,
                VolumeOperation::Write
            )
            .is_err()
        );
        let inherited =
            issuer.attenuate(&scope, "child", crate::Capabilities::new([write.clone()]))?;
        assert_eq!(inherited.agent(), Some(owner));
        ContentGrant::verify(
            &issuer.verifier(),
            &inherited,
            &private,
            VolumeOperation::Write,
        )?;
        let foreign_issuer = crate::core::AuthorityIssuer::new(
            "attached",
            [8; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Agent,
                id: attached.to_string(),
            },
        );
        let copied =
            foreign_issuer.root_for_agent(attached, "copied", crate::Capabilities::new([write]));
        assert!(
            ContentGrant::verify(
                &foreign_issuer.verifier(),
                &copied,
                &private,
                VolumeOperation::Write
            )
            .is_err()
        );
        let exact_file = file(owner, "scratch/note.txt")?;
        let delegated =
            issuer.delegate_private_file_read(&scope, attached, "attached-read", &exact_file)?;
        assert_eq!(delegated.agent(), Some(attached));
        ContentGrant::verify_file_read(&issuer.verifier(), &delegated, &exact_file)?;
        let internal = file(owner, ".system/execution/state.json")?;
        assert!(matches!(
            ContentGrant::verify_read(&issuer.verifier(), &scope, &internal),
            Err(Error::Unauthorized(_))
        ));
        let internal_exact = issuer.root_for_agent(
            owner,
            "internal-exact",
            crate::Capabilities::new([internal.read_capability()?]),
        );
        assert!(matches!(
            ContentGrant::verify_file_read(&issuer.verifier(), &internal_exact, &internal),
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            issuer.delegate_private_file_read(&scope, attached, "internal", &internal),
            Err(Error::Unauthorized(_))
        ));
        let inherited_context = file(owner, ".system/inherited-conversation/prefix.json")?;
        ContentGrant::verify_read(&issuer.verifier(), &scope, &inherited_context)?;
        let directory = issuer.delegate_private_directory_read(
            &scope,
            attached,
            "attached-directory",
            &private,
            "scratch",
        )?;
        let directory_grant = ContentGrant::verify_directory_read(
            &issuer.verifier(),
            &directory,
            &private,
            "scratch",
        )?;
        directory_grant.require_directory_path(&private, "scratch/deeper")?;
        directory_grant.require_file_read(&exact_file)?;
        assert!(
            directory_grant
                .require_directory_path(&private, "scratchpad")
                .is_err()
        );
        assert!(
            directory_grant
                .require_directory_path(&private, ".system")
                .is_err()
        );
        assert!(
            directory_grant
                .require(&private, VolumeOperation::Write)
                .is_err()
        );
        let foreign_owner_scope = foreign_issuer.root_for_agent(
            attached,
            "copied-read",
            crate::Capabilities::new([read]),
        );
        assert!(
            foreign_issuer
                .delegate_private_file_read(&foreign_owner_scope, attached, "invalid", &exact_file)
                .is_err()
        );
        Ok(())
    }

    fn file(agent: AgentId, path: &str) -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("acyclic", "filesystem", "2")?,
                "scratch",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            )?,
            path,
            "version-1",
            FileDescriptor::from_bytes(b"hello", "text/plain")?,
            "text.txt",
        )
    }

    #[test]
    fn descriptor_checks_exact_bytes() -> Result<()> {
        let descriptor = FileDescriptor::from_bytes(b"", "application/octet-stream")?;
        descriptor.verify(b"")?;
        assert!(descriptor.verify(b"x").is_err());
        let encoded =
            serde_json::to_value(&descriptor).map_err(|e| Error::Invalid(e.to_string()))?;
        let decoded: FileDescriptor =
            serde_json::from_value(encoded).map_err(|e| Error::Invalid(e.to_string()))?;
        assert_eq!(decoded, descriptor);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn native_descriptor_verification_preserves_actual_hash_work_and_static_denials() -> Result<()> {
        let descriptor = FileDescriptor::from_bytes(b"exact", "text/plain")?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED, &cancellation,
        ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        descriptor.verify_with_native_work(b"exact", &work)?;
        assert_eq!(work.receipt().bytes_hashed, 5);
        assert!(matches!(descriptor.verify_with_native_work(b"longer", &work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        assert_eq!(work.receipt().bytes_hashed, 5, "length mismatch executes no digest");
        assert!(matches!(descriptor.verify_with_native_work(b"wrong", &work),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        assert_eq!(work.receipt().bytes_hashed, 10);
        assert_eq!(work.live_allocation_bytes(), baseline);
        cancellation.cancel();
        assert!(matches!(descriptor.verify_with_native_work(b"exact", &work),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None })));
        assert_eq!(work.receipt().bytes_hashed, 10);
        let mut budget = acyclic_fs::WorkBudget::UNBOUNDED;
        budget.bytes_hashed = 0;
        let denied = acyclic_fs::NativeMaterializationWork::new(
            budget, &acyclic_fs::CancellationToken::new(),
        ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        assert!(matches!(descriptor.verify_with_native_work(b"exact", &denied),
            Err(Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Work(_), custody: None })));
        assert_eq!(denied.receipt().bytes_hashed, 0);
        assert_eq!(denied.receipt().backend_read_operations, 0);
        Ok(())
    }

    #[test]
    fn configured_limits_reject_oversized_references() -> Result<()> {
        Limits::default().validate_file(&file(AgentId::new(), &"x".repeat(4_097))?)?;
        let file = file(AgentId::new(), "message.txt")?;
        let mut limits = Limits {
            file_bytes: 4,
            ..Limits::default()
        };
        limits.validate()?;
        assert!(limits.validate_file(&file).is_err());
        limits.file_bytes = 5;
        limits.validate_file(&file)?;
        limits.model_events_per_step = 1;
        limits.validate()?;
        limits.attachments = 0;
        assert!(limits.validate().is_err());
        Limits {
            attachments: 65_537,
            model_steps: 1_000_001,
            model_events_per_step: 1_000_001,
            tool_calls_per_step: 1_000_001,
            context_messages: 1_000_001,
            ..Limits::default()
        }
        .validate()?;
        Ok(())
    }

    #[test]
    fn paths_and_ownership_are_validated_on_decode() -> Result<()> {
        let agent = AgentId::new();
        for path in ["/absolute", "../escape", "a//b", "a/./b", "a\\b"] {
            assert!(file(agent, path).is_err(), "{path}");
        }
        let mut encoded = serde_json::to_value(file(agent, "dir/a.txt")?)
            .map_err(|e| Error::Invalid(e.to_string()))?;
        encoded["path"] = serde_json::json!("../escape");
        assert!(serde_json::from_value::<FileRef>(encoded).is_err());
        Ok(())
    }

    #[test]
    fn conversation_enforces_binding_order_and_call_links() -> Result<()> {
        let agent = AgentId::new();
        let mut state = ConversationState::default();
        state.bind(agent)?;
        assert!(state.bind(agent).is_err());
        let call = ConversationMessage {
            id: Uuid::new_v4(),
            sequence: 1,
            kind: MessageKind::ToolCall,
            content: file(agent, "call.txt")?,
            attachments: Vec::new().into(),
            reply_to: None,
            tool_call_id: Some("call-1".into()),
            extensions: BTreeMap::new(),
        };
        state.append(call.clone())?;
        assert!(state.append(call.clone()).is_err());
        let result = ConversationMessage {
            id: Uuid::new_v4(),
            sequence: 2,
            kind: MessageKind::ToolResult,
            content: file(agent, "result.txt")?,
            attachments: Vec::new().into(),
            reply_to: Some(call.id),
            tool_call_id: Some("call-1".into()),
            extensions: BTreeMap::new(),
        };
        state.append(result)?;
        assert_eq!(state.messages.len(), 2);
        Ok(())
    }

    #[test]
    fn v2_conversation_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/conversation-message.json").trim();
        let message: ConversationMessage =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        message.validate()?;
        let encoded =
            serde_json::to_string(&message).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    #[test]
    fn every_v2_conversation_kind_round_trips_and_replays() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/conversation-kinds.json").trim();
        let messages: Vec<ConversationMessage> =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        let mut state = ConversationState::default();
        state.bind(AgentId::from_bytes([1; 16]))?;
        for message in &messages {
            state.append(message.clone())?;
        }
        assert_eq!(state.messages.len(), 9);
        let encoded =
            serde_json::to_string(&messages).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    #[test]
    fn v2_file_reference_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/file-ref.json").trim();
        let reference: FileRef =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        let encoded =
            serde_json::to_string(&reference).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    #[test]
    fn v2_file_reference_security_cases_match_the_shared_fixture() -> Result<()> {
        let base: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/v2/file-ref.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
        let corpus: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/v2/file-ref-security-cases.json"))
                .map_err(|error| Error::Invalid(error.to_string()))?;
        let cases = corpus["cases"]
            .as_array()
            .ok_or_else(|| Error::Invalid("security cases array missing".into()))?;
        assert_eq!(cases.len(), 13);
        for case in cases {
            let name = case["name"]
                .as_str()
                .ok_or_else(|| Error::Invalid("security case name missing".into()))?;
            let pointer = case["pointer"]
                .as_str()
                .ok_or_else(|| Error::Invalid("security case JSON pointer missing".into()))?;
            let (parent, key) = pointer
                .rsplit_once('/')
                .ok_or_else(|| Error::Invalid("security case pointer segment missing".into()))?;
            let mut modified = base.clone();
            let parent = modified
                .pointer_mut(parent)
                .ok_or_else(|| Error::Invalid("security fixture parent missing".into()))?
                .as_object_mut()
                .ok_or_else(|| Error::Invalid("security fixture parent is not an object".into()))?;
            parent.insert(key.to_owned(), case["value"].clone());
            if case["check"] == "decode" {
                assert!(
                    serde_json::from_value::<FileRef>(modified).is_err(),
                    "{name}"
                );
            } else {
                assert_eq!(case["check"], "bytes", "{name}");
                let reference: FileRef = serde_json::from_value(modified)
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                assert!(reference.descriptor().verify(b"hello").is_err(), "{name}");
            }
        }
        Ok(())
    }

    #[test]
    fn v2_private_directory_page_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/private-directory-page.json").trim();
        let page: PrivateDirectoryPage =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        page.validate()?;
        let encoded =
            serde_json::to_string(&page).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    #[test]
    fn v2_task_outcome_fixture_round_trips_canonically() -> Result<()> {
        let fixture = include_str!("../fixtures/v2/task-outcome.json").trim();
        let outcome: TaskOutcomeRecord =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        outcome.validate()?;
        let encoded =
            serde_json::to_string(&outcome).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(encoded, fixture);
        Ok(())
    }

    #[tokio::test]
    async fn composite_manifest_admission_routes_members_to_exact_providers() -> Result<()> {
        struct StaticContent {
            file: FileRef,
            bytes: Vec<u8>,
        }
        impl ContentResidencyVerifier for StaticContent {
            fn verify<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<()>> {
                Box::pin(async move {
                    if reference == &self.file {
                        Ok(())
                    } else {
                        Err(Error::NotFound("file".into()))
                    }
                })
            }
            fn read<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
                Box::pin(async move {
                    if reference != &self.file {
                        return Err(Error::NotFound("file".into()));
                    }
                    Ok(self.bytes.clone())
                })
            }
            fn load_manifest<'a>(
                &'a self,
                _reference: &'a FileRef,
                _item_count: u32,
            ) -> BoxFuture<'a, Result<Vec<Attachment>>> {
                // A provider's parsed list must not replace the pinned bytes.
                Box::pin(async { Ok(Vec::new()) })
            }
        }
        let owner = VolumeOwner::Agent(AgentId::from_bytes([1; 16]));
        let manifest_provider = ProviderRef::new("test", "filesystem", "a")?;
        let member_provider = ProviderRef::new("test", "filesystem", "b")?;
        let member = FileRef::new(
            VolumeRef::new(
                member_provider.clone(),
                "private",
                VolumeClass::AgentPrivate,
                owner.clone(),
            )?,
            "attachments/member.txt",
            "one",
            FileDescriptor::from_bytes(&vec![b'x'; 1_024], "text/plain")?,
            "member.txt",
        )?;
        let items = vec![Attachment {
            file: member.clone(),
            label: None,
        }];
        let bytes =
            serde_json::to_vec(&items).map_err(|error| Error::Invalid(error.to_string()))?;
        let manifest = FileRef::new(
            VolumeRef::new(
                manifest_provider.clone(),
                "private",
                VolumeClass::AgentPrivate,
                owner,
            )?,
            "attachments/list.json",
            "one",
            FileDescriptor::from_bytes(&bytes, "application/vnd.acyclic.harness.attachments+json")?,
            "list.json",
        )?;
        let first = Arc::new(StaticContent {
            file: manifest.clone(),
            bytes,
        });
        let second = Arc::new(StaticContent {
            file: member.clone(),
            bytes: vec![b'x'; 1_024],
        });
        let composite = CompositeContentVerifier::new(vec![
            (manifest.volume().clone(), first.clone()),
            (member.volume().clone(), second.clone()),
        ])?;
        composite
            .verify_manifest(&manifest, 1, &Limits::default())
            .await?;
        let mut narrow = Limits::default();
        narrow.file_bytes = manifest.descriptor().byte_length().saturating_add(1);
        narrow.render_bytes = narrow.file_bytes;
        assert!(narrow.file_bytes < member.descriptor().byte_length());
        assert!(matches!(
            composite.verify_manifest(&manifest, 1, &narrow).await,
            Err(Error::Invalid(_))
        ));
        let incomplete =
            CompositeContentVerifier::new(vec![(manifest.volume().clone(), first.clone())])?;
        assert!(matches!(
            incomplete
                .verify_manifest(&manifest, 1, &Limits::default())
                .await,
            Err(Error::Unsupported(_))
        ));
        struct StaticMount {
            volume: VolumeRef,
            reader: Arc<dyn ContentResidencyVerifier>,
            grant: ContentGrant,
        }
        impl ContentMountResolver for StaticMount {
            fn mount<'a>(
                &'a self,
                reference: &'a FileRef,
            ) -> ContentFuture<'a, Result<ContentMount>> {
                Box::pin(async move {
                    if reference.volume() != &self.volume {
                        return Err(Error::Unauthorized("foreign volume was not granted".into()));
                    }
                    Ok((self.grant.clone(), self.reader.clone()))
                })
            }
        }
        let issuer = crate::core::AuthorityIssuer::new(
            "mount-owner",
            [18; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Agent,
                id: AgentId::from_bytes([1; 16]).to_string(),
            },
        );
        let scope = issuer.root_for_agent(
            AgentId::from_bytes([1; 16]),
            "exact-member",
            crate::Capabilities::new([member.read_capability()?]),
        );
        let grant = ContentGrant::verify_file_read(&issuer.verifier(), &scope, &member)?;
        let lazy = CompositeContentVerifier::new(vec![(manifest.volume().clone(), first)])?
            .with_mount_resolver(Arc::new(StaticMount {
                volume: member.volume().clone(),
                reader: second,
                grant,
            }));
        lazy.verify_manifest(&manifest, 1, &Limits::default())
            .await?;
        let unauthorized = FileRef::new(
            member.volume().clone(),
            "attachments/another.txt",
            "one",
            FileDescriptor::from_bytes(b"another", "text/plain")?,
            "another.txt",
        )?;
        assert!(matches!(
            lazy.verify(&unauthorized).await,
            Err(Error::Unauthorized(_))
        ));
        let same_provider_member = FileRef::new(
            VolumeRef::new(
                manifest_provider,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
            )?,
            "attachments/other-owner.txt",
            "one",
            FileDescriptor::from_bytes(b"other-owner", "text/plain")?,
            "other-owner.txt",
        )?;
        let same_provider_items = vec![Attachment {
            file: same_provider_member.clone(),
            label: None,
        }];
        let same_provider_bytes = serde_json::to_vec(&same_provider_items)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let same_provider_manifest = FileRef::new(
            manifest.volume().clone(),
            "attachments/other-owner-list.json",
            "one",
            FileDescriptor::from_bytes(
                &same_provider_bytes,
                "application/vnd.acyclic.harness.attachments+json",
            )?,
            "other-owner-list.json",
        )?;
        let same_provider = CompositeContentVerifier::new(vec![
            (
                same_provider_manifest.volume().clone(),
                Arc::new(StaticContent {
                    file: same_provider_manifest.clone(),
                    bytes: same_provider_bytes,
                }),
            ),
            (
                same_provider_member.volume().clone(),
                Arc::new(StaticContent {
                    file: same_provider_member,
                    bytes: b"other-owner".to_vec(),
                }),
            ),
        ])?;
        same_provider
            .verify_manifest(&same_provider_manifest, 1, &Limits::default())
            .await?;
        Ok(())
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod native_grant_controls {
    use super::*;
    use crate::core::{AggregateKind, Authority, AuthorityIssuer};
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};

    fn budget() -> WorkBudget {
        WorkBudget {
            bytes_copied: 1_048_576, bytes_encoded: 1_048_576,
            bytes_hashed: 1_048_576, items_examined: 1024,
            allocation_operations: 1024, peak_allocation_bytes: 1_048_576,
            ..WorkBudget::default()
        }
    }

    fn owner() -> Result<(AuthorityIssuer, Scope, VolumeRef)> {
        let agent = AgentId::from_bytes([71; 16]);
        let volume = VolumeRef::new(
            ProviderRef::new("native-grant-control", "filesystem", "2")?,
            "private-control", VolumeClass::AgentPrivate, VolumeOwner::Agent(agent),
        )?;
        let issuer = AuthorityIssuer::new("native-grant-owner", [72; 32], Authority {
            kind: AggregateKind::Task, id: "native-grant-task".into(),
        });
        let scope = issuer.root_for_agent(agent, "native-grant-scope", crate::Capabilities::new([
            volume.capability(VolumeOperation::Read)?,
            volume.capability(VolumeOperation::Write)?,
        ]));
        Ok((issuer, scope, volume))
    }

    #[test]
    fn native_volume_grant_keeps_its_real_backing_until_data_drop() -> Result<()> {
        let (issuer, scope, volume) = owner()?;
        let verifier = issuer.verifier();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("finite original grant ledger");
        let baseline = work.remaining().expect("original quota").peak_allocation_bytes;
        let grant = ContentGrant::verify_with_native_work(
            &verifier, &scope, &volume, VolumeOperation::Write, &work,
        )?;
        grant.0.require(&volume, VolumeOperation::Write)?;
        assert!(grant.1.work().shares_ledger(&work));
        assert!(grant.1.bytes() >= std::mem::size_of::<ContentGrant>() as u64);
        assert!(work.remaining().expect("retained grant").peak_allocation_bytes < baseline);
        drop(grant);
        assert_eq!(work.remaining().expect("physically dropped grant").peak_allocation_bytes, baseline);
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        Ok(())
    }

    #[test]
    fn native_directory_grant_is_segment_bounded_and_never_writable() -> Result<()> {
        let (issuer, parent, volume) = owner()?;
        let child = issuer.delegate_private_directory_read(
            &parent, AgentId::from_bytes([73; 16]), "attached-directory", &volume, "private",
        )?;
        let verifier = issuer.verifier();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("finite original directory ledger");
        let baseline = work.remaining().expect("original quota").peak_allocation_bytes;
        let grant = ContentGrant::verify_directory_read_with_native_work(
            &verifier, &child, &volume, "private", &work,
        )?;
        grant.0.require_directory_path(&volume, "private/nested")?;
        assert!(grant.0.require_directory_path(&volume, "private-other").is_err());
        assert!(grant.0.require(&volume, VolumeOperation::Write).is_err());
        assert!(matches!(ContentGrant::verify_with_native_work(
            &verifier, &child, &volume, VolumeOperation::Write, &work,
        ), Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        drop(grant);
        assert_eq!(work.remaining().expect("dropped directory grant").peak_allocation_bytes, baseline);
        Ok(())
    }

    #[test]
    fn native_grant_cancellation_precedes_metadata_allocations() -> Result<()> {
        let (issuer, scope, volume) = owner()?;
        let verifier = issuer.verifier();
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(budget(), &cancellation)
            .expect("finite cancellable grant ledger");
        cancellation.cancel();
        let before = work.receipt().allocation_operations;
        assert!(matches!(ContentGrant::verify_with_native_work(
            &verifier, &scope, &volume, VolumeOperation::Read, &work,
        ), Err(Error::NativeWork { .. })));
        assert!(matches!(ContentGrant::verify_directory_read_with_native_work(
            &verifier, &scope, &volume, "", &work,
        ), Err(Error::NativeWork { .. })));
        assert_eq!(work.receipt().allocation_operations, before);
        assert_eq!(work.receipt().backend_read_operations, 0);
        Ok(())
    }
}
