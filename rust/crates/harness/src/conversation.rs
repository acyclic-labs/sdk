//! Canonical, payload-free conversation and file references.

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

/// Maximum normalized UTF-8 path length admitted by the Harness protocol.
pub const MAX_PATH_BYTES: usize = MAX_PORTABLE_COUNT;
/// Maximum UTF-8 bytes in a protocol label or display name.
pub const MAX_LABEL_BYTES: usize = crate::COMPONENT_LABEL_MAX_BYTES;
/// Largest integer that can be represented exactly by a JavaScript number.
pub const MAX_EXACT_JS_INTEGER: u64 = (1_u64 << 53) - 1;
#[allow(clippy::cast_precision_loss, reason = "2^53 - 1 is exact in f64")]
const MAX_EXACT_JS_FLOAT: f64 = MAX_EXACT_JS_INTEGER as f64;

/// Whether a JavaScript Number round-trips this value exactly: finite, not negative zero, and not an integral value beyond the safe-integer range.
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
/// Largest count representable by both this platform and the numeric wire format.
pub const MAX_PORTABLE_COUNT: usize =
    usize::MAX >> usize::BITS.saturating_sub(f64::MANTISSA_DIGITS);
/// Representable conversation page allowance; the caller chooses its budget.
pub const MAX_CONVERSATION_PAGE_MESSAGES: usize = u32::MAX as usize;
/// Default conversation read batch, replaceable by the caller.
pub const DEFAULT_CONVERSATION_PAGE_MESSAGES: u32 = 1_024;

/// Admission and rendering bounds.
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
            file_bytes: MAX_EXACT_JS_INTEGER,
            path_bytes: u32::MAX as usize,
            attachments: u32::MAX as usize,
            render_bytes: MAX_EXACT_JS_INTEGER,
            model_steps: u32::MAX as usize,
            model_events_per_step: u32::MAX as usize,
            tool_calls_per_step: u32::MAX as usize,
            context_messages: u32::MAX as usize,
        }
    }
}

impl Limits {
    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if [self.file_bytes, self.render_bytes].into_iter()
            .any(|limit| limit == 0 || limit > MAX_EXACT_JS_INTEGER)
            || [self.path_bytes, self.attachments, self.model_steps, self.model_events_per_step,
                self.tool_calls_per_step, self.context_messages].into_iter()
                .any(|limit| limit == 0 || limit as u64 > MAX_PORTABLE_COUNT as u64) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "harness limits are invalid"));
        }
        Ok(())
    }

    /// Checks an immutable file against these limits.
    pub fn validate_file(
        &self, file: &FileRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.validate(work)?;
        file.validate(work)?;
        if file.path().len() > self.path_bytes || file.descriptor().byte_length() > self.file_bytes
        {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "file exceeds harness limits"));
        }
        Ok(())
    }

    /// Checks every directly named file and attachment count before admission.
    pub fn validate_message(&self, message: &ConversationMessage, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        message.validate(work)?;
        self.validate_file(&message.content, work)?;
        match &message.attachments {
            ReferencedAttachments::Inline { items } => {
                if items.len() > self.attachments {
                    return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "attachments exceed harness limits"));
                }
                for item in items { self.validate_file(&item.file, work)?; }
            }
            ReferencedAttachments::Manifest { manifest, item_count } => {
                if *item_count as usize > self.attachments {
                    return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "attachments exceed harness limits"));
                }
                self.validate_file(manifest, work)?;
            }
        }
        for file in message.extensions.values() { self.validate_file(file, work)?; }
        Ok(())
    }
}

/// The three independently governed file namespaces.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema)]
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
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum VolumeOwner {
    /// Project workspace owner.
    Project(#[serde(deserialize_with = "deserialize_label")] String),
    /// Agent private-volume owner.
    Agent(#[schemars(with = "String")] AgentId),
    /// Shared session-volume owner.
    Session(#[serde(deserialize_with = "deserialize_label")] String),
}

/// Globally addressable volume identity with no embedded capability or endpoint.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct VolumeRef {
    provider: ProviderRef,
    #[serde(deserialize_with = "deserialize_label")]
    id: String,
    class: VolumeClass,
    owner: VolumeOwner,
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

    /// Constructs a volume from owned identity fields.
    pub fn new(
        provider: ProviderRef,
        id: String,
        class: VolumeClass,
        owner: VolumeOwner,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let value = Self { provider, id, class, owner };
        value.validate(work)?;
        Ok(value)
    }

    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let Self { provider, id, owner, .. } = self;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            inspect_native_metadata(native, 0)?;
            provider.validate_with_native_work(native)?;
        }
        if let Some(reason) = provider.validation_error() {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, reason));
        }
        validate_label(id, work)?;
        match owner {
            VolumeOwner::Project(id) | VolumeOwner::Session(id) => {
                validate_label(id, work)?;
            }
            VolumeOwner::Agent(_) => {}
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

    /// Enforces owner-only agent mutations.
    pub(crate) fn require_writer(&self, agent: Option<AgentId>,
        work: &Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        if let VolumeOwner::Agent(owner) = self.owner() && agent != Some(*owner) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Unauthorized,
                "only the owning agent may write this volume"));
        }
        Ok(())
    }

    /// Exact capability for one operation on this volume.
    pub fn capability(&self, operation: VolumeOperation, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<String> {
        content_label(operation.capability_prefix(), crate::contract::canonical_json_digest_with_work(self, u64::MAX, work)?, work)
    }

    /// Copies identity fields under the optional work observer.
    pub fn copy(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if work.is_none() { return Ok(self.clone()); }
        self.validate(work)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let volume = self.clone_in_quota(&mut quota)?;
        quota.transfer();
        Ok(volume)
    }

    pub(crate) fn clone_in_quota(
        &self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        let provider = crate::core::copy_provider(&self.provider, quota)?;
        Ok(Self {
            provider,
            id: crate::core::copy_text(&self.id, quota)?,
            class: self.class,
            owner: match &self.owner {
                VolumeOwner::Project(text) => VolumeOwner::Project(crate::core::copy_text(text, quota)?),
                VolumeOwner::Session(text) => VolumeOwner::Session(crate::core::copy_text(text, quota)?),
                VolumeOwner::Agent(id) => VolumeOwner::Agent(*id),
            },
        })
    }

    /// Owner-issued read capability for a directory and its descendants.
    pub fn directory_read_capability(&self, prefix: &str, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<String> {
        if !prefix.is_empty() { validate_content_path(prefix, work)?; }
        if is_internal_path(prefix) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "internal storage paths cannot be delegated"));
        }
        content_label("directory:read:", crate::contract::canonical_json_digest_with_work(&(self, prefix), u64::MAX, work)?, work)
    }

    /// Collision-resistant physical namespace for Filesystem adapters.
    pub fn storage_name(&self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<String> {
        content_label("harness-", crate::contract::canonical_json_digest_with_work(self, u64::MAX, work)?, work)
    }

}

fn label_bytes(prefix: &str, digest: [u8; 32]) -> ([u8; 80], usize) {
    let hash = blake3::Hash::from_bytes(digest).to_hex();
    let mut label = [0_u8; 80];
    let length = prefix.len() + hash.len();
    label[..prefix.len()].copy_from_slice(prefix.as_bytes());
    label[prefix.len()..length].copy_from_slice(hash.as_bytes());
    (label, length)
}

fn content_label(prefix: &str, digest: [u8; 32], work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<String> {
    let (label, length) = label_bytes(prefix, digest);
    let text = std::str::from_utf8(&label[..length]).expect("ASCII identity label");
    match work.as_deref_mut() { Some(io) => io.clone_text(text), None => Ok(text.to_owned()) }
}

fn contains_content_label(scope: &Scope, prefix: &str, digest: [u8; 32]) -> bool {
    let (label, length) = label_bytes(prefix, digest);
    scope.capabilities().contains(std::str::from_utf8(&label[..length]).expect("ASCII identity label"))
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
    const fn capability_prefix(self) -> &'static str {
        match self {
            Self::Read => "volume:read:",
            Self::Write => "volume:write:",
        }
    }
}

/// Verified, exact-volume authority for a single storage operation.
#[derive(Clone, Debug)]
pub struct ContentGrant {
    volume: VolumeRef,
    operation: VolumeOperation,
    exact_file: Option<(String, String, FileDescriptor, String)>,
    directory_prefix: Option<String>,
}

impl ContentGrant {
    /// Constructs read authority only from the sealed kernel proof.
    pub(crate) fn from_verified_project_source(
        proof: &crate::source_authorization::VerifiedProjectSourceRead<'_>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        Ok(Self { volume: proof.project().copy(work)?, operation: VolumeOperation::Read,
            exact_file: None, directory_prefix: Some(String::new()) })
    }

    /// Resolves an authenticated whole-volume, exact-file, or directory read.
    pub fn verify_read(verifier: &AuthorityVerifier, scope: &Scope, file: &FileRef) -> Result<Self> {
        file.validate(&mut None)?;
        if is_internal_path(file.path()) && !is_inherited_context_path(file.path()) {
            return Err(Error::Unauthorized("internal storage file is not public content".into()));
        }
        verifier.verify(scope, &mut None)?;
        if scope.capabilities().contains(&file.volume().capability(VolumeOperation::Read, &mut None)?) {
            return Self::verify(verifier, scope, file.volume(), VolumeOperation::Read, u64::MAX, &mut None);
        }
        if scope.capabilities().contains(&file.read_capability()?) {
            return Self::verify_file_read(verifier, scope, file);
        }
        if file.volume().class() == VolumeClass::AgentPrivate {
            for prefix in std::iter::once("").chain(file.path().match_indices('/').map(|(end, _)| &file.path()[..end])) {
                if file.volume().directory_read_capability(prefix, &mut None)
                    .is_ok_and(|capability| scope.capabilities().contains(&capability)) {
                    let grant = Self::verify_directory_read(verifier, scope, file.volume(), prefix, &mut None)?;
                    grant.require_file_read(file, None)?;
                    return Ok(grant);
                }
            }
        }
        Err(Error::Unauthorized("file read is not granted by its owner".into()))
    }

    /// Verifies signed authority and copies the exact granted volume.
    pub fn verify(verifier: &AuthorityVerifier, scope: &Scope, volume: &VolumeRef,
        operation: VolumeOperation, maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<Self> {
        Self::authorize_volume(verifier, scope, volume, operation, maximum_bytes, work)?;
        Ok(Self { volume: volume.copy(work)?, operation, exact_file: None, directory_prefix: None })
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = self.volume.logical_owned_bytes()?;
        if let Some((path, version, descriptor, display_name)) = &self.exact_file {
            bytes = [path.capacity(), version.capacity(), display_name.capacity()]
                .into_iter().try_fold(bytes, |bytes, capacity| bytes.checked_add(capacity as u64))
                .and_then(|bytes| bytes.checked_add(descriptor.logical_owned_bytes()))
                .ok_or_else(|| Error::Invalid("grant field ownership overflow".into()))?;
        }
        bytes.checked_add(self.directory_prefix.as_ref().map_or(0, |prefix| prefix.capacity() as u64))
            .ok_or_else(|| Error::Invalid("grant prefix ownership overflow".into()))
    }

    /// Checks signed authority without constructing an owned grant.
    pub(crate) fn authorize_volume(verifier: &AuthorityVerifier, scope: &Scope, volume: &VolumeRef,
        operation: VolumeOperation, maximum_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<()> {
        verifier.verify(scope, work)?;
        if operation == VolumeOperation::Write { volume.require_writer(scope.agent(), work)?; }
        volume.validate(work)?;
        let digest = crate::contract::canonical_json_digest_with_work(volume, maximum_bytes, work)?;
        if !contains_content_label(scope, operation.capability_prefix(), digest) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Unauthorized,
                "volume operation is not granted"));
        }
        Ok(())
    }

    /// Verifies a signed private-directory or whole-volume read grant.
    pub fn verify_directory_read(verifier: &AuthorityVerifier, scope: &Scope, volume: &VolumeRef,
        prefix: &str, work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<Self> {
        verifier.verify(scope, work)?;
        volume.validate(work)?;
        if !prefix.is_empty() { validate_content_path(prefix, work)?; }
        if is_internal_path(prefix) { return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "internal storage paths cannot be delegated")); }
        let directory = contains_content_label(scope, "directory:read:", crate::contract::canonical_json_digest_with_work(&(volume, prefix), u64::MAX, work)?);
        let entire = contains_content_label(scope, VolumeOperation::Read.capability_prefix(), crate::contract::canonical_json_digest_with_work(volume, u64::MAX, work)?);
        if scope.agent().is_none() || (!directory && !entire) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Unauthorized,
                "private directory read is not granted"));
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let grant = Self { volume: volume.clone_in_quota(&mut quota)?, operation: VolumeOperation::Read,
            exact_file: None, directory_prefix: Some(crate::core::copy_text(prefix, &mut quota)?) };
        quota.transfer();
        Ok(grant)
    }

    /// Authorizes only one pinned file version, including across an owner boundary.
    pub fn verify_file_read(verifier: &AuthorityVerifier, scope: &Scope, file: &FileRef) -> Result<Self> {
        file.validate(&mut None)?;
        if is_internal_path(file.path()) && !is_inherited_context_path(file.path()) {
            return Err(Error::Unauthorized("internal storage file is not public content".into()));
        }
        verifier.verify(scope, &mut None)?;
        if !scope.capabilities().contains(&file.read_capability()?) {
            return Err(Error::Unauthorized("exact file read is not granted".into()));
        }
        let FileRef { volume, path, version, descriptor, display_name } = file.clone();
        Ok(Self { volume, operation: VolumeOperation::Read,
            exact_file: Some((path, version, descriptor, display_name)), directory_prefix: None })
    }

    /// Checks exact volume and operation authority.
    pub fn require(&self, volume: &VolumeRef, operation: VolumeOperation, io: Option<&ContentIo>) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { io.check_cancellation()?; }
        if self.volume != *volume || self.operation != operation
            || self.exact_file.is_some() || self.directory_prefix.is_some() {
            return Err(content_error(io, crate::wire::ErrorCode::Unauthorized, "content grant does not match the volume"));
        }
        Ok(())
    }

    /// Checks whole-volume, exact-file, or delegated-directory read authority.
    pub fn require_file_read(&self, file: &FileRef, io: Option<&ContentIo>) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { io.check_cancellation()?; }
        if self.volume != *file.volume() || self.operation != VolumeOperation::Read
            || self.exact_file.as_ref().is_some_and(|(path, version, descriptor, display_name)|
                (path, version, descriptor, display_name) != (&file.path, &file.version, &file.descriptor, &file.display_name))
            || self.directory_prefix.as_deref().is_some_and(|prefix| is_internal_path(file.path()) || !path_below(file.path(), prefix)) {
            return Err(content_error(io, crate::wire::ErrorCode::Unauthorized, "content grant does not match the file"));
        }
        Ok(())
    }

    /// Checks any lazy directory path against the owner-issued read boundary.
    pub fn require_directory_path(&self, volume: &VolumeRef, prefix: &str, io: Option<&ContentIo>) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { io.check_cancellation()?; }
        if self.volume != *volume || self.operation != VolumeOperation::Read || is_internal_path(prefix)
            || !self.directory_prefix.as_deref().is_some_and(|granted| path_below(prefix, granted)) {
            return Err(content_error(io, crate::wire::ErrorCode::Unauthorized, "directory listing is not granted"));
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct FileDescriptor {
    sha256: [u8; 32],
    #[schemars(range(max = crate::conversation::MAX_EXACT_JS_INTEGER))]
    #[serde(deserialize_with = "deserialize_file_length")]
    byte_length: u64,
    #[serde(deserialize_with = "deserialize_media_type")]
    media_type: String,
}

impl FileDescriptor {
    pub(crate) fn logical_owned_bytes(&self) -> u64 {
        self.media_type.capacity() as u64
    }

    /// Constructs a descriptor after validating its media type.
    pub fn new(sha256: [u8; 32], byte_length: u64, media_type: String) -> Result<Self> {
        let value = Self {
            sha256,
            byte_length,
            media_type,
        };
        value.validate(&mut None)?;
        Ok(value)
    }

    /// Computes a descriptor under the optional work observer.
    pub fn from_bytes(bytes: &[u8], media_type: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>) -> Result<Self> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
            io.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: media_type.len() as u64 + 1 })?;
        }
        if let Some(reason) = Self::validation_fields(bytes.len() as u64, media_type) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, reason));
        }
        if let Some(io) = work.as_deref_mut() { io.hashed_bytes(bytes.len() as u64)?; }
        let sha256 = Sha256::digest(bytes).into();
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let media_type = crate::core::copy_text(media_type, &mut quota)?;
        quota.transfer();
        Ok(Self { sha256, byte_length: bytes.len() as u64, media_type })
    }

    /// Verifies through the existing borrowed cumulative history observer.
    pub fn verify(
        &self, bytes: &[u8], work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        if bytes.len() as u64 == self.byte_length {
            if let Some(io) = work.as_deref_mut() { io.hashed_bytes(bytes.len() as u64)?; }
            if Sha256::digest(bytes).as_slice() == self.sha256 { return Ok(()); }
        }
        Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "file content does not match its descriptor"))
    }

    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            inspect_native_metadata(native, self.media_type.len())?;
        }
        Self::validation_fields(self.byte_length, &self.media_type).map_or(Ok(()),
            |reason| Err(conversation_error(work, crate::wire::ErrorCode::Invalid, reason)))
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct FileRef {
    volume: VolumeRef,
    #[serde(deserialize_with = "deserialize_path")]
    path: String,
    #[serde(deserialize_with = "deserialize_label")]
    version: String,
    descriptor: FileDescriptor,
    #[serde(deserialize_with = "deserialize_label")]
    display_name: String,
}

impl FileRef {

    /// Creates a reference after checking paths, version and descriptor.
    pub fn new(
        volume: VolumeRef,
        path: String,
        version: String,
        descriptor: FileDescriptor,
        display_name: String,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let value = Self {
            volume,
            path,
            version,
            descriptor,
            display_name,
        };
        value.validate(work)?;
        Ok(value)
    }

    pub(crate) fn from_value(
        value: serde_json::Value,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
            let mut file = native_owned_wire_object(value,
                Some(&["volume", "path", "version", "descriptor", "display_name"]), work)?;
            let mut volume = native_owned_wire_object(native_owned_wire_field(&mut file, "volume", work)?,
                Some(&["provider", "id", "class", "owner"]), work)?;
            let provider = ProviderRef::from_owned_value_with_work(
                native_owned_wire_field(&mut volume, "provider", work)?, work)?;
            let id = native_owned_wire_text(native_owned_wire_field(&mut volume, "id", work)?, work)?;
            let class = native_owned_volume_class(native_owned_wire_field(&mut volume, "class", work)?, work)?;
            let mut owner = native_owned_wire_object(native_owned_wire_field(&mut volume, "owner", work)?,
                None, work)?;
            let kind = native_owned_wire_text(native_owned_wire_field(&mut owner, "kind", work)?, work)?;
            let owner_id = native_owned_wire_text(native_owned_wire_field(&mut owner, "id", work)?, work)?;
            let owner = match kind.as_str() {
                "project" => VolumeOwner::Project(owner_id),
                "session" => VolumeOwner::Session(owner_id),
                "agent" => {
                    let id = Uuid::parse_str(&owner_id)
                        .map_err(|_| conversation_error(work, crate::wire::ErrorCode::Invalid, "volume agent identity is invalid"))?;
                    VolumeOwner::Agent(AgentId::from_bytes(*id.as_bytes()))
                }
                _ => return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "volume owner kind is invalid")),
            };
            let volume = VolumeRef { provider, id, class, owner };
            volume.validate(work)?;
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
                return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "SHA-256 digest must contain exactly 32 bytes"));
            };
            if digest.len() != 32 {
                return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "SHA-256 digest must contain exactly 32 bytes"));
            }
            let mut sha256 = [0_u8; 32];
            for (slot, value) in sha256.iter_mut().zip(digest) {
                native_owned_wire_inspect(work)?;
                *slot = value.as_u64().and_then(|value| u8::try_from(value).ok())
                    .ok_or_else(|| conversation_error(work, crate::wire::ErrorCode::Invalid, "SHA-256 digest byte is invalid"))?;
            }
            let length = native_owned_wire_field(&mut descriptor, "byte_length", work)?;
            native_owned_wire_inspect(work)?;
            let byte_length = length.as_u64()
                .ok_or_else(|| conversation_error(work, crate::wire::ErrorCode::Invalid, "file byte length is invalid"))?;
            let media_type = native_owned_wire_text(
                native_owned_wire_field(&mut descriptor, "media_type", work)?, work)?;
            let descriptor = FileDescriptor { sha256, byte_length, media_type };
            descriptor.validate(work)?;
            let file = FileRef { volume, path, version, display_name, descriptor };
            file.validate(work)?;
            return Ok(file);
        }
        let file: Self = serde_json::from_value(value)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        file.validate(work)?;
        Ok(file)
    }

    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
            inspect_native_metadata(native, 0)?;
        }
        self.volume.validate(work)?;
        validate_content_path(&self.path, work)?;
        validate_label(&self.version, work)?;
        validate_label(&self.display_name, work)?;
        self.descriptor.validate(work)
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
        self.validate(&mut None)?;
        Ok(format!(
            "file:read:{}",
            blake3::Hash::from_bytes(crate::contract::canonical_json_digest(self)?).to_hex()
        ))
    }
}

/// Durable terminal task outcome.
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
            Self::Succeeded { result } => result.validate(&mut None),
            Self::Failed { message } if message.is_empty() => {
                Err(Error::Invalid("task failure description is invalid".into()))
            }
            _ => Ok(()),
        }
    }
}

/// Admission barrier for every version-pinned file in a conversation message.
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
    /// Rejects malformed, duplicate, or out-of-order names independently of the concrete Filesystem page implementation.
    pub fn validate(&self) -> Result<()> {
        self.generation.validate()?;
        if self.entries.len() as u64 > MAX_PRIVATE_DIRECTORY_PAGE as u64 {
            return Err(Error::Invalid(
                "private directory page exceeds protocol limit".into(),
            ));
        }
        let mut previous: Option<&[u8]> = None;
        for entry in &self.entries {
            validate_content_path(&entry.name, &mut None)?;
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

/// Optional content-operation accounting.
#[cfg(feature = "filesystem")]
pub type ContentIo = crate::filesystem::PreparationIo;
/// Optional content-operation accounting.
#[cfg(not(feature = "filesystem"))]
pub type ContentIo = ();
/// Retained content metadata ownership.
#[cfg(feature = "filesystem")]
pub type ContentHold<'a> = crate::filesystem::DecodeReservation<'a>;
/// Retained content metadata ownership.
#[cfg(not(feature = "filesystem"))]
pub type ContentHold<'a> = std::marker::PhantomData<&'a ()>;

/// An operation future whose allocation remains owned until its physical drop.
pub struct BoundedContentFuture<'a, T> {
    future: BoxFuture<'a, Result<T>>,
    #[cfg(feature = "filesystem")]
    _hold: Option<ContentHold<'a>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _native: Option<acyclic_fs::NativeAllocation>,
}

impl<'a, T> BoundedContentFuture<'a, T> {
    /// Admits the concrete future before invoking its builder.
    pub fn new<F>(io: Option<&'a ContentIo>, build: impl FnOnce() -> F) -> Result<Self>
    where F: std::future::Future<Output = Result<T>> + acyclic_stream::ProviderTask + 'a {
        #[cfg(feature = "filesystem")]
        let mut hold = None;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.check_cancellation()?;
            let bytes = std::mem::size_of::<F>() as u64;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.original_native_work() {
                let native = if bytes == 0 { work.retain_owned(0)? } else { work.claim(bytes)? };
                return Ok(Self { future: Box::pin(build()), _hold: None, _native: Some(native) });
            }
            io.retain_owned(bytes)?;
            hold = Some(io.guard_owned(bytes)?);
            if bytes != 0 { io.allocation_attempt(bytes)?; }
        }
        let _ = io;
        Ok(Self { future: Box::pin(build()),
            #[cfg(feature = "filesystem")] _hold: hold,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))] _native: None })
    }
}

impl<T> std::future::Future for BoundedContentFuture<'_, T> {
    type Output = Result<T>;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.get_mut().future.as_mut().poll(cx)
    }
}

fn content_error(io: Option<&ContentIo>, code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if io.is_some_and(|io| io.original_native_work().is_some()) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = io;
    match code {
        crate::wire::ErrorCode::Invalid => Error::Invalid(reason.into()),
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        _ => Error::Unsupported(reason.into()),
    }
}

/// Authenticated, bounded immutable-content operations.
pub trait ContentResidencyVerifier: acyclic_stream::ProviderPlatform {
    /// Capability availability; conveys no read authority.
    fn supports_read(&self, _volume: &VolumeRef, io: Option<&ContentIo>) -> Result<bool> {
        if io.is_some() { return Err(content_error(io, crate::wire::ErrorCode::Unsupported, "finite content reader capability is unavailable")); }
        Ok(true)
    }

    /// Reads an exact authorized file on the caller's existing operation ledger.
    fn read<'a>(&'a self, _reference: &'a FileRef, _maximum_bytes: u64, io: Option<&'a ContentIo>)
        -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        Err(content_error(io, crate::wire::ErrorCode::Unsupported, "content byte resolution is unavailable"))
    }

    /// Checks exact content length and digest under the selected byte allowance.
    fn verify<'a>(&'a self, reference: &'a FileRef, maximum_bytes: u64, io: Option<&'a ContentIo>)
        -> Result<BoundedContentFuture<'a, ()>> {
        BoundedContentFuture::new(io, || async move {
            verified_content_bytes(self, reference, maximum_bytes, io).await?;
            Ok(())
        })
    }

    /// Loads the manifest and verifies every member through its owning provider.
    fn verify_manifest<'a>(&'a self, reference: &'a FileRef, item_count: u32, limits: &'a Limits, io: Option<&'a ContentIo>)
        -> Result<BoundedContentFuture<'a, ()>> {
        BoundedContentFuture::new(io, || async move {
            verified_attachment_manifest(self, reference, item_count, limits, io).await?;
            Ok(())
        })
    }

    /// Reads an authenticated bounded prefix without fetching the remaining body.
    fn read_private_prefix<'a>(&'a self, _volume: &'a VolumeRef, _granted_prefix: &'a str,
        _path: &'a str, _generation: &'a GenerationRef, _maximum_bytes: u64, io: Option<&'a ContentIo>)
        -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        Err(content_error(io, crate::wire::ErrorCode::Unsupported, "bounded prefix reads are unavailable"))
    }

    /// Discovers a private subtree at a stable generation.
    fn list_private_directory<'a>(&'a self, _volume: &'a VolumeRef, _granted_prefix: &'a str,
        _path: &'a str, _expected_generation: Option<&'a GenerationRef>, _after: Option<&'a str>,
        _maximum_entries: u32, io: Option<&'a ContentIo>) -> Result<BoundedContentFuture<'a, PrivateDirectoryPage>> {
        Err(content_error(io, crate::wire::ErrorCode::Unsupported, "private directory discovery is unavailable"))
    }

    /// Resolves the named private file at the caller's pinned generation.
    fn read_private_path<'a, 'io>(&'a self, _volume: &'a VolumeRef, _granted_prefix: &'a str,
        _path: &'a str, _expected_generation: Option<&'a GenerationRef>, _maximum_bytes: u64,
        io: Option<&'io ContentIo>) -> Result<BoundedContentFuture<'a, (FileRef, acyclic_stream::OwnedBytes, Option<ContentHold<'io>>)>> where 'io: 'a {
        Err(content_error(io, crate::wire::ErrorCode::Unsupported, "private path resolution is unavailable"))
    }
}

/// Owner-bound, bounded publication on the caller's existing ledger.
pub trait ContentPublisher: acyclic_stream::ProviderPlatform {
    /// Exact writable volume.
    fn volume(&self) -> &VolumeRef;
    /// Rejects unavailable bounded publication before external side effects.
    fn supports_stage(&self, io: Option<&ContentIo>) -> Result<()> {
        if io.is_some() { return Err(content_error(io, crate::wire::ErrorCode::Unsupported, "finite publication is unavailable")); }
        Ok(())
    }
    /// Stages immutable bytes under one stable operation identity.
    fn stage<'a>(&'a self, _operation_id: OperationId, _path: &'a str, _bytes: &'a [u8],
        _media_type: &'a str, _display_name: &'a str, io: Option<&'a ContentIo>)
        -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        Err(content_error(io, crate::wire::ErrorCode::Unsupported, "file publication is unavailable"))
    }
    /// Publishes one complete file contract, including its path, per operation.
    fn stage_once<'a>(&'a self, _operation_id: OperationId, _path: &'a str, _bytes: &'a [u8],
        _media_type: &'a str, _display_name: &'a str, io: Option<&'a ContentIo>)
        -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        Err(content_error(io, crate::wire::ErrorCode::Unsupported, "operation-bound file publication is unavailable"))
    }
    /// Observes the original once-publication without substituting a mutable head.
    fn observe_staged_once<'a>(&'a self, _operation_id: OperationId, _maximum_bytes: u64, io: Option<&'a ContentIo>)
        -> Result<BoundedContentFuture<'a, (Option<FileRef>, Option<ContentHold<'a>>)>> {
        Err(content_error(io, crate::wire::ErrorCode::Unsupported, "publication observation is unavailable"))
    }
    /// Replaces bytes only at the immutable source generation.
    fn stage_at<'a>(&'a self, _operation_id: OperationId, _source: &'a FileRef,
        _bytes: &'a [u8], io: Option<&'a ContentIo>) -> Result<BoundedContentFuture<'a, (FileRef, Option<ContentHold<'a>>)>> {
        Err(content_error(io, crate::wire::ErrorCode::Unsupported, "generation-checked publication is unavailable"))
    }
}

/// Resolves an owner-mediated reader when a volume has not been explicitly bound.
pub trait ContentMountResolver: acyclic_stream::ProviderPlatform {
    /// Lazily opens the owner for one exact ref and returns its independently verified caller grant.
    fn mount<'a>(&'a self, reference: &'a FileRef) -> BoxFuture<'a, Result<(ContentGrant, Arc<dyn ContentResidencyVerifier>)>>;

    /// Lazily opens an unseen owner's directory without requiring a `FileRef`.
    fn mount_directory<'a>(
        &'a self,
        _volume: &'a VolumeRef,
        _granted_prefix: &'a str,
        _path: &'a str,
    ) -> BoxFuture<'a, Result<(ContentGrant, Arc<dyn ContentResidencyVerifier>)>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "private directory mount is unavailable".into(),
            ))
        })
    }
}

/// Routes admission reads by exact owner volume, including several agents on one provider and members stored separately from their manifests.
pub struct CompositeContentVerifier {
    volumes: BTreeMap<VolumeRef, Arc<dyn ContentResidencyVerifier>>,
    mount: Option<Arc<dyn ContentMountResolver>>,
}

impl CompositeContentVerifier {
    #[cfg(feature = "filesystem")]
    fn finite_bound_reader(&self, volume: &VolumeRef, io: &crate::filesystem::PreparationIo) -> Result<&dyn ContentResidencyVerifier> {
        volume.validate(&mut Some(&mut io.history_io()))?;
        self.volumes.get(volume).map(AsRef::as_ref).ok_or_else(|| content_error(
            Some(io), crate::wire::ErrorCode::Unsupported,
            "finite content routing requires an explicitly bound owner; lazy mount lacks finite IO"))
    }

    /// Rejects duplicate exact-volume bindings instead of silently replacing one.
    pub fn new(bindings: Vec<(VolumeRef, Arc<dyn ContentResidencyVerifier>)>) -> Result<Self> {
        let mut volumes = BTreeMap::new();
        for (volume, resolver) in bindings {
            volume.validate(&mut None)?;
            if volumes.insert(volume, resolver).is_some() {
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

    async fn owner(&self, reference: &FileRef) -> Result<Arc<dyn ContentResidencyVerifier>> {
        reference.validate(&mut None)?;
        if let Some(reader) = self.volumes.get(reference.volume()) {
            return Ok(reader.clone());
        }
        let (grant, reader) = self
            .mount
            .as_ref()
            .ok_or_else(|| Error::Unsupported("content volume is not registered".into()))?
            .mount(reference)
            .await?;
        grant.require_file_read(reference, None)?;
        Ok(reader)
    }

    async fn directory_owner(
        &self,
        volume: &VolumeRef,
        granted_prefix: &str,
        path: &str,
    ) -> Result<Arc<dyn ContentResidencyVerifier>> {
        volume.validate(&mut None)?;
        if let Some(reader) = self.volumes.get(volume) {
            return Ok(reader.clone());
        }
        let (grant, reader) = self
            .mount
            .as_ref()
            .ok_or_else(|| Error::Unsupported("content volume is not registered".into()))?
            .mount_directory(volume, granted_prefix, path)
            .await?;
        grant.require_directory_path(volume, path, None)?;
        Ok(reader)
    }
}

impl ContentResidencyVerifier for CompositeContentVerifier {
    fn supports_read(&self, volume: &VolumeRef, _io: Option<&ContentIo>) -> Result<bool> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = _io { return self.finite_bound_reader(volume, io)?.supports_read(volume, Some(io)); }
        volume.validate(&mut None)?;
        Ok(self.volumes.contains_key(volume) || self.mount.is_some())
    }
    fn read<'a>(&'a self, reference: &'a FileRef, maximum_bytes: u64, io: Option<&'a ContentIo>)
        -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { return self.finite_bound_reader(reference.volume(), io)?.read(reference, maximum_bytes, Some(io)); }
        BoundedContentFuture::new(io, || async move { self.owner(reference).await?.read(reference, maximum_bytes, io)?.await })
    }
    fn read_private_prefix<'a>(&'a self, volume: &'a VolumeRef, granted_prefix: &'a str,
        path: &'a str, generation: &'a GenerationRef, maximum_bytes: u64, io: Option<&'a ContentIo>)
        -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { return self.finite_bound_reader(volume, io)?.read_private_prefix(volume, granted_prefix, path, generation, maximum_bytes, Some(io)); }
        BoundedContentFuture::new(io, || async move { self.directory_owner(volume, granted_prefix, path).await?
            .read_private_prefix(volume, granted_prefix, path, generation, maximum_bytes, io)?.await })
    }
    fn list_private_directory<'a>(&'a self, volume: &'a VolumeRef, granted_prefix: &'a str,
        path: &'a str, expected_generation: Option<&'a GenerationRef>, after: Option<&'a str>,
        maximum_entries: u32, io: Option<&'a ContentIo>) -> Result<BoundedContentFuture<'a, PrivateDirectoryPage>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { return self.finite_bound_reader(volume, io)?.list_private_directory(volume, granted_prefix, path, expected_generation, after, maximum_entries, Some(io)); }
        BoundedContentFuture::new(io, || async move { self.directory_owner(volume, granted_prefix, path).await?
            .list_private_directory(volume, granted_prefix, path, expected_generation, after, maximum_entries, io)?.await })
    }
    fn read_private_path<'a, 'io>(&'a self, volume: &'a VolumeRef, granted_prefix: &'a str,
        path: &'a str, expected_generation: Option<&'a GenerationRef>, maximum_bytes: u64,
        io: Option<&'io ContentIo>) -> Result<BoundedContentFuture<'a, (FileRef, acyclic_stream::OwnedBytes, Option<ContentHold<'io>>)>> where 'io: 'a {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io { return self.finite_bound_reader(volume, io)?.read_private_path(volume, granted_prefix, path, expected_generation, maximum_bytes, Some(io)); }
        BoundedContentFuture::new(io, || async move { self.directory_owner(volume, granted_prefix, path).await?
            .read_private_path(volume, granted_prefix, path, expected_generation, maximum_bytes, io)?.await })
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
    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.file.validate(work)?;
        if let Some(label) = &self.label {
            validate_label(label, work)?;
        }
        Ok(())
    }

}

/// Encodes a validated, ordered attachment list in its single durable wire form.
pub fn encode_attachment_manifest(items: &[Attachment]) -> Result<Vec<u8>> {
    if items.len() as u64 > MAX_PORTABLE_COUNT as u64 {
        return Err(Error::Invalid("attachment count exceeds limit".into()));
    }
    for item in items {
        item.validate(&mut None)?;
    }
    serde_json::to_vec(items).map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(feature = "filesystem")]
pub(crate) fn manifest_owned_bytes(items: &[Attachment], capacity: usize) -> Result<u64> {
    let mut bytes = capacity.checked_mul(std::mem::size_of::<Attachment>())
        .ok_or_else(|| Error::Invalid("manifest vector ownership overflow".into()))? as u64;
    for item in items {
        bytes = bytes.checked_add(item.file.logical_owned_bytes()?)
            .and_then(|bytes| bytes.checked_add(item.label.as_ref().map_or(0, |label| label.capacity() as u64)))
            .ok_or_else(|| Error::Invalid("manifest field ownership overflow".into()))?;
    }
    Ok(bytes)
}

/// Decodes one complete canonical manifest under the optional content ledger.
pub fn decode_attachment_manifest(
    reference: &FileRef, bytes: &[u8], item_count: Option<u32>, io: Option<&ContentIo>,
) -> Result<Vec<Attachment>> {
    #[cfg(feature = "filesystem")]
    let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
    #[cfg(feature = "filesystem")]
    let mut work = observer.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
    #[cfg(not(feature = "filesystem"))]
    let mut work = None;
    reference.validate(&mut work)?;
    let maximum_members = item_count.map_or(MAX_PORTABLE_COUNT, |count| count as usize);
    if reference.descriptor().media_type() != "application/vnd.acyclic.harness.attachments+json"
        || maximum_members > MAX_PORTABLE_COUNT {
        return Err(content_error(io, crate::wire::ErrorCode::Invalid,
            "attachment manifest metadata or allowance is invalid"));
    }
    reference.descriptor().verify(bytes, &mut work)?;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
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
                    guard::<_, S::Error>(item.validate(&mut Some(&mut self.io.history_io())), self.failure)?;
                    if items.len() == items.capacity() {
                        let capacity = items.capacity().saturating_mul(2).max(4).min(self.maximum);
                        let bytes = guard::<_, S::Error>(capacity.checked_mul(std::mem::size_of::<Attachment>())
                            .map(|bytes| bytes as u64)
                            .ok_or_else(|| self.io.invalid("manifest vector capacity overflow")), self.failure)?;
                        guard::<_, S::Error>(self.io.allocation_attempt(bytes), self.failure)?;
                        items.reserve_exact(capacity - items.len());
                    }
                    items.push(item);
                }
                Ok(items)
            }
        }
        let mut failure = None;
        let result = io.decode_seed(
            ManifestSeed { maximum: maximum_members, io, failure: &mut failure },
            bytes, crate::wire::ErrorCode::Invalid,
        );
        let mut decoded = match result {
            Ok(decoded) => decoded,
            Err(transport) => {
                if let Some(primary) = failure.take() {
                    drop(transport);
                    return Err(primary);
                }
                return Err(transport);
            }
        };
        let actual = (std::mem::size_of::<Vec<Attachment>>() as u64)
            .checked_add(manifest_owned_bytes(&decoded.0, decoded.0.capacity())?)
            .ok_or_else(|| io.invalid("manifest ownership overflow"))?;
        decoded.1.substitute(actual)?;
        let encoded_hold;
        let encoded = io.encode(&decoded.0, io.remaining()?.bytes_encoded, false)?;
        encoded_hold = io.guard_bytes(&encoded)?;
        if encoded.as_ref() != bytes {
            return Err(io.invalid("attachment manifest is not canonical or complete"));
        }
        drop(encoded);
        drop(encoded_hold);
        if item_count.is_some_and(|count| decoded.0.len() != count as usize) {
            return Err(io.invalid("attachment manifest count is incomplete"));
        }
        return Ok(crate::filesystem::retain_decoded(decoded));
    }
    let items: Vec<Attachment> = crate::contract::json_from_slice(bytes)
        .map_err(|error| Error::Invalid(format!("attachment manifest is invalid: {error}")))?;
    if encode_attachment_manifest(&items)? != bytes {
        return Err(content_error(io, crate::wire::ErrorCode::Invalid,
            "attachment manifest is not canonical or complete"));
    }
    if item_count.is_some_and(|count| items.len() != count as usize) {
        return Err(content_error(io, crate::wire::ErrorCode::Invalid,
            "attachment manifest count is incomplete"));
    }
    Ok(items)
}


/// Reads and verifies exact bytes with their retained ownership.
pub async fn verified_content_bytes<'a, V: ContentResidencyVerifier + ?Sized>(
    verifier: &'a V, reference: &'a FileRef, maximum_bytes: u64, io: Option<&'a ContentIo>,
) -> Result<(acyclic_stream::OwnedBytes, Option<ContentHold<'a>>)> {
    if reference.descriptor().byte_length() > maximum_bytes {
        return Err(content_error(io, crate::wire::ErrorCode::Invalid, "content exceeds selected byte allowance"));
    }
    let hold;
    let bytes = verifier.read(reference, maximum_bytes, io)?.await?;
    #[cfg(feature = "filesystem")]
    {
        hold = match io {
            Some(io) => { io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?; io.guard_bytes(&bytes)? },
            None => None,
        };
        reference.descriptor().verify(&bytes, &mut io.map(crate::filesystem::PreparationIo::history_io).as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo))?;
    }
    #[cfg(not(feature = "filesystem"))]
    { hold = None; reference.descriptor().verify(&bytes, &mut None)?; }
    Ok((bytes, hold))
}

/// Loads canonical manifest bytes through their owning reader.
pub async fn load_manifest<'a, V: ContentResidencyVerifier + ?Sized>(
    verifier: &'a V, reference: &'a FileRef, item_count: u32, io: Option<&'a ContentIo>,
) -> Result<(Vec<Attachment>, Option<ContentHold<'a>>)> {
    if !verifier.supports_read(reference.volume(), io)? {
        return Err(content_error(io, crate::wire::ErrorCode::Unsupported, "manifest reader is unavailable"));
    }
    let _bytes_hold;
    let bytes = verifier.read(reference, reference.descriptor().byte_length(), io)?.await?;
    #[cfg(feature = "filesystem")]
    { _bytes_hold = match io {
        Some(io) => { io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?; io.guard_bytes(&bytes)? },
        None => None,
    }; }
    #[cfg(not(feature = "filesystem"))]
    { _bytes_hold = (); }
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let items = decode_attachment_manifest(reference, &bytes, Some(item_count), Some(io))?;
        let retained = std::mem::size_of::<Vec<Attachment>>() as u64 + manifest_owned_bytes(&items, items.capacity())?;
        return Ok((items, Some(io.guard_owned(retained)?)));
    }
    Ok((decode_attachment_manifest(reference, &bytes, Some(item_count), io)?, None))
}

/// Resolves a canonical manifest and verifies all members on the same ledger.
pub async fn verified_attachment_manifest<'a, V: ContentResidencyVerifier + ?Sized>(
    verifier: &'a V, reference: &'a FileRef, item_count: u32, limits: &'a Limits,
    io: Option<&'a ContentIo>,
) -> Result<(Vec<Attachment>, Option<ContentHold<'a>>)> {
    #[cfg(feature = "filesystem")]
    let mut observer = io.map(crate::filesystem::PreparationIo::history_io);
    #[cfg(feature = "filesystem")]
    let mut work = observer.as_mut().map(|io| io as &mut dyn crate::store::history::HistoryIo);
    #[cfg(not(feature = "filesystem"))]
    let mut work = None;
    limits.validate_file(reference, &mut work)?;
    if item_count as usize > limits.attachments {
        return Err(conversation_error(&work, crate::wire::ErrorCode::Invalid, "attachment manifest count exceeds limits"));
    }
    let output = load_manifest(verifier, reference, item_count, io).await?;
    for item in &output.0 {
        if let Some(observer) = work.as_deref_mut() {
            observer.codec_charge(crate::contract::JsonCodecCharge::Inspect { items: 1 })?;
        }
        limits.validate_file(&item.file, &mut work)?;
        verified_content_bytes(verifier, &item.file, limits.file_bytes, io).await?;
    }
    Ok(output)
}

/// A complete ordered attachment list, inline or in one version-pinned file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[allow(
    clippy::large_enum_variant,
    reason = "manifest refs retain the public tagged enum contract"
)]
pub enum ReferencedAttachments {
    /// Bounded small list retained directly in the event.
    Inline {
        /// Complete ordered attachments.
        #[serde(deserialize_with = "deserialize_inline_attachments")]
        items: Vec<Attachment>,
    },
    /// Complete canonical JSON list retained as a file, never a partial page.
    Manifest {
        /// Version-pinned file containing a canonical JSON array.
        #[serde(deserialize_with = "deserialize_manifest_file")]
        manifest: FileRef,
        /// Exact number of list members.
        item_count: u32,
    },
}

impl ReferencedAttachments {
    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        match self {
            Self::Inline { items } => {
                for item in items { item.validate(work)?; }
            }
            Self::Manifest { manifest, .. } => {
                manifest.validate(work)?;
                if manifest.descriptor().media_type()
                    != "application/vnd.acyclic.harness.attachments+json"
                {
                    return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "attachment manifest metadata is invalid"));
                }
            }
        }
        Ok(())
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
    /// Validates metadata under the caller's optional work observer.
    pub fn validate(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if self.id.is_nil() || self.reply_to.is_some_and(|id| id.is_nil()) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "message identity is nil"));
        }
        if self.sequence == 0 {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "message sequence must start at one"));
        }
        self.content.validate(work)?;
        self.attachments.validate(work)?;
        for (name, reference) in &self.extensions {
            validate_label(name, work)?;
            if !name.contains('.') {
                return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "extension key must be namespaced"));
            }
            reference.validate(work)?;
        }
        let has_call = self.tool_call_id.is_some();
        if has_call != matches!(self.kind, MessageKind::ToolCall | MessageKind::ToolResult) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "tool linkage does not match message kind"));
        }
        if let Some(call) = &self.tool_call_id {
            validate_label(call, work)?;
        }
        Ok(())
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn clone_native_reference_text(
    source: &str, work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    if source.is_empty() {
        return Ok((String::new(), work.retain_owned(0)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?));
    }
    work.admit(acyclic_fs::WorkCounters {
        bytes_copied: source.len() as u64, ..acyclic_fs::WorkCounters::default()
    }).map_err(|cause| Error::NativeWork { cause, custody: None })?;
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
    Ok((text, component))
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
        let owner = match &self.volume.owner {
            VolumeOwner::Project(text) | VolumeOwner::Session(text) => text.len() as u64,
            VolumeOwner::Agent(_) => 0,
        };
        let provider = &self.volume.provider;
        [provider.namespace().len(), provider.family().len(), provider.version().len(),
            self.volume.id.len(), self.path.len(), self.version.len(),
            self.descriptor.media_type.len(), self.display_name.len()]
            .into_iter().try_fold(owner, |bytes, length| bytes.checked_add(length as u64))
            .ok_or_else(|| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "reference clone bytes overflow" } }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                { Error::Invalid("file clone ownership overflow".into()) }
            })
    }

    /// Copies file fields under the optional work observer.
    pub fn copy(
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
        self.validate(quota.work())?;
        let volume = self.volume.clone_in_quota(quota)?;
        let path = crate::core::copy_text(&self.path, quota)?;
        let version = crate::core::copy_text(&self.version, quota)?;
        Ok(Self {
            volume, path, version,
            descriptor: FileDescriptor {
                sha256: self.descriptor.sha256, byte_length: self.descriptor.byte_length,
                media_type: crate::core::copy_text(&self.descriptor.media_type, quota)?,
            },
            display_name: crate::core::copy_text(&self.display_name, quota)?,
        })
    }
}

fn copy_preclaimed_conversation_vector<T: Copy>(
    source: &[T], target: &mut Vec<T>,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    let bytes = conversation_buffer_bytes::<T>(source.len())
        .map_err(|reason| conversation_error(work, crate::wire::ErrorCode::Invalid, reason))?;
    if let Some(io) = work.as_deref_mut() {
        io.admit_copy(bytes)?;
        io.copy_bytes(bytes)?;
    }
    target.extend_from_slice(source);
    Ok(())
}

impl ConversationMessage {
    pub(crate) fn clone_in_quota(
        &self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Self> {
        if quota.work().is_none() { return Ok(self.clone()); }
        self.validate(quota.work())?;
        let content = self.content.clone_in_quota(quota)?;
        let attachments = match &self.attachments {
            ReferencedAttachments::Inline { items } => {
                let items = crate::core::copy_iter(items.len(), items.iter(), quota, |item, quota| {
                    let file = item.file.clone_in_quota(quota)?;
                    let label = item.label.as_deref().map(|label|
                        crate::core::copy_text(label, quota)).transpose()?;
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
            .map(|id| crate::core::copy_text(id, quota)).transpose()?;
        let mut extensions = BTreeMap::new();
        for (key, file) in &self.extensions {
            let bytes = std::mem::size_of::<(String, FileRef)>() as u64;
            let io = quota.work().as_deref_mut().expect("observed message clone");
            io.admit_owned(bytes)?;
            io.retain_owned(bytes)?;
            quota.adopt(bytes)?;
            let key = crate::core::copy_text(key, quota)?;
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
    native_custody: NativeConversationCustody,
}

/// Only an issuer-authenticated reducer snapshot makes this a trusted prefix.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchivedConversationPrefix {
    through_sequence: u64,
    history_digest: [u8; 32],
    checkpoint: FileRef,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConversationStateWire {
    pub(crate) agent: Option<AgentId>,
    pub(crate) messages: Vec<ConversationMessage>,
    #[serde(default)]
    pub(crate) archived_prefix: Option<ArchivedConversationPrefix>,
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
    indexes: BTreeMap<(bool, Uuid), (acyclic_fs::NativeAllocation, acyclic_fs::NativeAllocation)>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    cache_carrier: Option<acyclic_fs::NativeAllocation>,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
const INDEX_METADATA_BYTES: u64 = (11 * (std::mem::size_of::<(bool, Uuid)>()
        + std::mem::size_of::<(acyclic_fs::NativeAllocation, acyclic_fs::NativeAllocation)>())
        + 12 * std::mem::size_of::<usize>()) as u64;

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
        while let Some((_, index)) = self.indexes.pop_first() { drop(index); }
    }
}


fn conversation_error(work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Unauthorized => Error::Unauthorized(reason.into()),
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
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
        return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "wire value must be an object"));
    };
    for key in object.keys() {
        native_owned_wire_inspect(work)?;
        if fields.is_some_and(|fields| !fields.contains(&key.as_str())) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "wire object has an unknown field"));
        }
    }
    Ok(object)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_owned_wire_field(
    object: &mut serde_json::Map<String, serde_json::Value>, name: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<serde_json::Value> {
    object.remove(name).ok_or_else(|| conversation_error(work, crate::wire::ErrorCode::Invalid, "wire object is missing a field"))
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn native_owned_wire_text(
    value: serde_json::Value,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<String> {
    native_owned_wire_inspect(work)?;
    match value {
        serde_json::Value::String(text) => Ok(text),
        _ => Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "wire value must be a string")),
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
                return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "volume class is invalid"));
            }
            name
        }
        _ => return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "volume class is invalid")),
    };
    match name.as_str() {
        "project" => Ok(VolumeClass::Project),
        "agent_private" => Ok(VolumeClass::AgentPrivate),
        "session_shared" => Ok(VolumeClass::SessionShared),
        _ => Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "volume class is invalid")),
    }
}



fn conversation_buffer_bytes<T>(capacity: usize) -> std::result::Result<u64, &'static str> {
    capacity.checked_mul(std::mem::size_of::<T>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or("conversation buffer capacity overflows")
}

fn reserve_conversation_buffer<T>(
    values: &mut Vec<T>, target: usize,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    owner: &mut Option<acyclic_fs::NativeAllocation>,
) -> Result<u64> {
    if target <= values.capacity() { return Ok(0); }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(io) = work.as_deref_mut().filter(|io| io.original_native_work().is_some()) {
        let bytes = conversation_buffer_bytes::<T>(target)
            .map_err(|reason| conversation_error(&Some(&mut *io), crate::wire::ErrorCode::Invalid, reason))?;
        io.admit_allocation(bytes)?;
        io.retain_owned(bytes)?;
        let incoming = io.take_native_custody(bytes)?;
        let retired = owner.replace(incoming);
        if let Err(cause) = values.try_reserve_exact(target - values.len()) {
            let rejected = std::mem::replace(owner, retired);
            drop(rejected);
            return Err(io.allocation_failure(cause));
        }
        let actual = conversation_buffer_bytes::<T>(values.capacity())
            .map_err(|reason| conversation_error(&Some(&mut *io), crate::wire::ErrorCode::Invalid, reason))?;
        let result = owner.as_mut().expect("installed conversation buffer custody")
            .reconcile_capacity(actual)
            .map_err(|cause| Error::NativeWork { cause, custody: None });
        drop(retired);
        result?;
        return Ok(0);
    }
    let old = values.capacity();
    if let Some(io) = work.as_deref_mut() {
        let requested = conversation_buffer_bytes::<T>(target - old)
            .map_err(|reason| conversation_error(&Some(&mut *io), crate::wire::ErrorCode::Invalid, reason))?;
        io.admit_allocation(requested)?;
        values.try_reserve_exact(target - values.len())
            .map_err(|cause| io.allocation_failure(cause))?;
        let actual = conversation_buffer_bytes::<T>(values.capacity() - old)
            .map_err(|reason| conversation_error(&Some(&mut *io), crate::wire::ErrorCode::Invalid, reason))?;
        io.retain_owned(actual)?;
        Ok(actual)
    } else {
        values.reserve_exact(target - values.len());
        Ok(0)
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn adopt_existing_buffer<T>(
    owner: &mut Option<acyclic_fs::NativeAllocation>, capacity: usize,
    io: &mut dyn crate::store::history::HistoryIo,
) -> Result<()> {
    if owner.is_none() && capacity != 0 {
        let bytes = conversation_buffer_bytes::<T>(capacity)
            .map_err(|reason| conversation_error(&Some(&mut *io), crate::wire::ErrorCode::Invalid, reason))?;
        io.admit_owned(bytes)?;
        io.retain_owned(bytes)?;
        *owner = Some(io.take_native_custody(bytes)?);
    }
    Ok(())
}

impl NativeConversationCustody {
    fn retain_index(
        &mut self, outcome: bool, id: Uuid,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work.as_deref_mut().filter(|io| io.original_native_work().is_some()) {
            if self.indexes.contains_key(&(outcome, id)) { return Ok(0); }
            let bytes = std::mem::size_of::<(Uuid, usize)>() as u64 + INDEX_METADATA_BYTES;
            io.admit_allocation(bytes)?;
            io.retain_owned(bytes)?;
            let mut allocation = io.take_native_custody(bytes)?;
            let source = allocation.split_off(std::mem::size_of::<(Uuid, usize)>() as u64)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?;
            self.indexes.insert((outcome, id), (source, allocation));
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
        Self::from_wire(wire, 1, &mut None).map(|(state, _)| state)
    }
}

impl ConversationState {
    pub(crate) fn from_wire(
        wire: ConversationStateWire,
        maximum_encoded_bytes: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        if let Some(prefix) = &wire.archived_prefix {
            prefix.checkpoint.validate(cache.work())?;
            if wire.agent.is_none() || prefix.through_sequence == 0 {
                return Err(conversation_error(cache.work(), crate::wire::ErrorCode::Invalid,
                    "archived conversation prefix is unbound"));
            }
        }
        let mut previous = wire.archived_prefix.as_ref()
            .map_or(0, |prefix| prefix.through_sequence);
        let mut state = Self {
            agent: wire.agent,
            messages: wire.messages,
            archived_prefix: wire.archived_prefix,
            ..Self::default()
        };
        let retained = reserve_conversation_buffer(
            &mut state.prefix_digests, state.messages.len(),cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut state.native_custody.digests,
        )?;
        cache.adopt(retained)?;
        for (position, message) in state.messages.iter().enumerate() {
            if let Some(io) = cache.work().as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            message.validate(cache.work())?;
            if message.sequence <= previous
                || (state.archived_prefix.is_some()
                    && previous.checked_add(1) != Some(message.sequence))
                || state.by_id.contains_key(&message.id)
            {
                return Err(conversation_error(cache.work(), crate::wire::ErrorCode::Invalid,
                    "conversation order or identity is invalid"));
            }
            let retained = state.native_custody.retain_index(false, message.id, cache.work())?;
            cache.adopt(retained)?;
            state.by_id.insert(message.id, position);
            previous = message.sequence;
            let digest = advance_history_digest(
                state.history_digest(), message, maximum_encoded_bytes, cache.work())?;
            state.prefix_digests.push(digest);
            if message.kind == MessageKind::User {
                state.pending_user = Some(message.id);
            }
        }
        for position in 0..state.messages.len() {
            if let Some(io) = cache.work().as_deref_mut() {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            let message = &state.messages[position];
            let retained = state.prepare_turn_index(model_visible(&message.kind),
                turn_outcome_parent(message), cache.work())?;
            state.index_turn(position);
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
            checkpoint.validate(&mut None)?;
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
        if (self.native_custody.messages.is_some() || self.native_custody.digests.is_some()
            || self.native_custody.positions.is_some() || self.native_custody.archive.is_some()
            || self.native_custody.cache_carrier.is_some() || !self.native_custody.indexes.is_empty())
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
        conversation_buffer_bytes::<ConversationMessage>(self.messages.capacity())
            .map_err(|reason| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason })
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
    pub(crate) fn install_native_archived_prefix(&mut self, allocation: acyclic_fs::NativeAllocation) {
        assert!(self.native_custody.archive.is_none(), "archived prefix already has custody");
        self.native_custody.archive = Some(allocation);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn native_unclaimed_cache_bytes(&self) -> Result<u64> {
        if self.native_custody.cache_carrier.is_some() { return Ok(0); }
        let mut bytes = 0_u64;
        if self.native_custody.digests.is_none() {
            bytes += conversation_buffer_bytes::<[u8; 32]>(self.prefix_digests.capacity())
                .map_err(|reason| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason })?;
        }
        if self.native_custody.positions.is_none() {
            bytes = bytes.checked_add(conversation_buffer_bytes::<usize>(self.model_positions.capacity())
                .map_err(|reason| Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason })?).ok_or(Error::NativeBoundary {
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
        if self.native_custody.cache_carrier.is_none() {
            self.native_custody.cache_carrier = Some(allocation);
        } else {
            assert_eq!(allocation.bytes(), 0, "cache source already has custody");
        }
        let carrier = self.native_custody.cache_carrier.as_mut().expect("installed cache source custody");
        if self.native_custody.digests.is_none() {
            let bytes = conversation_buffer_bytes::<[u8; 32]>(self.prefix_digests.capacity())
                .map_err(|reason| conversation_error(work, crate::wire::ErrorCode::Invalid, reason))?;
            self.native_custody.digests = Some(carrier.split_off(bytes)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?);
        }
        if self.native_custody.positions.is_none() {
            let bytes = conversation_buffer_bytes::<usize>(self.model_positions.capacity())
                .map_err(|reason| conversation_error(work, crate::wire::ErrorCode::Invalid, reason))?;
            self.native_custody.positions = Some(carrier.split_off(bytes)
                .map_err(|cause| Error::NativeWork { cause, custody: None })?);
        }
        for (outcome, map) in [(false, &self.by_id), (true, &self.outcomes)] {
            for id in map.keys() {
                if self.native_custody.indexes.contains_key(&(outcome, *id)) { continue; }
                let io = work.as_deref_mut().filter(|io| io.original_native_work().is_some())
                    .ok_or(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                        reason: "cache metadata requires original native work" })?;
                let bytes = INDEX_METADATA_BYTES;
                io.admit_allocation(bytes)?;
                io.retain_owned(bytes)?;
                let metadata = io.take_native_custody(bytes)?;
                let source = carrier.split_off(std::mem::size_of::<(Uuid, usize)>() as u64)
                    .map_err(|cause| Error::NativeWork { cause, custody: None })?;
                self.native_custody.indexes.insert((outcome, *id), (source, metadata));
            }
        }
        assert_eq!(carrier.bytes(), 0, "cache source transfer inventory must match");
        Ok(())
    }

    pub(crate) fn copy<'work>(
        &self, work: &mut Option<&'work mut dyn crate::store::history::HistoryIo>,
        mut retain: Option<&mut dyn FnMut(u64, u64, &mut Option<&'work mut dyn crate::store::history::HistoryIo>) -> Result<()>>,
    ) -> Result<(Self, u64)> {
        if retain.is_some() {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if !work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "message component custody requires original native work" });
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            return Err(Error::Invalid("message component custody requires original native work".into()));
        }
        self.require_native_work(work)?;
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut state = Self {
            agent: self.agent, pending_user: self.pending_user,
            ..Self::default()
        };
        let retained = reserve_conversation_buffer(
            &mut state.messages, self.messages.len(),cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut state.native_custody.messages,
        )?;
        cache.adopt(retained)?;
        let retained = reserve_conversation_buffer(
            &mut state.prefix_digests, self.prefix_digests.len(),cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut state.native_custody.digests,
        )?;
        cache.adopt(retained)?;
        let retained = reserve_conversation_buffer(
            &mut state.model_positions, self.model_positions.len(),cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut state.native_custody.positions,
        )?;
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
        if let Some(prefix) = &self.archived_prefix {
            let retained = state.copy_archive_checkpoint(
                prefix.through_sequence, prefix.history_digest, &prefix.checkpoint, cache.work())?;
            cache.adopt(retained)?;
        }
        let retained = cache.retained();
        cache.transfer();
        Ok((state, retained))
    }

    fn copy_archive_checkpoint(
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

    fn history_digest(&self) -> [u8; 32] {
        self.prefix_digests.last().copied().unwrap_or_else(||
            self.archived_prefix.as_ref().map_or([0; 32], |prefix| prefix.history_digest))
    }

    /// Logical history head, independent of the number of resident records.
    #[must_use]
    pub fn logical_revision(&self) -> u64 {
        self.messages
            .last()
            .map_or(self.resident_after_sequence(), |message| message.sequence)
    }

    /// Canonical records through this cut are archived, not resident.
    #[must_use]
    pub fn resident_after_sequence(&self) -> u64 {
        self.archived_prefix
            .as_ref()
            .map_or(0, |prefix| prefix.through_sequence)
    }

    pub(crate) fn history_prefix_digest(
        &self, through_sequence: u64,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        let base = self.resident_after_sequence();
        if self.logical_revision() != base.saturating_add(self.messages.len() as u64)
            || through_sequence < base
            || through_sequence > self.logical_revision()
        {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid,
                "history fingerprint requires a loaded dense logical prefix"));
        }
        if through_sequence == base {
            return Ok(self
                .archived_prefix
                .as_ref()
                .map_or([0; 32], |prefix| prefix.history_digest));
        }
        let position = usize::try_from(through_sequence - base - 1)
            .map_err(|_| conversation_error(work, crate::wire::ErrorCode::Invalid, "history prefix exceeds platform bounds"))?;
        self.prefix_digests
            .get(position)
            .copied()
            .ok_or_else(|| conversation_error(work, crate::wire::ErrorCode::Invalid, "history prefix digest is missing"))
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

    pub(crate) fn checkpointed_suffix<'work>(
        &self, selection: &ModelContextSelection,
        work: &mut Option<&'work mut dyn crate::store::history::HistoryIo>,
        mut retain: Option<&mut dyn FnMut(u64, u64, &mut Option<&'work mut dyn crate::store::history::HistoryIo>) -> Result<()>>,
    ) -> Result<(Self, u64)> {
        if retain.is_some() {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if !work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "message component custody requires original native work" });
            }
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            return Err(Error::Invalid("message component custody requires original native work".into()));
        }
        self.require_native_work(work)?;
        let reference = selection.checkpoint.as_ref()
            .ok_or_else(|| conversation_error(work, crate::wire::ErrorCode::Invalid, "history retirement requires a checkpoint pin"))?;
        let cut = selection.conversation_revision;
        if cut <= self.resident_after_sequence() || self.pending_user.is_some() {
            return self.copy(work, retain);
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
            return self.copy(work, retain);
        }
        drop(calls);
        drop(temporary);
        let digest = self.history_prefix_digest(cut, work)?;
        let start = self.messages.partition_point(|message| message.sequence <= cut);
        let suffix = self.messages.get(start..)
            .ok_or_else(|| conversation_error(work, crate::wire::ErrorCode::Invalid, "history retirement cut is invalid"))?;
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut carrier = Self::default();
        let retained = reserve_conversation_buffer(
            &mut carrier.messages, suffix.len(),cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut carrier.native_custody.messages,
        )?;
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
        let retained = carrier.copy_archive_checkpoint(cut, digest, reference, cache.work())?;
        cache.adopt(retained)?;
        let wire = ConversationStateWire {
            agent: self.agent, messages: std::mem::take(&mut carrier.messages),
            archived_prefix: carrier.archived_prefix.take(),
        };
        let (state, retained) = Self::from_wire(
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

    fn prepare_turn_index(
        &mut self, visible: bool, outcome_parent: Option<Uuid>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<u64> {
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        if visible && self.model_positions.len() == self.model_positions.capacity() {
            let target = self.model_positions.capacity().checked_mul(2)
                .map(|count| count.max(4))
                .ok_or_else(|| conversation_error(cache.work(), crate::wire::ErrorCode::Invalid,
                    "conversation position cache overflows"))?;
            let retained = reserve_conversation_buffer(
            &mut self.model_positions, target,cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut self.native_custody.positions,
        )?;
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
        reason = "the caller pre-admits every index and passes a validated in-bounds message"
    )]
    fn index_turn(&mut self, position: usize) {
        let message = &self.messages[position];
        if model_visible(&message.kind) {
            self.model_positions.push(position);
        }
        let outcome = turn_outcome_parent(message).is_some();
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

    /// Reads a bounded archive page at a caller-pinned logical tail.
    pub fn page(&self, after: u64, through: u64, maximum: usize) -> Result<&[ConversationMessage]> {
        let tail = self.logical_revision();
        if maximum == 0 || after < self.resident_after_sequence() || after > through || through > tail {
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
        if after < self.resident_after_sequence() || after > through || through > self.logical_revision() {
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

    fn adopt_existing_metadata(
        &mut self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(io) = work.as_deref_mut().filter(|io| io.original_native_work().is_some()) {
            if self.native_custody.cache_carrier.as_ref().is_some_and(|carrier| carrier.bytes() != 0) {
                let no_data = io.take_native_custody(0)?;
                self.install_native_preclaimed_caches(no_data, &mut Some(&mut *io))?;
            }
            adopt_existing_buffer::<ConversationMessage>(&mut self.native_custody.messages, self.messages.capacity(), io)?;
            adopt_existing_buffer::<[u8; 32]>(&mut self.native_custody.digests, self.prefix_digests.capacity(), io)?;
            adopt_existing_buffer::<usize>(&mut self.native_custody.positions, self.model_positions.capacity(), io)?;
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

    /// Appends one ordered, validated message.
    pub fn append(
        &mut self, message: ConversationMessage,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.require_native_work(work)?;
        self.validate_append(&message, work)?;
        let history_digest = advance_history_digest(
            self.history_digest(), &message, acyclic_stream::MAX_RECORD_BYTES as u64, work)?;
        self.adopt_existing_metadata(work)?;
        let mut cache = crate::wire_codec::CodecQuota::new(work, 0)?;
        let outcome_parent = turn_outcome_parent(&message)
            .filter(|parent| !self.outcomes.contains_key(parent));
        let preparation = (|| {
            if self.messages.len() == self.messages.capacity() {
                let target = self.messages.capacity().checked_mul(2).map(|count| count.max(4))
                    .ok_or_else(|| conversation_error(cache.work(), crate::wire::ErrorCode::Invalid,
                        "conversation message capacity overflows"))?;
                let retained = reserve_conversation_buffer(
            &mut self.messages, target,cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut self.native_custody.messages,
        )?;
                cache.adopt(retained)?;
            }
            if self.prefix_digests.len() == self.prefix_digests.capacity() {
                let target = self.prefix_digests.capacity().checked_mul(2).map(|count| count.max(4))
                    .ok_or_else(|| conversation_error(cache.work(), crate::wire::ErrorCode::Invalid,
                        "conversation digest cache overflows"))?;
                let retained = reserve_conversation_buffer(
            &mut self.prefix_digests, target,cache.work(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            &mut self.native_custody.digests,
        )?;
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
        self.by_id.insert(message.id, self.messages.len());
        if message.kind == MessageKind::User {
            self.pending_user = Some(message.id);
        }
        self.messages.push(message);
        self.prefix_digests.push(history_digest);
        self.index_turn(self.messages.len() - 1);
        cache.transfer();
        Ok(())
    }

    pub(crate) fn validate_append(
        &self, message: &ConversationMessage,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.agent
            .ok_or_else(|| conversation_error(work, crate::wire::ErrorCode::Conflict, "conversation is unbound"))?;
        message.validate(work)?;
        if self.messages.last().is_some_and(|last| {
            last.sequence
                != self
                    .resident_after_sequence()
                    .saturating_add(self.messages.len() as u64)
        }) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "sparse conversation history is read-only"));
        }
        let expected = self
            .logical_revision()
            .checked_add(1)
            .ok_or_else(|| conversation_error(work, crate::wire::ErrorCode::Invalid, "conversation sequence exhausted"))?;
        if message.sequence != expected || self.by_id.contains_key(&message.id) {
            return Err(conversation_error(work, crate::wire::ErrorCode::Conflict, "message sequence or identity is invalid"));
        }
        if let Some(parent) = message.reply_to
            && !self.by_id.contains_key(&parent)
        {
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "message reply target is missing"));
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
            return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "tool result has no preceding call"));
        }

        Ok(())
    }
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

fn advance_history_digest(
    previous: [u8; 32], message: &ConversationMessage, maximum_encoded_bytes: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<[u8; 32]> {
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

fn deserialize_inline_attachments<'de, D: Deserializer<'de>>(decoder: D) -> std::result::Result<Vec<Attachment>, D::Error> {
    let items = Vec::<Attachment>::deserialize(decoder)?;
    for item in &items { item.validate(&mut None).map_err(serde::de::Error::custom)?; }
    Ok(items)
}

fn deserialize_manifest_file<'de, D: Deserializer<'de>>(decoder: D) -> std::result::Result<FileRef, D::Error> {
    let file = FileRef::deserialize(decoder)?;
    if file.descriptor().media_type() != "application/vnd.acyclic.harness.attachments+json" {
        return Err(serde::de::Error::custom("attachment manifest metadata is invalid"));
    }
    Ok(file)
}

fn deserialize_label<'de, D: Deserializer<'de>>(decoder: D) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(decoder)?;
    validate_label(&value, &mut None).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn deserialize_path<'de, D: Deserializer<'de>>(decoder: D) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(decoder)?;
    validate_content_path(&value, &mut None).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn deserialize_media_type<'de, D: Deserializer<'de>>(decoder: D) -> std::result::Result<String, D::Error> {
    let value = String::deserialize(decoder)?;
    if let Some(reason) = FileDescriptor::validation_fields(0, &value) {
        return Err(serde::de::Error::custom(reason));
    }
    Ok(value)
}

fn deserialize_file_length<'de, D: Deserializer<'de>>(decoder: D) -> std::result::Result<u64, D::Error> {
    let value = u64::deserialize(decoder)?;
    if value > MAX_EXACT_JS_INTEGER {
        return Err(serde::de::Error::custom("file byte length exceeds cross-language precision"));
    }
    Ok(value)
}

fn validate_label(
    value: &str, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(native) = work.as_deref().and_then(|io| io.original_native_work()) {
        inspect_native_metadata(native, value.len())?;
    }
    if value.is_empty()
        || value.len() > MAX_LABEL_BYTES
        || value.chars().any(char::is_control)
        || value.contains('/')
        || value.contains('\\')
        || value == "."
        || value == ".." {
        return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "file or volume label is invalid"));
    }
    Ok(())
}

/// Checks a normalized volume-relative path.
pub fn validate_content_path(
    path: &str, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<()> {
    if let Some(io) = work.as_deref_mut() { io.check_cancellation()?; }
    if path.is_empty()
        || path.len() as u64 > MAX_PATH_BYTES as u64
        || path.starts_with('/')
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path.split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..") {
        return Err(conversation_error(work, crate::wire::ErrorCode::Invalid, "volume-relative path is invalid"));
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
        assert!(matches!(source.validate(&mut Some(&mut io)),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        source.path = "native-validation.txt".into();
        source.validate(&mut Some(&mut io))?;
        assert!(matches!(source.descriptor().verify(b"too long", &mut Some(&mut io)),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        assert!(matches!(source.descriptor().verify(b"other", &mut Some(&mut io)),
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, .. })));
        source.descriptor().verify(b"hello", &mut Some(&mut io))?;
        let mut allowance = native_conversation_budget();
        allowance.bytes_hashed = 0;
        let denied = NativeMaterializationWork::new(allowance, &cancellation).expect("hash denial ledger");
        let mut denied_io = crate::store::history::native_work_io(&denied).expect("hash denial observer");
        assert!(matches!(source.descriptor().verify(b"hello", &mut Some(&mut denied_io)),
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
        assert!(matches!(source.copy(&mut Some(&mut io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "bytes_copied", .. }), .. })));
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
    fn native_state_clone_and_suffix_transfer_only_actual_message_components() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        use crate::store::history::HistoryIo;
        let agent = AgentId::from_bytes([31; 16]);
        let content = file(agent, "native-component-copy.txt")?;
        let mut source = ConversationState::default();
        source.bind(agent)?;
        for sequence in 1..=4 { source.append(message(&content, sequence), &mut None)?; }
        let first_token = CancellationToken::new();
        let budget = native_conversation_budget();
        let first = NativeMaterializationWork::new(budget, &first_token).expect("snapshot copy ledger");
        let mut io = crate::store::history::native_work_io(&first).expect("snapshot copy observer");
        let (held, produced) = source.copy(&mut Some(&mut io), None)?;
        assert_eq!(held, source);
        assert_eq!(held.native_unclaimed_message_buffer_bytes()?, 0);
        assert_eq!(held.native_unclaimed_cache_bytes()?, 0);
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
        let (suffix, produced) = held.checkpointed_suffix(
            &selection, &mut Some(&mut io), Some(&mut retain))?;
        drop(retain);
        assert_eq!(produced, 0);
        assert_eq!(io.into_custody().bytes(), 0);
        assert_eq!(components.iter().map(|(sequence, _)| *sequence).collect::<Vec<_>>(), [3, 4]);
        assert!(components.iter().all(|(_, guard)| guard.work().shares_ledger(&second)));
        assert_eq!(first.receipt(), original_receipt, "cancelled source ledger is neither rebound nor absorbed");
        assert_eq!(suffix.history_digest(), source.history_digest());
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

    fn message(content: &FileRef, sequence: u64) -> ConversationMessage {
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
            state.append(message(&content, sequence), &mut Some(&mut io))?;
        }
        let head = state.history_digest();
        let mut denied_budget = budget;
        denied_budget.bytes_hashed = 0;
        let denied = NativeMaterializationWork::new(denied_budget, &cancellation).expect("hash denial ledger");
        let mut io = crate::store::history::native_work_io(&denied).expect("hash denial observer");
        assert!(matches!(state.append(message(&content, 5), &mut Some(&mut io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "bytes_hashed", .. }), .. })));
        assert_eq!(denied.receipt().bytes_hashed, 0);
        drop(io);
        assert_eq!(state.history_digest(), head);
        assert_eq!(state.messages.len(), 4);
        let probe = NativeMaterializationWork::new(budget, &cancellation).expect("original hash control");
        let mut probe_io = crate::store::history::native_work_io(&probe).expect("original hash observer");
        advance_history_digest(head, &message(&content, 5),
            acyclic_stream::MAX_RECORD_BYTES as u64, &mut Some(&mut probe_io))?;
        denied_budget = budget;
        denied_budget.allocation_operations = probe.receipt().allocation_operations;
        drop(probe_io);
        let denied = NativeMaterializationWork::new(denied_budget, &cancellation).expect("growth denial ledger");
        let mut io = crate::store::history::native_work_io(&denied).expect("growth denial observer");
        assert!(matches!(state.append(message(&content, 5), &mut Some(&mut io)),
            Err(Error::NativeWork { cause: NativeTreeMaterializationError::Work(
                WorkError::BudgetExceeded { counter: "allocation_operations", .. }), .. })));
        assert!(denied.receipt().bytes_hashed > 0, "the original message hash completed before growth");
        drop(io);
        assert_eq!(state.history_digest(), head);
        assert_eq!(state.messages.len(), 4);
        assert!(!state.by_id.contains_key(&Uuid::from_u128(5)));
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
            }, &mut None)?;
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
        let rebuilt = ConversationState::from_wire(
            wire, bytes.len() as u64, &mut Some(&mut work));
        let receipt = work.finish(rebuilt).map_err(|failure| failure.error)?;
        assert_eq!(receipt.value.0, original);
        assert!(receipt.work.bytes_encoded > 0);
        let mut denied = budget;
        denied.bytes_hashed = 0;
        let mut work = HistoryWork::new(denied, &cancellation);
        let wire = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let result = ConversationState::from_wire(
            wire, bytes.len() as u64, &mut Some(&mut work));
        let failed = work.finish(result).err().expect("first original message hash must reject");
        assert_eq!(failed.work.bytes_hashed, 0);
        assert!(failed.work.bytes_encoded > 0);
        assert_eq!(failed.work.output_bytes, 0);
        cancellation.cancel();
        let mut work = HistoryWork::new(budget, &cancellation);
        let wire = crate::contract::json_from_slice(&bytes)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let result = ConversationState::from_wire(
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
            state.append(message(&content, sequence), &mut None)?;
        }
        let digest = state.history_prefix_digest(1_000, &mut None)?;
        let selection = ModelContextSelection {
            conversation_revision: 998,
            message_ids: vec![Uuid::from_u128(997), Uuid::from_u128(998)],
            checkpoint: Some(checkpoint.clone()),
        };
        let suffix = state.checkpointed_suffix(&selection, &mut None, None).map(|(state, _)| state)?;
        assert_eq!(suffix.messages().len(), 2);
        assert_eq!(suffix.logical_revision(), 1_000);
        assert_eq!(suffix.resident_after_sequence(), 998);
        assert_eq!(suffix.history_prefix_digest(1_000, &mut None)?, digest);
        assert!(suffix.history_prefix_digest(997, &mut None).is_err());
        assert!(suffix.page(0, 1_000, 10).is_err());
        assert!(suffix.model_messages_between(0, 1_000, 10).is_err());
        assert_eq!(suffix.page(998, 1_000, 10)?.len(), 2);
        assert!(suffix.checkpoint_covers_archived_prefix(&checkpoint, 998));
        assert!(!suffix.checkpoint_covers_archived_prefix(&content, 998));
        let bytes = crate::contract::canonical_json_bytes(&suffix)?;
        let mut restored: ConversationState =
            serde_json::from_slice(&bytes).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(restored, suffix);
        assert_eq!(restored.history_prefix_digest(1_000, &mut None)?, digest);
        let next = message(&content, 1_001);
        restored.append(next, &mut None)?;
        assert_eq!(restored.logical_revision(), 1_001);
        assert_eq!(restored.messages().len(), 3);
        assert_eq!(restored.unresolved_user(), Some(Uuid::from_u128(1_001)));
        assert_eq!(restored.checkpointed_suffix(&selection, &mut None, None).map(|(state, _)| state)?, restored);
        Ok(())
    }

    #[test]
    fn full_history_fork_fingerprint_is_incremental_and_rebuilds_after_decode() -> Result<()> {
        let agent = AgentId::new();
        let content = file(agent, "fingerprint.txt")?;
        let mut state = ConversationState::default();
        state.bind(agent)?;
        for sequence in 1..=10_000_u64 {
            state.append(ConversationMessage {
                kind: if sequence % 2 == 0 { MessageKind::Interaction } else { MessageKind::User },
                reply_to: None,
                ..message(&content, sequence)
            }, &mut None)?;
        }
        let authority = crate::core::Authority {
            kind: crate::core::AggregateKind::Conversation,
            id: "fingerprint-parent".into(),
        };
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
        assert_eq!(pinned.format_version, 3);
        let old = state.history_prefix_digest(2, &mut None)?;
        let encoded = crate::contract::canonical_json_bytes(&state)?;
        let mut reopened: ConversationState =
            serde_json::from_slice(&encoded).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(
            reopened.history_prefix_digest(10_000, &mut None)?,
            pinned.message_digest
        );
        let mut next = reopened
            .messages()
            .last()
            .ok_or_else(|| Error::Invalid("fixture history is empty".into()))?
            .clone();
        next.sequence += 1;
        next.id = Uuid::from_u128(10_001);
        reopened.append(next.clone(), &mut None)?;
        assert_ne!(
            reopened.history_prefix_digest(10_001, &mut None)?,
            pinned.message_digest
        );
        assert_eq!(
            reopened.history_prefix_digest(10_000, &mut None)?,
            pinned.message_digest
        );
        assert_eq!(reopened.history_prefix_digest(2, &mut None)?, old);
        let before = reopened.history_prefix_digest(10_001, &mut None)?;
        assert!(reopened.append(next, &mut None).is_err());
        assert_eq!(reopened.history_prefix_digest(10_001, &mut None)?, before);
        let sparse = ConversationState::from_wire(ConversationStateWire { agent: None, archived_prefix: None, messages: vec![
            reopened
                .messages()
                .last()
                .ok_or_else(|| Error::Invalid("fixture history is empty".into()))?
                .clone(),
        ] }, acyclic_stream::MAX_RECORD_BYTES as u64, &mut None).map(|(state, _)| state)?;
        assert!(sparse.history_prefix_digest(1, &mut None).is_err());
        assert!(state.history_prefix_digest(10_001, &mut None).is_err());
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
            let mut record = ConversationMessage { kind, reply_to, ..message(content, sequence) };
            if terminal {
                record.extensions.insert("acyclic.turn.outcome".into(), content.clone());
            }
            let id = record.id;
            state.append(record, &mut None)?;
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
                kind: MessageKind::User,
                reply_to: None,
                ..message(&content, sequence)
            }, &mut None)?;
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
            reply_to: Some(Uuid::new_v4()),
            ..message(&file(agent, "history.txt")?, 1)
        };
        let before = state.copy(&mut None, None).map(|(state, _)| state)?;
        assert!(state.append(candidate.clone(), &mut None).is_err());
        assert_eq!(state, before);
        assert!(state.message(candidate.id).is_none());
        candidate.reply_to = None;
        state.append(candidate.clone(), &mut None)?;
        assert_eq!(state.message(candidate.id), Some(&candidate));
        let mut wire =
            serde_json::to_value(&state).map_err(|error| Error::Invalid(error.to_string()))?;
        wire["messages"][0]["sequence"] = 3.into();
        let mut sparse: ConversationState =
            serde_json::from_value(wire).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(sparse.page(0, 3, 1)?.len(), 1);
        candidate.id = Uuid::new_v4();
        candidate.sequence = 2;
        let (before, _) = sparse.copy(&mut None, None)?;
        assert!(sparse.append(candidate, &mut None).is_err());
        assert_eq!(sparse, before);
        Ok(())
    }

    #[test]
    fn owner_bound_private_write_rejects_an_attached_agent_with_a_copied_capability() -> Result<()>
    {
        let owner = AgentId::from_bytes([1; 16]);
        let attached = AgentId::from_bytes([2; 16]);
        let private = file(owner, "scratch/note.txt")?.volume().clone();
        let write = private.capability(VolumeOperation::Write, &mut None)?;
        let issuer = crate::core::AuthorityIssuer::new(
            "owner",
            [7; 32],
            crate::core::Authority {
                kind: crate::core::AggregateKind::Agent,
                id: owner.to_string(),
            },
        );
        let read = private.capability(VolumeOperation::Read, &mut None)?;
        let scope = issuer.issue("write", &crate::Capabilities::new([write.clone(), read.clone()]), Some(owner), None, &mut None).expect("scope proof");
        ContentGrant::verify(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &private, VolumeOperation::Write, u64::MAX, &mut None)?;
        let unsigned = issuer.issue("copied-capability", &crate::Capabilities::new([write.clone()]), None, None, &mut None).expect("scope proof");
        assert!(
            ContentGrant::verify(
                &issuer.verifier(&mut None).expect("verifier copy"),
                &unsigned,
                &private,
                VolumeOperation::Write
            , u64::MAX, &mut None)
            .is_err()
        );
        let inherited =
            issuer.attenuate(&scope, "child", crate::Capabilities::new([write.clone()]))?;
        assert_eq!(inherited.agent(), Some(owner));
        ContentGrant::verify(
            &issuer.verifier(&mut None).expect("verifier copy"),
            &inherited,
            &private,
            VolumeOperation::Write,
            u64::MAX, &mut None,
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
            foreign_issuer.issue("copied", &crate::Capabilities::new([write]), Some(attached), None, &mut None).expect("scope proof");
        assert!(
            ContentGrant::verify(
                &foreign_issuer.verifier(&mut None).expect("verifier copy"),
                &copied,
                &private,
                VolumeOperation::Write
            , u64::MAX, &mut None)
            .is_err()
        );
        let exact_file = file(owner, "scratch/note.txt")?;
        let delegated =
            issuer.delegate_private_file_read(&scope, attached, "attached-read", &exact_file)?;
        assert_eq!(delegated.agent(), Some(attached));
        ContentGrant::verify_file_read(&issuer.verifier(&mut None).expect("verifier copy"), &delegated, &exact_file)?;
        let internal = file(owner, ".system/execution/state.json")?;
        assert!(matches!(
            ContentGrant::verify_read(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &internal),
            Err(Error::Unauthorized(_))
        ));
        let internal_exact = issuer.issue("internal-exact", &crate::Capabilities::new([internal.read_capability()?]), Some(owner), None, &mut None).expect("scope proof");
        assert!(matches!(
            ContentGrant::verify_file_read(&issuer.verifier(&mut None).expect("verifier copy"), &internal_exact, &internal),
            Err(Error::Unauthorized(_))
        ));
        assert!(matches!(
            issuer.delegate_private_file_read(&scope, attached, "internal", &internal),
            Err(Error::Unauthorized(_))
        ));
        let inherited_context = file(owner, ".system/inherited-conversation/prefix.json")?;
        ContentGrant::verify_read(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &inherited_context)?;
        let directory = issuer.delegate_private_directory_read(
            &scope,
            attached,
            "attached-directory",
            &private,
            "scratch",
        )?;
        let directory_grant = ContentGrant::verify_directory_read(
            &issuer.verifier(&mut None).expect("verifier copy"),
            &directory,
            &private,
            "scratch",
            &mut None,
        )?;
        directory_grant.require_directory_path(&private, "scratch/deeper", None)?;
        directory_grant.require_file_read(&exact_file, None)?;
        assert!(
            directory_grant
                .require_directory_path(&private, "scratchpad", None)
                .is_err()
        );
        assert!(
            directory_grant
                .require_directory_path(&private, ".system", None)
                .is_err()
        );
        assert!(
            directory_grant
                .require(&private, VolumeOperation::Write, None)
                .is_err()
        );
        let foreign_owner_scope = foreign_issuer.issue("copied-read", &crate::Capabilities::new([read]), Some(attached), None, &mut None).expect("scope proof");
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
                "scratch".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(agent),
            &mut None,
            )?,
            (path).into(),
            ("version-1").into(),
            FileDescriptor::from_bytes(b"hello", "text/plain", &mut None)?,
            ("text.txt").into(),
            &mut None,
        )
    }

    #[test]
    fn descriptor_checks_exact_bytes() -> Result<()> {
        let descriptor = FileDescriptor::from_bytes(b"", "application/octet-stream", &mut None)?;
        descriptor.verify(b"", &mut None)?;
        assert!(descriptor.verify(b"x", &mut None).is_err());
        let encoded =
            serde_json::to_value(&descriptor).map_err(|e| Error::Invalid(e.to_string()))?;
        let decoded: FileDescriptor =
            serde_json::from_value(encoded).map_err(|e| Error::Invalid(e.to_string()))?;
        assert_eq!(decoded, descriptor);
        Ok(())
    }

    #[test]
    fn configured_limits_reject_oversized_references() -> Result<()> {
        Limits::default().validate_file(&file(AgentId::new(), &"x".repeat(4_097))?, &mut None)?;
        let file = file(AgentId::new(), "message.txt")?;
        let mut limits = Limits {
            file_bytes: 4,
            ..Limits::default()
        };
        limits.validate(&mut None)?;
        assert!(limits.validate_file(&file, &mut None).is_err());
        limits.file_bytes = 5;
        limits.validate_file(&file, &mut None)?;
        limits.model_events_per_step = 1;
        limits.validate(&mut None)?;
        limits.attachments = 0;
        assert!(limits.validate(&mut None).is_err());
        Limits {
            attachments: 65_537,
            model_steps: 1_000_001,
            model_events_per_step: 1_000_001,
            tool_calls_per_step: 1_000_001,
            context_messages: 1_000_001,
            ..Limits::default()
        }
        .validate(&mut None)?;
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
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: None,
            tool_call_id: Some("call-1".into()),
            extensions: BTreeMap::new(),
        };
        state.append(call.clone(), &mut None)?;
        assert!(state.append(call.clone(), &mut None).is_err());
        let result = ConversationMessage {
            id: Uuid::new_v4(),
            sequence: 2,
            kind: MessageKind::ToolResult,
            content: file(agent, "result.txt")?,
            attachments: ReferencedAttachments::Inline { items: Vec::new() },
            reply_to: Some(call.id),
            tool_call_id: Some("call-1".into()),
            extensions: BTreeMap::new(),
        };
        state.append(result, &mut None)?;
        assert_eq!(state.messages.len(), 2);
        Ok(())
    }

    #[test]
    fn v2_fixtures_round_trip_canonically_and_replay() -> Result<()> {
        fn round_trip<T: serde::de::DeserializeOwned + Serialize>(fixture: &str) -> Result<T> {
            let fixture = fixture.trim();
            let value: T = serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
            assert_eq!(serde_json::to_string(&value).map_err(|error| Error::Invalid(error.to_string()))?, fixture);
            Ok(value)
        }
        round_trip::<ConversationMessage>(include_str!("../fixtures/v2/conversation-message.json"))?.validate(&mut None)?;
        round_trip::<FileRef>(include_str!("../fixtures/v2/file-ref.json"))?;
        round_trip::<PrivateDirectoryPage>(include_str!("../fixtures/v2/private-directory-page.json"))?.validate()?;
        round_trip::<TaskOutcomeRecord>(include_str!("../fixtures/v2/task-outcome.json"))?.validate()?;
        let messages: Vec<ConversationMessage> = round_trip(include_str!("../fixtures/v2/conversation-kinds.json"))?;
        let mut state = ConversationState::default();
        state.bind(AgentId::from_bytes([1; 16]))?;
        for message in &messages {
            state.append(message.clone(), &mut None)?;
        }
        assert_eq!(state.messages.len(), 9);
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
                assert!(reference.descriptor().verify(b"hello", &mut None).is_err(), "{name}");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn composite_manifest_admission_routes_members_to_exact_providers() -> Result<()> {
        struct StaticContent {
            file: FileRef,
            bytes: Vec<u8>,
        }
        impl ContentResidencyVerifier for StaticContent {
            fn read<'a>(&'a self, reference: &'a FileRef, maximum_bytes: u64, io: Option<&'a ContentIo>)
                -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
                BoundedContentFuture::new(io, || async move {
                    if reference != &self.file { return Err(Error::NotFound("file".into())); }
                    if self.bytes.len() as u64 > maximum_bytes { return Err(Error::Invalid("content exceeds selected byte allowance".into())); }
                    Ok(self.bytes.clone().into())
                })
            }
        }
        let owner = VolumeOwner::Agent(AgentId::from_bytes([1; 16]));
        let manifest_provider = ProviderRef::new("test", "filesystem", "a")?;
        let member_provider = ProviderRef::new("test", "filesystem", "b")?;
        let member = FileRef::new(
            VolumeRef::new(
                member_provider.clone(),
                "private".into(),
                VolumeClass::AgentPrivate,
                owner.clone(),
            &mut None,
            )?,
            ("attachments/member.txt").into(),
            ("one").into(),
            FileDescriptor::from_bytes(&vec![b'x'; 1_024], "text/plain", &mut None)?,
            ("member.txt").into(),
            &mut None,
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
                "private".into(),
                VolumeClass::AgentPrivate,
                owner,
            &mut None,
            )?,
            ("attachments/list.json").into(),
            ("one").into(),
            FileDescriptor::from_bytes(&bytes, "application/vnd.acyclic.harness.attachments+json", &mut None)?,
            ("list.json").into(),
            &mut None,
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
            .verify_manifest(&manifest, 1, &Limits::default(), None)?
            .await?;
        let mut narrow = Limits::default();
        narrow.file_bytes = manifest.descriptor().byte_length().saturating_add(1);
        narrow.render_bytes = narrow.file_bytes;
        assert!(narrow.file_bytes < member.descriptor().byte_length());
        assert!(matches!(
            composite.verify_manifest(&manifest, 1, &narrow, None)?.await,
            Err(Error::Invalid(_))
        ));
        let incomplete =
            CompositeContentVerifier::new(vec![(manifest.volume().clone(), first.clone())])?;
        assert!(matches!(
            incomplete
                .verify_manifest(&manifest, 1, &Limits::default(), None)?
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
            ) -> BoxFuture<'a, Result<(ContentGrant, Arc<dyn ContentResidencyVerifier>)>> {
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
        let scope = issuer.issue("exact-member", &crate::Capabilities::new([member.read_capability()?]), Some(AgentId::from_bytes([1; 16])), None, &mut None).expect("scope proof");
        let grant = ContentGrant::verify_file_read(&issuer.verifier(&mut None).expect("verifier copy"), &scope, &member)?;
        let lazy = CompositeContentVerifier::new(vec![(manifest.volume().clone(), first)])?
            .with_mount_resolver(Arc::new(StaticMount {
                volume: member.volume().clone(),
                reader: second,
                grant,
            }));
        lazy.verify_manifest(&manifest, 1, &Limits::default(), None)?
            .await?;
        let unauthorized = FileRef::new(
            member.volume().clone(),
            ("attachments/another.txt").into(),
            ("one").into(),
            FileDescriptor::from_bytes(b"another", "text/plain", &mut None)?,
            ("another.txt").into(),
            &mut None,
        )?;
        assert!(matches!(
            lazy.verify(&unauthorized, unauthorized.descriptor().byte_length(), None)?.await,
            Err(Error::Unauthorized(_))
        ));
        let same_provider_member = FileRef::new(
            VolumeRef::new(
                manifest_provider,
                "private".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
            &mut None,
            )?,
            ("attachments/other-owner.txt").into(),
            ("one").into(),
            FileDescriptor::from_bytes(b"other-owner", "text/plain", &mut None)?,
            ("other-owner.txt").into(),
            &mut None,
        )?;
        let same_provider_items = vec![Attachment {
            file: same_provider_member.clone(),
            label: None,
        }];
        let same_provider_bytes = serde_json::to_vec(&same_provider_items)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let same_provider_manifest = FileRef::new(
            manifest.volume().clone(),
            ("attachments/other-owner-list.json").into(),
            ("one").into(),
            FileDescriptor::from_bytes(
                &same_provider_bytes,
                "application/vnd.acyclic.harness.attachments+json",
                &mut None,
            )?,
            ("other-owner-list.json").into(),
            &mut None,
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
            .verify_manifest(&same_provider_manifest, 1, &Limits::default(), None)?
            .await?;
        Ok(())
    }
}
