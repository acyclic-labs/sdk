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
        if self.file_bytes == 0
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
        {
            return Err(Error::Invalid("harness limits are invalid".into()));
        }
        Ok(())
    }

    /// Checks one immutable version against this runtime's tighter limits.
    pub fn validate_file(&self, file: &FileRef) -> Result<()> {
        self.validate()?;
        file.validate()?;
        if file.path().len() > self.path_bytes || file.descriptor().byte_length() > self.file_bytes
        {
            return Err(Error::Invalid("file exceeds harness limits".into()));
        }
        Ok(())
    }

    /// Checks every directly named file and attachment count before admission.
    pub fn validate_message(&self, message: &ConversationMessage) -> Result<()> {
        message.validate()?;
        self.validate_file(&message.content)?;
        match &message.attachments {
            ReferencedAttachments::Inline { items } => {
                if items.len() > self.attachments {
                    return Err(Error::Invalid("attachments exceed harness limits".into()));
                }
                for item in items {
                    self.validate_file(&item.file)?;
                }
            }
            ReferencedAttachments::Manifest {
                manifest,
                item_count,
            } => {
                if *item_count as usize > self.attachments {
                    return Err(Error::Invalid("attachments exceed harness limits".into()));
                }
                self.validate_file(manifest)?;
            }
        }
        for file in message.extensions.values() {
            self.validate_file(file)?;
        }
        Ok(())
    }
}

/// The three independently governed file namespaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum VolumeOwner {
    /// Project workspace owner.
    Project(String),
    /// Agent private-volume owner.
    Agent(AgentId),
    /// Shared session-volume owner.
    Session(String),
}

/// Globally addressable volume identity with no embedded capability or endpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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
    /// Validates independent consumer role and owner identities.
    pub fn new(
        provider: ProviderRef,
        id: impl Into<String>,
        class: VolumeClass,
        owner: VolumeOwner,
    ) -> Result<Self> {
        let value = Self {
            provider,
            id: id.into(),
            class,
            owner,
        };
        value.validate()?;
        Ok(value)
    }

    /// Rejects malformed provider and owner identities.
    pub fn validate(&self) -> Result<()> {
        self.provider.validate()?;
        validate_label(&self.id, MAX_LABEL_BYTES)?;
        match &self.owner {
            VolumeOwner::Project(id) | VolumeOwner::Session(id) => {
                validate_label(id, MAX_LABEL_BYTES)?;
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

    /// Enforces owner-only agent mutations independently of the consumer role.
    pub(crate) fn require_writer(&self, agent: Option<AgentId>) -> Result<()> {
        if let VolumeOwner::Agent(owner) = self.owner()
            && agent != Some(*owner)
        {
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
        verifier.verify(scope)?;
        if operation == VolumeOperation::Write {
            volume.require_writer(scope.agent())?;
        }
        if !scope
            .capabilities()
            .contains(&volume.capability(operation)?)
        {
            return Err(Error::Unauthorized(
                "volume operation is not granted".into(),
            ));
        }
        Ok(Self {
            volume: volume.clone(),
            operation,
            exact_file: None,
            directory_prefix: None,
        })
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
        if self.volume != *volume
            || self.operation != operation
            || self.exact_file.is_some()
            || self.directory_prefix.is_some()
        {
            return Err(Error::Unauthorized(
                "content grant does not match the volume".into(),
            ));
        }
        Ok(())
    }

    /// Checks a read against either a whole-volume or exact-version grant.
    pub fn require_file_read(&self, file: &FileRef) -> Result<()> {
        if self.volume != *file.volume()
            || self.operation != VolumeOperation::Read
            || self.exact_file.as_ref().is_some_and(|exact| exact != file)
            || self.directory_prefix.as_deref().is_some_and(|prefix| {
                is_internal_path(file.path()) || !path_below(file.path(), prefix)
            })
        {
            return Err(Error::Unauthorized(
                "content grant does not match the file".into(),
            ));
        }
        Ok(())
    }

    /// Checks any lazy directory path against the owner-issued read boundary.
    pub fn require_directory_path(&self, volume: &VolumeRef, prefix: &str) -> Result<()> {
        if self.volume != *volume
            || self.operation != VolumeOperation::Read
            || is_internal_path(prefix)
            || self
                .directory_prefix
                .as_deref()
                .is_none_or(|granted| !path_below(prefix, granted))
        {
            return Err(Error::Unauthorized(
                "directory listing is not granted".into(),
            ));
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
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FileDescriptor {
    sha256: [u8; 32],
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

    /// Verifies both digest and exact length, including empty files.
    pub fn verify(&self, bytes: &[u8]) -> Result<()> {
        if bytes.len() as u64 != self.byte_length || Sha256::digest(bytes).as_slice() != self.sha256
        {
            return Err(Error::Invalid(
                "file content does not match its descriptor".into(),
            ));
        }
        Ok(())
    }

    /// Checks metadata without fetching bytes.
    pub fn validate(&self) -> Result<()> {
        if self.byte_length > MAX_EXACT_JS_INTEGER {
            return Err(Error::Invalid(
                "file byte length exceeds cross-language precision".into(),
            ));
        }
        let Some((kind, subtype)) = self.media_type.split_once('/') else {
            return Err(Error::Invalid("media type is invalid".into()));
        };
        if kind.is_empty()
            || subtype.is_empty()
            || self.media_type.chars().any(|character| {
                !character.is_ascii_alphanumeric()
                    && !matches!(character, '/' | '-' | '+' | '.' | '_')
            })
        {
            return Err(Error::Invalid("media type is invalid".into()));
        }
        Ok(())
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

/// An immutable version of a file within a scoped volume.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
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

    /// Validates a ref decoded at any external boundary.
    pub fn validate(&self) -> Result<()> {
        self.volume.validate()?;
        validate_content_path(&self.path)?;
        validate_label(&self.version, MAX_LABEL_BYTES)?;
        validate_label(&self.display_name, MAX_LABEL_BYTES)?;
        self.descriptor.validate()
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
        self.file.validate()?;
        if let Some(label) = &self.label {
            validate_label(label, MAX_LABEL_BYTES)?;
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
    limits.validate_file(reference)?;
    if item_count as usize > limits.attachments {
        return Err(Error::Invalid(
            "attachment manifest count exceeds limits".into(),
        ));
    }
    let bytes = verified_content_bytes(verifier, reference).await?;
    let items = decode_attachment_manifest(reference, &bytes, item_count)?;
    for item in &items {
        limits.validate_file(&item.file)?;
        verified_content_bytes(verifier, &item.file).await?;
    }
    Ok(items)
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
        match self {
            Self::Inline { items } => {
                for item in items {
                    item.validate()?;
                }
            }
            Self::Manifest { manifest, .. } => {
                manifest.validate()?;
                if manifest.descriptor().media_type()
                    != "application/vnd.acyclic.harness.attachments+json"
                {
                    return Err(Error::Invalid(
                        "attachment manifest metadata is invalid".into(),
                    ));
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
        if self.id.is_nil() || self.reply_to.is_some_and(|id| id.is_nil()) {
            return Err(Error::Invalid("message identity is nil".into()));
        }
        if self.sequence == 0 {
            return Err(Error::Invalid("message sequence must start at one".into()));
        }
        self.content.validate()?;
        self.attachments.validate()?;
        for (name, reference) in &self.extensions {
            validate_label(name, MAX_LABEL_BYTES)?;
            if !name.contains('.') {
                return Err(Error::Invalid("extension key must be namespaced".into()));
            }
            reference.validate()?;
        }
        let has_call = self.tool_call_id.is_some();
        if has_call != matches!(self.kind, MessageKind::ToolCall | MessageKind::ToolResult) {
            return Err(Error::Invalid(
                "tool linkage does not match message kind".into(),
            ));
        }
        if let Some(call) = &self.tool_call_id {
            validate_label(call, MAX_LABEL_BYTES)?;
        }
        Ok(())
    }
}

/// Projection of one agent-owned conversation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
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
struct ConversationStateWire {
    agent: Option<AgentId>,
    messages: Vec<ConversationMessage>,
    #[serde(default)]
    archived_prefix: Option<ArchivedConversationPrefix>,
}

impl TryFrom<ConversationStateWire> for ConversationState {
    type Error = Error;

    fn try_from(wire: ConversationStateWire) -> Result<Self> {
        let mut by_id = BTreeMap::new();
        if let Some(prefix) = &wire.archived_prefix {
            prefix.checkpoint.validate()?;
            if wire.agent.is_none() || prefix.through_sequence == 0 {
                return Err(Error::Invalid(
                    "archived conversation prefix is unbound".into(),
                ));
            }
        }
        let mut previous = wire
            .archived_prefix
            .as_ref()
            .map_or(0, |prefix| prefix.through_sequence);
        let mut history_digest = wire
            .archived_prefix
            .as_ref()
            .map_or([0; 32], |prefix| prefix.history_digest);
        let mut prefix_digests = Vec::with_capacity(wire.messages.len());
        for (position, message) in wire.messages.iter().enumerate() {
            message.validate()?;
            if message.sequence <= previous
                || (wire.archived_prefix.is_some()
                    && previous.checked_add(1) != Some(message.sequence))
                || by_id.insert(message.id, position).is_some()
            {
                return Err(Error::Invalid(
                    "conversation order or identity is invalid".into(),
                ));
            }
            previous = message.sequence;
            history_digest = advance_history_digest(history_digest, message)?;
            prefix_digests.push(history_digest);
        }
        let pending_user = wire
            .messages
            .iter()
            .rev()
            .find(|message| message.kind == MessageKind::User)
            .map(|message| message.id);
        let mut state = Self {
            agent: wire.agent,
            messages: wire.messages,
            archived_prefix: wire.archived_prefix,
            prefix_digests,
            by_id,
            pending_user,
            history_digest,
            ..Self::default()
        };
        for position in 0..state.messages.len() {
            state.index_turn(position);
        }
        Ok(state)
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
        let base = self.archived_through();
        if self.logical_revision() != base.saturating_add(self.messages.len() as u64)
            || through_sequence < base
            || through_sequence > self.logical_revision()
        {
            return Err(Error::Invalid(
                "history fingerprint requires a loaded dense logical prefix".into(),
            ));
        }
        if through_sequence == base {
            return Ok(self
                .archived_prefix
                .as_ref()
                .map_or([0; 32], |prefix| prefix.history_digest));
        }
        let position = usize::try_from(through_sequence - base - 1)
            .map_err(|_| Error::Invalid("history prefix exceeds platform bounds".into()))?;
        self.prefix_digests
            .get(position)
            .copied()
            .ok_or_else(|| Error::Invalid("history prefix digest is missing".into()))
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
    pub(crate) fn checkpointed_suffix(&self, selection: &ModelContextSelection) -> Result<Self> {
        let reference = selection
            .checkpoint
            .as_ref()
            .ok_or_else(|| Error::Invalid("history retirement requires a checkpoint pin".into()))?;
        let cut = selection.conversation_revision;
        if cut <= self.archived_through() || self.pending_user.is_some() {
            return Ok(self.clone());
        }
        crate::projection::validate_model_context_selection_at_revision(self, selection, cut)?;
        let mut calls = BTreeSet::new();
        for message in self
            .messages
            .iter()
            .take_while(|message| message.sequence <= cut)
        {
            match message.kind {
                MessageKind::ToolCall => {
                    calls.insert(message.id);
                }
                MessageKind::ToolResult => {
                    if let Some(parent) = message.reply_to {
                        calls.remove(&parent);
                    }
                }
                _ => {}
            }
        }
        if !calls.is_empty() {
            return Ok(self.clone());
        }
        let digest = self.history_prefix_digest(cut)?;
        let start = self
            .messages
            .partition_point(|message| message.sequence <= cut);
        Self::try_from(ConversationStateWire {
            agent: self.agent,
            messages: self
                .messages
                .get(start..)
                .ok_or_else(|| Error::Invalid("history retirement cut is invalid".into()))?
                .to_vec(),
            archived_prefix: Some(ArchivedConversationPrefix {
                through_sequence: cut,
                history_digest: digest,
                checkpoint: reference.clone(),
            }),
        })
    }

    /// Rebuilds only a bounded, already attested selection for byte projection.
    /// This unbound sparse view cannot authorize append or stand in for history.
    pub(crate) fn selected_view(messages: Vec<ConversationMessage>) -> Result<Self> {
        Self::try_from(ConversationStateWire {
            agent: None,
            messages,
            archived_prefix: None,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "only append-after-push and decode's in-bounds enumeration call this private indexer"
    )]
    fn index_turn(&mut self, position: usize) {
        let message = &self.messages[position];
        if matches!(
            message.kind,
            MessageKind::System
                | MessageKind::User
                | MessageKind::Assistant
                | MessageKind::ToolCall
                | MessageKind::ToolResult
        ) {
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
        self.validate_append(&message)?;
        let history_digest = advance_history_digest(self.history_digest, &message)?;
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
        Ok(())
    }

    /// Admits the next append through rebuildable identity indexes without
    /// copying or scanning the complete history.
    pub(crate) fn validate_append(&self, message: &ConversationMessage) -> Result<()> {
        self.agent
            .ok_or_else(|| Error::Conflict("conversation is unbound".into()))?;
        message.validate()?;
        if self.messages.last().is_some_and(|last| {
            last.sequence
                != self
                    .archived_through()
                    .saturating_add(self.messages.len() as u64)
        }) {
            return Err(Error::Invalid(
                "sparse conversation history is read-only".into(),
            ));
        }
        let expected = self
            .logical_revision()
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("conversation sequence exhausted".into()))?;
        if message.sequence != expected || self.by_id.contains_key(&message.id) {
            return Err(Error::Conflict(
                "message sequence or identity is invalid".into(),
            ));
        }
        if let Some(parent) = message.reply_to
            && !self.by_id.contains_key(&parent)
        {
            return Err(Error::Invalid("message reply target is missing".into()));
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
            return Err(Error::Invalid("tool result has no preceding call".into()));
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

fn advance_history_digest(previous: [u8; 32], message: &ConversationMessage) -> Result<[u8; 32]> {
    #[cfg(test)]
    HISTORY_HASH_CALLS.with(|calls| calls.set(calls.get() + 1));
    let digest = crate::contract::canonical_json_digest(message)?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness:conversation-prefix:v3\0");
    hash.update(&previous);
    hash.update(&digest);
    Ok(*hash.finalize().as_bytes())
}

fn validate_label(value: &str, limit: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > limit
        || value.chars().any(char::is_control)
        || value.contains('/')
        || value.contains('\\')
        || value == "."
        || value == ".."
    {
        return Err(Error::Invalid("file or volume label is invalid".into()));
    }
    Ok(())
}

/// Checks a normalized volume-relative content path at every provider boundary.
/// Backslashes, traversal segments, control characters, and oversized paths are rejected.
pub fn validate_content_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() as u64 > MAX_PATH_BYTES as u64
        || path.starts_with('/')
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(Error::Invalid("volume-relative path is invalid".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let suffix = state.checkpointed_suffix(&selection)?;
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
        assert_eq!(restored.checkpointed_suffix(&selection)?, restored);
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
        let before = state.clone();
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
        let before = sparse.clone();
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
