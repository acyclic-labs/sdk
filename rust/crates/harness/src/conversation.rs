//! Canonical, payload-free conversation and file references.
//!
//! A reference identifies an immutable version of a file. A volume head may be
//! edited, but edits create a new version and never change a recorded message.

use crate::{
    AgentId, Error, OperationId, Result,
    core::{AuthorityVerifier, Scope},
    resources::ProviderRef,
};
use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest as _, Sha256};
use std::{collections::BTreeMap, sync::Arc};
use uuid::Uuid;

use std::{future::Future, pin::Pin};

const MAX_PATH_BYTES: usize = 4_096;
const MAX_LABEL_BYTES: usize = 255;
const MAX_EXACT_JS_INTEGER: u64 = (1_u64 << 53) - 1;

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
            file_bytes: 64 * 1024 * 1024,
            path_bytes: MAX_PATH_BYTES,
            attachments: 65_536,
            render_bytes: 128 * 1024,
            model_steps: 64,
            model_events_per_step: 4_096,
            tool_calls_per_step: 64,
            context_messages: 256,
        }
    }
}

impl Limits {
    /// Prevents zero bounds or configuration that widens the wire protocol.
    pub fn validate(&self) -> Result<()> {
        if self.file_bytes == 0
            || self.file_bytes > MAX_EXACT_JS_INTEGER
            || self.path_bytes == 0
            || self.path_bytes > MAX_PATH_BYTES
            || self.attachments == 0
            || self.attachments > 65_536
            || self.render_bytes == 0
            || self.render_bytes > self.file_bytes
            || self.model_steps == 0
            || self.model_steps > 1_000_000
            || self.model_events_per_step == 0
            || self.model_events_per_step > 1_000_000
            || self.tool_calls_per_step == 0
            || self.tool_calls_per_step > self.model_events_per_step
            || self.context_messages == 0
            || self.context_messages > 1_000_000
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
    /// Copy-on-write project files; the sole mergeable class.
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
    /// Validates the owner/class pair at construction and decoding.
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

    /// Rejects malformed identities and mismatched owner classes.
    pub fn validate(&self) -> Result<()> {
        self.provider.validate()?;
        validate_label(&self.id, MAX_LABEL_BYTES)?;
        let matches = matches!(
            (&self.class, &self.owner),
            (VolumeClass::Project, VolumeOwner::Project(_))
                | (VolumeClass::AgentPrivate, VolumeOwner::Agent(_))
                | (VolumeClass::SessionShared, VolumeOwner::Session(_))
        );
        if !matches {
            return Err(Error::Invalid(
                "volume owner does not match its class".into(),
            ));
        }
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

    /// Access and fork semantics of this namespace.
    #[must_use]
    pub const fn class(&self) -> VolumeClass {
        self.class
    }

    /// Logical owner, used to route authorized reads.
    #[must_use]
    pub const fn owner(&self) -> &VolumeOwner {
        &self.owner
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
    /// Empty prefix names the whole private volume. It never implies a write.
    pub fn directory_read_capability(&self, prefix: &str) -> Result<String> {
        if self.class != VolumeClass::AgentPrivate {
            return Err(Error::Invalid(
                "directory delegation requires an agent-private volume".into(),
            ));
        }
        if !prefix.is_empty() {
            validate_path(prefix)?;
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
        if operation == VolumeOperation::Write && volume.class() == VolumeClass::AgentPrivate {
            match (scope.agent(), volume.owner()) {
                (Some(agent), VolumeOwner::Agent(owner)) if &agent == owner => {}
                _ => {
                    return Err(Error::Unauthorized(
                        "only the original agent may write its private volume".into(),
                    ));
                }
            }
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

    /// Verifies a signed, reader-bound private-directory grant. Resolution is
    /// lazy and independent of fork or workspace ancestry.
    pub fn verify_directory_read(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        volume: &VolumeRef,
        prefix: &str,
    ) -> Result<Self> {
        verifier.verify(scope)?;
        if scope.agent().is_none()
            || !scope
                .capabilities()
                .contains(&volume.directory_read_capability(prefix)?)
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

fn is_internal_path(path: &str) -> bool {
    path == ".system" || path.starts_with(".system/")
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
        if self.media_type.len() > 127
            || kind.is_empty()
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
        validate_path(&self.path)?;
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
            Self::Failed { message } if message.is_empty() || message.len() > 4_096 => {
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
pub trait ContentResidencyVerifier: Send + Sync {
    /// Resolves and checks the exact referenced file version.
    fn verify<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;

    /// Reads exact verified bytes for schema validation at an admission boundary.
    fn read<'a>(
        &'a self,
        _reference: &'a FileRef,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "content byte resolution is unavailable".into(),
            ))
        })
    }

    /// Resolves a complete referenced list and enforces configured limits on every member.
    fn verify_manifest<'a>(
        &'a self,
        _reference: &'a FileRef,
        _item_count: u32,
        _limits: &'a Limits,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
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
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Attachment>>> + Send + 'a>> {
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
pub trait ContentPublisher: Send + Sync {
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
    ) -> Pin<Box<dyn Future<Output = Result<FileRef>> + Send + 'a>>;
}

/// Routes admission reads by exact owner volume, including several agents on
/// one provider and members stored separately from their manifests.
pub struct CompositeContentVerifier {
    volumes: BTreeMap<String, Arc<dyn ContentResidencyVerifier>>,
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
        Ok(Self { volumes })
    }

    fn key(volume: &VolumeRef) -> Result<String> {
        volume.validate()?;
        serde_json::to_string(volume).map_err(|error| Error::Invalid(error.to_string()))
    }

    fn owner(&self, reference: &FileRef) -> Result<&Arc<dyn ContentResidencyVerifier>> {
        self.volumes
            .get(&Self::key(reference.volume())?)
            .ok_or_else(|| Error::Unsupported("content volume is not registered".into()))
    }
}

impl ContentResidencyVerifier for CompositeContentVerifier {
    fn verify<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { self.owner(reference)?.verify(reference).await })
    }

    fn read<'a>(
        &'a self,
        reference: &'a FileRef,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send + 'a>> {
        Box::pin(async move { self.owner(reference)?.read(reference).await })
    }

    fn load_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Attachment>>> + Send + 'a>> {
        Box::pin(async move {
            self.owner(reference)?
                .load_manifest(reference, item_count)
                .await
        })
    }

    fn verify_manifest<'a>(
        &'a self,
        reference: &'a FileRef,
        item_count: u32,
        limits: &'a Limits,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            limits.validate_file(reference)?;
            let items = self.load_manifest(reference, item_count).await?;
            if items.len() != item_count as usize || items.len() > limits.attachments {
                return Err(Error::Invalid(
                    "attachment manifest count exceeds limits".into(),
                ));
            }
            for item in items {
                item.validate()?;
                limits.validate_file(&item.file)?;
                self.verify(&item.file).await?;
            }
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
    if items.len() > 65_536 {
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
    let items: Vec<Attachment> = serde_json::from_slice(bytes)
        .map_err(|error| Error::Invalid(format!("attachment manifest is invalid: {error}")))?;
    if encode_attachment_manifest(&items)? != bytes {
        return Err(Error::Invalid(
            "attachment manifest is not canonical or complete".into(),
        ));
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
    /// Rejects unbounded inline records and malformed manifest metadata.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Inline { items } => {
                if items.len() > 128 {
                    return Err(Error::Invalid(
                        "inline attachment count exceeds limit".into(),
                    ));
                }
                for item in items {
                    item.validate()?;
                }
            }
            Self::Manifest {
                manifest,
                item_count,
            } => {
                manifest.validate()?;
                if *item_count > 65_536
                    || manifest.descriptor().media_type()
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
#[serde(deny_unknown_fields)]
pub struct ConversationState {
    /// Set once at admission.
    pub agent: Option<AgentId>,
    /// Ordered canonical history.
    pub messages: Vec<ConversationMessage>,
}

/// Exact history revision and ordered subset selected for one model request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelContextSelection {
    /// Number of canonical messages observed when selecting.
    pub conversation_revision: u64,
    /// Ordered, unique message identities; omitted history is deliberate.
    pub message_ids: Vec<Uuid>,
}

impl ModelContextSelection {
    /// Rejects stale, missing, duplicated, or out-of-order source identities.
    pub fn validate(&self, conversation: &ConversationState) -> Result<()> {
        if self.conversation_revision != conversation.messages.len() as u64 {
            return Err(Error::Conflict(
                "model context selection has a stale conversation revision".into(),
            ));
        }
        let by_id = conversation
            .messages
            .iter()
            .map(|message| (message.id, message.sequence))
            .collect::<BTreeMap<_, _>>();
        if by_id.len() != conversation.messages.len() {
            return Err(Error::Invalid(
                "conversation has duplicate message identities".into(),
            ));
        }
        let mut previous_sequence = 0;
        for id in &self.message_ids {
            let sequence = by_id
                .get(id)
                .copied()
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
        self.agent
            .ok_or_else(|| Error::Conflict("conversation is unbound".into()))?;
        message.validate()?;
        let expected = (self.messages.len() as u64)
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("conversation sequence exhausted".into()))?;
        if message.sequence != expected || self.messages.iter().any(|prior| prior.id == message.id)
        {
            return Err(Error::Conflict(
                "message sequence or identity is invalid".into(),
            ));
        }
        if let Some(parent) = message.reply_to
            && !self.messages.iter().any(|prior| prior.id == parent)
        {
            return Err(Error::Invalid("message reply target is missing".into()));
        }
        if message.kind == MessageKind::ToolResult
            && !self.messages.iter().any(|prior| {
                prior.kind == MessageKind::ToolCall
                    && Some(prior.id) == message.reply_to
                    && prior.tool_call_id.as_deref() == message.tool_call_id.as_deref()
            })
        {
            return Err(Error::Invalid("tool result has no preceding call".into()));
        }
        // Foreign-owned refs may be carried globally. Their bytes are gated by
        // the provider's owner-mediated read grant at event admission/resolution.
        self.messages.push(message);
        Ok(())
    }
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

fn validate_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > MAX_PATH_BYTES
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
        let file = file(AgentId::new(), "message.txt")?;
        let mut limits = Limits {
            file_bytes: 4,
            render_bytes: 4,
            ..Limits::default()
        };
        assert!(limits.validate_file(&file).is_err());
        limits.file_bytes = 5;
        limits.validate_file(&file)?;
        limits.attachments = 0;
        assert!(limits.validate().is_err());
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
            items: Option<Vec<Attachment>>,
        }
        impl ContentResidencyVerifier for StaticContent {
            fn verify<'a>(
                &'a self,
                reference: &'a FileRef,
            ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
                Box::pin(async move {
                    if reference == &self.file {
                        Ok(())
                    } else {
                        Err(Error::NotFound("file".into()))
                    }
                })
            }
            fn load_manifest<'a>(
                &'a self,
                reference: &'a FileRef,
                item_count: u32,
            ) -> Pin<Box<dyn Future<Output = Result<Vec<Attachment>>> + Send + 'a>> {
                Box::pin(async move {
                    let items = self
                        .items
                        .as_ref()
                        .ok_or_else(|| Error::Unsupported("manifest".into()))?;
                    if reference != &self.file || items.len() != item_count as usize {
                        return Err(Error::Invalid("manifest identity or count mismatch".into()));
                    }
                    Ok(items.clone())
                })
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
            items: Some(items),
        });
        let second = Arc::new(StaticContent {
            file: member.clone(),
            items: None,
        });
        let composite = CompositeContentVerifier::new(vec![
            (manifest.volume().clone(), first.clone()),
            (member.volume().clone(), second),
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
        let incomplete = CompositeContentVerifier::new(vec![(manifest.volume().clone(), first)])?;
        assert!(matches!(
            incomplete
                .verify_manifest(&manifest, 1, &Limits::default())
                .await,
            Err(Error::Unsupported(_))
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
                    items: Some(same_provider_items),
                }),
            ),
            (
                same_provider_member.volume().clone(),
                Arc::new(StaticContent {
                    file: same_provider_member,
                    items: None,
                }),
            ),
        ])?;
        same_provider
            .verify_manifest(&same_provider_manifest, 1, &Limits::default())
            .await?;
        Ok(())
    }
}
