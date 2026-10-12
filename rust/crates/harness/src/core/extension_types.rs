//! extension types semantics.
use super::*;
/// Explicit treatment of one extension's state at a child fork.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionForkPolicy {
    /// Retain the selected immutable state revision for the child.
    Inherit,
    /// Require a fresh child-owned state revision.
    Reset,
    /// Refuse to fork while this extension state is selected.
    Reject,
}

/// One versioned extension state record. Bytes live only at `content`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionRecord {
    /// Namespaced schema identity.
    pub name: String,
    /// Positive schema version.
    pub version: u32,
    /// Digest pinning the exact registered JSON Schema.
    pub schema_digest: [u8; 32],
    /// Digest pinning the admitted implementation.
    pub implementation_digest: [u8; 32],
    /// Fork treatment pinned at admission.
    pub fork_policy: ExtensionForkPolicy,
    /// Immutable JSON payload reference.
    pub content: FileRef,
}

/// Version transition that never rewrites or removes its retained input.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionStateMigration {
    /// Latest state event that was validated as the migration input.
    pub previous: EventReference,
    /// New schema-validated state revision.
    pub record: ExtensionRecord,
}

impl ExtensionStateMigration {
    /// Checks the portable envelope; the reducer additionally checks that `previous` is the latest state event under the same authority.
    pub fn validate(&self) -> Result<()> {
        if self.previous.revision == 0 {
            return Err(Error::Invalid("extension migration source is empty".into()));
        }
        self.previous.authority.stream_path(&mut None)?;
        self.record.validate()
    }
}

impl ExtensionRecord {
    /// Checks the envelope before consulting its pinned schema registry.
    pub fn validate(&self) -> Result<()> {
        validate_extension_name(&self.name)?;
        if self.version == 0
            || self.schema_digest == [0; 32]
            || self.implementation_digest == [0; 32]
            || self.content.descriptor().media_type() != "application/json"
        {
            return Err(Error::Invalid("extension record is invalid".into()));
        }
        self.content.validate(&mut None)
    }
}

fn extension_name_valid(name: &str) -> bool {
    name.contains('.')
        && !name.starts_with("acyclic.")
        && !name.split('.').any(str::is_empty)
        && name
            .chars()
            .all(|character| character.is_alphanumeric() || matches!(character, '.' | '_' | '-'))
}

pub(crate) fn validate_extension_name(name: &str) -> Result<()> {
    if !extension_name_valid(name) {
        return Err(Error::Invalid(
            "extension name must be a bounded namespace".into(),
        ));
    }
    Ok(())
}

/// Exact installed version required by another extension at activation.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionDependency {
    /// Namespaced extension identity.
    pub name: String,
    /// Required positive schema and implementation version.
    pub version: u32,
}

impl ExtensionDependency {
    /// Rejects ambiguous or unbounded dependency identities.
    pub fn validate(&self) -> Result<()> {
        validate_extension_name(&self.name)?;
        if self.version == 0 {
            return Err(Error::Invalid(
                "extension dependency version is zero".into(),
            ));
        }
        Ok(())
    }
}

/// File-backed configuration pinned to one installed extension schema.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionConfiguration {
    /// Exact installed extension version.
    pub extension: ExtensionDependency,
    /// Digest of the immutable configuration JSON Schema.
    pub schema_digest: [u8; 32],
    /// Immutable provider-owned JSON configuration.
    pub content: FileRef,
}

/// Immutable agent selection captured for task admission. The source points
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtensionAdmission {
    pub(in crate::core) source: EventReference,
    pub(in crate::core) selected: Vec<ExtensionDependency>,
    pub(in crate::core) configurations: Vec<ExtensionConfiguration>,
}

impl ExtensionAdmission {
    /// Actual heap ownership, excluding inline proofs and identifiers.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        ownership::owned_sum([
            self.source.authority.id.capacity() as u64,
            ownership::owned_vector(&self.selected, |extension| {
                Ok(extension.name.capacity() as u64)
            })?,
            ownership::owned_vector(&self.configurations, ownership::owned_configuration)?,
        ])
    }

    pub(crate) fn copy(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Self, u64)> {
        crate::wire_codec::CodecQuota::produce(work, |quota| {
            if quota.work().is_none() {
                return Ok(self.clone());
            }
            Ok(Self {
                source: copying::copy_reference(&self.source, quota)?,
                selected: copying::copy_vector(&self.selected, quota, copying::copy_dependency)?,
                configurations: copying::copy_vector(
                    &self.configurations,
                    quota,
                    copying::copy_configuration,
                )?,
            })
        })
    }

    /// Exact committed selection event that supplied these bindings.
    #[must_use]
    pub const fn source(&self) -> &EventReference {
        &self.source
    }

    /// Dependency-ordered installed versions frozen for this admission.
    #[must_use]
    pub fn selected(&self) -> &[ExtensionDependency] {
        &self.selected
    }

    /// File-backed configuration revisions frozen for this admission.
    #[must_use]
    pub fn configurations(&self) -> &[ExtensionConfiguration] {
        &self.configurations
    }

    /// Checks the frozen selection and its ordered configuration records.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        use crate::fork::boundary_error;
        use crate::wire::ErrorCode::{Conflict, Invalid};
        if self.source.authority.kind != AggregateKind::Agent || self.source.revision == 0 {
            return Err(boundary_error(
                work,
                Invalid,
                "extension admission source is invalid",
            ));
        }
        self.source.authority.validate_with_work(work)?;
        for (index, extension) in self.selected.iter().enumerate() {
            if let Some(io) = work.as_deref() {
                io.check_cancellation()?;
            }
            if !extension_name_valid(&extension.name) || extension.version == 0 {
                return Err(boundary_error(
                    work,
                    Invalid,
                    "extension dependency is invalid",
                ));
            }
            if self.selected[..index]
                .iter()
                .any(|prior| prior.name == extension.name)
            {
                return Err(boundary_error(
                    work,
                    Conflict,
                    "duplicate selected extension",
                ));
            }
        }
        let mut last_position = None;
        for configuration in &self.configurations {
            if let Some(io) = work.as_deref() {
                io.check_cancellation()?;
            }
            if !extension_name_valid(&configuration.extension.name)
                || configuration.extension.version == 0
                || configuration.schema_digest == [0; 32]
                || configuration.content.descriptor().media_type() != "application/json"
            {
                return Err(boundary_error(
                    work,
                    Invalid,
                    "extension configuration is invalid",
                ));
            }
            configuration.content.validate(work)?;
            let position = self
                .selected
                .iter()
                .position(|extension| extension == &configuration.extension);
            if position.is_none_or(|position| last_position.is_some_and(|last| position <= last)) {
                return Err(boundary_error(
                    work,
                    Conflict,
                    "extension admission configuration is not uniquely selected",
                ));
            }
            last_position = position;
        }
        Ok(())
    }
}

impl ExtensionConfiguration {
    /// Validates the envelope independently of the installed schema.
    pub fn validate(&self) -> Result<()> {
        self.extension.validate()?;
        if self.schema_digest == [0; 32]
            || self.content.descriptor().media_type() != "application/json"
        {
            return Err(Error::Invalid("extension configuration is invalid".into()));
        }
        self.content.validate(&mut None)
    }
}
