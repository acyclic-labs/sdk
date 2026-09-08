//! Provider-neutral references to state owned by other Acyclic SDK families.

use crate::{Error, Result};
use serde::{Deserialize, Deserializer, Serialize};

const MAX_REFERENCE_KEY_BYTES: usize = 4_096;

/// Stable identity of a replaceable resource provider.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ProviderRef {
    /// Globally unique namespace, such as `acyclic` or a reverse DNS name.
    namespace: String,
    /// Provider family, such as `filesystem` or `machines`.
    family: String,
    /// Immutable provider contract version.
    version: String,
}

impl ProviderRef {
    /// Creates a validated provider identity.
    pub fn new(
        namespace: impl Into<String>,
        family: impl Into<String>,
        version: impl Into<String>,
    ) -> Result<Self> {
        let value = Self {
            namespace: namespace.into(),
            family: family.into(),
            version: version.into(),
        };
        value.validate()?;
        Ok(value)
    }

    /// Validates a value received through an untrusted serialization boundary.
    pub fn validate(&self) -> Result<()> {
        for part in [&self.namespace, &self.family, &self.version] {
            if part.is_empty() || part.len() > 128 || part.chars().any(char::is_control) {
                return Err(Error::Invalid("provider identity is invalid".into()));
            }
        }
        Ok(())
    }

    /// Globally unique provider namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Semantic provider family.
    #[must_use]
    pub fn family(&self) -> &str {
        &self.family
    }

    /// Immutable provider contract version.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }
}

#[derive(Deserialize)]
struct ProviderRefWire {
    namespace: String,
    family: String,
    version: String,
}

impl<'de> Deserialize<'de> for ProviderRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = ProviderRefWire::deserialize(deserializer)?;
        Self::new(wire.namespace, wire.family, wire.version).map_err(serde::de::Error::custom)
    }
}

/// Semantic kind of a provider-owned reference.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    /// Mutable workspace head.
    Workspace,
    /// Immutable workspace generation.
    Generation,
    /// Immutable content or machine image.
    Artifact,
    /// Live isolated execution allocation.
    Sandbox,
    /// Immutable sandbox checkpoint.
    Checkpoint,
    /// Durable event stream.
    Stream,
    /// Immutable model context.
    Context,
    /// Recoverable model run.
    Run,
}

/// Bounded opaque identity whose interpretation belongs only to its provider.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResourceRef {
    kind: ResourceKind,
    provider: ProviderRef,
    key: Vec<u8>,
    version: Option<String>,
}

#[derive(Deserialize)]
struct ResourceRefWire {
    kind: ResourceKind,
    provider: ProviderRef,
    key: Vec<u8>,
    version: Option<String>,
}

impl<'de> Deserialize<'de> for ResourceRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = ResourceRefWire::deserialize(deserializer)?;
        Self::new(wire.kind, wire.provider, wire.key, wire.version)
            .map_err(serde::de::Error::custom)
    }
}

impl ResourceRef {
    /// Creates a validated opaque reference without embedding credentials or endpoints.
    pub fn new(
        kind: ResourceKind,
        provider: ProviderRef,
        key: impl Into<Vec<u8>>,
        version: Option<String>,
    ) -> Result<Self> {
        let value = Self {
            kind,
            provider,
            key: key.into(),
            version,
        };
        value.validate()?;
        Ok(value)
    }

    /// Validates a value received through an untrusted serialization boundary.
    pub fn validate(&self) -> Result<()> {
        self.provider.validate()?;
        if self.key.is_empty() || self.key.len() > MAX_REFERENCE_KEY_BYTES {
            return Err(Error::Invalid(
                "resource key is empty or exceeds its bound".into(),
            ));
        }
        if self.version.as_ref().is_some_and(|value| {
            value.is_empty() || value.len() > 256 || value.chars().any(char::is_control)
        }) {
            return Err(Error::Invalid("resource version is invalid".into()));
        }
        Ok(())
    }

    /// Returns the semantic reference kind.
    #[must_use]
    pub const fn kind(&self) -> ResourceKind {
        self.kind
    }

    /// Returns the provider that owns interpretation of the opaque key.
    #[must_use]
    pub const fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    /// Returns the bounded opaque provider key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the immutable referenced version, when the resource is versioned.
    #[must_use]
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }
}

macro_rules! typed_reference {
    ($name:ident, $kind:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $name(ResourceRef);

        impl $name {
            /// Creates a provider-owned typed reference.
            pub fn new(
                provider: ProviderRef,
                key: impl Into<Vec<u8>>,
                version: Option<String>,
            ) -> Result<Self> {
                ResourceRef::new(ResourceKind::$kind, provider, key, version).map(Self)
            }

            /// Borrows the shared provider-neutral representation.
            #[must_use]
            pub const fn as_resource(&self) -> &ResourceRef {
                &self.0
            }

            /// Validates an untrusted serialized reference and its semantic kind.
            pub fn validate(&self) -> Result<()> {
                self.0.validate()?;
                if self.0.kind() != ResourceKind::$kind {
                    return Err(Error::Invalid("typed resource kind mismatch".into()));
                }
                Ok(())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(
                deserializer: D,
            ) -> std::result::Result<Self, D::Error> {
                let resource = ResourceRef::deserialize(deserializer)?;
                if resource.kind() != ResourceKind::$kind {
                    return Err(serde::de::Error::custom("typed resource kind mismatch"));
                }
                Ok(Self(resource))
            }
        }
    };
}

typed_reference!(
    WorkspaceRef,
    Workspace,
    "Reference to a mutable Filesystem workspace."
);
typed_reference!(
    GenerationRef,
    Generation,
    "Reference to an immutable Filesystem generation."
);
typed_reference!(
    ArtifactRef,
    Artifact,
    "Reference to immutable content or a machine image."
);
typed_reference!(
    SandboxRef,
    Sandbox,
    "Reference to a Machines-owned isolated sandbox."
);
typed_reference!(
    CheckpointRef,
    Checkpoint,
    "Reference to an immutable sandbox checkpoint."
);
typed_reference!(StreamRef, Stream, "Reference to a Stream-owned history.");
typed_reference!(
    ContextRef,
    Context,
    "Reference to an immutable model context."
);
typed_reference!(RunRef, Run, "Reference to a recoverable model run.");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn references_are_typed_bounded_and_provider_owned() -> Result<()> {
        let provider = ProviderRef::new("acyclic", "filesystem", "2")?;
        let workspace = WorkspaceRef::new(provider.clone(), [7; 16], None)?;
        let generation = GenerationRef::new(provider, [8; 32], Some("immutable".into()))?;
        assert_eq!(workspace.as_resource().kind(), ResourceKind::Workspace);
        assert_eq!(generation.as_resource().kind(), ResourceKind::Generation);
        assert!(
            WorkspaceRef::new(
                ProviderRef::new("acyclic", "filesystem", "2")?,
                Vec::new(),
                None
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn deserialization_cannot_bypass_reference_validation() {
        let invalid_provider = r#"{"namespace":"","family":"filesystem","version":"2"}"#;
        assert!(serde_json::from_str::<ProviderRef>(invalid_provider).is_err());

        let wrong_kind = r#"{
            "kind":"generation",
            "provider":{"namespace":"acyclic","family":"filesystem","version":"2"},
            "key":[1],
            "version":null
        }"#;
        assert!(serde_json::from_str::<WorkspaceRef>(wrong_kind).is_err());
    }
}
