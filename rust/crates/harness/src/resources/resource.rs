use super::*;

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
    pub(super) kind: ResourceKind,
    pub(super) provider: ProviderRef,
    pub(super) key: Vec<u8>,
    pub(super) version: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceRefWire {
    kind: ResourceKind,
    provider: ProviderRef,
    key: Vec<u8>,
    version: Option<String>,
}

impl<'de> Deserialize<'de> for ResourceRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let wire = ResourceRefWire::deserialize(deserializer)?;
        Self::new(wire.kind, wire.provider, wire.key, wire.version, &mut None)
            .map_err(serde::de::Error::custom)
    }
}

impl ResourceRef {
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.provider
            .logical_owned_bytes()?
            .checked_add(self.key.capacity() as u64)
            .and_then(|bytes| {
                bytes.checked_add(
                    self.version
                        .as_ref()
                        .map_or(0, |value| value.capacity() as u64),
                )
            })
            .ok_or_else(|| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                {
                    Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "resource capacity inventory overflows",
                    }
                }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                {
                    Error::Invalid("resource capacity inventory overflows".into())
                }
            })
    }

    /// Creates a validated opaque reference without embedding credentials or endpoints.
    pub fn new(
        kind: ResourceKind,
        provider: ProviderRef,
        key: Vec<u8>,
        version: Option<String>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let value = Self {
            kind,
            provider,
            key,
            version,
        };
        value.validate(work)?;
        Ok(value)
    }

    /// Validates a value received through an untrusted serialization boundary.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        self.provider.validate(work)?;
        if self.key.is_empty() {
            return Err(resource_validation_error("resource key is empty", work));
        }
        if self
            .version
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.chars().any(char::is_control))
        {
            return Err(resource_validation_error(
                "resource version is invalid",
                work,
            ));
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
