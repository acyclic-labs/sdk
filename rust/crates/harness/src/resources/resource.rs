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
        capacity_sum([
            self.key.capacity(),
            self.version.as_ref().map_or(0, String::capacity),
        ])?
        .checked_add(self.provider.logical_owned_bytes()?)
        .ok_or_else(|| invalid("resource ownership overflows"))
    }
    pub(super) fn copy(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let copy = crate::core::copy_resource(self, &mut quota)?;
        quota.transfer();
        Ok(copy)
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
        self.provider.validate(work)?;
        if self.key.is_empty() {
            return Err(invalid("resource key is empty"));
        }
        validate_text(self.version.as_deref(), work, "resource version is invalid")?;
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
