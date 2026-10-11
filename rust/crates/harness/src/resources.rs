//! Provider-neutral references to state owned by other Acyclic SDK families.

use crate::{Error, Result};
use serde::{Deserialize, Deserializer, Serialize};

/// Copies a known metadata field immediately after its own allocation admission.
/// The caller retains the closed value's capacity ceiling before entering.
#[cfg(feature = "filesystem")]
pub(crate) fn copy_metadata_string(
    source: &str, io: &crate::filesystem::PreparationIo,
) -> Result<String> {
    let copying = acyclic_fs::WorkCounters { bytes_copied: source.len() as u64, ..Default::default() };
    io.preflight(copying)?;
    io.allocation_attempt(source.len() as u64)?;
    let value = source.to_owned();
    io.charge(copying)?;
    Ok(value)
}

/// Stable identity of a replaceable resource provider.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, schemars::JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ProviderRef {
    /// Globally unique namespace, such as `acyclic` or a reverse DNS name.
    namespace: String,
    /// Provider family, such as `filesystem` or `machines`.
    family: String,
    /// Immutable provider contract version.
    version: String,
}

impl ProviderRef {
    /// Owned byte-buffer capacities only; opaque allocator metadata is excluded.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        [self.namespace.capacity(), self.family.capacity(), self.version.capacity()]
            .into_iter().try_fold(0_u64, |bytes, capacity| bytes.checked_add(capacity as u64)
                .ok_or_else(|| Error::Invalid("provider identity ownership overflow".into())))
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn clone_with_io(&self, io: &crate::filesystem::PreparationIo) -> Result<Self> {
        let allowance = self.logical_owned_bytes()?;
        io.retain_owned(allowance)?;
        let held = io.guard_owned(allowance)?;
        let value = self.copy_fields_with_io(io)?;
        let unused = allowance.checked_sub(value.logical_owned_bytes()?)
            .ok_or_else(|| io.invalid("provider clone exceeded its source capacity"))?;
        io.release(unused)?;
        held.into_retained();
        Ok(value)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn copy_fields_with_io(&self, io: &crate::filesystem::PreparationIo) -> Result<Self> {
        Ok(Self {
            namespace: copy_metadata_string(&self.namespace, io)?,
            family: copy_metadata_string(&self.family, io)?,
            version: copy_metadata_string(&self.version, io)?,
        })
    }
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
        self.validation_reason().map_or(Ok(()), |reason| Err(Error::Invalid(reason.into())))
    }

    fn validation_reason(&self) -> Option<&'static str> {
        self.parts().into_iter().any(|part| part.is_empty() || part.chars().any(char::is_control))
            .then_some("provider identity is invalid")
    }

    fn parts(&self) -> [&str; 3] {
        [&self.namespace, &self.family, &self.version]
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn validate_with_native_work(
        &self, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        for part in self.parts() {
            crate::conversation::inspect_native_metadata(work, part.len())?;
        }
        self.validation_reason().map_or(Ok(()), |reason| Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid, reason,
        }))
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
#[serde(deny_unknown_fields)]
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
        if self.key.is_empty() {
            return Err(Error::Invalid("resource key is empty".into()));
        }
        if self
            .version
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.chars().any(char::is_control))
        {
            return Err(Error::Invalid("resource version is invalid".into()));
        }
        Ok(())
    }
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.provider.logical_owned_bytes()?.checked_add(self.key.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(self.version.as_ref().map_or(0, String::capacity) as u64))
            .ok_or_else(|| Error::Invalid("resource identity ownership overflow".into()))
    }


    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.provider.logical_owned_bytes()?.checked_add(self.key.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(self.version.as_ref().map_or(0, String::capacity) as u64))
            .ok_or_else(|| Error::Invalid("resource identity ownership overflow".into()))
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

            pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
                self.0.logical_owned_bytes()
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

impl GenerationRef {
    /// Copies one generation identity on the original native ledger.
    /// Custody covers deep SDK-owned capacities; the enclosing DTO owns its
    /// inline reference slot. This operation grants no storage authority.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        provider: &ProviderRef, key: &[u8], version: Option<&str>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        provider.validate_with_native_work(work)?;
        if key.is_empty() { return Err(io.invalid("resource key is empty")); }
        if let Some(version) = version {
            crate::conversation::inspect_native_metadata(work, version.len())?;
            if version.is_empty() || version.chars().any(char::is_control) {
                return Err(io.invalid("resource version is invalid"));
            }
        }
        let allowance = provider.logical_owned_bytes()?.checked_add(key.len() as u64)
            .and_then(|bytes| bytes.checked_add(version.map_or(0, str::len) as u64))
            .ok_or_else(|| io.invalid("generation identity ownership overflow"))?;
        let value = Self::copy_fields_with_io(provider, key, version, allowance, &io)?;
        let retained = value.logical_owned_bytes()?;
        Ok((value, io.into_native_custody(retained)?))
    }
    /// Decodes a Filesystem identity without allocating before finite admission.
    #[cfg(feature = "filesystem")]
    pub(crate) fn from_hex_with_io(
        provider: &ProviderRef, encoded: &str, io: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        let inspection = acyclic_fs::WorkCounters {
            items_examined: encoded.len() as u64,
            ..Default::default()
        };
        io.preflight(inspection)?;
        io.charge(inspection)?;
        if encoded.is_empty() || encoded.len() % 2 != 0
            || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(io.invalid("file version is not a Filesystem generation"));
        }
        for part in provider.parts() {
            let inspection = acyclic_fs::WorkCounters {
                items_examined: part.len() as u64, ..Default::default()
            };
            io.preflight(inspection)?;
            io.charge(inspection)?;
        }
        if let Some(reason) = provider.validation_reason() {
            return Err(io.invalid(reason));
        }
        let provider = provider.clone_with_io(io)?;
        let provider_hold = io.guard_owned(provider.logical_owned_bytes()?)?;
        let length = encoded.len() / 2;
        io.retain_owned(length as u64)?;
        let key_hold = io.guard_owned(length as u64)?;
        let decoding = acyclic_fs::WorkCounters {
            source_bytes_read: encoded.len() as u64, bytes_copied: length as u64,
            ..Default::default()
        };
        io.preflight(decoding)?;
        io.allocation_attempt(length as u64)?;
        let mut key = Vec::with_capacity(length);
        for chunk in encoded.as_bytes().chunks(64) {
            let mut decoded = [0_u8; 32];
            let count = chunk.len() / 2;
            hex::decode_to_slice(chunk, &mut decoded[..count])
                .map_err(|_| io.invalid("file version is not a Filesystem generation"))?;
            key.extend_from_slice(&decoded[..count]);
        }
        io.charge(decoding)?;
        let unused = (length as u64).checked_sub(key.capacity() as u64)
            .ok_or_else(|| io.invalid("generation key exceeded admitted capacity"))?;
        io.release(unused)?;
        let value = Self(ResourceRef { kind: ResourceKind::Generation, provider, key, version: None });
        key_hold.into_retained();
        provider_hold.into_retained();
        Ok(value)
    }
    /// Copies this closed identity with admission immediately before each allocation.
    #[cfg(feature = "filesystem")]
    pub(crate) fn clone_with_io(&self, io: &crate::filesystem::PreparationIo) -> Result<Self> {
        Self::copy_fields_with_io(
            &self.0.provider, &self.0.key, self.0.version.as_deref(), self.logical_owned_bytes()?, io,
        )
    }

    #[cfg(feature = "filesystem")]
    fn copy_fields_with_io(
        provider: &ProviderRef, source_key: &[u8], source_version: Option<&str>,
        allowance: u64, io: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        io.retain_owned(allowance)?;
        let held = io.guard_owned(allowance)?;
        let provider = provider.copy_fields_with_io(io)?;
        let copying = acyclic_fs::WorkCounters {
            bytes_copied: source_key.len() as u64, ..Default::default()
        };
        io.preflight(copying)?;
        io.allocation_attempt(source_key.len() as u64)?;
        let key = source_key.to_vec();
        io.charge(copying)?;
        let version = source_version
            .map(|version| copy_metadata_string(version, io)).transpose()?;
        let value = Self(ResourceRef { kind: ResourceKind::Generation, provider, key, version });
        let actual = value.logical_owned_bytes()?;
        let unused = allowance.checked_sub(actual)
            .ok_or_else(|| io.invalid("generation clone exceeded its source capacity"))?;
        io.release(unused)?;
        held.into_retained();
        Ok(value)
    }
}
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

impl CheckpointRef {
    pub(crate) fn clone_with_work(
        &self, work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let Some(io) = work.as_deref_mut() else { return Ok(self.clone()); };
        io.check_cancellation()?;
        let reference = self.as_resource();
        let provider = reference.provider();
        let bytes = [provider.namespace(), provider.family(), provider.version(), reference.version().unwrap_or("")]
            .into_iter().try_fold(reference.key().len() as u64, |sum, text| {
                sum.checked_add(text.len() as u64).ok_or_else(|| {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    if io.original_native_work().is_some() {
                        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                            reason: "checkpoint clone ownership overflows" };
                    }
                    Error::Invalid("checkpoint clone ownership overflows".into())
                })
            })?;
        io.admit_owned(bytes)?;
        io.admit_copy(bytes)?;
        for text in [provider.namespace(), provider.family(), provider.version(), reference.version().unwrap_or("")] {
            io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            if !text.is_empty() { io.admit_allocation(text.len() as u64)?; }
        }
        if !reference.key().is_empty() { io.admit_allocation(reference.key().len() as u64)?; }
        io.copy_bytes(bytes)?;
        io.retain_owned(bytes)?;
        Ok(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_resource_ownership_counts_spare_buffer_capacity() -> Result<()> {
        let mut namespace = String::with_capacity(128);
        namespace.push_str("harness");
        let mut family = String::with_capacity(256);
        family.push_str("filesystem");
        let mut version = String::with_capacity(512);
        version.push_str("v1");
        let provider = ProviderRef::new(namespace, family, version)?;
        let provider_bytes = provider.namespace.capacity() + provider.family.capacity()
            + provider.version.capacity();
        assert_eq!(provider.logical_owned_bytes()?, provider_bytes as u64);
        let mut key = Vec::with_capacity(1024);
        key.extend_from_slice(b"generation");
        let mut generation_version = String::with_capacity(2048);
        generation_version.push_str("one");
        let reference = GenerationRef::new(provider, key, Some(generation_version))?;
        assert_eq!(reference.logical_owned_bytes()?, (provider_bytes
            + reference.0.key.capacity()
            + reference.0.version.as_ref().map_or(0, String::capacity)) as u64);
        Ok(())
    }

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

        let unknown_field = r#"{
            "kind":"workspace",
            "provider":{"namespace":"acyclic","family":"filesystem","version":"2"},
            "key":[1],
            "version":null,
            "endpoint":"https://should-not-enter-a-reference.invalid"
        }"#;
        assert!(serde_json::from_str::<ResourceRef>(unknown_field).is_err());
    }
}
