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
        Self::new_with_work(namespace.into(), family.into(), version.into(), &mut None)
    }

    pub(crate) fn new_with_work(
        namespace: String,
        family: String,
        version: String,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let value = Self { namespace, family, version };
        value.validate_with_work(work)?;
        Ok(value)
    }

    /// Copies a validated identity under the caller's original native work.
    /// Constructing an identity grants no access to the represented provider.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        namespace: &str, family: &str, version: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let mut observed = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
        Self::validate_fields_with_work(namespace, family, version, &mut observed)?;
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
        let namespace = crate::core::copy_text(namespace, &mut quota)?;
        let family = crate::core::copy_text(family, &mut quota)?;
        let version = crate::core::copy_text(version, &mut quota)?;
        quota.transfer();
        drop(quota);
        Ok((Self { namespace, family, version }, io.into_custody()))
    }

    pub(crate) fn from_owned_value_with_work(
        value: serde_json::Value,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(|io| io.original_native_work().is_some()) {
            use crate::conversation::{
                native_owned_wire_field, native_owned_wire_object, native_owned_wire_text,
            };
            let mut object = native_owned_wire_object(value,
                Some(&["namespace", "family", "version"]), work)?;
            let namespace = native_owned_wire_text(
                native_owned_wire_field(&mut object, "namespace", work)?, work)?;
            let family = native_owned_wire_text(
                native_owned_wire_field(&mut object, "family", work)?, work)?;
            let version = native_owned_wire_text(
                native_owned_wire_field(&mut object, "version", work)?, work)?;
            return Self::new_with_work(namespace, family, version, work);
        }
        let provider: Self = serde_json::from_value(value)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        provider.validate_with_work(work)?;
        Ok(provider)
    }

    /// Validates a value received through an untrusted serialization boundary.
    pub fn validate(&self) -> Result<()> {
        self.validation_reason().map_or(Ok(()), |reason| Err(Error::Invalid(reason.into())))
    }

    /// Validates the same identity fields on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.validate_with_work(&mut Some(&mut io))
    }
    pub(crate) fn validate_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        Self::validate_fields_with_work(&self.namespace, &self.family, &self.version, work)
    }

    fn validation_reason(&self) -> Option<&'static str> {
        self.parts().into_iter().any(|part| part.is_empty() || part.chars().any(char::is_control))
            .then_some("provider identity is invalid")
    }

    fn parts(&self) -> [&str; 3] {
        [&self.namespace, &self.family, &self.version]
    }

    fn validate_fields_with_work(
        namespace: &str, family: &str, version: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        for part in [namespace, family, version] {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            if part.is_empty() {
                return Err(resource_validation_error("provider identity is invalid", work));
            }
            for character in part.chars() {
                if let Some(observer) = work.as_deref_mut() {
                    observer.check_cancellation()?;
                    observer.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                }
                if character.is_control() {
                    return Err(resource_validation_error("provider identity is invalid", work));
                }
            }
        }
        Ok(())
    }


    pub(crate) fn validation_error(&self) -> Option<&'static str> {
        self.validation_reason()
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        [self.namespace.capacity(), self.family.capacity(), self.version.capacity()]
            .into_iter().try_fold(0_u64, |bytes, capacity| {
                bytes.checked_add(capacity as u64).ok_or_else(|| {
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                        reason: "provider capacity inventory overflows" } }
                    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                    { Error::Invalid("provider capacity inventory overflows".into()) }
                })
            })
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn clone_in_native_allocation(
        &self, work: &acyclic_fs::NativeMaterializationWork,
        allocation: &mut acyclic_fs::NativeAllocation,
    ) -> Result<Self> {
        Ok(Self {
            namespace: crate::conversation::clone_native_reference_text_into(
                &self.namespace, work, allocation)?,
            family: crate::conversation::clone_native_reference_text_into(
                &self.family, work, allocation)?,
            version: crate::conversation::clone_native_reference_text_into(
                &self.version, work, allocation)?,
        })
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
    /// Observable owned payload capacity, excluding opaque allocator metadata.
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.provider.logical_owned_bytes()?
            .checked_add(self.key.capacity() as u64)
            .and_then(|bytes| bytes.checked_add(
                self.version.as_ref().map_or(0, |value| value.capacity() as u64)))
            .ok_or_else(|| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                { Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                    reason: "resource capacity inventory overflows" } }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                { Error::Invalid("resource capacity inventory overflows".into()) }
            })
    }

    /// Creates a validated opaque reference without embedding credentials or endpoints.
    pub fn new(
        kind: ResourceKind,
        provider: ProviderRef,
        key: impl Into<Vec<u8>>,
        version: Option<String>,
    ) -> Result<Self> {
        Self::new_with_work(kind, provider, key.into(), version, &mut None)
    }

    pub(crate) fn new_with_work(
        kind: ResourceKind,
        provider: ProviderRef,
        key: Vec<u8>,
        version: Option<String>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let value = Self { kind, provider, key, version };
        value.validate_with_work(work)?;
        Ok(value)
    }

    /// Validates a value received through an untrusted serialization boundary.
    pub fn validate(&self) -> Result<()> {
        self.validate_with_work(&mut None)
    }

    pub(crate) fn validate_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() { observer.check_cancellation()?; }
        self.provider.validate_with_work(work)?;
        if self.key.is_empty() {
            return Err(resource_validation_error("resource key is empty", work));
        }
        if self
            .version
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.chars().any(char::is_control))
        {
            return Err(resource_validation_error("resource version is invalid", work));
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

fn resource_validation_error(
    reason: &'static str,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_ref().is_some_and(|work| work.original_native_work().is_some()) {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
    }
    let _ = work;
    Error::Invalid(reason.into())
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

            pub(crate) fn from_resource_with_work(
                resource: ResourceRef,
                work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
            ) -> Result<Self> {
                let value = Self(resource);
                value.validate_with_work(work)?;
                Ok(value)
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
                self.validate_with_work(&mut None)
            }

            pub(crate) fn validate_with_work(
                &self,
                work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
            ) -> Result<()> {
                self.0.validate_with_work(work)?;
                if self.0.kind() != ResourceKind::$kind {
                    return Err(resource_validation_error("typed resource kind mismatch", work));
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

impl WorkspaceRef {
    pub(crate) fn into_resource(self) -> ResourceRef { self.0 }
}
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

    /// Validates the same immutable reference and semantic kind on native work.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn validate_with_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        let mut io = crate::store::history::native_work_io(work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        self.validate_with_work(&mut Some(&mut io))
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
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_provider_creation_preserves_policy_and_partial_copy_custody() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget {
            items_examined: 256, allocation_operations: 3, bytes_copied: 64,
            peak_allocation_bytes: 256, ..acyclic_fs::WorkBudget::default()
        };
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .expect("original provider ledger");
        let (provider, custody) = ProviderRef::new_with_native_work("acyclic", "filesystem", "2", &work)?;
        assert_eq!(provider, ProviderRef::new("acyclic", "filesystem", "2")?);
        assert_eq!(custody.bytes(), provider.logical_owned_bytes()?);
        drop(provider);
        drop(custody);
        drop(work.retain_owned(budget.peak_allocation_bytes)
            .expect("all original provider custody retired"));
        let invalid = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .expect("invalid provider ledger");
        assert_eq!(ProviderRef::new_with_native_work("acyclic", "file\nsystem", "2", &invalid)
            .unwrap_err().code(), crate::wire::ErrorCode::Invalid);
        assert_eq!(invalid.receipt().allocation_operations, 0);
        let denied = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget { bytes_copied: 7, ..budget }, &cancellation)
            .expect("partial provider ledger");
        assert!(ProviderRef::new_with_native_work("acyclic", "filesystem", "2", &denied).is_err());
        drop(denied.retain_owned(budget.peak_allocation_bytes)
            .expect("partial field copies physically retired"));
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
