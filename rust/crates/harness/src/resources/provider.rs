use super::*;

/// Stable identity of a replaceable resource provider.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, schemars::JsonSchema)]
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
    /// Creates a provider identity from owned fields on the optional observer.
    pub fn new(
        namespace: String,
        family: String,
        version: String,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let value = Self {
            namespace,
            family,
            version,
        };
        value.validate(work)?;
        Ok(value)
    }

    /// Validates the reference on the optional history observer.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        Self::validate_fields_with_work(&self.namespace, &self.family, &self.version, work)
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

    #[cfg(feature = "filesystem")]
    pub(crate) fn clone_with_io(&self, io: &crate::filesystem::PreparationIo) -> Result<Self> {
        let allowance = self.logical_owned_bytes()?;
        io.retain_owned(allowance)?;
        let held = io.guard_owned(allowance)?;
        let value = self.copy_fields_with_io(io)?;
        let unused = allowance
            .checked_sub(value.logical_owned_bytes()?)
            .ok_or_else(|| io.invalid("provider clone exceeded its source capacity"))?;
        io.release(unused)?;
        held.into_retained();
        Ok(value)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn copy_fields_with_io(
        &self,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        Ok(Self {
            namespace: copy_metadata_string(&self.namespace, io)?,
            family: copy_metadata_string(&self.family, io)?,
            version: copy_metadata_string(&self.version, io)?,
        })
    }

    /// Copies a validated identity on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(
        namespace: &str,
        family: &str,
        version: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut io =
            crate::store::history::native_work_io(work).map_err(|cause| Error::NativeWork {
                cause,
                custody: None,
            })?;
        let mut observed = Some(&mut io as &mut dyn crate::store::history::HistoryIo);
        Self::validate_fields_with_work(namespace, family, version, &mut observed)?;
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
        let namespace = crate::core::copy_text(namespace, &mut quota)?;
        let family = crate::core::copy_text(family, &mut quota)?;
        let version = crate::core::copy_text(version, &mut quota)?;
        quota.transfer();
        drop(quota);
        Ok((
            Self {
                namespace,
                family,
                version,
            },
            io.into_custody(),
        ))
    }

    pub(crate) fn from_owned_value_with_work(
        value: serde_json::Value,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work
            .as_deref()
            .is_some_and(|io| io.original_native_work().is_some())
        {
            use crate::conversation::{
                native_owned_wire_field, native_owned_wire_object, native_owned_wire_text,
            };
            let mut object =
                native_owned_wire_object(value, Some(&["namespace", "family", "version"]), work)?;
            let namespace = native_owned_wire_text(
                native_owned_wire_field(&mut object, "namespace", work)?,
                work,
            )?;
            let family = native_owned_wire_text(
                native_owned_wire_field(&mut object, "family", work)?,
                work,
            )?;
            let version = native_owned_wire_text(
                native_owned_wire_field(&mut object, "version", work)?,
                work,
            )?;
            return Self::new(namespace, family, version, work);
        }
        let provider: Self =
            serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
        provider.validate(work)?;
        Ok(provider)
    }

    pub(crate) fn validation_error(&self) -> Option<&'static str> {
        self.parts()
            .into_iter()
            .any(|part| part.is_empty() || part.chars().any(char::is_control))
            .then_some("provider identity is invalid")
    }

    pub(super) fn parts(&self) -> [&str; 3] {
        [&self.namespace, &self.family, &self.version]
    }

    fn validate_fields_with_work(
        namespace: &str,
        family: &str,
        version: &str,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        for part in [namespace, family, version] {
            if let Some(observer) = work.as_deref_mut() {
                observer.check_cancellation()?;
                observer
                    .codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            }
            if part.is_empty() {
                return Err(resource_validation_error(
                    "provider identity is invalid",
                    work,
                ));
            }
            for character in part.chars() {
                if let Some(observer) = work.as_deref_mut() {
                    observer.check_cancellation()?;
                    observer.codec_charge(crate::contract::JsonCodecCharge::Work {
                        bytes: 0,
                        items: 1,
                    })?;
                }
                if character.is_control() {
                    return Err(resource_validation_error(
                        "provider identity is invalid",
                        work,
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        [
            self.namespace.capacity(),
            self.family.capacity(),
            self.version.capacity(),
        ]
        .into_iter()
        .try_fold(0_u64, |bytes, capacity| {
            bytes.checked_add(capacity as u64).ok_or_else(|| {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                {
                    Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Invalid,
                        reason: "provider capacity inventory overflows",
                    }
                }
                #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
                {
                    Error::Invalid("provider capacity inventory overflows".into())
                }
            })
        })
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
        Self::new(wire.namespace, wire.family, wire.version, &mut None)
            .map_err(serde::de::Error::custom)
    }
}
