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
    pub(super) fn parts(&self) -> [&str; 3] { [&self.namespace, &self.family, &self.version] }
    pub(crate) fn validation_error(&self) -> Option<&'static str> {
        self.parts().into_iter().any(|p| p.is_empty() || p.chars().any(char::is_control))
            .then_some("provider identity is invalid")
    }
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        capacity_sum([self.namespace.capacity(), self.family.capacity(), self.version.capacity()])
    }
    #[cfg(feature = "filesystem")]
    pub(crate) fn clone_with_io(&self, io: &crate::filesystem::PreparationIo) -> Result<Self> {
        crate::executor::with_history_io(Some(io), |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let copy = crate::core::copy_provider(self, &mut quota)?;
            quota.transfer(); Ok(copy)
        })
    }
    /// Copies a validated provider identity on the original ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work(namespace: &str, family: &str, version: &str,
        work: &acyclic_fs::NativeMaterializationWork) -> Result<(Self, acyclic_fs::NativeAllocation)> {
        let mut io = crate::store::history::native_work_io(work)?;
        let mut observed = Some(&mut io as &mut dyn HistoryIo);
        validate_text([namespace, family, version], &mut observed, "provider identity is invalid")?;
        let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
        let copy = Self { namespace: crate::core::copy_text(namespace, &mut quota)?,
            family: crate::core::copy_text(family, &mut quota)?,
            version: crate::core::copy_text(version, &mut quota)? };
        quota.transfer(); drop(quota);
        Ok((copy, io.into_custody()))
    }

    pub(crate) fn copy_fields(&self, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<Self> {
        Ok(Self { namespace: crate::core::copy_text(&self.namespace, quota)?,
            family: crate::core::copy_text(&self.family, quota)?,
            version: crate::core::copy_text(&self.version, quota)? })
    }

}

impl ProviderRef {
    /// Creates a validated provider identity.
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

    /// Validates a value received through an untrusted serialization boundary.
    pub fn validate(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        validate_text(self.parts(), work, "provider identity is invalid")
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
        Self::new(wire.namespace, wire.family, wire.version, &mut None).map_err(serde::de::Error::custom)
    }
}

