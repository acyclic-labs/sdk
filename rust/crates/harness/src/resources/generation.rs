use super::*;

/// Reference to an immutable Filesystem generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct GenerationRef(ResourceRef);

impl GenerationRef {
    pub(crate) fn from_resource_with_work(
        resource: ResourceRef,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        let value = Self(resource);
        value.validate(work)?;
        Ok(value)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.0.logical_owned_bytes()
    }

    /// Borrows the shared provider-neutral representation.
    #[must_use]
    pub const fn as_resource(&self) -> &ResourceRef {
        &self.0
    }

    /// Validates an untrusted serialized reference and its semantic kind.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.0.validate(work)?;
        if self.0.kind() != ResourceKind::Generation {
            return Err(resource_validation_error(
                "typed resource kind mismatch",
                work,
            ));
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for GenerationRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let resource = ResourceRef::deserialize(deserializer)?;
        if resource.kind() != ResourceKind::Generation {
            return Err(serde::de::Error::custom("typed resource kind mismatch"));
        }
        Ok(Self(resource))
    }
}

impl GenerationRef {
    /// Copies a generation identity under the optional native work handle.
    pub fn new(
        provider: &ProviderRef,
        key: &[u8],
        version: Option<&str>,
        work: crate::NativeWork<'_>,
    ) -> Result<(Self, crate::NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(work) = work {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            provider.validate(&mut Some(&mut crate::store::native_work_io(work)?))?;
            if key.is_empty() {
                return Err(io.invalid("resource key is empty"));
            }
            if let Some(version) = version {
                crate::conversation::inspect_native_metadata(work, version.len())?;
                if version.is_empty() || version.chars().any(char::is_control) {
                    return Err(io.invalid("resource version is invalid"));
                }
            }
            let allowance = provider
                .logical_owned_bytes()?
                .checked_add(key.len() as u64)
                .and_then(|bytes| bytes.checked_add(version.map_or(0, str::len) as u64))
                .ok_or_else(|| io.invalid("generation identity ownership overflow"))?;
            let value = Self::copy_fields_with_io(provider, key, version, allowance, &io)?;
            let retained = value.logical_owned_bytes()?;
            return Ok((value, Some(io.into_native_custody(retained)?)));
        }
        let _ = work;
        let resource = ResourceRef::new(
            ResourceKind::Generation,
            provider.clone(),
            key.to_vec(),
            version.map(str::to_owned),
            &mut None,
        )?;
        Ok((Self(resource), None))
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn from_hex_with_io(
        provider: &ProviderRef,
        encoded: &str,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        let inspection = acyclic_fs::WorkCounters {
            items_examined: encoded.len() as u64,
            ..Default::default()
        };
        io.preflight(inspection)?;
        io.charge(inspection)?;
        if encoded.is_empty()
            || encoded.len() % 2 != 0
            || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(io.invalid("file version is not a Filesystem generation"));
        }
        for part in provider.parts() {
            let inspection = acyclic_fs::WorkCounters {
                items_examined: part.len() as u64,
                ..Default::default()
            };
            io.preflight(inspection)?;
            io.charge(inspection)?;
        }
        if let Some(reason) = provider.validation_error() {
            return Err(io.invalid(reason));
        }
        let provider = provider.clone_with_io(io)?;
        let provider_hold = io.guard_owned(provider.logical_owned_bytes()?)?;
        let length = encoded.len() / 2;
        io.retain_owned(length as u64)?;
        let key_hold = io.guard_owned(length as u64)?;
        let decoding = acyclic_fs::WorkCounters {
            source_bytes_read: encoded.len() as u64,
            bytes_copied: length as u64,
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
        let unused = (length as u64)
            .checked_sub(key.capacity() as u64)
            .ok_or_else(|| io.invalid("generation key exceeded admitted capacity"))?;
        io.release(unused)?;
        let value = Self(ResourceRef {
            kind: ResourceKind::Generation,
            provider,
            key,
            version: None,
        });
        key_hold.into_retained();
        provider_hold.into_retained();
        Ok(value)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn clone_with_io(&self, io: &crate::filesystem::PreparationIo) -> Result<Self> {
        Self::copy_fields_with_io(
            &self.0.provider,
            &self.0.key,
            self.0.version.as_deref(),
            self.logical_owned_bytes()?,
            io,
        )
    }

    #[cfg(feature = "filesystem")]
    fn copy_fields_with_io(
        provider: &ProviderRef,
        source_key: &[u8],
        source_version: Option<&str>,
        allowance: u64,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<Self> {
        io.retain_owned(allowance)?;
        let held = io.guard_owned(allowance)?;
        let provider = provider.copy_fields_with_io(io)?;
        let copying = acyclic_fs::WorkCounters {
            bytes_copied: source_key.len() as u64,
            ..Default::default()
        };
        io.preflight(copying)?;
        io.allocation_attempt(source_key.len() as u64)?;
        let key = source_key.to_vec();
        io.charge(copying)?;
        let version = source_version
            .map(|version| copy_metadata_string(version, io))
            .transpose()?;
        let value = Self(ResourceRef {
            kind: ResourceKind::Generation,
            provider,
            key,
            version,
        });
        let actual = value.logical_owned_bytes()?;
        let unused = allowance
            .checked_sub(actual)
            .ok_or_else(|| io.invalid("generation clone exceeded its source capacity"))?;
        io.release(unused)?;
        held.into_retained();
        Ok(value)
    }
}
