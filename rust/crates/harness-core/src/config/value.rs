//! Evaluated data and its original allocation ownership move together.
use crate::{Error, Result};

/// Evaluated configuration data, retaining the credit that funds its storage.
/// Moving a result through a fold or consuming a composition moves this owner;
/// borrowing, indexing and serialization do not detach its data.
/// Cached JSON shares its producer's immutable backing without another copy.
/// Decoding a new record spends the receiving caller's original allowance.
#[derive(Debug)]
pub struct ConfigValue<T = serde_json::Value> {
    storage: Storage<T>,
    work: Option<acyclic_fs::NativeMaterializationWork>,
}

#[derive(Debug)]
enum Storage<T> {
    Owned(crate::javascript_allocator::EngineResult<T>),
    Shared(std::sync::Arc<(crate::javascript_allocator::EngineResult<T>, super::Form)>),
}

impl<T> std::ops::Deref for ConfigValue<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.data().value
    }
}

impl<T: serde::Serialize> serde::Serialize for ConfigValue<T> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.data().value.serialize(serializer)
    }
}

impl<T: PartialEq> PartialEq for ConfigValue<T> {
    fn eq(&self, other: &Self) -> bool {
        self.data().value == other.data().value
    }
}

impl PartialEq<serde_json::Value> for ConfigValue {
    fn eq(&self, other: &serde_json::Value) -> bool {
        self.data().value == *other
    }
}

impl<T> ConfigValue<T> {
    pub(crate) fn new(
        value: T,
        custody: acyclic_fs::NativeCustody,
        work: Option<acyclic_fs::NativeMaterializationWork>,
    ) -> Self {
        Self {
            storage: Storage::Owned(crate::javascript_allocator::EngineResult { value, custody }),
            work,
        }
    }

    pub(crate) fn shared(
        value: std::sync::Arc<(crate::javascript_allocator::EngineResult<T>, super::Form)>,
        work: Option<acyclic_fs::NativeMaterializationWork>,
    ) -> Self {
        Self {
            storage: Storage::Shared(value),
            work,
        }
    }

    fn data(&self) -> &crate::javascript_allocator::EngineResult<T> {
        match &self.storage {
            Storage::Owned(value) => value,
            Storage::Shared(value) => &value.0,
        }
    }

    /// Returns owned data to an unbounded host. Shared funded backing is copied,
    /// so its original credit still retires with its original storage.
    /// A bounded receiver must retain this owner or decode on its original ledger.
    pub fn into_unbounded(self) -> Result<T>
    where
        T: Clone,
    {
        if self.work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "bounded configuration data must retain its original owner",
            });
        }
        match self.storage {
            Storage::Owned(value) if value.custody.is_none() => Ok(value.value),
            Storage::Owned(value) => Ok(value.value.clone()),
            Storage::Shared(value) if value.0.custody.is_some() => Ok(value.0.value.clone()),
            Storage::Shared(value) => match std::sync::Arc::try_unwrap(value) {
                Ok((value, _)) => Ok(value.value),
                Err(value) => Ok(value.0.value.clone()),
            },
        }
    }
}

impl ConfigValue {
    pub(crate) fn record<T: crate::filesystem::PreparationRecord>(
        &self,
    ) -> Result<crate::javascript_allocator::EngineResult<T>> {
        if let Some(work) = &self.work {
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            let (value, custody) = crate::filesystem::native_decoded(
                io.value_decode_preparation(&self.data().value, crate::wire::ErrorCode::Invalid)?,
            )?;
            return Ok(crate::javascript_allocator::EngineResult {
                value,
                custody: Some(custody),
            });
        }
        Ok(crate::javascript_allocator::EngineResult {
            value: serde::Deserialize::deserialize(&self.data().value)
                .map_err(|error| Error::Invalid(error.to_string()))?,
            custody: None,
        })
    }

    pub(crate) fn work(&self) -> acyclic_fs::NativeWork<'_> {
        self.work.as_ref()
    }

    /// Decodes an admitted record without detaching its allocation credit.
    /// The target uses this value's original ledger, never a caller replacement.
    pub fn decode<T: crate::filesystem::UploadRecord>(&self) -> Result<ConfigValue<T>> {
        let (value, custody) = crate::tool::decode(&self.data().value, self.work.as_ref())?;
        Ok(ConfigValue::new(value, custody, self.work.clone()))
    }
}

impl crate::filesystem::UploadRecord for super::AgentConfig {
    fn logical_owned_bytes(&self) -> Result<u64> {
        crate::core::owned_sum([
            self.model.capacity() as u64,
            self.replay
                .as_ref()
                .map_or(0, |text| text.capacity() as u64),
        ])
    }
}

impl crate::filesystem::UploadRecord for super::RequestMatch {
    fn logical_owned_bytes(&self) -> Result<u64> {
        let bytes = crate::core::owned_sum([
            crate::core::owned_buffer::<String>(self.action.capacity())?,
            self.command
                .as_ref()
                .map_or(0, |text| text.capacity() as u64),
            self.path.as_ref().map_or(0, |text| text.capacity() as u64),
        ])?;
        self.action.iter().try_fold(bytes, |bytes, text| {
            crate::core::owned_sum([bytes, text.capacity() as u64])
        })
    }
}

impl crate::filesystem::UploadRecord for super::Policy {
    fn logical_owned_bytes(&self) -> Result<u64> {
        [&self.deny, &self.only]
            .into_iter()
            .try_fold(0, |total, rules| {
                let Some(rules) = rules else { return Ok(total) };
                rules.iter().try_fold(
                    crate::core::owned_sum([
                        total,
                        crate::core::owned_buffer::<super::PolicyRule>(rules.capacity())?,
                    ])?,
                    |total, rule| {
                        crate::core::owned_sum([
                            total,
                            rule.path.capacity() as u64,
                            crate::core::owned_buffer::<super::Action>(rule.actions.capacity())?,
                        ])
                    },
                )
            })
    }
}
