use super::*;

/// Deterministic capability set.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct Capabilities(BTreeSet<String>);

impl Capabilities {
    /// Builds a capability set.
    pub fn new(values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self(values.into_iter().map(Into::into).collect())
    }

    /// Builds exactly the requested grants on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn new_with_native_work<'a>(
        values: impl IntoIterator<Item = &'a str>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<(Self, acyclic_fs::NativeAllocation)> {
        use crate::store::history::HistoryIo as _;
        let mut io = crate::filesystem::PreparationIo::from_native_work(work).map(crate::store::HistoryWork::new)?;
        let mut target = BTreeSet::new();
        for value in values {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            }).map_err(|cause| crate::Error::NativeWork { cause, custody: None })?;
            if target.contains(value) {
                continue;
            }
            let slot = std::mem::size_of::<String>() as u64;
            let bytes = slot.checked_add(value.len() as u64).ok_or_else(|| {
                crate::Error::NativeWork {
                    cause: acyclic_fs::NativeTreeMaterializationError::Work(
                        acyclic_fs::WorkError::Overflow,
                    ),
                    custody: None,
                }
            })?;
            io.admit_owned(bytes)?;
            io.retain_owned(slot)?;
            let copy = io.clone_text(value)?;
            target.insert(copy);
        }
        io.check_cancellation()?;
        Ok((Self(target), io.into_custody()?))
    }

    /// Copies the original records through the same bounded native constructor.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub fn clone_with_native_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> crate::Result<(Self, acyclic_fs::NativeAllocation)> {
        Self::new_with_native_work(self.iter(), work)
    }

    pub(crate) fn from_owned_set(values: BTreeSet<String>) -> Self {
        Self(values)
    }
    /// Returns whether a capability is present.
    #[must_use]
    pub fn contains(&self, value: &str) -> bool {
        self.0.contains(value)
    }

    /// Returns whether this set attenuates an ancestor set.
    #[must_use]
    pub fn is_subset_of(&self, ancestor: &Self) -> bool {
        self.0.is_subset(&ancestor.0)
    }

    /// Iterates in canonical lexical order.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        self.0.iter().try_fold(0_u64, |bytes, capability| {
            bytes.checked_add(std::mem::size_of::<String>() as u64)
                .and_then(|bytes| bytes.checked_add(capability.capacity() as u64))
                .ok_or_else(|| Error::Invalid("capability owned byte count overflows".into()))
        })
    }

    /// Returns the deterministic intersection of two grants.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        Self(self.0.intersection(&other.0).cloned().collect())
    }

    /// Removes explicit denials from this grant.
    #[must_use]
    pub fn without(&self, denied: &Self) -> Self {
        Self(self.0.difference(&denied.0).cloned().collect())
    }
}

/// One explicit authority policy layer; grants intersect and denials always win.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuthorityPolicy {
    /// Capabilities this layer permits.
    pub grants: Capabilities,
    /// Capabilities this layer explicitly denies.
    pub denies: Capabilities,
}

/// Ordered authority-resolution level from the runtime root to one invocation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(all(feature = "wasm", target_arch = "wasm32"), derive(tsify::Tsify))]
#[serde(rename_all = "snake_case")]
pub enum AuthorityLevel {
    /// Runtime-wide policy.
    Runtime,
    /// Agent policy.
    Agent,
    /// Conversation policy.
    Conversation,
    /// Session policy.
    Session,
    /// Turn policy.
    Turn,
    /// Durable task policy.
    Task,
    /// Exact invocation policy.
    Invocation,
}

/// One named layer in an explicit authority chain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PolicyLayer {
    /// Hierarchy position.
    pub level: AuthorityLevel,
    /// Grants and denials contributed by this level.
    pub policy: AuthorityPolicy,
}

/// Resolves runtime → invocation policy layers without ambient authority.
#[must_use]
pub fn resolve_policies(layers: &[AuthorityPolicy]) -> Capabilities {
    let Some(first) = layers.first() else {
        return Capabilities::default();
    };
    let granted = layers
        .iter()
        .skip(1)
        .fold(first.grants.clone(), |current, layer| {
            current.intersect(&layer.grants)
        });
    let denied = Capabilities::new(layers.iter().flat_map(|layer| layer.denies.iter()));
    granted.without(&denied)
}

/// Validates hierarchy order and resolves the exact effective grant.
pub fn resolve_policy_layers(layers: &[PolicyLayer]) -> Result<Capabilities> {
    if layers
        .first()
        .is_none_or(|layer| layer.level != AuthorityLevel::Runtime)
        || layers
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left.level >= right.level))
    {
        return Err(Error::Invalid(
            "policy layers must start at runtime and be strictly ordered".into(),
        ));
    }
    Ok(resolve_policies(
        &layers
            .iter()
            .map(|layer| layer.policy.clone())
            .collect::<Vec<_>>(),
    ))
}

