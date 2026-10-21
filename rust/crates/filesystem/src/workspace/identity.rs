use super::*;

/// Canonical immutable customer workspace name.
#[derive(Clone)]
pub struct WorkspaceName(Arc<(String, KernelOwnedAllocation)>);

impl WorkspaceName {
    /// Canonicalizes a name under the supplied work limits.
    pub fn new<W: crate::facade::FsWork>(
        name: impl AsRef<str>,
        work: &W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = Self::with_accounting(name.as_ref(), &mut accounting);
        work.finish(result, &accounting)
    }

    pub(crate) fn with_accounting(
        name: &str,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, WorkspaceError> {
        Self::with_probe(name, accounting, char::is_ascii, &mut |_, _, _| {})
    }

    fn with_probe(
        name: &str,
        accounting: &mut KernelAccounting<'_>,
        ascii: impl Fn(&char) -> bool,
        probe: &mut impl FnMut(u8, u64, u64),
    ) -> Result<Self, WorkspaceError> {
        accounting.check()?;
        let (all_ascii, invalid_ascii, maximum_pending, maximum_chunk) =
            plan::inspect(name, accounting, ascii, probe)?;
        if !all_ascii {
            return normalize::parse(name, maximum_pending, maximum_chunk, accounting, probe);
        }
        // No normalization scratch or temporary UTF-8 copy for ASCII. Retain
        // the zero-work admission before reporting canonical-name errors.
        normalize::admit_scratch(0, accounting)?;
        if name.len() > MAXIMUM_WORKSPACE_NAME_BYTES {
            return Err(WorkspaceNameError::TooLong.into());
        }
        if name.is_empty() {
            return Err(WorkspaceNameError::Empty.into());
        }
        if name == "." || name == ".." {
            return Err(WorkspaceNameError::Reserved.into());
        }
        if invalid_ascii {
            return Err(WorkspaceNameError::InvalidCharacter.into());
        }
        Self::retain(name, accounting, probe)
    }

    fn retain(
        canonical: &str,
        accounting: &mut KernelAccounting<'_>,
        probe: &mut impl FnMut(u8, u64, u64),
    ) -> Result<Self, WorkspaceError> {
        let length = canonical.len();
        let copied = copy_work(length as u64);
        accounting.admit(copied)?;
        let mut owned = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
        // Convert the empty, admitted buffer before copying valid UTF-8; this
        // keeps String's invariant without validating the copied bytes again.
        let mut name = String::from_utf8(owned.value).map_err(WorkspaceError::engine)?;
        append(canonical, &mut name, accounting, probe)?;
        let control = const {
            match crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<(
                String,
                KernelOwnedAllocation,
            )>()) {
                Ok(bytes) => bytes as u64,
                Err(_) => panic!("workspace name Arc layout overflow"),
            }
        };
        owned
            .allocation
            .absorb(&mut accounting.claim_owned(control, 1)?)?;
        let output = Self(Arc::new((name, owned.allocation)));
        #[cfg(test)]
        #[cfg(all(not(acyclic_fs_loom), not(target_arch = "wasm32")))]
        name_tests::redundant_allocation();
        Ok(output)
    }

    /// Returns the canonical customer spelling.
    pub fn as_str(&self) -> &str {
        &self.0.0
    }

    pub(crate) fn retained_bytes(&self) -> u64 {
        self.as_str().len() as u64
    }
}

impl PartialEq for WorkspaceName {
    fn eq(&self, other: &Self) -> bool {
        self.as_str() == other.as_str()
    }
}
impl Eq for WorkspaceName {}
impl PartialOrd for WorkspaceName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for WorkspaceName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}
impl std::hash::Hash for WorkspaceName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.as_str(), state);
    }
}
impl fmt::Debug for WorkspaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("WorkspaceName")
            .field(&self.as_str())
            .finish()
    }
}
impl fmt::Display for WorkspaceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable opaque identity of one named workspace.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkspaceId([u8; 16]);

impl WorkspaceId {
    /// Restores a stable workspace identity from canonical bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Derives the stable identity for a canonical name in one deployment namespace. The namespace is deliberately internal to the selected `Fs` deployment.
    pub(crate) fn derive<W: crate::facade::FsWork>(
        namespace: [u8; 16],
        name: &WorkspaceName,
        work: &W,
        cancellation: &CancellationToken,
    ) -> W::Output<Self, WorkspaceError> {
        let mut accounting = work.accounting(cancellation);
        let result = Self::with_accounting(namespace, name, &mut accounting);
        work.finish(result, &accounting)
    }

    fn with_accounting(
        namespace: [u8; 16],
        name: &WorkspaceName,
        accounting: &mut KernelAccounting<'_>,
    ) -> Result<Self, WorkspaceError> {
        accounting.check()?;
        let performed = crate::WorkCounters {
            bytes_hashed: (WORKSPACE_ID_DOMAIN.len() + 16 + 8 + name.as_str().len()) as u64,
            bytes_copied: 16,
            ..crate::WorkCounters::UNCHARGED
        };
        Ok(Self(accounting.perform(performed, || {
            let mut hasher = blake3::Hasher::new();
            hasher.update(WORKSPACE_ID_DOMAIN);
            hasher.update(&namespace);
            hasher.update(
                &u64::try_from(name.as_str().len())
                    .unwrap_or(u64::MAX)
                    .to_le_bytes(),
            );
            hasher.update(name.as_str().as_bytes());
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
            #[cfg(test)]
            #[cfg(all(acyclic_fs_loom, not(target_arch = "wasm32")))]
            model::completed(
                bytes.len() as u64,
                (WORKSPACE_ID_DOMAIN.len() + namespace.len() + 8 + name.as_str().len()) as u64,
            );
            bytes
        })?))
    }

    /// Returns the stable opaque bytes for persistence or transport.
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    /// Returns the volume identity represented by this workspace identity.
    pub const fn volume_id(self) -> VolumeId {
        VolumeId::from_bytes(self.0)
    }
}

/// Stable retry identity for one customer-visible mutation, fork, or join.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdempotencyKey([u8; 16]);

impl IdempotencyKey {
    /// Creates a fresh time-ordered retry identity.
    pub fn new() -> Self {
        Self(OperationId::new().into_bytes())
    }

    /// Restores an exact key after an ambiguous outcome.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Returns the stable wire bytes.
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    pub(crate) const fn operation_id(self) -> OperationId {
        OperationId::from_bytes(self.0)
    }
}

impl Default for IdempotencyKey {
    fn default() -> Self {
        Self::new()
    }
}

#[path = "name_normalize.rs"]
mod normalize;
#[path = "name_plan.rs"]
mod plan;

fn append(
    canonical: &str,
    output: &mut String,
    accounting: &mut KernelAccounting<'_>,
    probe: &mut impl FnMut(u8, u64, u64),
) -> Result<(), FsError> {
    charged(accounting, copy_work(canonical.len() as u64), || {
        output.push_str(canonical);
        probe(4, canonical.len() as u64, 0);
    })
}

/// Checks and records `delta` in one atomic pass, then runs the infallible
/// `effect`: concurrent owners cannot spend the same credit, and nothing is
/// reserved across the effect.
fn charged<T>(
    accounting: &mut KernelAccounting<'_>,
    delta: crate::WorkCounters,
    effect: impl FnOnce() -> T,
) -> Result<T, FsError> {
    #[cfg(test)]
    #[cfg(all(acyclic_fs_loom, not(target_arch = "wasm32")))]
    if name_model::check_only() {
        accounting.admit(delta)?;
        let output = effect();
        accounting.record_completed(delta)?;
        return Ok(output);
    }
    accounting.charge(delta)?;
    Ok(effect())
}

#[cfg(test)]
#[cfg(all(not(acyclic_fs_loom), not(target_arch = "wasm32")))]
#[path = "identity_tests.rs"]
mod tests;

#[cfg(test)]
#[cfg(all(acyclic_fs_loom, not(target_arch = "wasm32")))]
#[path = "identity_loom.rs"]
mod model;

#[cfg(test)]
#[cfg(all(not(acyclic_fs_loom), not(target_arch = "wasm32")))]
#[path = "name_tests.rs"]
mod name_tests;

#[cfg(all(not(acyclic_fs_loom), any(test, feature = "fuzzing")))]
#[path = "name_reference.rs"]
mod reference;

#[cfg(test)]
#[cfg(all(acyclic_fs_loom, not(target_arch = "wasm32")))]
#[path = "name_loom.rs"]
mod name_model;

#[cfg(all(not(acyclic_fs_loom), any(test, feature = "fuzzing")))]
#[path = "name_fuzz.rs"]
mod name_fuzz;
#[cfg(all(not(acyclic_fs_loom), any(test, feature = "fuzzing")))]
pub use name_fuzz::check_workspace_name;
