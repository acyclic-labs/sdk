//! Declarative merge-driver boundary over immutable conflict inputs.
//!
//! Drivers never receive a checkout, store, or publication authority. They
//! transform a complete immutable conflict view into a resolution proposal;
//! callers then submit that proposal to the filesystem's validation and
//! publication path.

use crate::{FileId, GenerationId};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use thiserror::Error;

const RESOLUTION_KEY_DOMAIN: &[u8] = b"acyclic-fs-resolution-key-v1\0";

/// Exact stable identity of one conflict region.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum ConflictKey {
    /// A path-independent file record.
    File(FileId),
    /// One directory name binding.
    Binding {
        /// Stable parent directory.
        directory_id: FileId,
        /// Opaque canonical name bytes.
        name: Vec<u8>,
    },
    /// One regular-file byte range.
    ContentRange {
        /// Stable file identity.
        file_id: FileId,
        /// Inclusive start byte.
        offset: u64,
        /// Range length.
        length: u64,
    },
    /// Metadata attached to one stable file record.
    Metadata(FileId),
    /// Hard-link topology involving one stable record.
    HardLinks(FileId),
    /// One directory record or subtree.
    Directory(FileId),
    /// One rename relationship represented independently from path spelling.
    Rename {
        /// Stable renamed record.
        file_id: FileId,
        /// Canonical source path.
        from: String,
        /// Canonical destination path.
        to: String,
    },
    /// Symbolic-link, device, reparse, or another special payload.
    SpecialPayload(FileId),
}

/// Semantic conflict category exposed to policy and user interfaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ConflictKind {
    /// Complete path-independent file record whose detailed kind is not loaded.
    Record,
    /// UTF-8 regular content eligible for marker projection.
    Text,
    /// Opaque regular content.
    Binary,
    /// File metadata.
    Metadata,
    /// Directory name or rename topology.
    Binding,
    /// Competing rename topology.
    Rename,
    /// Directory record or subtree.
    Directory,
    /// Hard-link topology.
    HardLink,
    /// Symbolic-link payload.
    SymbolicLink,
    /// Symbolic-link, device, reparse, or another opaque payload.
    Special,
}

/// One immutable side of a conflict.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ConflictValue {
    /// The item is absent on this side.
    Absent,
    /// Complete UTF-8 content.
    Text(String),
    /// Complete opaque content or special payload.
    Binary(Vec<u8>),
    /// Canonical serialized metadata.
    Metadata(Vec<u8>),
    /// Stable binding target, or `None` for deletion.
    Binding(Option<FileId>),
    /// Exact generation and stable record for lazy inspection.
    Record {
        /// Immutable generation containing the record.
        generation: GenerationId,
        /// Stable record identity.
        file_id: FileId,
    },
    /// Lazy immutable file-record lookup that may resolve to absence.
    RecordReference {
        /// Immutable generation to inspect.
        generation: GenerationId,
        /// Stable record identity to look up.
        file_id: FileId,
    },
    /// Lazy immutable directory-binding lookup that may resolve to absence.
    BindingReference {
        /// Immutable generation to inspect.
        generation: GenerationId,
        /// Stable parent directory.
        directory_id: FileId,
        /// Encoding-tagged canonical name bytes.
        name: Vec<u8>,
    },
}

/// Complete immutable input supplied to one merge driver.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConflictView {
    /// Stable conflict-region key.
    pub key: ConflictKey,
    /// Best-effort portable path; identity remains authoritative.
    pub path: Option<String>,
    /// Semantic category.
    pub kind: ConflictKind,
    /// Common-ancestor value.
    pub base: ConflictValue,
    /// Target-side value.
    pub ours: ConflictValue,
    /// Source-side value.
    pub theirs: ConflictValue,
}

/// Side selected without synthesizing new state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ConflictSide {
    /// Common ancestor.
    Base,
    /// Target side.
    Ours,
    /// Source side.
    Theirs,
}

/// Declarative resolution proposal. Validation remains filesystem-owned.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MergeResolution {
    /// Select one exact immutable side.
    Select(ConflictSide),
    /// Install synthesized UTF-8 content.
    Text(String),
    /// Install synthesized opaque content.
    Binary(Vec<u8>),
    /// Install canonical serialized metadata.
    Metadata(Vec<u8>),
    /// Install or remove one exact directory binding.
    Binding(Option<FileId>),
    /// Leave the conflict for a later caller or human.
    Unresolved,
}

/// Content-addressed key for safe resolution reuse.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ResolutionKey([u8; 32]);

impl ResolutionKey {
    /// Hashes exact merge inputs, conflict identity, and driver fingerprint.
    #[must_use]
    pub fn new(
        base: GenerationId,
        ours: GenerationId,
        theirs: GenerationId,
        conflict: &ConflictKey,
        driver_fingerprint: &[u8],
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(RESOLUTION_KEY_DOMAIN);
        hasher.update(base.digest().as_bytes());
        hasher.update(ours.digest().as_bytes());
        hasher.update(theirs.digest().as_bytes());
        hash_conflict_key(&mut hasher, conflict);
        hasher.update(
            &u64::try_from(driver_fingerprint.len())
                .unwrap_or(u64::MAX)
                .to_le_bytes(),
        );
        hasher.update(driver_fingerprint);
        Self(*hasher.finalize().as_bytes())
    }

    /// Returns canonical key bytes.
    #[must_use]
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }
}

fn hash_conflict_key(hasher: &mut blake3::Hasher, key: &ConflictKey) {
    match key {
        ConflictKey::File(file_id) => {
            hasher.update(&[0]);
            hasher.update(&file_id.into_bytes());
        }
        ConflictKey::Binding { directory_id, name } => {
            hasher.update(&[1]);
            hasher.update(&directory_id.into_bytes());
            hasher.update(&u64::try_from(name.len()).unwrap_or(u64::MAX).to_le_bytes());
            hasher.update(name);
        }
        ConflictKey::ContentRange {
            file_id,
            offset,
            length,
        } => {
            hasher.update(&[2]);
            hasher.update(&file_id.into_bytes());
            hasher.update(&offset.to_le_bytes());
            hasher.update(&length.to_le_bytes());
        }
        ConflictKey::Metadata(file_id) => {
            hasher.update(&[3]);
            hasher.update(&file_id.into_bytes());
        }
        ConflictKey::HardLinks(file_id) => {
            hasher.update(&[4]);
            hasher.update(&file_id.into_bytes());
        }
        ConflictKey::Directory(file_id) => {
            hasher.update(&[5]);
            hasher.update(&file_id.into_bytes());
        }
        ConflictKey::Rename { file_id, from, to } => {
            hasher.update(&[6]);
            hasher.update(&file_id.into_bytes());
            hash_sized(hasher, from.as_bytes());
            hash_sized(hasher, to.as_bytes());
        }
        ConflictKey::SpecialPayload(file_id) => {
            hasher.update(&[7]);
            hasher.update(&file_id.into_bytes());
        }
    }
}

fn hash_sized(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_le_bytes());
    hasher.update(bytes);
}

/// Whether a driver may be safely rerun after the merge plan changes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MergeDriverMode {
    /// Equal immutable input always produces an equivalent result without effects.
    Deterministic,
    /// Resolution may consult an effectful service or require a human decision.
    Effectful,
}

/// Pure merge driver operating without filesystem authority.
pub trait MergeDriver: Send + Sync {
    /// Stable implementation/version fingerprint used by resolution caching.
    fn fingerprint(&self) -> &[u8];

    /// Declares whether replanning may rerun this driver.
    fn mode(&self) -> MergeDriverMode {
        MergeDriverMode::Effectful
    }

    /// Resolves one complete immutable conflict.
    fn resolve(&self, conflict: &ConflictView) -> Result<MergeResolution, DriverError>;
}

/// Immutable typed merge input, separate from resolution and publication.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MergePlan {
    /// Exact common ancestor.
    pub base: GenerationId,
    /// Exact target side.
    pub ours: GenerationId,
    /// Exact source side.
    pub theirs: GenerationId,
    /// Complete bounded conflict set.
    pub conflicts: Vec<ConflictView>,
    /// Whether additional conflicts exceeded the retained bound.
    pub truncated: bool,
}

/// Validated declarations that still have no publication authority.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UnpublishedMergeCandidate {
    /// Immutable input plan.
    pub plan: MergePlan,
    /// Exact declarative resolutions by conflict identity.
    pub resolutions: BTreeMap<ConflictKey, MergeResolution>,
}

/// Exact cache entry, including driver semantics for audit and recovery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CachedMergeResolution {
    /// Driver execution semantics.
    pub mode: MergeDriverMode,
    /// Declarative result.
    pub resolution: MergeResolution,
}

/// Resolution cache keyed by exact generations, conflict, and driver version.
pub trait MergeResolutionCache {
    /// Returns one exact cached resolution.
    fn get(&self, key: ResolutionKey) -> Option<CachedMergeResolution>;

    /// Stores one exact resolution.
    fn insert(&mut self, key: ResolutionKey, resolution: CachedMergeResolution);
}

/// Process-local exact resolution cache.
#[derive(Default)]
pub struct MemoryMergeResolutionCache {
    entries: BTreeMap<ResolutionKey, CachedMergeResolution>,
}

impl MergeResolutionCache for MemoryMergeResolutionCache {
    fn get(&self, key: ResolutionKey) -> Option<CachedMergeResolution> {
        self.entries.get(&key).cloned()
    }

    fn insert(&mut self, key: ResolutionKey, resolution: CachedMergeResolution) {
        self.entries.insert(key, resolution);
    }
}

/// Merge-plan resolution failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MergePlanResolutionError {
    /// No registered driver accepts one conflict.
    #[error("no merge driver is registered for conflict {0:?}")]
    MissingDriver(ConflictKey),
    /// Driver execution failed.
    #[error(transparent)]
    Driver(#[from] DriverError),
    /// Replanning invalidated an effectful or deferred decision.
    #[error("effectful merge resolution became stale after replanning")]
    StaleEffectful,
}

/// Resolves an immutable plan without granting drivers workspace authority.
///
/// Exact cache hits are reusable for every driver mode. During replanning,
/// deterministic drivers may run again, while missing effectful results return
/// stale and require an explicit caller decision.
pub fn resolve_merge_plan(
    plan: MergePlan,
    registry: &MergeDriverRegistry,
    cache: &mut impl MergeResolutionCache,
    replanning: bool,
) -> Result<UnpublishedMergeCandidate, MergePlanResolutionError> {
    let mut resolutions = BTreeMap::new();
    for conflict in &plan.conflicts {
        let path = conflict.path.as_deref().unwrap_or("");
        let driver = registry
            .select(path)
            .ok_or_else(|| MergePlanResolutionError::MissingDriver(conflict.key.clone()))?;
        let key = ResolutionKey::new(
            plan.base,
            plan.ours,
            plan.theirs,
            &conflict.key,
            driver.fingerprint(),
        );
        let resolution = match cache.get(key) {
            Some(cached) => cached.resolution,
            None if replanning && driver.mode() == MergeDriverMode::Effectful => {
                return Err(MergePlanResolutionError::StaleEffectful);
            }
            None => {
                let resolution = driver.resolve(conflict)?;
                cache.insert(
                    key,
                    CachedMergeResolution {
                        mode: driver.mode(),
                        resolution: resolution.clone(),
                    },
                );
                resolution
            }
        };
        resolutions.insert(conflict.key.clone(), resolution);
    }
    Ok(UnpublishedMergeCandidate { plan, resolutions })
}

/// Merge-driver failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DriverError {
    /// Driver does not accept this semantic kind.
    #[error("merge driver does not support this conflict kind")]
    Unsupported,
    /// Driver rejected malformed or incomplete inputs.
    #[error("merge driver received invalid conflict input: {0}")]
    InvalidInput(String),
    /// Driver-specific deterministic failure.
    #[error("merge driver failed: {0}")]
    Failed(String),
}

/// Git-attribute-like path rule selecting a registered driver.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AttributeRule {
    /// Portable glob pattern supporting exact paths, `*`, and `**`.
    pub pattern: String,
    /// Registered driver name.
    pub driver: String,
}

/// Registry construction failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum DriverRegistrationError {
    /// Empty driver names are not stable configuration keys.
    #[error("merge driver name cannot be empty")]
    EmptyName,
    /// A name was already registered.
    #[error("merge driver '{0}' is already registered")]
    Duplicate(String),
    /// An attribute rule references an unknown driver.
    #[error("attribute rule references unknown merge driver '{0}'")]
    UnknownDriver(String),
}

/// Typed driver registry with ordered Git-style path rules.
#[derive(Default)]
pub struct MergeDriverRegistry {
    drivers: BTreeMap<String, Arc<dyn MergeDriver>>,
    rules: Vec<AttributeRule>,
    default: Option<String>,
}

impl MergeDriverRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers one named driver.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        driver: Arc<dyn MergeDriver>,
    ) -> Result<(), DriverRegistrationError> {
        let name = name.into();
        if name.is_empty() {
            return Err(DriverRegistrationError::EmptyName);
        }
        if self.drivers.contains_key(&name) {
            return Err(DriverRegistrationError::Duplicate(name));
        }
        self.drivers.insert(name, driver);
        Ok(())
    }

    /// Sets ordered selection rules. Later matching rules win, like attributes.
    pub fn set_rules(&mut self, rules: Vec<AttributeRule>) -> Result<(), DriverRegistrationError> {
        for rule in &rules {
            if !self.drivers.contains_key(&rule.driver) {
                return Err(DriverRegistrationError::UnknownDriver(rule.driver.clone()));
            }
        }
        self.rules = rules;
        Ok(())
    }

    /// Selects the fallback driver by registered name.
    pub fn set_default(&mut self, name: impl Into<String>) -> Result<(), DriverRegistrationError> {
        let name = name.into();
        if !self.drivers.contains_key(&name) {
            return Err(DriverRegistrationError::UnknownDriver(name));
        }
        self.default = Some(name);
        Ok(())
    }

    /// Selects a driver for a portable path.
    #[must_use]
    pub fn select(&self, path: &str) -> Option<&dyn MergeDriver> {
        let selected = self
            .rules
            .iter()
            .rfind(|rule| glob_matches(&rule.pattern, path))
            .map(|rule| rule.driver.as_str())
            .or(self.default.as_deref())?;
        self.drivers.get(selected).map(AsRef::as_ref)
    }
}

fn glob_matches(pattern: &str, path: &str) -> bool {
    if pattern == "**" || pattern == "*" {
        return true;
    }
    if let Some(suffix) = pattern.strip_prefix("**/*") {
        return path.ends_with(suffix);
    }
    if let Some(suffix) = pattern.strip_prefix('*') {
        return path.ends_with(suffix);
    }
    if let Some(prefix) = pattern.strip_suffix("/**") {
        return path == prefix || path.starts_with(&format!("{prefix}/"));
    }
    pattern == path
}

/// Conservative UTF-8 driver with diff3-style marker projection.
pub struct DefaultTextMergeDriver;

impl MergeDriver for DefaultTextMergeDriver {
    fn fingerprint(&self) -> &[u8] {
        b"acyclic-default-text-v1"
    }

    fn mode(&self) -> MergeDriverMode {
        MergeDriverMode::Deterministic
    }

    fn resolve(&self, conflict: &ConflictView) -> Result<MergeResolution, DriverError> {
        let (ConflictValue::Text(base), ConflictValue::Text(ours), ConflictValue::Text(theirs)) =
            (&conflict.base, &conflict.ours, &conflict.theirs)
        else {
            return Err(DriverError::Unsupported);
        };
        let merged = crate::text_merge::merge_text(base, ours, theirs, "ours", "theirs");
        if merged.clean && merged.content == *ours {
            Ok(MergeResolution::Select(ConflictSide::Ours))
        } else if merged.clean && merged.content == *theirs {
            Ok(MergeResolution::Select(ConflictSide::Theirs))
        } else {
            Ok(MergeResolution::Text(merged.content))
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    struct MarkerDriver(&'static [u8]);

    impl MergeDriver for MarkerDriver {
        fn fingerprint(&self) -> &[u8] {
            self.0
        }

        fn mode(&self) -> MergeDriverMode {
            MergeDriverMode::Deterministic
        }

        fn resolve(&self, _: &ConflictView) -> Result<MergeResolution, DriverError> {
            Err(DriverError::Unsupported)
        }
    }

    fn generation(byte: u8) -> GenerationId {
        GenerationId::new(crate::Digest::from_bytes([byte; 32]))
    }

    #[test]
    fn attribute_selection_is_ordered_and_text_projection_is_deterministic() {
        let mut registry = MergeDriverRegistry::new();
        registry
            .register("text", Arc::new(DefaultTextMergeDriver))
            .expect("register driver");
        registry
            .set_rules(vec![AttributeRule {
                pattern: "*.rs".to_owned(),
                driver: "text".to_owned(),
            }])
            .expect("set rule");
        let driver = registry.select("src/lib.rs").expect("matching driver");
        let conflict = ConflictView {
            key: ConflictKey::Metadata(FileId::new()),
            path: Some("src/lib.rs".to_owned()),
            kind: ConflictKind::Text,
            base: ConflictValue::Text("base".to_owned()),
            ours: ConflictValue::Text("ours".to_owned()),
            theirs: ConflictValue::Text("theirs".to_owned()),
        };
        assert!(matches!(
            driver.resolve(&conflict).expect("resolve"),
            MergeResolution::Text(text)
                if crate::text_merge::has_conflict_markers(&text)
                    && text.contains("<<<<<<< ours")
                    && text.contains(">>>>>>> theirs")
        ));
    }

    #[test]
    fn duplicate_registration_keeps_the_original_driver() {
        let mut registry = MergeDriverRegistry::new();
        registry
            .register("driver", Arc::new(MarkerDriver(b"original")))
            .expect("register original");
        assert_eq!(
            registry.register("driver", Arc::new(MarkerDriver(b"replacement"))),
            Err(DriverRegistrationError::Duplicate("driver".to_owned()))
        );
        registry.set_default("driver").expect("select original");
        assert_eq!(
            registry
                .select("anything")
                .expect("registered driver")
                .fingerprint(),
            b"original"
        );
    }

    #[test]
    fn resolution_reuse_is_bound_to_exact_sides_and_driver_version() {
        let mut registry = MergeDriverRegistry::new();
        registry
            .register("text", Arc::new(DefaultTextMergeDriver))
            .expect("register driver");
        registry.set_default("text").expect("default driver");
        let conflict = ConflictView {
            key: ConflictKey::ContentRange {
                file_id: FileId::new(),
                offset: 0,
                length: 1,
            },
            path: Some("file.txt".to_owned()),
            kind: ConflictKind::Text,
            base: ConflictValue::Text("base".to_owned()),
            ours: ConflictValue::Text("ours".to_owned()),
            theirs: ConflictValue::Text("theirs".to_owned()),
        };
        let plan = MergePlan {
            base: generation(1),
            ours: generation(2),
            theirs: generation(3),
            conflicts: vec![conflict],
            truncated: false,
        };
        let mut cache = MemoryMergeResolutionCache::default();
        let first = resolve_merge_plan(plan.clone(), &registry, &mut cache, false)
            .expect("initial resolution");
        assert_eq!(
            resolve_merge_plan(plan, &registry, &mut cache, true).expect("exact cache reuse"),
            first
        );
    }
}
