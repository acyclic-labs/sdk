//! Layered reads, lowest first: seed, user/org, scope, template, parent, home.
//! A read looks in the highest layer first, and each file has one rule
//! (listed in the seed's `sys/kernel.md`). Layers are separate pinned
//! volumes; resolving a path reads at most one entry per layer and copies
//! nothing into a volume.

/// The agent's own writable layer, bound as its home volume.
pub const HOME: &str = "home";
/// The read-only layer built into the binary (see [`crate::seed`]).
pub const SEED: &str = "seed";
/// User or organization configuration, above the seed.
pub const USER_ORG: &str = "user-org";
/// Scope (swarm) configuration.
pub const SCOPE: &str = "scope";
/// The selected template's configuration.
pub const TEMPLATE: &str = "template";
/// Configuration supplied by the parent.
pub const PARENT: &str = "parent";

/// Configuration precedence. This is the set of semantic layer positions,
/// independent of the number of agents, files or generations in their volumes.
pub const ORDER: [&str; 6] = [SEED, USER_ORG, SCOPE, TEMPLATE, PARENT, HOME];

mod manifest;
#[cfg(not(target_arch = "wasm32"))]
pub use manifest::ManifestTree;
pub use manifest::{HomeApply, Manifest};

/// Tool path of the agent's home volume.
pub const HOME_PATH: &str = "volume:///home/";
/// Read-only tool path of the calling agent's original private output volume.
/// This is a model read route; it creates no guest mount or per-agent work.
pub const OUTPUT_PATH: &str = "volume:///output/";
/// Tool address of agent-authored private UI files; never a guest mount.
pub const PRIVATE_PATH: &str = "volume:///private/";
/// Tool path of the seed volume.
pub const SEED_PATH: &str = "volume:///seed/";
/// Read-only configuration supplied by the user or organization.
pub const USER_ORG_PATH: &str = "volume:///user-org/";
/// Read-only configuration supplied by the scope.
pub const SCOPE_PATH: &str = "volume:///scope/";
/// Read-only configuration supplied by the selected template.
pub const TEMPLATE_PATH: &str = "volume:///template/";
/// Read-only configuration supplied by the parent.
pub const PARENT_PATH: &str = "volume:///parent/";
/// Concrete paths in effective-file precedence order.
pub(crate) const PRECEDENCE_PATHS: [&str; 6] = [
    HOME_PATH,
    PARENT_PATH,
    TEMPLATE_PATH,
    SCOPE_PATH,
    USER_ORG_PATH,
    SEED_PATH,
];
/// Tool path of host-published system files.
pub const SYSTEM_PATH: &str = "volume:///system/";
/// Tool path of the effective view over the complete layer stack, read-only.
pub const EFFECTIVE_PATH: &str = "volume:///effective/";
/// Suffix naming a composed file's provenance chain beside it.
pub const WHY_SUFFIX: &str = ".why.json";

/// Read-only system files, served from the seed or run metadata, never the home.
const SYS: &str = "sys/";

/// How a higher layer's copy of a path combines with the copies below it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule {
    /// The highest layer holding the file wins whole.
    Replace,
    /// A settings entry point ([`crate::config::composes`]): each layer's
    /// module default-exports a value, replacing the result below, or a
    /// function of it; the kernel folds them lowest first.
    Compose,
    /// A file holding only `off: true` removes the lower layers' file.
    Off,
}

impl Rule {
    /// Every rule, in the order `sys/kernel.md` lists them.
    pub const ALL: [Self; 3] = [Self::Replace, Self::Compose, Self::Off];

    /// The rule's name in `sys/kernel.md`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Replace => "replace",
            Self::Compose => "compose",
            Self::Off => "off",
        }
    }
}

/// Whether `bytes` disable the lower layers' copy of their path.
#[must_use]
pub fn is_off(bytes: &[u8]) -> bool {
    let mut marker = OffMarker::default();
    marker.push(bytes);
    marker.finish()
}

mod off;
#[doc(hidden)]
pub use off::OffMarker;
pub(crate) use off::{MarkerChunk, equals_from_prefix, off_from_prefix};

/// The complete built-in skill at this path, when one exists. Old authored
/// copies have no layer semantics of their own: only byte equality with the
/// built-in version makes a copy redundant. Headers alone never prove that.
#[must_use]
pub fn seed_skill(path: &str) -> Option<&'static [u8]> {
    path.strip_prefix("skills/")?
        .strip_suffix("/SKILL.md")
        .filter(|name| !name.is_empty() && !name.contains('/'))?;
    crate::seed::file(path)
}

/// Whether a tool path names a read-only layer view: the effective tree or
/// the seed, original tool output or the host's system metadata. Writes go to the home.
#[must_use]
pub fn read_only(path: &str) -> bool {
    path.starts_with(EFFECTIVE_PATH)
        || path.starts_with(SEED_PATH)
        || [USER_ORG_PATH, SCOPE_PATH, TEMPLATE_PATH, PARENT_PATH]
            .iter()
            .any(|prefix| path.starts_with(prefix))
        || path.starts_with(SYSTEM_PATH)
        || path.starts_with(OUTPUT_PATH)
}

/// A host-published read-only layer, shared by every agent in a composition.
/// The generation is part of the binding, so a later host publication cannot
/// change what an already-open composition reads.
#[derive(Clone)]
pub struct PinnedLayer {
    /// The volume on the composition's original content reader.
    pub volume: crate::conversation::VolumeRef,
    /// Exact generation published by the host.
    pub generation: crate::resources::GenerationRef,
}

impl PinnedLayer {
    /// Checks the original volume and generation provider binding.
    pub fn validate(&self) -> crate::Result<()> {
        self.volume.validate(&mut None)?;
        self.generation.validate(&mut None)?;
        if self.volume.provider() != self.generation.as_resource().provider() {
            return Err(crate::Error::Invalid(
                "read-only layer belongs to another provider".into(),
            ));
        }
        Ok(())
    }
}

/// Whether `path` (relative to the effective tree) is layered; `sys/` is
/// served from the seed or the host's pinned system metadata.
#[must_use]
pub fn layered(path: &str) -> bool {
    !path.starts_with(SYS)
}

/// The identity of a layer stack: a hash of its ordered `(layer,
/// generation)` pairs, lowest first. An unchanged identity means every
/// effective file is unchanged, so a step boundary compares one hash.
#[must_use]
pub fn stack_digest<'a>(layers: impl IntoIterator<Item = (&'a str, &'a [u8])>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic.harness.layer-stack.v1");
    for (layer, generation) in layers {
        hasher.update(&(layer.len() as u64).to_be_bytes());
        hasher.update(layer.as_bytes());
        hasher.update(&(generation.len() as u64).to_be_bytes());
        hasher.update(generation);
    }
    hasher.finalize().into()
}

/// One layer's copy of a path.
#[derive(Clone, Copy, Debug)]
pub struct Layer<'a> {
    /// Layer name, such as [`HOME`] or [`SEED`].
    pub name: &'a str,
    /// The pinned generation the copy was read at, as provenance.
    pub generation: &'a str,
    /// The copy's bytes; `None` when the layer does not hold the path.
    pub bytes: Option<&'a [u8]>,
}

/// The layers whose copies of a path count, lowest first: those holding it
/// above the highest `off` marker. [`Rule::Replace`] reads the last one;
/// [`Rule::Compose`] folds them all.
#[must_use]
pub fn held<'a, 'b>(layers: &'b [Layer<'a>]) -> impl DoubleEndedIterator<Item = &'b Layer<'a>> {
    held_indexed(layers).map(|(_, layer)| layer)
}

pub(crate) fn held_indexed<'a, 'b>(
    layers: &'b [Layer<'a>],
) -> impl DoubleEndedIterator<Item = (usize, &'b Layer<'a>)> {
    let above = layers
        .iter()
        .rposition(|layer| layer.bytes.is_some_and(is_off))
        .map_or(0, |off| off + 1);
    layers
        .iter()
        .enumerate()
        .skip(above)
        .filter(|(_, layer)| layer.bytes.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layers<'a>(home: Option<&'a str>, seed: Option<&'a str>) -> [Layer<'a>; 2] {
        [
            Layer {
                name: SEED,
                generation: "s1",
                bytes: seed.map(str::as_bytes),
            },
            Layer {
                name: HOME,
                generation: "h1",
                bytes: home.map(str::as_bytes),
            },
        ]
    }

    fn names<'a>(layers: &[Layer<'a>]) -> Vec<&'a str> {
        held(layers).map(|layer| layer.name).collect()
    }

    #[test]
    fn off_hides_every_lower_copy() {
        assert_eq!(names(&layers(Some("mine"), Some("default"))), [SEED, HOME]);
        assert_eq!(names(&layers(None, Some("default"))), [SEED]);
        assert!(
            names(&layers(
                Some(
                    " off: true
"
                ),
                Some("default")
            ))
            .is_empty()
        );
        assert!(names(&layers(None, None)).is_empty());
        // Negative control: `off` inside other text is ordinary content.
        assert_eq!(names(&layers(Some("off: true, really"), None)), [HOME]);
    }

    #[test]
    fn stack_identity_changes_with_any_layer_generation() {
        let base = stack_digest([(SEED, &b"s1"[..]), (HOME, &b"h1"[..])]);
        assert_ne!(base, stack_digest([(SEED, &b"s1"[..]), (HOME, &b"h2"[..])]));
        assert_ne!(base, stack_digest([(SEED, &b"s2"[..]), (HOME, &b"h1"[..])]));
        assert_ne!(base, stack_digest([(HOME, &b"h1"[..]), (SEED, &b"s1"[..])]));
    }
}
