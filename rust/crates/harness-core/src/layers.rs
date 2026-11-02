//! Layered reads. An agent's effective tree is its `home` over the `seed`:
//! a read looks in the highest layer first, and each file type has one merge
//! rule (listed in the seed's `sys/kernel.md`). Layers are separate pinned
//! volumes; resolving a path reads at most one entry per layer and copies
//! nothing into a volume.
use crate::{Error, Result};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// The agent's own writable layer, bound as its home volume.
pub const HOME: &str = "home";
/// The read-only layer built into the binary (see [`crate::seed`]).
pub const SEED: &str = "seed";

/// Tool path of the agent's home volume.
pub const HOME_PATH: &str = ".volumes/home/";
/// Tool path of the seed volume.
pub const SEED_PATH: &str = ".volumes/seed/";
/// Tool path of the effective view: `home` over `seed`, read-only.
pub const EFFECTIVE_PATH: &str = ".volumes/effective/";
/// Suffix naming a merged file's provenance beside it.
pub const WHY_SUFFIX: &str = ".why.json";

/// Read-only kernel files, served from the seed and never layered.
const SYS: &str = "sys/";

/// How a higher layer's copy of a path combines with the copies below it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Rule {
    /// The highest layer holding the file wins whole (Markdown and every
    /// file that is not configuration).
    Replace,
    /// TOML and JSON configuration: objects merge per key, the higher layer
    /// wins, and every other value (arrays included) replaces.
    Merge,
    /// A file holding only `off: true` removes the lower layers' file.
    Off,
}

impl Rule {
    /// Every rule, in the order `sys/kernel.md` lists them.
    pub const ALL: [Self; 3] = [Self::Replace, Self::Merge, Self::Off];

    /// The rule's name in `sys/kernel.md` and in provenance.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Replace => "replace",
            Self::Merge => "merge",
            Self::Off => "off",
        }
    }

    /// The rule combining the layers' copies of `path`.
    #[must_use]
    pub fn of(path: &str) -> Self {
        if path.ends_with(".toml") || path.ends_with(".json") {
            Self::Merge
        } else {
            Self::Replace
        }
    }
}

/// Whether `bytes` disable the lower layers' copy of their path.
#[must_use]
pub fn is_off(bytes: &[u8]) -> bool {
    bytes.trim_ascii() == b"off: true"
}

/// Whether a tool path names a read-only layer view: the effective tree or
/// the seed. Writes go to the home.
#[must_use]
pub fn read_only(path: &str) -> bool {
    path.starts_with(EFFECTIVE_PATH) || path.starts_with(SEED_PATH)
}

/// Whether `path` (relative to the effective tree) is layered; `sys/` is
/// served from the seed alone.
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

/// Where an effective file came from: `<path>.why.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Why {
    /// Format version.
    pub version: u32,
    /// The path in the effective tree.
    pub path: String,
    /// The rule that combined the layers.
    pub rule: Rule,
    /// Contributing layers, lowest first.
    pub sources: Vec<Source>,
}

/// One layer's contribution to an effective file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Source {
    /// Layer name.
    pub layer: String,
    /// The pinned generation read.
    pub generation: String,
    /// Dotted keys whose effective value this layer set; empty for a whole file.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<String>,
}

/// An effective file and its provenance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Effective {
    /// The effective bytes. A file only one layer holds keeps its exact bytes.
    pub bytes: Vec<u8>,
    /// Where they came from.
    pub why: Why,
}

/// The effective copy of `path` over `layers`, given lowest first; `None`
/// when no layer holds it or the highest copy is `off`.
pub fn resolve(path: &str, layers: &[Layer<'_>]) -> Result<Option<Effective>> {
    let rule = Rule::of(path);
    // Copies above the highest `off` marker, highest first.
    let mut held = Vec::new();
    for layer in layers.iter().rev() {
        let Some(bytes) = layer.bytes else { continue };
        if is_off(bytes) {
            break;
        }
        held.push((layer, bytes));
        if rule == Rule::Replace {
            break;
        }
    }
    let why = |rule, sources| Why {
        version: 1,
        path: path.to_owned(),
        rule,
        sources,
    };
    let whole = |layer: &Layer<'_>| Source {
        layer: layer.name.to_owned(),
        generation: layer.generation.to_owned(),
        keys: Vec::new(),
    };
    match held.as_slice() {
        [] => Ok(None),
        [(layer, bytes)] => Ok(Some(Effective {
            bytes: bytes.to_vec(),
            why: why(rule, vec![whole(layer)]),
        })),
        _ => {
            let toml = path.ends_with(".toml");
            let mut merged = Map::new();
            let mut set = BTreeMap::new();
            for (index, (_, bytes)) in held.iter().rev().enumerate() {
                let Value::Object(object) = parse(bytes, toml)? else {
                    return Err(Error::Invalid(format!("{path} is not a table")));
                };
                overlay(&mut merged, object, "", index, &mut set);
            }
            let sources = held
                .iter()
                .rev()
                .enumerate()
                .map(|(index, (layer, _))| Source {
                    keys: set
                        .iter()
                        .filter(|(_, setter)| **setter == index)
                        .map(|(key, _)| key.clone())
                        .collect(),
                    ..whole(layer)
                })
                .collect();
            let bytes = if toml {
                toml::to_string(&merged)
                    .map_err(|error| Error::Invalid(error.to_string()))?
                    .into_bytes()
            } else {
                serde_json::to_vec_pretty(&merged)
                    .map_err(|error| Error::Invalid(error.to_string()))?
            };
            Ok(Some(Effective {
                bytes,
                why: why(rule, sources),
            }))
        }
    }
}

fn parse(bytes: &[u8], toml: bool) -> Result<Value> {
    let invalid = |error: &dyn std::fmt::Display| Error::Invalid(error.to_string());
    if toml {
        let text = std::str::from_utf8(bytes).map_err(|error| invalid(&error))?;
        let table: toml::Table = toml::from_str(text).map_err(|error| invalid(&error))?;
        serde_json::to_value(table).map_err(|error| invalid(&error))
    } else {
        serde_json::from_slice(bytes).map_err(|error| invalid(&error))
    }
}

/// Merges `over` into `base` per key, recording in `set` which layer set
/// each dotted key.
fn overlay(
    base: &mut Map<String, Value>,
    over: Map<String, Value>,
    prefix: &str,
    layer: usize,
    set: &mut BTreeMap<String, usize>,
) {
    for (key, value) in over {
        let path = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        let nested = format!("{path}.");
        if !matches!(
            (base.get(&key), &value),
            (Some(Value::Object(_)), Value::Object(_))
        ) {
            // A replaced value keeps no provenance from the copy below.
            set.retain(|key, _| key != &path && !key.starts_with(&nested));
        }
        match (base.get_mut(&key), value) {
            (Some(Value::Object(below)), Value::Object(above)) => {
                overlay(below, above, &path, layer, set);
            }
            // A new non-empty table records its leaves, so a higher layer
            // that later sets one of them takes only that key.
            (_, Value::Object(above)) if !above.is_empty() => {
                let mut table = Map::new();
                overlay(&mut table, above, &path, layer, set);
                base.insert(key, Value::Object(table));
            }
            (_, value) => {
                set.insert(path, layer);
                base.insert(key, value);
            }
        }
    }
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

    #[test]
    fn markdown_replaces_and_off_disables() -> Result<()> {
        let home = resolve("AGENT.md", &layers(Some("mine"), Some("default")))?.unwrap();
        assert_eq!(home.bytes, b"mine");
        assert_eq!(home.why.sources.len(), 1);
        assert_eq!(home.why.sources[0].layer, HOME);
        let seed = resolve("AGENT.md", &layers(None, Some("default")))?.unwrap();
        assert_eq!(seed.why.sources[0].layer, SEED);
        assert!(resolve("AGENT.md", &layers(Some(" off: true\n"), Some("default")))?.is_none());
        assert!(resolve("AGENT.md", &layers(None, None))?.is_none());
        Ok(())
    }

    #[test]
    fn configuration_merges_per_key_with_provenance() -> Result<()> {
        let merged = resolve(
            "model.toml",
            &layers(
                Some("effort = \"high\"\n[limits]\nsteps = 9\n"),
                Some("model = \"a\"\neffort = \"low\"\ntags = [1, 2]\n[limits]\nsteps = 1\ntokens = 5\n"),
            ),
        )?
        .unwrap();
        let value: toml::Table =
            toml::from_str(std::str::from_utf8(&merged.bytes).unwrap()).unwrap();
        assert_eq!(value["effort"].as_str(), Some("high"));
        assert_eq!(value["model"].as_str(), Some("a"));
        assert_eq!(value["limits"]["steps"].as_integer(), Some(9));
        assert_eq!(value["limits"]["tokens"].as_integer(), Some(5));
        assert_eq!(
            merged.why.sources[0].keys,
            ["limits.tokens", "model", "tags"]
        );
        assert_eq!(merged.why.sources[1].keys, ["effort", "limits.steps"]);
        // A replaced array keeps no element of the lower copy.
        let json = resolve(
            "ui/settings.json",
            &layers(Some(r#"{"a":[3]}"#), Some(r#"{"a":[1,2]}"#)),
        )?
        .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&json.bytes).unwrap(),
            serde_json::json!({"a": [3]})
        );
        // A table replaced by a scalar credits none of its old keys.
        let replaced = resolve(
            "a.json",
            &layers(Some(r#"{"limits":9}"#), Some(r#"{"limits":{"steps":1}}"#)),
        )?
        .unwrap();
        assert!(replaced.why.sources[0].keys.is_empty());
        assert_eq!(replaced.why.sources[1].keys, ["limits"]);
        // Negative control: an `off` marker stops the merge at its layer.
        assert!(
            resolve(
                "model.toml",
                &layers(Some("off: true"), Some("model = \"a\""))
            )?
            .is_none()
        );
        assert!(resolve("model.toml", &layers(Some("= 1"), Some("model = \"a\""))).is_err());
        Ok(())
    }

    #[test]
    fn stack_identity_changes_with_any_layer_generation() {
        let base = stack_digest([(SEED, &b"s1"[..]), (HOME, &b"h1"[..])]);
        assert_ne!(base, stack_digest([(SEED, &b"s1"[..]), (HOME, &b"h2"[..])]));
        assert_ne!(base, stack_digest([(SEED, &b"s2"[..]), (HOME, &b"h1"[..])]));
        assert_ne!(base, stack_digest([(HOME, &b"h1"[..]), (SEED, &b"s1"[..])]));
    }
}
