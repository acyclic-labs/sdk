//! Plugins: the unit of default functionality (FILESYSTEM §2.6).
//!
//! A plugin is a directory, `plugins/<name>/`, laid out like a home: a
//! `plugin.ts` manifest, `hooks/`, skills, commands and tools. `plugins.ts`
//! is the one manifest that enables plugins and holds their settings; it
//! composes over the layers like every settings entry point
//! ([`crate::config::compose`]). An enabled plugin's directory is a layer
//! fragment just under the layer that enabled it. A fragment never
//! contributes `plugins.ts` or `policy.ts`: the loader reads only the files
//! named here, so the inclusion graph is flat.
//!
//! The loader turns the enabled set into hook rules for the one hook runner
//! ([`crate::hooks`]): a plugin's `hooks/before-tool.{sh,ps1}` becomes a
//! before-tool hook judged by the rewrite-safe chain, pinned by its bytes and
//! its settings. A hook this host cannot run yet makes `plugins.ts` invalid,
//! so the host retains the last admitted set or refuses the session. The
//! admission names the hook; an enabled hook is never silently skipped.
//! The authoring types are generated
//! from these decode shapes by [`crate::config::typescript`].
use crate::{
    Error, Result,
    config::{self, Surface, Tree},
    hooks::{HookEvent, HookFailure, HookRule, HookRun, HookSpec, Hooks, RequestMatch},
    layers::Layer,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use ts_rs::TS;

/// The one manifest: `.volumes/home/plugins.ts`.
pub const MANIFEST: &str = "plugins.ts";

/// The enabled plugins and their settings, decoded from `plugins.ts`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(transparent)]
pub struct Plugins(pub BTreeMap<String, PluginEntry>);

/// One `plugins.ts` entry. To change a plugin's code, copy its folder into
/// `.volumes/home/plugins/` under a new name.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginEntry {
    /// Whether the plugin applies.
    pub enabled: bool,
    /// A folder elsewhere, pinned by content (`acyclic digest <path>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub source: Option<PluginSource>,
    /// Its settings; the only place to change them. Absent: the plugin's
    /// `defaults`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "unknown")]
    pub config: Option<Value>,
}

/// A plugin folder outside `plugins/`, pinned by its content digest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginSource {
    /// The folder.
    pub path: String,
    /// Its content digest.
    pub digest: String,
}

/// `plugins/<name>/plugin.ts`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PluginManifest {
    /// The folder's name.
    pub name: String,
    /// What it does.
    pub description: String,
    /// Plugins that must be enabled with it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requires: Vec<String>,
    /// Its settings when `plugins.ts` names none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional, type = "unknown")]
    pub defaults: Option<Value>,
    /// What each `hooks/after-<n>.{ts,sh,ps1}` watches, keyed by `<n>`.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub after: BTreeMap<String, AfterSource>,
    /// Which requests each `hooks/before-<event>` runs on, keyed by event;
    /// others skip it without running it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub matcher: BTreeMap<Event, RequestMatch>,
}

/// The path an after-hook watches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AfterSource {
    /// A file, log or directory.
    pub path: String,
    /// `start`: also what is there already; `now`: only later changes.
    pub from: Start,
}

/// Where an after-hook starts reading.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum Start {
    /// Everything already there, then each change.
    Start,
    /// Only changes after the hook is enabled.
    Now,
}

/// A request kind a before-hook judges: `hooks/before-<event>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum Event {
    /// A tool call.
    Tool,
    /// A model call.
    Model,
    /// Creating a child.
    Spawn,
    /// Copying an agent.
    Fork,
    /// Merging a child's work.
    Merge,
    /// Committing files.
    Commit,
}

impl Event {
    /// Every event, in file-name order.
    pub const ALL: [Self; 6] = [
        Self::Tool,
        Self::Model,
        Self::Spawn,
        Self::Fork,
        Self::Merge,
        Self::Commit,
    ];

    /// The `<event>` in `hooks/before-<event>`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Tool => "tool",
            Self::Model => "model",
            Self::Spawn => "spawn",
            Self::Fork => "fork",
            Self::Merge => "merge",
            Self::Commit => "commit",
        }
    }

    /// The runner site judging this event, where this host has one.
    const fn site(self) -> Option<HookEvent> {
        match self {
            Self::Tool => Some(HookEvent::PreToolUse),
            Self::Model | Self::Spawn | Self::Fork | Self::Merge | Self::Commit => None,
        }
    }
}

/// The enabled plugin set, lowered to hook rules.
#[derive(Debug, Default)]
pub struct Enabled {
    /// Enabled plugin names, sorted.
    pub names: Vec<String>,
    /// Before-hook rules, in authority order.
    pub rules: Hooks,
}

/// Loads the plugins `plugins.ts` enables over `layers` (lowest first), as
/// enabled by `enabler`. Plugin folders and their imports resolve in `tree`.
///
/// # Errors
/// [`Error::Invalid`] when `plugins.ts` or an enabled plugin is invalid: an
/// unknown or disabled requirement, a manifest naming another folder, a
/// hook file outside the hook grammar, or a hook this host cannot run.
pub async fn load(tree: &dyn Tree, layers: &[Layer<'_>], enabler: &str) -> Result<Enabled> {
    load_result(tree, layers, enabler).await?
}

/// Load without retaining input evidence. Operational tree failures remain
/// outer errors, even when their variant is [`Error::Invalid`]. The inner
/// result is the semantic decision made from the original inputs.
pub async fn load_result(
    tree: &dyn Tree,
    layers: &[Layer<'_>],
    enabler: &str,
) -> Result<Result<Enabled>> {
    let (value, _) =
        match config::compose_result(MANIFEST, layers, tree, Value::Object(Default::default()))
            .await?
        {
            Ok(composed) => composed,
            Err(error) => return Ok(Err(error)),
        };
    match load_enabled(tree, value, enabler, None).await {
        Ok(enabled) => Ok(Ok(enabled)),
        Err(LoadFailure::Semantic(error)) => Ok(Err(error)),
        Err(LoadFailure::Operational(error)) => Err(error),
    }
}

/// One plugin selection and the original inputs that admitted or refused it.
/// This evidence grants no caller, hook-execution or publication authority.
#[derive(Debug)]
pub struct Decision {
    identity: [u8; 32],
    result: Result<Enabled>,
}

impl Decision {
    /// Identity of the loader, enabler, layered selection, manifest dependency
    /// cuts and original hook observations, including absent or invalid files.
    #[must_use]
    pub const fn identity(&self) -> [u8; 32] {
        self.identity
    }

    /// Return the original semantic decision after retaining its input identity.
    pub fn into_result(self) -> Result<Enabled> {
        self.result
    }
}

/// Load through the same path as [`load`], retaining the original input cut
/// even on semantic refusal. Operational tree failures remain outer errors;
/// they can never become stable semantic decisions. No file is read again
/// to collect this evidence. The caller retains its original pinned tree.
pub async fn load_decision(
    tree: &dyn Tree,
    layers: &[Layer<'_>],
    enabler: &str,
) -> Result<Decision> {
    let mut identity = blake3::Hasher::new_derive_key("acyclic.harness.plugins.v1");
    identity.update(&(enabler.len() as u64).to_be_bytes());
    identity.update(enabler.as_bytes());
    let composition =
        config::compose_with_evidence(MANIFEST, layers, tree, Value::Object(Default::default()))
            .await?;
    identity.update(&composition.identity());
    let result = match composition.into_result() {
        Ok((value, _)) => match load_enabled(tree, value, enabler, Some(&mut identity)).await {
            Ok(enabled) => Ok(enabled),
            Err(LoadFailure::Semantic(error)) => Err(error),
            Err(LoadFailure::Operational(error)) => return Err(error),
        },
        Err(error) => Err(error),
    };
    Ok(Decision {
        identity: identity.finalize().into(),
        result,
    })
}

// Classify by the original producer, never by an operational error's variant.
// A Tree is allowed to refuse a read with Error::Invalid too.
enum LoadFailure {
    Operational(Error),
    Semantic(Error),
}

impl From<Error> for LoadFailure {
    fn from(error: Error) -> Self {
        Self::Semantic(error)
    }
}

fn observe(identity: Option<&mut blake3::Hasher>, path: &str, bytes: Option<&[u8]>) {
    let Some(identity) = identity else { return };
    identity.update(&[0]);
    identity.update(&(path.len() as u64).to_be_bytes());
    identity.update(path.as_bytes());
    identity.update(&[u8::from(bytes.is_some())]);
    if let Some(bytes) = bytes {
        identity.update(blake3::hash(bytes).as_bytes());
    }
}

async fn read_hook(
    tree: &dyn Tree,
    identity: Option<&mut blake3::Hasher>,
    hook: String,
) -> std::result::Result<Option<(String, String)>, LoadFailure> {
    let bytes = tree.read(&hook).await.map_err(LoadFailure::Operational)?;
    observe(identity, &hook, bytes.as_deref());
    bytes
        .map(|bytes| {
            std::str::from_utf8(&bytes)
                .map(str::to_owned)
                .map_err(|_| Error::Invalid(format!("{MANIFEST}: {hook} is not UTF-8")))
        })
        .transpose()
        .map(|script| script.map(|script| (hook, script)))
        .map_err(Into::into)
}

async fn load_enabled(
    tree: &dyn Tree,
    value: Value,
    enabler: &str,
    mut identity: Option<&mut blake3::Hasher>,
) -> std::result::Result<Enabled, LoadFailure> {
    let invalid = |reason: String| Error::Invalid(format!("{MANIFEST}: {reason}"));
    let Plugins(entries) =
        serde_json::from_value(value).map_err(|error| invalid(error.to_string()))?;
    let enabled: BTreeMap<_, _> = entries
        .into_iter()
        .filter(|(_, entry)| entry.enabled)
        .collect();
    let mut loaded = Enabled::default();
    for (name, entry) in &enabled {
        if name.is_empty() || name.contains(['/', '\\']) || name.starts_with('.') {
            return Err(invalid(format!("{name:?} is not a plugin folder name")).into());
        }
        if entry.source.is_some() {
            return Err(invalid(format!(
                "{name}: plugins from another folder are not supported on this host yet"
            ))
            .into());
        }
        let root = format!("plugins/{name}/");
        let path = format!("{root}plugin.ts");
        let source = tree.read(&path).await.map_err(LoadFailure::Operational)?;
        let Some(source) = source else {
            observe(identity.as_deref_mut(), &path, None);
            return Err(invalid(format!("{name} is enabled but {path} does not exist")).into());
        };
        let manifest = config::evaluate_decision(&path, &source, tree, &Value::Null, Surface::Data)
            .await
            .map_err(LoadFailure::Operational)?;
        if let Some(identity) = identity.as_deref_mut() {
            identity.update(&[1]);
            identity.update(&manifest.identity());
        }
        let manifest = manifest.into_result()?;
        let manifest: PluginManifest = serde_json::from_value(manifest.value)
            .map_err(|error| invalid(format!("{path}: {error}")))?;
        if manifest.name != *name {
            return Err(invalid(format!("{path} names {:?}", manifest.name)).into());
        }
        if let Some(missing) = manifest
            .requires
            .iter()
            .find(|required| !enabled.contains_key(*required))
        {
            return Err(invalid(format!("{name} requires {missing}, which is not enabled")).into());
        }
        if let Some(after) = manifest.after.keys().next() {
            return Err(invalid(format!(
                "{root}hooks/after-{after}: after-hooks do not run on this host yet"
            ))
            .into());
        }
        let empty = Value::Object(Default::default());
        let settings = entry
            .config
            .as_ref()
            .or(manifest.defaults.as_ref())
            .unwrap_or(&empty);
        let settings = String::from_utf8(crate::contract::canonical_json_bytes(settings)?)
            .map_err(|error| invalid(error.to_string()))?;
        for event in Event::ALL {
            let path = |form: &str| format!("{root}hooks/before-{}.{form}", event.name());
            let (ts, sh, ps1) = (
                read_hook(tree, identity.as_deref_mut(), path("ts")).await?,
                read_hook(tree, identity.as_deref_mut(), path("sh")).await?,
                read_hook(tree, identity.as_deref_mut(), path("ps1")).await?,
            );
            if let Some((hook, _)) = &ts {
                return Err(invalid(format!(
                    "{hook}: TypeScript hooks do not run on this host yet"
                ))
                .into());
            }
            if sh.is_none() && ps1.is_none() {
                continue;
            }
            let Some(site) = event.site() else {
                return Err(invalid(format!(
                    "{root}hooks/before-{}: this host does not judge {} requests yet",
                    event.name(),
                    event.name()
                ))
                .into());
            };
            let hook = HookSpec {
                run: HookRun::Plugin {
                    instance: format!("{enabler}:{MANIFEST}#{name}:hooks/before-{}", event.name()),
                    sh: sh.map(|(_, script)| script),
                    ps1: ps1.map(|(_, script)| script),
                    config: settings.clone(),
                    matcher: manifest.matcher.get(&event).cloned(),
                },
                // A hook holds only its own request; it has no deadline.
                timeout_ms: u64::MAX,
                failure: HookFailure::Block,
            };
            loaded
                .rules
                .add_rules([HookRule {
                    on: site,
                    matcher: String::new(),
                    hooks: vec![hook],
                }])
                .map_err(|error| {
                    invalid(format!("{root}hooks/before-{}: {error}", event.name()))
                })?;
        }
        loaded.names.push(name.clone());
    }
    Ok(loaded)
}

#[cfg(test)]
#[path = "plugins/tests.rs"]
mod tests;
