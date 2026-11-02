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
//! so the last admitted set stays and the admission names the hook; an
//! enabled hook is never silently skipped. The authoring types are generated
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
    let invalid = |reason: String| Error::Invalid(format!("{MANIFEST}: {reason}"));
    let (value, _) =
        config::compose(MANIFEST, layers, tree, Value::Object(Default::default())).await?;
    let Plugins(entries) =
        serde_json::from_value(value).map_err(|error| invalid(error.to_string()))?;
    let enabled: BTreeMap<_, _> = entries
        .into_iter()
        .filter(|(_, entry)| entry.enabled)
        .collect();
    let mut loaded = Enabled::default();
    for (name, entry) in &enabled {
        if name.is_empty() || name.contains(['/', '\\']) || name.starts_with('.') {
            return Err(invalid(format!("{name:?} is not a plugin folder name")));
        }
        if entry.source.is_some() {
            return Err(invalid(format!(
                "{name}: plugins from another folder are not supported on this host yet"
            )));
        }
        let root = format!("plugins/{name}/");
        let path = format!("{root}plugin.ts");
        let source = tree
            .read(&path)
            .await?
            .ok_or_else(|| invalid(format!("{name} is enabled but {path} does not exist")))?;
        let manifest = config::evaluate(&path, &source, tree, &Value::Null, Surface::Data).await?;
        let manifest: PluginManifest = serde_json::from_value(manifest.value)
            .map_err(|error| invalid(format!("{path}: {error}")))?;
        if manifest.name != *name {
            return Err(invalid(format!("{path} names {:?}", manifest.name)));
        }
        if let Some(missing) = manifest
            .requires
            .iter()
            .find(|required| !enabled.contains_key(*required))
        {
            return Err(invalid(format!(
                "{name} requires {missing}, which is not enabled"
            )));
        }
        if let Some(after) = manifest.after.keys().next() {
            return Err(invalid(format!(
                "{root}hooks/after-{after}: after-hooks do not run on this host yet"
            )));
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
            let read = |form: &str| {
                let hook = format!("{root}hooks/before-{}.{form}", event.name());
                async move {
                    let bytes = tree.read(&hook).await?;
                    bytes
                        .map(|bytes| {
                            String::from_utf8(bytes)
                                .map_err(|_| invalid(format!("{hook} is not UTF-8")))
                        })
                        .transpose()
                        .map(|script| script.map(|script| (hook, script)))
                }
            };
            let (ts, sh, ps1) = (read("ts").await?, read("sh").await?, read("ps1").await?);
            if let Some((hook, _)) = &ts {
                return Err(invalid(format!(
                    "{hook}: TypeScript hooks do not run on this host yet"
                )));
            }
            if sh.is_none() && ps1.is_none() {
                continue;
            }
            let Some(site) = event.site() else {
                return Err(invalid(format!(
                    "{root}hooks/before-{}: this host does not judge {} requests yet",
                    event.name(),
                    event.name()
                )));
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
                timeout_ms: None,
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
