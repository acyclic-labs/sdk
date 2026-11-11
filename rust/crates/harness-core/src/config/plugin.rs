//! Plugin settings records, shared by admission, authoring and hook selection.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use ts_rs::TS;

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
}

/// A plugin hook's `matcher` for one event: every named field must match.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout, TS)]
#[serde(default, deny_unknown_fields)]
pub struct RequestMatch {
    /// Normalized actions (`exec`, `read`, `write`, `edit`, `search`, `web`,
    /// `mcp`, `other`); empty matches every action.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub action: Vec<String>,
    /// A regular expression over an `exec` request's command.
    #[ts(optional)]
    pub command: Option<String>,
    /// A regular expression over a `read`, `write` or `edit` request's path.
    #[ts(optional)]
    pub path: Option<String>,
}
