//! One authoring contract for programs used as tools and hooks.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ts_rs::TS;

/// `tools/<name>/program.ts` or `hooks/<name>/program.ts`. Loading a
/// manifest grants no execution authority: the caller must still admit the
/// invocation through the ordinary tool or hook execution boundary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS, crate::HeapLayout)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProgramManifest {
    /// Persisted authoring format; currently 1.
    pub version: u32,
    /// The containing directory's name.
    pub name: String,
    /// Loaded on demand, rather than included in a name-only listing.
    pub description: String,
    /// The ordinary tool argument schema, also used for hook event input.
    #[ts(type = "unknown")]
    pub input_schema: Value,
    /// Entry files relative to this program's directory.
    pub entry: ProgramEntry,
}

/// Native forms of the same program. An absent native form refuses execution;
/// it never selects another operating system's interpreter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub struct ProgramEntry {
    /// POSIX shell entry file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub sh: Option<String>,
    /// PowerShell entry file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub ps1: Option<String>,
}

impl crate::filesystem::PreparationRecord for ProgramManifest {}
