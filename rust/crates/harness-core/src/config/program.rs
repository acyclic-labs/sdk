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
    /// A hook's source selector. Tools omit this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub on: Option<ProgramTrigger>,
}

/// The existing hook runner's lifecycle and durable source events.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, TS, crate::HeapLayout)]
pub enum HookEvent {
    /// A session was opened, resumed, forked, or compacted.
    SessionStart,
    /// A user prompt is about to reach the model.
    UserPromptSubmit,
    /// A tool is about to execute.
    PreToolUse,
    /// A tool returned successfully.
    PostToolUse,
    /// The model is about to finish its turn.
    Stop,
    /// A committed volume publication changed a selected path.
    FileChanged,
    /// A followed stream published a selected record.
    StreamRecord,
}

/// A hook follows a source through the source's existing event mechanism.
/// The selector grants neither read access to that source nor execution rights.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ProgramTrigger {
    /// An existing lifecycle boundary.
    Lifecycle {
        /// The lifecycle event; source events require their own selector.
        event: HookEvent,
    },
    /// Changes below a normalized root-relative path prefix in a volume.
    File {
        /// The caller-visible volume address.
        volume: String,
        /// Empty selects the root; otherwise a complete path component prefix.
        prefix: String,
        /// Existing changes or only changes after admission.
        from: super::Start,
    },
    /// Records on one exact stream path.
    Stream {
        /// The source stream path, independently authorized at follow admission.
        path: String,
        /// Existing records or only records after admission.
        from: super::Start,
    },
}

impl ProgramTrigger {
    pub(crate) fn owned_bytes(&self) -> crate::Result<u64> {
        match self {
            Self::Lifecycle { .. } => Ok(0),
            Self::File { volume, prefix, .. } => {
                crate::core::owned_sum([volume.capacity() as u64, prefix.capacity() as u64])
            }
            Self::Stream { path, .. } => Ok(path.capacity() as u64),
        }
    }

    /// The one runner event delivered by this selector.
    #[must_use]
    pub const fn event(&self) -> HookEvent {
        match self {
            Self::Lifecycle { event } => *event,
            Self::File { .. } => HookEvent::FileChanged,
            Self::Stream { .. } => HookEvent::StreamRecord,
        }
    }

    /// Checks selector syntax. The source is independently resolved and
    /// authorized by its host before subscribing; this grants no authority.
    pub fn validate(&self) -> crate::Result<()> {
        match self {
            Self::Lifecycle {
                event: HookEvent::FileChanged | HookEvent::StreamRecord,
            } => Err(crate::Error::Invalid(
                "source events require a file or stream selector".into(),
            )),
            Self::Lifecycle { .. } => Ok(()),
            Self::File { volume, prefix, .. } => {
                if volume.is_empty() || volume.trim() != volume || volume.contains('\0') {
                    return Err(crate::Error::Invalid(
                        "hook volume address is invalid".into(),
                    ));
                }
                if !prefix.is_empty() {
                    crate::conversation::validate_content_path(prefix, &mut None)?;
                }
                Ok(())
            }
            Self::Stream { path, .. } => acyclic_stream::StreamPath::validate(path)
                .map_err(|_| crate::Error::Invalid("hook stream path is invalid".into())),
        }
    }
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
