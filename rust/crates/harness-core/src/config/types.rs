//! Settings shapes decoded from evaluated modules, and the records the
//! kernel persists about them. The seed's `sys/types/config.d.ts` is
//! generated from these definitions ([`super::typescript`]), so the authoring
//! types and the authoritative decode cannot drift.
use serde::{Deserialize, Serialize};
use ts_rs::TS;

/// `.volumes/home/agent.ts`: the agent's model and how it is called. You may
/// change your own; `model` must be one your permissions allow (sys/models/),
/// otherwise its `Admission` is "rejected" with the reason.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentConfig {
    /// "<provider>/<model>", one of the models in sys/models/.
    pub model: String,
    /// Reasoning effort; null or absent leaves the model's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub effort: Option<Effort>,
    /// Sampling for every model call; an absent field keeps the provider's.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub sampling: Option<Sampling>,
    /// Run inside another agent host instead of the built-in loop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub host: Option<Host>,
    /// Replay the recorded run at this path instead of calling a live model.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional = nullable)]
    pub replay: Option<String>,
}

/// Reasoning effort.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum Effort {
    /// Least reasoning.
    Low,
    /// The middle setting.
    Medium,
    /// Most reasoning.
    High,
}

/// Per-call sampling.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Sampling {
    /// Nonnegative temperature in the provider's units.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub temperature: Option<f64>,
    /// Probability mass in (0, 1].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub top_p: Option<f64>,
    /// Most output tokens per call; positive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub max_output_tokens: Option<u32>,
}

/// The loop an agent runs in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "kebab-case")]
pub enum Host {
    /// The harness's own loop.
    Builtin,
    /// Claude Code.
    ClaudeCode,
    /// Codex.
    Codex,
    /// Pi.
    Pi,
}

/// `policy.ts`: plain data, `export default { deny: [...] }`, never a
/// function of the defaults. One you wrote yourself is advisory; one your
/// issuer wrote (through spawn or fork `files`) binds you and is read-only
/// to you. Every policy over you is met: everything is allowed unless a
/// rule here, or one set above you, denies it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    /// Denied actions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub deny: Option<Vec<PolicyRule>>,
    /// Restrict to only these: anything no `only` rule matches is denied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub only: Option<Vec<PolicyRule>>,
}

/// One policy rule. `path` is in the subject's own tree; for "spawn" it
/// matches child names, for "call" actions or tool paths.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
#[ts(rename = "Rule")]
pub struct PolicyRule {
    /// A path pattern.
    pub path: String,
    /// The actions it covers.
    pub actions: Vec<Action>,
}

/// An action a policy rule covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    /// Reading a file.
    Read,
    /// Writing a file.
    Write,
    /// Appending to a log.
    Append,
    /// Creating a child.
    Spawn,
    /// Calling an action or tool.
    Call,
}

/// `live/agents/self/admissions/<version>.json`: what happened to one write
/// of a settings file, where <version> is the version the write produced.
/// "invalid" and "rejected" leave the previous good settings in force.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct Admission {
    /// The settings file.
    pub path: String,
    /// The file version admitted.
    pub version: String,
    /// The outcome.
    pub status: AdmissionStatus,
    /// Why it was not applied.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[ts(optional)]
    pub error: Option<String>,
    /// When, as UTC RFC 3339.
    pub at: String,
}

/// The outcome of one settings write.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum AdmissionStatus {
    /// In force.
    Applied,
    /// The module threw, ran out of fuel or returned something that is not
    /// the expected shape.
    Invalid,
    /// Well formed, but not allowed or not available.
    Rejected,
}

/// One layer's contribution to a composed settings file; the chain, lowest
/// first, is `<path>.why.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Link {
    /// The layer, such as "seed" or "home".
    pub layer: String,
    /// The pinned generation the module was read at.
    pub generation: String,
    /// blake3 of the module and everything it imports.
    pub module_digest: String,
    /// Whether the module replaced the value below or was a function of it.
    pub form: Form,
}

/// How a module's default export combined with the value below it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(rename_all = "lowercase")]
pub enum Form {
    /// A value, replacing the one below.
    Value,
    /// A function called with the value below.
    Function,
}

/// `millis` since the Unix epoch as UTC RFC 3339 with milliseconds.
#[must_use]
pub fn utc(millis: u64) -> String {
    let (days, rest) = (millis / 86_400_000, millis % 86_400_000);
    // Civil date from days since 1970-01-01 (Hinnant's algorithm).
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + u64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
        rest / 3_600_000,
        rest / 60_000 % 60,
        rest / 1000 % 60,
        rest % 1000
    )
}
