//! Configuration is code. A settings file is a TypeScript module whose
//! default export is a value, replacing the result of the layers below it,
//! or a function of that result ([`Form`]). A function nested in the result
//! (a plugin entry, or its `config`) is likewise called with the base at its
//! own path, so `Config<T>` composes at every level. The kernel folds a settings
//! entry point's modules from the lowest layer to the highest
//! ([`compose`]); every other module replaces by path.
//!
//! Evaluation ([`evaluate`]) is pure and bounded: TypeScript is stripped,
//! the module and its static imports from the effective tree and `sys/` run
//! in QuickJS with no clock, randomness, I/O or tools, under a deterministic
//! instruction budget, and the result must be plain data. A result or a
//! failure is cached under the digest of everything it read. The most recent
//! evaluation is reused without retaining a history of settings stacks.
pub mod policy;
mod types;

pub use types::{
    Action, Admission, AdmissionStatus, AgentConfig, Effort, Form, Host, Link, Policy, PolicyRule,
    Sampling, utc,
};

#[cfg(not(target_arch = "wasm32"))]
mod evaluate;
#[cfg(not(target_arch = "wasm32"))]
pub use evaluate::{Evaluated, Surface, Tree, compose, evaluate};

/// Evaluates and decodes a `policy.ts` holding `source`: plain data, so a
/// function export is invalid with its own error.
///
/// # Errors
/// [`crate::Error::Invalid`] naming why the module is not a policy.
#[cfg(not(target_arch = "wasm32"))]
pub async fn admit_policy(source: &[u8], tree: &dyn Tree) -> crate::Result<Policy> {
    let evaluated = evaluate(
        POLICY,
        source,
        tree,
        &serde_json::Value::Null,
        Surface::Data,
    )
    .await?;
    policy::decode(evaluated.value)
}

/// The agent's settings entry point.
pub const AGENT: &str = "agent.ts";
/// The agent's own policy: plain data, intersected by the kernel.
pub const POLICY: &str = "policy.ts";

/// Where the generated declarations sit in the seed.
pub const TYPES_PATH: &str = "sys/types/config.d.ts";

/// Whether `path` (relative to the effective tree) is a settings entry point
/// whose layers compose: `agent.ts`, `plugins.ts` and a provider plugin's
/// `provider.ts`. Every other module replaces by path.
#[must_use]
pub fn composes(path: &str) -> bool {
    path == AGENT
        || path == "plugins.ts"
        || path
            .strip_prefix("plugins/")
            .and_then(|rest| rest.strip_suffix("/provider.ts"))
            .is_some_and(|name| !name.is_empty() && !name.contains('/'))
}

/// `sys/types/config.d.ts`: the settings and record declarations, generated
/// from the kernel's own decode types.
#[must_use]
pub fn typescript() -> &'static str {
    static TEXT: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    TEXT.get_or_init(|| {
        use ts_rs::TS;
        let config = ts_rs::Config::default();
        let mut text = String::from(
            "// Generated from the harness's own types. Read-only.\n\n\
             /** A settings file's default export: a value, replacing the one below, or a function of it. */\n\
             export type Config<T> = T | ((base: T) => T);\n",
        );
        macro_rules! declarations { ($($ty:ty),+ $(,)?) => {$(
            text.push_str("\nexport ");
            text.push_str(&<$ty as TS>::decl(&config));
            text.push('\n');
        )+}; }
        declarations!(
            AgentConfig,
            Effort,
            Sampling,
            Host,
            Policy,
            PolicyRule,
            Action,
            Admission,
            AdmissionStatus,
            Link,
            Form
        );
        #[cfg(all(feature = "hooks", not(target_arch = "wasm32")))]
        declarations!(
            crate::plugins::Plugins,
            crate::plugins::PluginEntry,
            crate::plugins::PluginSource,
            crate::plugins::PluginManifest,
            crate::plugins::AfterSource,
            crate::plugins::Start,
            crate::plugins::Event,
            crate::hooks::RequestMatch
        );
        text
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_settings_entry_points_compose() {
        for path in [AGENT, "plugins.ts", "plugins/anthropic/provider.ts"] {
            assert!(composes(path), "{path}");
        }
        for path in [
            POLICY,
            "context.ts",
            "tools/x.ts",
            "plugins/provider.ts",
            "plugins/a/b/provider.ts",
            "plugins/a/watches/x.ts",
            "home/agent.ts",
        ] {
            assert!(!composes(path), "{path}");
        }
    }

    #[test]
    fn declarations_name_every_record_and_its_fields() {
        let text = typescript();
        for needle in [
            "export type Config<T>",
            "type AgentConfig",
            "effort?: Effort | null",
            "topP?: number",
            "\"claude-code\"",
            "type Rule",
            "type Admission",
            "moduleDigest: string",
        ] {
            assert!(text.contains(needle), "{needle} missing from:\n{text}");
        }
    }

    #[test]
    fn utc_formats_civil_time() {
        assert_eq!(utc(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(utc(951_782_400_123), "2000-02-29T00:00:00.123Z");
        assert_eq!(utc(1_793_577_600_000), "2026-11-02T00:00:00.000Z");
    }
}
