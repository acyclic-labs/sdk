//! The private `CODEX_HOME` each turn runs in.
//!
//! Every key here is accepted by `codex exec --strict-config` on 0.155.1; the
//! probe config it was checked against is `fixtures/codex-0.155.1/probe-config.toml`.

use std::{io, path::Path};
use toml::{Table, Value};

/// The model provider id Codex is pointed at. Only this crate's proxy serves it.
pub const PROVIDER_ID: &str = "acyclic";

/// The MCP server id our tools appear under, so Codex names them `acyclic.<tool>`.
pub const MCP_SERVER_ID: &str = "acyclic";

/// Environment variable carrying the dummy key Codex sends to the proxy.
/// The real upstream key never enters Codex's environment.
pub const PROXY_KEY_ENV: &str = "ACYCLIC_CODEX_PROXY_KEY";

/// Environment variable carrying the bearer token Codex sends to the MCP endpoint.
pub const MCP_TOKEN_ENV: &str = "ACYCLIC_CODEX_MCP_TOKEN";

/// What goes into `CODEX_HOME/config.toml` for one turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HomeConfig {
    /// Upstream model id, e.g. `gpt-5.5`.
    pub model: String,
    /// The proxy's base URL, ending in `/v1`.
    pub proxy_base_url: String,
    /// The MCP endpoint URL, or `None` when the turn exposes no tools.
    pub mcp_url: Option<String>,
    /// Seconds Codex waits on one of our tool calls.
    pub tool_timeout_sec: u64,
    /// Whether Codex may split work with its own subagents.
    pub subagents: bool,
    /// HTTP retries Codex makes per model call. Upstream retries are the proxy's job.
    pub request_max_retries: u32,
    /// Stream reconnects Codex makes per model call.
    pub stream_max_retries: u32,
}

impl HomeConfig {
    /// Renders `config.toml`.
    #[must_use]
    pub fn render(&self) -> String {
        let mut root = Table::new();
        root.insert("model".into(), self.model.clone().into());
        root.insert("model_provider".into(), PROVIDER_ID.into());
        // The sandbox box is the security boundary; Codex must never stop to ask.
        root.insert("approval_policy".into(), "never".into());
        root.insert("sandbox_mode".into(), "danger-full-access".into());
        // Web access goes through our metered acyclic.web tool, not Codex's own.
        root.insert("web_search".into(), "disabled".into());
        root.insert("check_for_update_on_startup".into(), false.into());
        root.insert("analytics".into(), table([("enabled", false.into())]));
        root.insert("feedback".into(), table([("enabled", false.into())]));
        root.insert(
            "features".into(),
            table([
                ("multi_agent", self.subagents.into()),
                // Without this Codex calls github.com and chatgpt.com at startup.
                ("plugins", false.into()),
                ("goals", false.into()),
            ]),
        );
        root.insert(
            "model_providers".into(),
            table([(
                PROVIDER_ID,
                table([
                    ("name", "acyclic metered proxy".into()),
                    ("base_url", self.proxy_base_url.clone().into()),
                    ("wire_api", "responses".into()),
                    ("env_key", PROXY_KEY_ENV.into()),
                    (
                        "request_max_retries",
                        i64::from(self.request_max_retries).into(),
                    ),
                    (
                        "stream_max_retries",
                        i64::from(self.stream_max_retries).into(),
                    ),
                ]),
            )]),
        );
        if let Some(url) = &self.mcp_url {
            root.insert(
                "mcp_servers".into(),
                table([(
                    MCP_SERVER_ID,
                    table([
                        ("url", url.clone().into()),
                        ("bearer_token_env_var", MCP_TOKEN_ENV.into()),
                        // A required server that fails stops Codex before the turn,
                        // instead of running silently without our tools.
                        ("required", true.into()),
                        // 0.155.1 hides MCP tools behind its tool_search tool by
                        // default; ours must be in the model's tool list.
                        (
                            "omit_tools_from",
                            Value::Array(vec!["deferred".into(), "code_mode".into()]),
                        ),
                        ("startup_timeout_sec", 10.into()),
                        (
                            "tool_timeout_sec",
                            i64::try_from(self.tool_timeout_sec)
                                .unwrap_or(i64::MAX)
                                .into(),
                        ),
                    ]),
                )]),
            );
        }
        root.to_string()
    }

    /// Writes `config.toml` and `AGENTS.md` into a fresh `CODEX_HOME`.
    ///
    /// # Errors
    /// When either file cannot be written.
    pub fn write(&self, home: &Path, instructions: &str) -> io::Result<()> {
        std::fs::write(home.join("config.toml"), self.render())?;
        std::fs::write(home.join("AGENTS.md"), instructions)
    }
}

fn table<const N: usize>(entries: [(&str, Value); N]) -> Value {
    Value::Table(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_owned(), value))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> HomeConfig {
        HomeConfig {
            model: "gpt-5.5".into(),
            proxy_base_url: "http://127.0.0.1:4000/v1".into(),
            mcp_url: Some("http://127.0.0.1:4000/mcp".into()),
            tool_timeout_sec: 300,
            subagents: true,
            request_max_retries: 2,
            stream_max_retries: 2,
        }
    }

    #[test]
    fn the_rendered_config_is_pinned() {
        let expected = r#"approval_policy = "never"
check_for_update_on_startup = false
model = "gpt-5.5"
model_provider = "acyclic"
sandbox_mode = "danger-full-access"
web_search = "disabled"

[analytics]
enabled = false

[features]
goals = false
multi_agent = true
plugins = false

[feedback]
enabled = false

[mcp_servers.acyclic]
bearer_token_env_var = "ACYCLIC_CODEX_MCP_TOKEN"
omit_tools_from = ["deferred", "code_mode"]
required = true
startup_timeout_sec = 10
tool_timeout_sec = 300
url = "http://127.0.0.1:4000/mcp"

[model_providers.acyclic]
base_url = "http://127.0.0.1:4000/v1"
env_key = "ACYCLIC_CODEX_PROXY_KEY"
name = "acyclic metered proxy"
request_max_retries = 2
stream_max_retries = 2
wire_api = "responses"
"#;
        assert_eq!(config().render(), expected);
    }

    #[test]
    fn no_tools_means_no_mcp_server() {
        let rendered = HomeConfig {
            mcp_url: None,
            ..config()
        }
        .render();
        assert!(!rendered.contains("mcp_servers"));
    }

    #[test]
    fn every_rendered_key_was_accepted_by_the_probe() -> Result<(), toml::de::Error> {
        // The probe config passed `--strict-config` on the pinned Codex. Any key
        // we render must appear there too, or the e2e gate is the first to know.
        let probe: Table = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/codex-0.155.1/probe-config.toml"
        ))
        .parse()?;
        let ours: Table = config().render().parse()?;
        assert_keys_within(&ours, &probe, "");
        Ok(())
    }

    fn assert_keys_within(ours: &Table, probe: &Table, path: &str) {
        for (key, value) in ours {
            // Provider and MCP server ids are names, not schema keys.
            let probe_value = match path {
                "model_providers" | "mcp_servers" => probe.values().next(),
                _ => probe.get(key),
            };
            let Some(probe_value) = probe_value else {
                panic!("{path}.{key} is not in the strict-config probe");
            };
            if let (Value::Table(ours), Value::Table(probe)) = (value, probe_value) {
                assert_keys_within(ours, probe, key);
            }
        }
    }
}
