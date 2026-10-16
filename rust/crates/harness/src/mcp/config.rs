//! Native client declarations, normalized before opening any connection.

use crate::{Error, Result};
use serde::Deserialize;
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

/// Host-owned connection configuration. Credentials are never serialized into task records.
#[derive(Default)]
pub struct McpConfig {
    pub(super) servers: BTreeMap<String, McpServer>,
}

/// One normalized server declaration; credentials remain host-owned.
pub struct McpServer {
    /// Selected connection implementation and parameters.
    pub transport: McpTransport,
    /// Finite initialization deadline.
    pub startup_timeout_ms: u32,
    /// Finite deadline for each tool exchange.
    pub tool_timeout_ms: u32,
    /// Optional allowlist of server-local tool names.
    pub enabled_tools: Option<Vec<String>>,
    /// Server-local tools excluded even when allowlisted.
    pub disabled_tools: Vec<String>,
    /// Whether connection failure prevents session startup.
    pub required: bool,
    pub(super) dialect: Dialect,
}

/// Finite native connection vocabulary, without an embedded command language.
pub enum McpTransport {
    /// One persistent child process.
    Stdio {
        /// Executable, passed directly to process creation.
        command: String,
        /// Separate argument values.
        args: Vec<String>,
        /// Explicit environment additions.
        env: BTreeMap<String, String>,
        /// Environment names to inherit from the host.
        env_vars: Vec<String>,
        /// Optional process working directory.
        cwd: Option<PathBuf>,
    },
    /// Streamable HTTP endpoint.
    Http {
        /// Endpoint URL.
        url: String,
        /// Explicit HTTP header additions.
        headers: BTreeMap<String, String>,
        /// Header names mapped to host environment variable names.
        env_headers: BTreeMap<String, String>,
        /// Host environment variable supplying the bearer token.
        bearer_token_env_var: Option<String>,
    },
}

#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Dialect {
    Claude,
    Codex,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ImportedServer {
    #[serde(rename = "type")]
    kind: Option<String>,
    command: Option<String>,
    args: Vec<String>,
    env: BTreeMap<String, String>,
    env_vars: Vec<String>,
    cwd: Option<PathBuf>,
    url: Option<String>,
    #[serde(alias = "http_headers")]
    headers: BTreeMap<String, String>,
    env_http_headers: BTreeMap<String, String>,
    bearer_token_env_var: Option<String>,
    startup_timeout_sec: Option<f64>,
    tool_timeout_sec: Option<f64>,
    enabled_tools: Option<Vec<String>>,
    disabled_tools: Vec<String>,
    required: bool,
    enabled: Option<bool>,
}

impl McpConfig {
    /// Enabled servers in stable namespace order, after whole-entry overrides.
    pub fn server_names(&self) -> impl Iterator<Item = &str> {
        self.servers.keys().map(String::as_str)
    }

    /// Enabled declarations in namespace order, with disabled entries removed.
    pub fn servers(&self) -> impl Iterator<Item = (&str, &McpServer)> {
        self.servers
            .iter()
            .map(|(name, server)| (name.as_str(), server))
    }

    pub(crate) fn insert(
        &mut self,
        name: String,
        imported: ImportedServer,
        dialect: Dialect,
    ) -> Result<()> {
        if !crate::is_valid_component_label(&name)
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(Error::Invalid("invalid MCP server name".into()));
        }
        if imported.enabled == Some(false) {
            self.servers.remove(&name);
        } else {
            self.servers.insert(name, imported.normalize(dialect)?);
        }
        Ok(())
    }
}

impl ImportedServer {
    fn normalize(self, dialect: Dialect) -> Result<McpServer> {
        let kind = self.kind.as_deref().unwrap_or(match dialect {
            Dialect::Codex if self.url.is_some() => "http",
            _ => "stdio",
        });
        let transport = match kind {
            "stdio"
                if self.url.is_none()
                    && self.headers.is_empty()
                    && self.env_http_headers.is_empty()
                    && self.bearer_token_env_var.is_none() =>
            {
                let command = self
                    .command
                    .filter(|command| !command.trim().is_empty() && !command.contains('\0'))
                    .ok_or_else(|| Error::Invalid("MCP stdio needs a command".into()))?;
                if self.args.iter().any(|argument| argument.contains('\0'))
                    || self
                        .env
                        .keys()
                        .chain(self.env_vars.iter())
                        .any(|name| name.is_empty() || name.contains(['=', '\0']))
                    || self.env.values().any(|value| value.contains('\0'))
                {
                    return Err(Error::Invalid(
                        "invalid MCP process arguments or environment".into(),
                    ));
                }
                McpTransport::Stdio {
                    command,
                    args: self.args,
                    env: self.env,
                    env_vars: self.env_vars,
                    cwd: self.cwd,
                }
            }
            "http" | "streamable-http"
                if self.command.is_none()
                    && self.args.is_empty()
                    && self.env.is_empty()
                    && self.env_vars.is_empty()
                    && self.cwd.is_none() =>
            {
                let url = self
                    .url
                    .filter(|url| !url.trim().is_empty())
                    .ok_or_else(|| Error::Invalid("MCP HTTP needs a URL".into()))?;
                McpTransport::Http {
                    url,
                    headers: self.headers,
                    env_headers: self.env_http_headers,
                    bearer_token_env_var: self.bearer_token_env_var,
                }
            }
            "stdio" | "http" | "streamable-http" => {
                return Err(Error::Invalid("conflicting MCP transport fields".into()));
            }
            _ => {
                return Err(Error::Unsupported(
                    "MCP transport must be stdio or HTTP".into(),
                ));
            }
        };
        Ok(McpServer {
            transport,
            startup_timeout_ms: milliseconds(self.startup_timeout_sec.unwrap_or(10.0))?,
            tool_timeout_ms: milliseconds(self.tool_timeout_sec.unwrap_or(60.0))?,
            enabled_tools: self.enabled_tools,
            disabled_tools: self.disabled_tools,
            required: self.required,
            dialect,
        })
    }
}

fn milliseconds(seconds: f64) -> Result<u32> {
    let invalid = || Error::Invalid("MCP timeout must be positive whole milliseconds".into());
    let duration = Duration::try_from_secs_f64(seconds).map_err(|_| invalid())?;
    let milliseconds = u32::try_from(duration.as_millis()).map_err(|_| invalid())?;
    if milliseconds == 0 || !duration.subsec_nanos().is_multiple_of(1_000_000) {
        return Err(invalid());
    }
    Ok(milliseconds)
}

#[cfg(test)]
mod tests;
