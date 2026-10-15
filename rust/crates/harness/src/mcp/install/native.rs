use super::*;
use crate::{
    OperationId,
    mcp::{McpTransport, config::Dialect},
};
use std::collections::BTreeSet;

enum Connection {
    #[cfg(feature = "mcp-stdio")]
    Stdio(super::super::stdio::StdioMcpTransport),
    #[cfg(feature = "mcp-http")]
    Http(super::super::http::HttpMcpTransport),
}

pub(super) async fn connect(
    server: &McpServer,
    workspace: &Path,
    limits: McpClientLimits,
) -> Result<Connected> {
    let (connection, binding) = match &server.transport {
        #[cfg(feature = "mcp-stdio")]
        McpTransport::Stdio { .. } => {
            let child = command_for(server, workspace, |name| std::env::var_os(name))?;
            let binding = command_binding(&child, server);
            (
                Connection::Stdio(
                    super::super::stdio::StdioMcpTransport::spawn(
                        child,
                        limits.catalog_bytes,
                        server.startup_timeout_ms,
                    )
                    .await?,
                ),
                binding,
            )
        }
        #[cfg(feature = "mcp-http")]
        McpTransport::Http {
            url,
            headers,
            env_headers,
            bearer_token_env_var,
        } => {
            use super::super::http::{HttpMcpTransport, McpHttpProvider, NativeMcpHttpProvider};
            let environment = |name: &str| std::env::var_os(name);
            let headers = headers_for(
                headers,
                env_headers,
                bearer_token_env_var.as_deref(),
                server.dialect == Dialect::Claude,
                environment,
            )?;
            let url = expand(url, server.dialect == Dialect::Claude, environment)?;
            let binding = http_binding(&url, &headers);
            let provider = Arc::new(NativeMcpHttpProvider::new(headers)?);
            let transport = HttpMcpTransport::new(
                provider.clone(),
                url.into_owned(),
                None,
                limits.catalog_bytes,
                server.startup_timeout_ms,
                None,
            )?;
            let operation = OperationId::new();
            let request = transport.initialization_request(
                operation,
                "acyclic-harness",
                env!("CARGO_PKG_VERSION"),
            )?;
            let response = provider.exchange(operation, request).await?;
            let (transport, _, initialized) =
                transport.accept_initialization(operation, response).await?;
            let response = provider.exchange(OperationId::new(), initialized).await?;
            if !matches!(response.status, 202 | 204) {
                return Err(Error::Invalid(
                    "MCP initialized notification was rejected".into(),
                ));
            }
            (Connection::Http(transport), binding)
        }
        #[cfg(not(all(feature = "mcp-stdio", feature = "mcp-http")))]
        _ => {
            return Err(Error::Unsupported(
                "configured MCP transport feature is disabled".into(),
            ));
        }
    };
    let tools = discover(limits, |cursor| connection.list(cursor)).await?;
    let transport = match connection {
        #[cfg(feature = "mcp-stdio")]
        Connection::Stdio(transport) => {
            Arc::new(transport.with_tool_limits(limits.tool_bytes, server.tool_timeout_ms)?)
                as Arc<dyn McpToolTransport>
        }
        #[cfg(feature = "mcp-http")]
        Connection::Http(transport) => {
            Arc::new(transport.with_tool_limits(limits.tool_bytes, server.tool_timeout_ms)?)
                as Arc<dyn McpToolTransport>
        }
    };
    Ok(Connected {
        transport,
        tools,
        binding,
    })
}

fn binding_hasher(kind: &[u8]) -> blake3::Hasher {
    let mut hasher = blake3::Hasher::new_derive_key("acyclic-harness/mcp-binding/v1");
    hash_part(&mut hasher, super::super::PROTOCOL_VERSION.as_bytes());
    hash_part(&mut hasher, kind);
    hasher
}

fn hash_part(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(feature = "mcp-stdio")]
pub(super) fn command_binding(command: &std::process::Command, server: &McpServer) -> [u8; 32] {
    let McpTransport::Stdio { env, env_vars, .. } = &server.transport else {
        unreachable!("stdio binding")
    };
    let explicit: BTreeSet<_> = env.keys().chain(env_vars).map(String::as_str).collect();
    let mut hasher = binding_hasher(b"stdio");
    hash_part(
        &mut hasher,
        if server.dialect == Dialect::Claude {
            b"inherit"
        } else {
            b"clear"
        },
    );
    hash_part(&mut hasher, command.get_program().as_encoded_bytes());
    hash_part(
        &mut hasher,
        &(command.get_args().len() as u64).to_le_bytes(),
    );
    for argument in command.get_args() {
        hash_part(&mut hasher, b"argument");
        hash_part(&mut hasher, argument.as_encoded_bytes());
    }
    hash_part(&mut hasher, b"environment");
    for (name, value) in command.get_envs() {
        // Ambient environment is deliberately not part of a restart-stable identity.
        if !name.to_str().is_some_and(|name| explicit.contains(name)) {
            continue;
        }
        hash_part(&mut hasher, name.as_encoded_bytes());
        hash_part(
            &mut hasher,
            if value.is_some() { b"set" } else { b"remove" },
        );
        if let Some(value) = value {
            hash_part(&mut hasher, value.as_encoded_bytes());
        }
    }
    hash_part(&mut hasher, b"cwd");
    if let Some(cwd) = command.get_current_dir() {
        hash_part(&mut hasher, cwd.as_os_str().as_encoded_bytes());
    }
    *hasher.finalize().as_bytes()
}

#[cfg(feature = "mcp-http")]
pub(super) fn http_binding(url: &str, headers: &reqwest::header::HeaderMap) -> [u8; 32] {
    let mut hasher = binding_hasher(b"http");
    hash_part(&mut hasher, url.as_bytes());
    let mut headers: Vec<_> = headers.iter().collect();
    headers.sort_unstable_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
    for (name, value) in headers {
        hash_part(&mut hasher, name.as_str().as_bytes());
        hash_part(&mut hasher, value.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

impl Connection {
    async fn list(&self, cursor: Option<String>) -> Result<rmcp::model::ListToolsResult> {
        match self {
            #[cfg(feature = "mcp-stdio")]
            Self::Stdio(transport) => transport.list_tools(OperationId::new(), cursor).await,
            #[cfg(feature = "mcp-http")]
            Self::Http(transport) => transport.list_tools(OperationId::new(), cursor).await,
        }
    }
}

pub(super) async fn discover<F, Fut>(
    limits: McpClientLimits,
    list: F,
) -> Result<Vec<McpToolDefinition>>
where
    F: Fn(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<rmcp::model::ListToolsResult>>,
{
    let mut cursor = None;
    let mut seen = BTreeSet::new();
    let mut tools = Vec::new();
    let mut remaining = u64::from(limits.catalog_bytes);
    for _ in 0..limits.pages_per_server {
        let page = list(cursor).await?;
        if tools.len() as u64 + page.tools.len() as u64 > u64::from(limits.tools_per_server) {
            return Err(Error::Invalid("MCP discovery exceeds tool bound".into()));
        }
        let bytes = crate::executor::json::json_byte_length(&page, remaining, &mut None)?;
        remaining = remaining
            .checked_sub(bytes)
            .ok_or_else(|| Error::Invalid("MCP discovery exceeds byte bound".into()))?;
        tools.extend(page.tools.into_iter().map(McpToolDefinition::from));
        cursor = page.next_cursor;
        let Some(next) = &cursor else {
            return Ok(tools);
        };
        if next.is_empty() || !seen.insert(next.clone()) {
            return Err(Error::Invalid(
                "MCP discovery repeated an invalid cursor".into(),
            ));
        }
    }
    Err(Error::Invalid("MCP discovery exceeds page bound".into()))
}

#[cfg(feature = "mcp-http")]
pub(super) fn headers_for(
    headers: &std::collections::BTreeMap<String, String>,
    env_headers: &std::collections::BTreeMap<String, String>,
    bearer: Option<&str>,
    expand_variables: bool,
    environment: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<reqwest::header::HeaderMap> {
    let invalid = || Error::Invalid("invalid MCP HTTP header configuration".into());
    let mut result = reqwest::header::HeaderMap::new();
    for (name, value) in headers {
        let value = expand(value, expand_variables, &environment)?;
        let mut value = reqwest::header::HeaderValue::from_str(&value).map_err(|_| invalid())?;
        value.set_sensitive(true);
        result.insert(
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|_| invalid())?,
            value,
        );
    }
    for (name, variable) in env_headers {
        let value = environment(variable)
            .and_then(|value| value.into_string().ok())
            .ok_or_else(|| Error::Invalid("MCP HTTP credential environment is missing".into()))?;
        let mut value = reqwest::header::HeaderValue::from_str(&value).map_err(|_| invalid())?;
        value.set_sensitive(true);
        result.insert(
            reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|_| invalid())?,
            value,
        );
    }
    if let Some(variable) = bearer {
        let value = environment(variable)
            .and_then(|value| value.into_string().ok())
            .ok_or_else(|| Error::Invalid("MCP bearer credential environment is missing".into()))?;
        if value.trim().is_empty() || result.contains_key(reqwest::header::AUTHORIZATION) {
            return Err(invalid());
        }
        let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {value}"))
            .map_err(|_| invalid())?;
        value.set_sensitive(true);
        result.insert(reqwest::header::AUTHORIZATION, value);
    }
    Ok(result)
}

#[cfg(feature = "mcp-stdio")]
pub(super) fn command_for(
    server: &McpServer,
    workspace: &Path,
    environment: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<std::process::Command> {
    let McpTransport::Stdio {
        command,
        args,
        env,
        env_vars,
        cwd,
    } = &server.transport
    else {
        return Err(Error::Invalid("expected MCP stdio configuration".into()));
    };
    let expand_variables = server.dialect == Dialect::Claude;
    let command = expand(command, expand_variables, &environment)?;
    let cwd = cwd
        .as_ref()
        .map_or_else(|| workspace.to_owned(), |cwd| workspace.join(cwd));
    let configured_path = env
        .iter()
        .find(|(name, _)| {
            if cfg!(windows) {
                name.eq_ignore_ascii_case("PATH")
            } else {
                name.as_str() == "PATH"
            }
        })
        .map(|(_, value)| expand(value, expand_variables, &environment))
        .transpose()?;
    let search_path = configured_path
        .map(|value| std::ffi::OsString::from(value.as_ref()))
        .or_else(|| environment("PATH"));
    // Resolve once, then spawn this absolute executable; do not search PATH again at spawn.
    let executable = super::executable::resolve(command.as_ref(), search_path, &cwd)
        .map_err(Error::process_io)?;
    let mut child = std::process::Command::new(executable);
    if server.dialect == Dialect::Codex {
        child.env_clear();
        for name in [
            "HOME",
            "LOGNAME",
            "PATH",
            "SHELL",
            "USER",
            "__CF_USER_TEXT_ENCODING",
            "LANG",
            "LC_ALL",
            "TERM",
            "TMPDIR",
            "TZ",
            "SYSTEMROOT",
            "COMSPEC",
            "PATHEXT",
            "TEMP",
            "TMP",
            "USERPROFILE",
            "APPDATA",
            "LOCALAPPDATA",
        ] {
            if let Some(value) = environment(name) {
                child.env(name, value);
            }
        }
    }
    for argument in args {
        child.arg(expand(argument, expand_variables, &environment)?.as_ref());
    }
    for name in env_vars {
        if let Some(value) = environment(name) {
            child.env(name, value);
        }
    }
    for (name, value) in env {
        child.env(
            name,
            expand(value, expand_variables, &environment)?.as_ref(),
        );
    }
    child.current_dir(cwd).env("CLAUDE_PROJECT_DIR", workspace);
    Ok(child)
}

fn expand<'a>(
    value: &'a str,
    enabled: bool,
    environment: impl Fn(&str) -> Option<std::ffi::OsString>,
) -> Result<std::borrow::Cow<'a, str>> {
    if !enabled || !value.contains("${") {
        return Ok(std::borrow::Cow::Borrowed(value));
    }
    let mut result = String::with_capacity(value.len());
    let mut tail = value;
    while let Some(start) = tail.find("${") {
        result.push_str(&tail[..start]);
        tail = &tail[start + 2..];
        let end = tail
            .find('}')
            .ok_or_else(|| Error::Invalid("invalid MCP environment reference".into()))?;
        let (name, fallback) = tail[..end]
            .split_once(":-")
            .map_or((&tail[..end], None), |(name, fallback)| {
                (name, Some(fallback))
            });
        if name.is_empty() || name.contains(['=', '\0']) {
            return Err(Error::Invalid(
                "invalid MCP environment variable name".into(),
            ));
        }
        match environment(name) {
            Some(value) => result.push_str(
                &value
                    .into_string()
                    .map_err(|_| Error::Invalid("MCP environment reference is not UTF-8".into()))?,
            ),
            None => result
                .push_str(fallback.ok_or_else(|| {
                    Error::Invalid("MCP environment reference is missing".into())
                })?),
        }
        tail = &tail[end + 1..];
    }
    result.push_str(tail);
    Ok(std::borrow::Cow::Owned(result))
}
