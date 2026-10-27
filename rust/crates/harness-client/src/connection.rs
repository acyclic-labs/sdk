//! One endpoint/credential selection policy for native and WASM frontends.

/// Managed product endpoint; local and self-hosted endpoints are explicit.
pub const DEFAULT_ENDPOINT: &str = "https://swarm.cloud.acyclic.dev";
/// The public harness client's only endpoint environment variable.
pub const ENDPOINT_ENV: &str = "ACYCLIC_ENDPOINT";
/// Account API credential shared with other Cloud products.
pub const CREDENTIAL_ENV: &str = "ACYCLIC_API_KEY";

/// Configuration refusal before a transport is constructed. Diagnostics never
/// contain endpoint input or credentials.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionConfigError {
    /// The selected endpoint is not an admitted HTTPS base URL.
    InvalidEndpoint,
    /// Neither the explicit argument nor the environment supplies a credential.
    MissingCredential,
    /// The selected credential fails the shared opaque credential contract.
    InvalidCredential,
}

impl ConnectionConfigError {
    /// Stable public classification shared with the WASM error name.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidEndpoint => "client_invalid_endpoint",
            Self::MissingCredential => "client_missing_credential",
            Self::InvalidCredential => "client_invalid_credential",
        }
    }
}

impl std::fmt::Display for ConnectionConfigError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str(self.code())
    }
}

impl std::error::Error for ConnectionConfigError {}

/// How a validated endpoint is reached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointTransport {
    /// HTTPS: every managed and remote endpoint.
    Tls,
    /// Plain HTTP to a loopback address on this machine, such as a
    /// self-hosted server under test. Only an explicit `http://` URL whose
    /// host is a loopback IP literal or `localhost` selects it.
    Loopback,
}

/// Validated connection inputs. This value does not assert that the endpoint is
/// reachable, bootstrap a capability or create/configure an agent.
pub struct ConnectionOptions {
    endpoint: String,
    transport: EndpointTransport,
    credential: String,
}

impl std::fmt::Debug for ConnectionOptions {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output
            .debug_struct("ConnectionOptions")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl ConnectionOptions {
    /// Validated base URL, with the transport's trailing slash normalization.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    /// How the endpoint is reached.
    #[must_use]
    pub const fn transport(&self) -> EndpointTransport {
        self.transport
    }
    /// Original admitted account credential, without conversion or issuance.
    #[must_use]
    pub fn credential(&self) -> &str {
        &self.credential
    }

    /// Read only unshadowed environment values; no config file or local fallback.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_env(
        endpoint: Option<&str>,
        credential: Option<&str>,
    ) -> Result<Self, ConnectionConfigError> {
        fn read(
            name: &str,
            error: ConnectionConfigError,
        ) -> Result<Option<String>, ConnectionConfigError> {
            match std::env::var(name) {
                Ok(value) => Ok(Some(value)),
                Err(std::env::VarError::NotPresent) => Ok(None),
                Err(std::env::VarError::NotUnicode(_)) => Err(error),
            }
        }
        let environment_endpoint = if endpoint.is_none() {
            read(ENDPOINT_ENV, ConnectionConfigError::InvalidEndpoint)?
        } else {
            None
        };
        let environment_credential = if credential.is_none() {
            read(CREDENTIAL_ENV, ConnectionConfigError::InvalidCredential)?
        } else {
            None
        };
        resolve_connection_options(
            endpoint,
            credential,
            environment_endpoint.as_deref(),
            environment_credential.as_deref(),
        )
    }
}

/// Pure admission shared by Rust and TS. Explicit values win even when invalid:
/// an invalid explicit endpoint must never fall back to a valid env/default.
pub fn resolve_connection_options(
    endpoint: Option<&str>,
    credential: Option<&str>,
    environment_endpoint: Option<&str>,
    environment_credential: Option<&str>,
) -> Result<ConnectionOptions, ConnectionConfigError> {
    let endpoint = endpoint
        .or(environment_endpoint)
        .unwrap_or(DEFAULT_ENDPOINT);
    if endpoint
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(ConnectionConfigError::InvalidEndpoint);
    }
    let parsed = url::Url::parse(endpoint).map_err(|_| ConnectionConfigError::InvalidEndpoint)?;
    let transport = match parsed.scheme() {
        "https" => EndpointTransport::Tls,
        "http" if loopback(&parsed) => EndpointTransport::Loopback,
        _ => return Err(ConnectionConfigError::InvalidEndpoint),
    };
    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(ConnectionConfigError::InvalidEndpoint);
    }
    let credential = credential
        .or(environment_credential)
        .ok_or(ConnectionConfigError::MissingCredential)?;
    if !acyclic_auth::credential::valid_credential(credential) {
        return Err(ConnectionConfigError::InvalidCredential);
    }
    Ok(ConnectionOptions {
        endpoint: parsed.as_str().trim_end_matches('/').to_owned(),
        transport,
        credential: credential.to_owned(),
    })
}

/// Whether `url` names this machine only: a loopback IP literal or `localhost`.
fn loopback(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precedence_and_invalid_endpoints_are_resolved_without_io()
    -> Result<(), ConnectionConfigError> {
        let explicit = resolve_connection_options(
            Some("https://explicit.example"),
            Some("explicit-key"),
            Some("http://invalid-env"),
            Some("\n"),
        )?;
        assert_eq!(explicit.endpoint(), "https://explicit.example");
        assert_eq!(explicit.credential(), "explicit-key");
        let environment =
            resolve_connection_options(None, None, Some("https://env.example"), Some("env-key"))?;
        assert_eq!(environment.endpoint(), "https://env.example");
        assert_eq!(environment.credential(), "env-key");
        assert_eq!(
            resolve_connection_options(None, Some("key"), None, None)?.endpoint(),
            DEFAULT_ENDPOINT
        );
        for (endpoint, transport) in [
            ("http://127.0.0.1:7443", EndpointTransport::Loopback),
            ("http://[::1]:7443", EndpointTransport::Loopback),
            ("http://localhost:7443", EndpointTransport::Loopback),
            ("https://127.0.0.1:7443", EndpointTransport::Tls),
        ] {
            assert_eq!(
                resolve_connection_options(Some(endpoint), Some("key"), None, None)?.transport(),
                transport,
                "{endpoint}"
            );
        }
        assert_eq!(explicit.transport(), EndpointTransport::Tls);
        for endpoint in [
            "http://example.com",
            "http://10.0.0.1",
            "http://localhost.example.com",
            "unix:/tmp/socket",
            "not a URL",
            "",
            "https://",
            "https://user:key@example.com",
            "https://example.com/#fragment",
        ] {
            assert_eq!(
                resolve_connection_options(
                    Some(endpoint),
                    Some("key"),
                    Some("https://valid-env.example"),
                    None
                )
                .err(),
                Some(ConnectionConfigError::InvalidEndpoint)
            );
        }
        assert_eq!(
            resolve_connection_options(None, None, None, None).err(),
            Some(ConnectionConfigError::MissingCredential)
        );
        assert_eq!(
            resolve_connection_options(None, Some(""), None, Some("valid-env-key")).err(),
            Some(ConnectionConfigError::InvalidCredential)
        );
        assert!(!format!("{explicit:?}").contains("explicit-key"));
        Ok(())
    }
}
