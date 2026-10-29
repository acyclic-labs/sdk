//! Host-owned credential binding. Protected transports never return secret bytes.
use crate::{HttpTransport, invalid};
use acyclic_harness_core::{Error, Result};
use acyclic_stream::ProviderPlatform;
use std::sync::Arc;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// Authentication header selected by the protocol, independently of secret storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationScheme {
    /// `Authorization: Bearer`.
    Bearer,
    /// Anthropic's `x-api-key`.
    ApiKey,
    /// Gemini's `x-goog-api-key`.
    GoogleApiKey,
}

/// An already selected source. Deliberately neither Debug nor serializable.
pub enum CredentialSource<'a> {
    /// The original catalog generation's admitted native key.
    ApiKey(Arc<str>),
    /// A declared logical reference; its value stays outside guest memory.
    SecretReference(&'a str),
}

/// Authorization selected without IO. Each physical send on a protected
/// transport must check current authority and inject the current credential.
pub struct CredentialBinding {
    pub(crate) key: Option<Arc<str>>,
    pub(crate) scope: Option<[u8; 32]>,
    pub(crate) transport: Arc<dyn HttpTransport>,
}

impl CredentialBinding {
    /// Preserve the admitted key and the existing funded header producer.
    pub fn api_key(key: Arc<str>, transport: Arc<dyn HttpTransport>) -> Self {
        Self {
            key: Some(key),
            scope: None,
            transport,
        }
    }

    /// Bind an authenticated credential/account scope and host-protected
    /// exchange. Scope comes from the original admitted host binding, never
    /// a reference hash. Rotation in the same account retains that scope.
    pub fn protected(scope: [u8; 32], transport: Arc<dyn HttpTransport>) -> Self {
        Self {
            key: None,
            scope: Some(scope),
            transport,
        }
    }
}

/// The host's one credential selector for API keys and protected references.
/// Binding selects existing authority without IO or materializing a secret.
/// The returned transport owns request-time resolution, origin/revocation
/// checks, cancellation and producer-loss errors. No retry owner is added.
pub trait CredentialResolver: ProviderPlatform {
    /// Bind exactly this origin and protocol header to the selected source.
    fn bind(
        &self,
        source: CredentialSource<'_>,
        origin: &str,
        scheme: AuthorizationScheme,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<CredentialBinding>;
}

pub(crate) struct NativeCredentials;

impl CredentialResolver for NativeCredentials {
    fn bind(
        &self,
        source: CredentialSource<'_>,
        _: &str,
        _: AuthorizationScheme,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<CredentialBinding> {
        match source {
            CredentialSource::ApiKey(key) => Ok(CredentialBinding::api_key(key, transport)),
            CredentialSource::SecretReference(_) => Err(Error::Unsupported(
                "protected model credential binding is unavailable".into(),
            )),
        }
    }
}

pub(crate) fn reference(value: &str) -> Result<()> {
    if acyclic_actors::domain::SecretSelection::valid_reference(value) {
        Ok(())
    } else {
        Err(invalid("invalid model secret reference"))
    }
}

pub(crate) fn bind(
    config: &crate::ProviderSpec,
    key: Option<Arc<str>>,
    resolver: Option<&dyn CredentialResolver>,
    transport: Arc<dyn HttpTransport>,
) -> Result<CredentialBinding> {
    let reference = config.authentication.secret_reference();
    let source = reference
        .map(CredentialSource::SecretReference)
        .or_else(|| key.map(CredentialSource::ApiKey));
    let Some(source) = source else {
        return Ok(CredentialBinding {
            key: None,
            scope: None,
            transport,
        });
    };
    let scheme = if matches!(config.authentication, crate::Authentication::Bearer { .. }) {
        AuthorizationScheme::Bearer
    } else {
        match config.adapter {
            crate::Adapter::Anthropic => AuthorizationScheme::ApiKey,
            crate::Adapter::Gemini => AuthorizationScheme::GoogleApiKey,
            _ => AuthorizationScheme::Bearer,
        }
    };
    let Some(resolver) = resolver else {
        return NativeCredentials.bind(source, "", scheme, transport);
    };
    let origin = crate::registry::validate_url(&config.base_url)?
        .origin()
        .ascii_serialization();
    let binding = resolver.bind(source, &origin, scheme, transport)?;
    if reference.is_some() && (binding.key.is_some() || binding.scope.is_none()) {
        return Err(invalid(
            "protected credential binding must not expose a secret value",
        ));
    }
    Ok(binding)
}
