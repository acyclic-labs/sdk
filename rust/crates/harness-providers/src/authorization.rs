//! Host-owned credential binding. Protected transports never return secret bytes.
use crate::{HttpTransport, invalid};
use acyclic_harness_core::{Error, Result};

use std::sync::Arc;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

pub use acyclic_harness_core::http::{
    AuthorizationScheme, CredentialBinding, CredentialResolver, CredentialSource,
};

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
        return Ok(CredentialBinding::anonymous(transport));
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
    let (key, scope, transport) = binding.into_parts();
    if reference.is_some() && (key.is_some() || scope.is_none()) {
        return Err(invalid(
            "protected credential binding must not expose a secret value",
        ));
    }
    Ok(CredentialBinding::from_parts(key, scope, transport))
}
