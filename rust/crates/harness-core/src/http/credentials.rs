//! Host-owned credential binding shared by all HTTP providers.
use super::HttpTransport;
use crate::Result;
use acyclic_stream::ProviderPlatform;
use std::sync::Arc;
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
    /// The canonical versioned reference; its value stays outside guest memory.
    SecretReference(&'a acyclic_auth::secrets::SecretRef),
}

/// Authorization selected without IO. Each physical send on a protected
/// transport must check current authority and inject the current credential.
pub struct CredentialBinding {
    key: Option<Arc<str>>,
    scope: Option<[u8; 32]>,
    transport: Arc<dyn HttpTransport>,
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

impl CredentialBinding {
    /// Constructs a binding for the existing model header producer.
    pub fn from_parts(
        key: Option<Arc<str>>,
        scope: Option<[u8; 32]>,
        transport: Arc<dyn HttpTransport>,
    ) -> Self {
        Self {
            key,
            scope,
            transport,
        }
    }
    /// An unauthenticated exchange.
    pub fn anonymous(transport: Arc<dyn HttpTransport>) -> Self {
        Self {
            key: None,
            scope: None,
            transport,
        }
    }
    /// Returns the original binding for the model header producer.
    pub fn into_parts(self) -> (Option<Arc<str>>, Option<[u8; 32]>, Arc<dyn HttpTransport>) {
        (self.key, self.scope, self.transport)
    }
    /// Accepts only host-protected authority; never exposes a local key.
    pub fn protected_transport(self) -> Result<Arc<dyn HttpTransport>> {
        if self.key.is_some() || self.scope.is_none() {
            return Err(crate::Error::Invalid(
                "credential binding exposed a value; recovery: use a protected host binding".into(),
            ));
        }
        Ok(self.transport)
    }
}
