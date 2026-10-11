//! Optional host-only OAuth composition.
use crate::{Error, OperationId, Result, interaction::{Interaction, InteractionOutcome}, runtime::TaskContext};
use acyclic_stream::BoxProviderFuture;
use std::sync::Arc;
pub use rmcp::transport::auth::{AuthError, AuthorizationCallback, AuthorizationManager, AuthorizationRequest, AuthorizationSession, CredentialRefreshGuard, CredentialStore, OAuthClientConfig, OAuthHttpClient, OAuthHttpRedirectPolicy, StateStore, StoredAuthorizationState, StoredCredentials, default_oauth_http_client};

/// Host-only browser/callback interaction; no URL, code or token enters model context.
pub trait McpOAuthInteraction: Send + Sync {
    /// Presents the approved private authorization URL and returns the redirect URL.
    fn authorize<'a>(&'a self, operation: OperationId, authorization_url: &'a str) -> BoxProviderFuture<'a, Result<String>>;
}

/// Host-only private authorization inputs. Never serialize this into model data.
pub struct McpOAuthAuthorization<S, T> {
    /// Exact resource endpoint whose discovery route was admitted.
    pub endpoint: String,
    /// Existing host network admission capability.
    pub http: Arc<dyn OAuthHttpClient>,
    /// Host persistent credential owner, including refresh coordination.
    pub credentials: S,
    /// Host persistent PKCE/CSRF state owner.
    pub states: T,
    /// Host-selected client identity, scopes and callback configuration.
    pub request: AuthorizationRequest,
    /// Host private presentation and callback owner.
    pub interaction: Arc<dyn McpOAuthInteraction>,
}

/// Performs an explicitly selected authorization flow.
pub async fn authorize<S: CredentialStore + 'static, T: StateStore + 'static>(
    context: &TaskContext,
    operation: OperationId,
    authorization: McpOAuthAuthorization<S, T>,
) -> Result<AuthorizationManager> {
    let McpOAuthAuthorization { endpoint, http, credentials, states, request, interaction } = authorization;
    let mut manager = AuthorizationManager::new_with_oauth_http_client(&endpoint, http).await.map_err(auth_error)?;
    manager.set_credential_store(credentials);
    manager.set_state_store(states);
    if manager.initialize_from_store().await.map_err(auth_error)? { return Ok(manager); }
    let metadata = manager.resolve_metadata_from_challenge(request.challenge.as_deref()).await.map_err(auth_error)?;
    manager.set_metadata(metadata.metadata);
    let session = AuthorizationSession::new(manager, request).await.map_err(|(_, error)| auth_error(error))?;
    let url = session.get_authorization_url();
    approve(context, operation, &endpoint, url).await?;
    let callback = interaction.authorize(operation, url).await?;
    session.handle_callback_url(&callback).await.map_err(|error| token_error(operation, error))?;
    // OAuthState is used only to recover the manager after the library session; no library HTTP worker, refresh-on-401 retry or autonomous session is started.
    let mut state = rmcp::transport::auth::OAuthState::Session(session);
    state.complete_authorization().await.map_err(auth_error)?;
    state.into_authorization_manager().ok_or_else(|| Error::Invalid("MCP authorization manager unavailable".into()))
}
/// Private restart input retained by the host's existing admitted request owner.
pub struct McpOAuthRecovery<S, T> {
    /// Same resource endpoint as the original admission.
    pub endpoint: String,
    /// Existing host OAuth network route, never an autonomous retry client.
    pub http: Arc<dyn OAuthHttpClient>,
    /// Same persistent credential owner.
    pub credentials: S,
    /// Same persistent PKCE state owner.
    pub states: T,
    /// Exact original client configuration retained outside model context.
    pub client: OAuthClientConfig,
    /// Host-observed callback URL for the original approved interaction.
    pub callback: String,
    /// Exact original approved authorization URL retained by the host.
    pub authorization_url: String,
}

/// Recovers the original PKCE callback after host restart.
pub async fn recover<S: CredentialStore + 'static, T: StateStore + 'static>(
    context: &TaskContext,
    operation: OperationId,
    recovery: McpOAuthRecovery<S, T>,
) -> Result<AuthorizationManager> {
    let McpOAuthRecovery { endpoint, http, credentials, states, client, callback, authorization_url } = recovery;
    approve(context, operation, &endpoint, &authorization_url).await?;
    let mut manager = AuthorizationManager::new_with_oauth_http_client(&endpoint, http).await.map_err(auth_error)?;
    manager.set_credential_store(credentials);
    manager.set_state_store(states);
    if manager.initialize_from_store().await.map_err(auth_error)? { return Ok(manager); }
    let metadata = manager.resolve_metadata().await.map_err(auth_error)?;
    manager.set_metadata(metadata.metadata);
    manager.configure_client(client).map_err(auth_error)?;
    let callback = AuthorizationCallback::from_redirect_url(&callback).map_err(auth_error)?;
    manager.exchange_code_for_token_with_issuer(&callback.code, &callback.csrf_token, callback.issuer.as_deref()).await.map_err(|error| token_error(operation, error))?;
    Ok(manager)
}

async fn approve(context: &TaskContext, operation: OperationId, endpoint: &str, url: &str) -> Result<()> {
    let outcome = context.interact(operation, Interaction::Approval {
        prompt: "Authorize access to the explicitly selected MCP server using the host's private browser interaction".into(),
        operation_id: operation,
        action_digest: crate::contract::canonical_json_digest(&("harness/mcp-oauth/v1", endpoint, url))?,
    }).await?;
    match outcome {
        InteractionOutcome::Approved => Ok(()),
        InteractionOutcome::Indeterminate { .. } => Err(Error::Indeterminate(operation)),
        _ => Err(Error::Unauthorized("MCP authorization was not approved".into())),
    }
}

fn auth_error(error: AuthError) -> Error {
    // Provider errors may embed token responses, URLs or codes.
    match error {
        AuthError::CredentialStoreError(_) => Error::Storage("private MCP credential owner failed".into()),
        _ => Error::Unauthorized("MCP OAuth operation failed; inspect the private credential owner".into()),
    }
}

fn token_error(operation: OperationId, error: AuthError) -> Error {
    match error {
        AuthError::AuthorizationRequired | AuthError::TokenRefreshRejected(_) => Error::Unauthorized("MCP OAuth authorization is required".into()),
        AuthError::TokenExchangeFailed(_) | AuthError::TokenRefreshFailed(_) | AuthError::CredentialStoreError(_) | AuthError::HttpError(_) => Error::Indeterminate(operation),
        _ => auth_error(error),
    }
}

#[cfg(feature = "mcp-http")]
mod native {
    use super::*;
    use crate::mcp::http::{McpHttpProvider, McpHttpRequest, McpHttpResponse, NativeMcpHttpProvider};
    /// Explicit credential-bound HTTP capability.
    pub struct OAuthMcpHttpProvider {
        endpoint: String,
        manager: Arc<AuthorizationManager>,
        network: NativeMcpHttpProvider,
    }
    impl OAuthMcpHttpProvider {
        /// The manager is already authorized through the ordinary host admission.
        pub fn new(endpoint: String, manager: Arc<AuthorizationManager>) -> Result<Self> {
            let uri: http::Uri = endpoint.parse().map_err(|_| Error::Invalid("invalid OAuth MCP endpoint".into()))?;
            if uri.scheme_str() != Some("https") || uri.authority().is_none_or(|a| a.as_str().contains('@')) || endpoint.contains('#') {
                return Err(Error::Invalid("OAuth MCP provider requires an exact HTTPS endpoint".into()));
            }
            Ok(Self { endpoint, manager, network: NativeMcpHttpProvider::new(reqwest::header::HeaderMap::new())? })
        }
    }
    impl McpHttpProvider for OAuthMcpHttpProvider {
        fn exchange<'a>(&'a self, operation: OperationId, request: McpHttpRequest) -> BoxProviderFuture<'a, Result<McpHttpResponse>> {
            Box::pin(async move {
                if request.endpoint != self.endpoint || request.headers.keys().any(|key| key.eq_ignore_ascii_case("authorization")) {
                    return Err(Error::Unauthorized("MCP OAuth endpoint or credential binding differs".into()));
                }
                let token = self.manager.get_access_token().await.map_err(|error| token_error(operation, error))?;
                let mut bearer = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                    .map_err(|_| Error::Invalid("MCP credential header invalid".into()))?;
                bearer.set_sensitive(true);
                self.network.exchange_authenticated(operation, request, Some(bearer)).await
            })
        }
        fn reconcile<'a>(&'a self, _operation: OperationId) -> BoxProviderFuture<'a, Result<Option<McpHttpResponse>>> {
            Box::pin(async { Ok(None) })
        }
    }
}
#[cfg(feature = "mcp-http")]
pub use native::OAuthMcpHttpProvider;
