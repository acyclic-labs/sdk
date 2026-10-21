//! Host-only OAuth over the existing approval and private credential owners.
use crate::{
    Error, OperationId, Result,
    interaction::{Interaction, InteractionOutcome},
    runtime::TaskContext,
};
use acyclic_stream::BoxProviderFuture;
pub use rmcp::transport::auth::{
    AuthError, AuthorizationRequest, CredentialRefreshGuard, CredentialStore, OAuthClientConfig,
    OAuthHttpClient, StateStore, StoredAuthorizationState, StoredCredentials,
};
use rmcp::transport::auth::{AuthorizationCallback, AuthorizationManager, AuthorizationSession};
use std::sync::Arc;

/// An authorized MCP OAuth session. Only [`authorize`] and [`recover`] create
/// one, over an explicit [`OAuthHttpClient`]. The rmcp manager stays private
/// and only its token operation is exposed, so rmcp's provider-unaware
/// `AuthorizationManager::new` is not reachable through this type:
///
/// ```
/// use acyclic_harness_core::mcp::oauth::McpAuthorization;
/// async fn token(session: &McpAuthorization) -> Option<String> {
///     session.get_access_token().await.ok()
/// }
/// ```
///
/// ```compile_fail
/// use acyclic_harness_core::mcp::oauth::McpAuthorization;
/// type Manager = <McpAuthorization as std::ops::Deref>::Target;
/// async fn bypass() {
///     drop(Manager::new("http://127.0.0.1:9/").await);
/// }
/// ```
pub struct McpAuthorization(AuthorizationManager);

impl McpAuthorization {
    /// Returns the current access token, refreshing it through the session's
    /// own OAuth HTTP client when it has expired.
    pub async fn get_access_token(&self) -> std::result::Result<String, AuthError> {
        self.0.get_access_token().await
    }
}

/// The OAuth HTTP client over the build's one rustls provider, ring. rmcp's
/// reqwest carries no provider of its own, so a second (aws-lc-rs) provider
/// never enters the graph and provider-less rustls builders stay unambiguous.
pub fn default_oauth_http_client() -> std::result::Result<impl OAuthHttpClient, AuthError> {
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        // A concurrent first install of the same provider is equivalent.
        drop(rustls::crypto::ring::default_provider().install_default());
    }
    rmcp::transport::auth::default_oauth_http_client()
}

/// Private browser/callback capability.
pub trait McpOAuthInteraction: Send + Sync {
    /// Presents the approved URL and returns its private redirect URL.
    fn authorize<'a>(
        &'a self,
        operation: OperationId,
        authorization_url: &'a str,
    ) -> BoxProviderFuture<'a, Result<String>>;
}
/// Authorizes the selected endpoint with host credentials and private interaction.
pub async fn authorize<S: CredentialStore + 'static, T: StateStore + 'static>(
    owner: (&TaskContext, OperationId),
    endpoint: &str,
    http: Arc<dyn OAuthHttpClient>,
    credentials: S,
    states: T,
    request: AuthorizationRequest,
    interaction: &dyn McpOAuthInteraction,
) -> Result<McpAuthorization> {
    let (context, operation) = owner;
    let mut manager = AuthorizationManager::new_with_oauth_http_client(endpoint, http)
        .await
        .map_err(|error| auth_error(&error))?;
    manager.set_credential_store(credentials);
    manager.set_state_store(states);
    if manager
        .initialize_from_store()
        .await
        .map_err(|error| auth_error(&error))?
    {
        return Ok(McpAuthorization(manager));
    }
    let metadata = manager
        .resolve_metadata_from_challenge(request.challenge.as_deref())
        .await
        .map_err(|error| auth_error(&error))?;
    manager.set_metadata(metadata.metadata);
    let session = AuthorizationSession::new(manager, request)
        .await
        .map_err(|(_, error)| auth_error(&error))?;
    let url = session.get_authorization_url();
    approve((context, operation), endpoint, url).await?;
    let callback = interaction.authorize(operation, url).await?;
    session
        .handle_callback_url(&callback)
        .await
        .map_err(|error| token_error(operation, &error))?;
    let mut state = rmcp::transport::auth::OAuthState::Session(session);
    state
        .complete_authorization()
        .await
        .map_err(|error| auth_error(&error))?;
    state
        .into_authorization_manager()
        .map(McpAuthorization)
        .ok_or_else(|| Error::Invalid("MCP authorization manager unavailable".into()))
}
/// Recovers the approved PKCE callback without starting another authorization.
pub async fn recover<S: CredentialStore + 'static, T: StateStore + 'static>(
    owner: (&TaskContext, OperationId),
    endpoint: &str,
    http: Arc<dyn OAuthHttpClient>,
    credentials: S,
    states: T,
    client: OAuthClientConfig,
    callback: (&str, &str),
) -> Result<McpAuthorization> {
    let (context, operation) = owner;
    let (callback, authorization_url) = callback;
    approve((context, operation), endpoint, authorization_url).await?;
    let mut manager = AuthorizationManager::new_with_oauth_http_client(endpoint, http)
        .await
        .map_err(|error| auth_error(&error))?;
    manager.set_credential_store(credentials);
    manager.set_state_store(states);
    if manager
        .initialize_from_store()
        .await
        .map_err(|error| auth_error(&error))?
    {
        return Ok(McpAuthorization(manager));
    }
    let metadata = manager
        .resolve_metadata()
        .await
        .map_err(|error| auth_error(&error))?;
    manager.set_metadata(metadata.metadata);
    manager
        .configure_client(client)
        .map_err(|error| auth_error(&error))?;
    let callback =
        AuthorizationCallback::from_redirect_url(callback).map_err(|error| auth_error(&error))?;
    manager
        .exchange_code_for_token_with_issuer(
            &callback.code,
            &callback.csrf_token,
            callback.issuer.as_deref(),
        )
        .await
        .map_err(|error| token_error(operation, &error))?;
    Ok(McpAuthorization(manager))
}
async fn approve(owner: (&TaskContext, OperationId), endpoint: &str, url: &str) -> Result<()> {
    let (context, operation) = owner;
    match context.interact(operation, Interaction::Approval {
        prompt: "Authorize access to the explicitly selected MCP server using the host's private browser interaction".into(),
        operation_id: operation,
        action_digest: crate::executor::json::canonical_json_digest(&("harness/mcp-oauth/v1", endpoint, url), u64::MAX, &mut None)?,
    }).await? {
        InteractionOutcome::Approved {} => Ok(()),
        InteractionOutcome::Indeterminate { .. } => Err(Error::Indeterminate(operation)),
        _ => Err(Error::Unauthorized("MCP authorization was not approved".into())),
    }
}
fn auth_error(error: &AuthError) -> Error {
    // Provider errors may contain private URLs, codes or tokens.
    match error {
        AuthError::CredentialStoreError(_) => {
            Error::Storage("private MCP credential owner failed".into())
        }
        _ => Error::Unauthorized(
            "MCP OAuth operation failed; inspect the private credential owner".into(),
        ),
    }
}
fn token_error(operation: OperationId, error: &AuthError) -> Error {
    match error {
        AuthError::AuthorizationRequired | AuthError::TokenRefreshRejected(_) => {
            Error::Unauthorized("MCP OAuth authorization is required".into())
        }
        AuthError::TokenExchangeFailed(_)
        | AuthError::TokenRefreshFailed(_)
        | AuthError::CredentialStoreError(_)
        | AuthError::HttpError(_) => Error::Indeterminate(operation),
        _ => auth_error(error),
    }
}
