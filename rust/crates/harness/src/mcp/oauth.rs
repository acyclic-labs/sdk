//! Host-only OAuth over the existing approval and private credential owners.
use crate::{
    Error, OperationId, Result,
    interaction::{Interaction, InteractionOutcome},
    runtime::TaskContext,
};
use acyclic_stream::BoxProviderFuture;
pub use rmcp::transport::auth::{
    AuthError, AuthorizationManager, AuthorizationRequest, CredentialRefreshGuard, CredentialStore,
    OAuthClientConfig, OAuthHttpClient, StateStore, StoredAuthorizationState, StoredCredentials,
    default_oauth_http_client,
};
use rmcp::transport::auth::{AuthorizationCallback, AuthorizationSession};
use std::sync::Arc;

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
    context: &TaskContext,
    operation: OperationId,
    endpoint: &str,
    http: Arc<dyn OAuthHttpClient>,
    credentials: S,
    states: T,
    request: AuthorizationRequest,
    interaction: &dyn McpOAuthInteraction,
) -> Result<AuthorizationManager> {
    let mut manager = AuthorizationManager::new_with_oauth_http_client(endpoint, http)
        .await
        .map_err(auth_error)?;
    manager.set_credential_store(credentials);
    manager.set_state_store(states);
    if manager.initialize_from_store().await.map_err(auth_error)? {
        return Ok(manager);
    }
    let metadata = manager
        .resolve_metadata_from_challenge(request.challenge.as_deref())
        .await
        .map_err(auth_error)?;
    manager.set_metadata(metadata.metadata);
    let session = AuthorizationSession::new(manager, request)
        .await
        .map_err(|(_, error)| auth_error(error))?;
    let url = session.get_authorization_url();
    approve(context, operation, endpoint, url).await?;
    let callback = interaction.authorize(operation, url).await?;
    session
        .handle_callback_url(&callback)
        .await
        .map_err(|error| token_error(operation, error))?;
    let mut state = rmcp::transport::auth::OAuthState::Session(session);
    state.complete_authorization().await.map_err(auth_error)?;
    state
        .into_authorization_manager()
        .ok_or_else(|| Error::Invalid("MCP authorization manager unavailable".into()))
}
/// Recovers the approved PKCE callback without starting another authorization.
pub async fn recover<S: CredentialStore + 'static, T: StateStore + 'static>(
    context: &TaskContext,
    operation: OperationId,
    endpoint: &str,
    http: Arc<dyn OAuthHttpClient>,
    credentials: S,
    states: T,
    client: OAuthClientConfig,
    callback: &str,
    authorization_url: &str,
) -> Result<AuthorizationManager> {
    approve(context, operation, endpoint, authorization_url).await?;
    let mut manager = AuthorizationManager::new_with_oauth_http_client(endpoint, http)
        .await
        .map_err(auth_error)?;
    manager.set_credential_store(credentials);
    manager.set_state_store(states);
    if manager.initialize_from_store().await.map_err(auth_error)? {
        return Ok(manager);
    }
    let metadata = manager.resolve_metadata().await.map_err(auth_error)?;
    manager.set_metadata(metadata.metadata);
    manager.configure_client(client).map_err(auth_error)?;
    let callback = AuthorizationCallback::from_redirect_url(callback).map_err(auth_error)?;
    manager
        .exchange_code_for_token_with_issuer(
            &callback.code,
            &callback.csrf_token,
            callback.issuer.as_deref(),
        )
        .await
        .map_err(|error| token_error(operation, error))?;
    Ok(manager)
}
async fn approve(
    context: &TaskContext,
    operation: OperationId,
    endpoint: &str,
    url: &str,
) -> Result<()> {
    match context.interact(operation, Interaction::Approval {
        prompt: "Authorize access to the explicitly selected MCP server using the host's private browser interaction".into(),
        operation_id: operation,
        action_digest: crate::contract::canonical_json_digest(&("harness/mcp-oauth/v1", endpoint, url), u64::MAX, &mut None)?,
    }).await? {
        InteractionOutcome::Approved => Ok(()),
        InteractionOutcome::Indeterminate { .. } => Err(Error::Indeterminate(operation)),
        _ => Err(Error::Unauthorized("MCP authorization was not approved".into())),
    }
}
fn auth_error(error: AuthError) -> Error {
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
fn token_error(operation: OperationId, error: AuthError) -> Error {
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
