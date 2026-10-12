//! Real loopback discovery/authorization/token endpoints and private disk stores.
use super::{journal, services_cases::{Participant, admitted_context}};
use acyclic_harness::{Capabilities, Error, OperationId, Result, conversation::Limits, runtime::RuntimeScope,
    mcp::oauth::{self, AuthError, CredentialRefreshGuard, CredentialStore, StateStore, StoredCredentials, StoredAuthorizationState, AuthorizationRequest, OAuthClientConfig, McpOAuthInteraction, default_oauth_http_client}};
use acyclic_stream::BoxProviderFuture;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{path::{Path, PathBuf}, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use base64::Engine as _;
use sha2::Digest as _;

fn store_error(error: impl std::fmt::Display) -> AuthError { AuthError::CredentialStoreError(error.to_string()) }
async fn load_json<T: DeserializeOwned>(path: &Path) -> std::result::Result<Option<T>, AuthError> {
    match tokio::fs::read(path).await {
        Ok(bytes) => serde_json::from_slice(&bytes).map(Some).map_err(store_error),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(store_error(error)),
    }
}
async fn save_json<T: Serialize>(path: &Path, value: &T) -> std::result::Result<(), AuthError> {
    let bytes = serde_json::to_vec(value).map_err(store_error)?;
    tokio::fs::write(path, bytes).await.map_err(store_error)
}
#[derive(Clone)]
struct DiskCredentials { path: PathBuf, lock: Arc<tokio::sync::Mutex<()>> }
#[async_trait::async_trait]
impl CredentialStore for DiskCredentials {
    async fn load(&self) -> std::result::Result<Option<StoredCredentials>, AuthError> { load_json(&self.path).await }
    async fn save(&self, value: StoredCredentials) -> std::result::Result<(), AuthError> { save_json(&self.path, &value).await }
    async fn clear(&self) -> std::result::Result<(), AuthError> {
        match tokio::fs::remove_file(&self.path).await { Ok(()) => Ok(()), Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()), Err(error) => Err(store_error(error)) }
    }
    async fn acquire_refresh_guard(&self) -> std::result::Result<Option<CredentialRefreshGuard>, AuthError> {
        Ok(Some(CredentialRefreshGuard::new(Arc::clone(&self.lock).lock_owned().await)))
    }
}
#[derive(Clone)]
struct DiskStates(PathBuf);
impl DiskStates {
    fn path(&self, key: &str) -> std::result::Result<PathBuf, AuthError> {
        if key.is_empty() || !key.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')) { return Err(store_error("invalid private state key")); }
        Ok(self.0.join(format!("{key}.json")))
    }
}
#[async_trait::async_trait]
impl StateStore for DiskStates {
    async fn save(&self, key: &str, value: StoredAuthorizationState) -> std::result::Result<(), AuthError> { save_json(&self.path(key)?, &value).await }
    async fn load(&self, key: &str) -> std::result::Result<Option<StoredAuthorizationState>, AuthError> { load_json(&self.path(key)?).await }
    async fn delete(&self, key: &str) -> std::result::Result<(), AuthError> { tokio::fs::remove_file(self.path(key)?).await.map_err(store_error) }
}
struct Browser { private_receipt: PathBuf, interrupt: bool }
impl McpOAuthInteraction for Browser {
    fn authorize<'a>(&'a self, operation: OperationId, url: &'a str) -> BoxProviderFuture<'a, Result<String>> {
        Box::pin(async move {
            let client = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none()).retry(reqwest::retry::never()).build().map_err(|error| Error::Storage(error.to_string()))?;
            let response = client.get(url).send().await.map_err(|error| Error::Storage(error.to_string()))?;
            assert_eq!(response.status().as_u16(), 302);
            let callback = response.headers().get("location").ok_or_else(|| Error::Invalid("private callback absent".into()))?.to_str().map_err(|error| Error::Invalid(error.to_string()))?.to_owned();
            save_json(&self.private_receipt, &json!({"authorizationUrl":url,"callback":callback})).await.map_err(|error| Error::Storage(error.to_string()))?;
            if self.interrupt { return Err(Error::Indeterminate(operation)); }
            Ok(callback)
        })
    }
}
async fn wire_request(socket: &mut tokio::net::TcpStream) -> Result<(String, Vec<u8>)> {
    let mut bytes = Vec::new(); let mut buffer = [0;512];
    loop {
        let count = socket.read(&mut buffer).await.map_err(|error| Error::Storage(error.to_string()))?;
        if count == 0 { return Err(Error::Invalid("OAuth endpoint disconnected".into())); }
        bytes.extend_from_slice(buffer.get(..count).ok_or_else(|| Error::Invalid("read length".into()))?);
        if bytes.len() > 32_768 { return Err(Error::Invalid("OAuth request overflow".into())); }
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let header = std::str::from_utf8(bytes.get(..end).ok_or_else(|| Error::Invalid("header".into()))?).map_err(|error| Error::Invalid(error.to_string()))?;
            let target = header.lines().next().and_then(|line| line.split_whitespace().nth(1)).ok_or_else(|| Error::Invalid("OAuth request target absent".into()))?.to_owned();
            let length = header.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length:").and_then(|value| value.trim().parse::<usize>().ok())).unwrap_or(0);
            if bytes.len() >= end + 4 + length { return Ok((target, bytes.get(end+4..end+4+length).ok_or_else(|| Error::Invalid("body".into()))?.to_vec())); }
        }
    }
}
async fn serve(listener: tokio::net::TcpListener, base: String, states: DiskStates, lost_token: bool, tokens: Arc<AtomicUsize>, refreshes: Arc<AtomicUsize>, stop: tokio::sync::oneshot::Receiver<()>) -> Result<()> {
    tokio::pin!(stop);
    let mut csrf = None;
    let mut challenge = None;
    loop {
        let (mut socket, _) = tokio::select! { _ = &mut stop => return Ok(()), accepted = listener.accept() => accepted.map_err(|error| Error::Storage(error.to_string()))? };
        let (target, body) = wire_request(&mut socket).await?;
        let url = reqwest::Url::parse(&format!("{base}{target}")).map_err(|error| Error::Invalid(error.to_string()))?;
        let endpoint = format!("{base}/mcp");
        let mut status = "200 OK"; let mut extra = String::new();
        let value = if url.path().contains("oauth-protected-resource") {
            json!({"resource":endpoint,"authorization_servers":[base],"scopes_supported":["mcp"],"bearer_methods_supported":["header"]})
        } else if url.path().contains("oauth-authorization-server") || url.path().contains("openid-configuration") {
            json!({"issuer":base,"authorization_endpoint":format!("{base}/authorize"),"token_endpoint":format!("{base}/token"),"response_types_supported":["code"],"grant_types_supported":["authorization_code","refresh_token"],"code_challenge_methods_supported":["S256"],"scopes_supported":["mcp"],"authorization_response_iss_parameter_supported":true})
        } else if url.path() == "/authorize" {
            let query = url.query_pairs().into_owned().collect::<std::collections::BTreeMap<_,_>>();
            assert_eq!(query.get("code_challenge_method").map(String::as_str), Some("S256"));
            assert_eq!(query.get("code_challenge").map(String::len), Some(43));
            challenge = query.get("code_challenge").cloned();
            assert_eq!(query.get("resource"), Some(&endpoint));
            let state = query.get("state").ok_or_else(|| Error::Invalid("state absent".into()))?.clone();
            assert!(states.load(&state).await.map_err(|error| Error::Storage(error.to_string()))?.is_some());
            csrf = Some(state.clone());
            let mut callback = reqwest::Url::parse("http://127.0.0.1/callback").map_err(|error| Error::Invalid(error.to_string()))?;
            callback.query_pairs_mut().append_pair("code", "authorized").append_pair("state", &state).append_pair("iss", &base);
            status = "302 Found"; extra = format!("Location: {callback}\r\n"); json!({})
        } else if url.path() == "/token" {
            let form = reqwest::Url::parse(&format!("{base}/?{}", String::from_utf8(body).map_err(|error| Error::Invalid(error.to_string()))?)).map_err(|error| Error::Invalid(error.to_string()))?.query_pairs().into_owned().collect::<std::collections::BTreeMap<_,_>>();
            assert_eq!(form.get("resource"), Some(&endpoint));
            if form.get("grant_type").map(String::as_str) == Some("authorization_code") {
                tokens.fetch_add(1, Ordering::SeqCst);
                assert_eq!(form.get("code").map(String::as_str), Some("authorized"));
                assert!(form.get("code_verifier").is_some_and(|verifier| verifier.len() >= 43));
                let verifier = form.get("code_verifier").ok_or_else(|| Error::Invalid("verifier absent".into()))?;
                assert_eq!(Some(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sha2::Sha256::digest(verifier.as_bytes()))), challenge);
                assert!(states.load(csrf.as_deref().ok_or_else(|| Error::Invalid("state never observed".into()))?).await.map_err(|error| Error::Storage(error.to_string()))?.is_none());
                if lost_token { drop(socket); continue; }
                json!({"access_token":"private-access","token_type":"Bearer","refresh_token":"private-refresh","expires_in":3600,"scope":"mcp"})
            } else {
                refreshes.fetch_add(1, Ordering::SeqCst);
                assert_eq!(form.get("refresh_token").map(String::as_str), Some("private-refresh"));
                json!({"access_token":"private-refreshed","token_type":"Bearer","expires_in":3600,"scope":"mcp"})
            }
        } else { status = "401 Unauthorized"; extra = format!("WWW-Authenticate: Bearer resource_metadata=\"{base}/.well-known/oauth-protected-resource/mcp\"\r\n"); json!({}) };
        let body = value.to_string();
        let reply = format!("HTTP/1.1 {status}\r\nContent-Type: application/json\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
        socket.write_all(reply.as_bytes()).await.map_err(|error| Error::Storage(error.to_string()))?;
    }
}

#[tokio::test]
async fn oauth_discovery_pkce_host_interaction_refresh_and_private_restart() -> Result<()> {
    for mode in ["normal", "before-token", "lost-token"] {
        let root = tempfile::tempdir().map_err(|error| Error::Storage(error.to_string()))?;
        let (disk, issuer, _) = journal(root.path(), true).await?;
        let scope = RuntimeScope::new(Capabilities::new(["interaction:route"]), Limits::default())?;
        let context = admitted_context(scope.clone(), Arc::new(Participant { journal: disk, issuer }), None).await?;
        let states = DiskStates(root.path().join("states"));
        tokio::fs::create_dir(&states.0).await.map_err(|error| Error::Storage(error.to_string()))?;
        let credentials = DiskCredentials { path: root.path().join("credentials.json"), lock: Arc::new(tokio::sync::Mutex::new(())) };
        let receipt = root.path().join("private-interaction.json");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.map_err(|error| Error::Storage(error.to_string()))?;
        let base = format!("http://{}", listener.local_addr().map_err(|error| Error::Storage(error.to_string()))?);
        let tokens = Arc::new(AtomicUsize::new(0)); let refreshes = Arc::new(AtomicUsize::new(0));
        let (send, stop) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve(listener, base.clone(), states.clone(), mode == "lost-token", Arc::clone(&tokens), Arc::clone(&refreshes), stop));
        let endpoint = format!("{base}/mcp"); let operation = OperationId::from_bytes([101;16]);
        let request = AuthorizationRequest::new("http://127.0.0.1/callback").with_preregistered_client("fixed-client").with_scopes(["mcp"]);
        let browser = Browser { private_receipt: receipt.clone(), interrupt: mode == "before-token" };
        let first = oauth::authorize(&context, operation, &endpoint,
            Arc::new(default_oauth_http_client().map_err(|error| Error::Storage(error.to_string()))?),
            credentials.clone(), states.clone(), request, &browser).await;
        if mode == "normal" {
            let manager = first?;
            assert_eq!(manager.get_access_token().await.map_err(|error| Error::Storage(error.to_string()))?, "private-access");
            let mut stored = credentials.load().await.map_err(|error| Error::Storage(error.to_string()))?.ok_or_else(|| Error::NotFound("private credentials".into()))?;
            stored.token_received_at = Some(0);
            credentials.save(stored).await.map_err(|error| Error::Storage(error.to_string()))?;
            assert_eq!(manager.get_access_token().await.map_err(|error| Error::Storage(error.to_string()))?, "private-refreshed");
            assert_eq!(refreshes.load(Ordering::SeqCst), 1);
            drop(manager);
        } else { assert!(first.is_err()); }
        drop(context);
        let private: Value = load_json(&receipt).await.map_err(|error| Error::Storage(error.to_string()))?.ok_or_else(|| Error::NotFound("private callback receipt".into()))?;
        let (disk, issuer, _) = journal(root.path(), false).await?;
        let context = admitted_context(scope, Arc::new(Participant { journal: disk, issuer }), None).await?;
        let callback = private.get("callback").and_then(Value::as_str).ok_or_else(|| Error::Invalid("callback absent".into()))?;
        let authorization_url = private.get("authorizationUrl").and_then(Value::as_str).ok_or_else(|| Error::Invalid("authorization URL absent".into()))?;
        let recovery = |callback: String| {
            let context = &context;
            let endpoint = &endpoint;
            let credentials = credentials.clone();
            let states = states.clone();
            async move {
                oauth::recover(context, operation, endpoint,
                    Arc::new(default_oauth_http_client().map_err(|error| Error::Storage(error.to_string()))?),
                    credentials, states,
                    OAuthClientConfig::new("fixed-client", "http://127.0.0.1/callback").with_scopes(vec!["mcp".into()]),
                    &callback, authorization_url).await
            }
        };
        if mode == "before-token" {
            for (key, value) in [("iss", Some("http://127.0.0.1/forged")), ("iss", None), ("state", Some("forged"))] {
                let mut rejected = reqwest::Url::parse(callback).map_err(|error| Error::Invalid(error.to_string()))?;
                let retained = rejected.query_pairs().into_owned().filter(|(name, _)| name != key).collect::<Vec<_>>();
                rejected.query_pairs_mut().clear().extend_pairs(retained);
                if let Some(value) = value { rejected.query_pairs_mut().append_pair(key, value); }
                let error = recovery(rejected.to_string()).await.err().ok_or_else(|| Error::Invalid("forged OAuth response was accepted".into()))?;
                assert!(!error.to_string().contains("authorized"));
                assert_eq!(tokens.load(Ordering::SeqCst), 0);
            }
        }
        let recovered = recovery(callback.into()).await;
        if mode == "lost-token" { assert!(recovered.is_err()); }
        else { assert!(recovered.is_ok()); }
        assert_eq!(tokens.load(Ordering::SeqCst), 1);
        let _ = send.send(());
        server.await.map_err(|error| Error::Storage(error.to_string()))??;
    }
    Ok(())
}
