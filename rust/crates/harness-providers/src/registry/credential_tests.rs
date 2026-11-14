use super::*;
use crate::credentials::Source;
use acyclic_auth::credential::{CredentialValidity, CredentialWatch};
use acyclic_harness_core::model::ModelProvider as _;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::TryStreamExt as _;
use serde_json::json;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

proptest::proptest! {
    #[test]
    fn arbitrary_credential_source_changes_follow_only_their_watch_events(
        operations in proptest::collection::vec((
            proptest::array::uniform3(proptest::option::of(proptest::num::u8::ANY)),
            proptest::bool::ANY, proptest::bool::ANY,
        ), 1..24),
        environment_only in proptest::bool::ANY,
    ) {
        let state = Arc::new(Mutex::new(([None; 3], false, 0usize)));
        let observed = state.clone();
        let (events, _) = tokio::sync::watch::channel(0);
        let mut credentials = if environment_only { Credentials::environment() } else { Credentials::default() }
            .with_watch(watched(events.clone()));
        credentials.source = Some(Arc::new(move |provider, source| {
            assert!(!environment_only || source == Source::Environment,
                "environment-only credentials consulted another source");
            let mut state = observed.lock().unwrap();
            state.2 += 1;
            let index = match source {
                Source::Keychain if state.1 => return Err(crate::credentials::KeychainDecode { provider: provider.into() }),
                Source::Keychain => 0,
                Source::Environment => 1,
                Source::Configuration => 2,
                Source::CodexHome => return Ok(None),
            };
            Ok(state.0[index].map(|key| format!("watched-key-{key}")))
        }));
        let inputs = crate::credentials::CredentialInputs::new(credentials);
        let selected = || inputs.select("openai").map(|credential| match credential {
            crate::credentials::Credential::Key(key) => Some(key.to_string()),
            crate::credentials::Credential::Missing => None,
            crate::credentials::Credential::Codex(_) => panic!("fixture has no login"),
        }).map_err(|_| ());
        let mut expected = selected();
        for (revision, (keys, decode_error, changed)) in operations.into_iter().enumerate() {
            let before = {
                let mut state = state.lock().unwrap();
                state.0 = keys;
                state.1 = decode_error;
                state.2
            };
            if changed {
                events.send_replace(revision as u64 + 1);
                expected = if environment_only {
                    Ok(keys[1].map(|key| format!("watched-key-{key}")))
                } else if decode_error { Err(()) } else {
                    Ok(keys.into_iter().flatten().next().map(|key| format!("watched-key-{key}")))
                };
            }
            proptest::prop_assert_eq!(selected(), expected.clone());
            if !changed {
                proptest::prop_assert_eq!(state.lock().unwrap().2, before);
            }
        }
    }
}

struct Server(Mutex<Vec<crate::HttpRequest>>);
impl HttpTransport for Server {
    fn send(
        &self,
        request: crate::HttpRequest,
    ) -> acyclic_stream::BoxProviderFuture<'_, Result<crate::HttpResponse>> {
        self.0.lock().unwrap().push(request);
        Box::pin(async {
            Ok(crate::HttpResponse {
                request_id: None,
                retry_after: None,
                status: 200,
                body: Box::pin(futures::stream::once(async {
                    Ok(bytes::Bytes::from_static(b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"done\",\"status\":\"completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n"))
                })),
            })
        })
    }
    fn delay(
        &self,
        wait: std::time::Duration,
    ) -> acyclic_stream::BoxProviderFuture<'static, Result<()>> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
}

pub(crate) fn watched(
    sender: tokio::sync::watch::Sender<u64>,
) -> impl Fn() -> CredentialWatch + Send + Sync {
    move || {
        let mut receiver = sender.subscribe();
        let mut revision = [0; 32];
        revision[..8].copy_from_slice(&receiver.borrow_and_update().to_be_bytes());
        CredentialWatch::from_custodian(
            CredentialValidity {
                now_unix_seconds: 0,
                revision: Some(revision.into()),
            },
            async move {
                receiver
                    .changed()
                    .await
                    .map_err(|_| acyclic_auth::credential::CredentialCacheError::Unavailable)
            },
        )
    }
}

/// A catalog memo fails at the second request; a per-request reader fails the
/// unchanged-read assertion. The same prepared request is used throughout.
#[tokio::test]
async fn credential_watch_login_creation_refresh_and_account_pin() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(acyclic_fs::NativeTreeMaterializationError::Io)?;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("auth.json");
    let state = Arc::new(Mutex::new(Some("initial-key".to_owned())));
    let reads = Arc::new(AtomicUsize::new(0));
    let (events, _) = tokio::sync::watch::channel(0);
    let mut credentials = Credentials::default().with_watch(watched(events.clone()));
    let keys = state.clone();
    let observed = reads.clone();
    let home = root.path().to_string_lossy().into_owned();
    credentials.source = Some(Arc::new(move |_, source| {
        observed.fetch_add(1, Ordering::SeqCst);
        Ok(match source {
            Source::Environment => keys.lock().unwrap().clone(),
            Source::CodexHome => Some(home.clone()),
            _ => None,
        })
    }));
    let server = Arc::new(Server(Mutex::new(Vec::new())));
    let registry = Registry::with_transport(credentials, server.clone())?;
    let provider = registry.resolve("openai/gpt-5")?;
    let (request, dispatch) = crate::tests::request(&provider)?;
    let frozen = request.bytes().to_vec();
    provider
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    let unchanged = reads.load(Ordering::SeqCst);
    provider
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        reads.load(Ordering::SeqCst),
        unchanged,
        "unchanged requests perform no source reads"
    );
    let auth = |token: &str, account: &str| {
        let jwt = format!(
            "{token}.{}.signature",
            URL_SAFE_NO_PAD.encode(
                json!({"exp":time::OffsetDateTime::now_utc().unix_timestamp()+3600}).to_string()
            )
        );
        (
            jwt.clone(),
            json!({"tokens":{"access_token":jwt,"refresh_token":"refresh","account_id":account}}),
        )
    };
    *state.lock().unwrap() = None;
    for (revision, token, account) in [(1, "created", "account"), (2, "refreshed", "account")] {
        let (bearer, document) = auth(token, account);
        std::fs::write(&path, document.to_string()).unwrap();
        events.send_replace(revision);
        provider
            .generate(&request, dispatch)
            .try_collect::<Vec<_>>()
            .await?;
        let sent = server.0.lock().unwrap();
        let next = sent.last().unwrap();
        assert_eq!(next.headers["authorization"], format!("Bearer {bearer}"));
        assert_eq!(request.bytes(), frozen);
        let first: serde_json::Value = serde_json::from_slice(&sent[0].body).unwrap();
        let next: serde_json::Value = serde_json::from_slice(&next.body).unwrap();
        assert_eq!(first["instructions"], next["instructions"]);
        assert_eq!(first["tools"], next["tools"]);
        assert_eq!(first["input"], next["input"]);
        assert_eq!(first["prompt_cache_key"], next["prompt_cache_key"]);
        drop(sent);
        // No watch event means no reread. The former per-request auth.json
        // load fails this control even though source discovery was memoized.
        std::fs::remove_file(&path).unwrap();
        provider
            .generate(&request, dispatch)
            .try_collect::<Vec<_>>()
            .await?;
    }
    std::fs::write(path, auth("switched", "other").1.to_string()).unwrap();
    events.send_replace(3);
    let before = server.0.lock().unwrap().len();
    let error = provider
        .generate(&request, dispatch)
        .try_collect::<Vec<_>>()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("account changed"), "{error}");
    assert_eq!(server.0.lock().unwrap().len(), before);
    Ok(())
}
