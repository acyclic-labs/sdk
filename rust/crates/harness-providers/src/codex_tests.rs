use super::*;
use crate::HttpResponse;
use acyclic_stream::BoxProviderFuture;
use bytes::Bytes;
use proptest::prelude::*;
use std::sync::{Arc, Mutex};

struct Refresh {
    status: u16,
    response: Value,
    sent: Mutex<Vec<HttpRequest>>,
}

impl HttpTransport for Refresh {
    fn delay(&self, wait: std::time::Duration) -> acyclic_stream::BoxProviderFuture<'static, ()> {
        crate::fixtures::delay(wait)
    }
    fn now(&self) -> std::time::Duration {
        crate::fixtures::now()
    }
    fn send(&self, request: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        Box::pin(async move {
            self.sent
                .lock()
                .map_err(|_| invalid("fixture poisoned"))?
                .push(request);
            tokio::task::yield_now().await;
            let chunk = Bytes::from(self.response.to_string());
            Ok(HttpResponse {
                retry_after: None,
                status: self.status,
                body: Box::pin(futures::stream::once(async { Ok(chunk) })),
            })
        })
    }
}

fn jwt(expiry: i64) -> String {
    format!(
        "header.{}.signature",
        URL_SAFE_NO_PAD.encode(json!({"exp":expiry}).to_string())
    )
}

fn auth(access: &str) -> Value {
    json!({"auth_mode":"chatgpt", "tokens":{"access_token":access,"id_token":"old-id",
        "refresh_token":"old-refresh","account_id":"account"},"agent_identity":{"preserve":true},
        "last_refresh":"2026-01-01T00:00:00Z"})
}

fn fixture(access: &str, status: u16) -> Result<(tempfile::TempDir, CodexLogin, Refresh)> {
    let root = tempfile::tempdir().map_err(|error| invalid(error.to_string()))?;
    let path = root.path().join("auth.json");
    std::fs::write(&path, auth(access).to_string()).map_err(|error| invalid(error.to_string()))?;
    let transport = Refresh {
        status,
        response: json!({"access_token":jwt(crate::codex::fixed_now().unix_timestamp() + 3600),
        "id_token":"new-id","refresh_token":"new-refresh"}),
        sent: Mutex::new(Vec::new()),
    };
    Ok((
        root,
        CodexLogin::from_path(path).with_clock(crate::codex::fixed_now),
        transport,
    ))
}

#[tokio::test]
async fn fresh_login_uses_local_bearer_without_refresh() -> Result<()> {
    let access = jwt(crate::codex::fixed_now().unix_timestamp() + 3600);
    let (root, login, transport) = fixture(&access, 200)?;
    let (bearer, account) = login.bearer(&transport, None, None).await?;
    assert_eq!(bearer, access);
    assert_eq!(account.as_deref(), Some("account"));
    assert!(
        transport
            .sent
            .lock()
            .map_err(|_| invalid("fixture poisoned"))?
            .is_empty()
    );
    assert!(root.path().join("auth.json").is_file());
    Ok(())
}

#[tokio::test]
async fn proactive_refresh_rotates_tokens_and_preserves_the_auth_document() -> Result<()> {
    for expiry in [0, crate::codex::fixed_now().unix_timestamp() + 299] {
        let (root, login, transport) = fixture(&jwt(expiry), 200)?;
        let (access, account) = login.bearer(&transport, None, None).await?;
        assert_eq!(access, transport.response["access_token"]);
        assert_eq!(account.as_deref(), Some("account"));
        let stored = crate::json(
            &std::fs::read(root.path().join("auth.json")).map_err(|e| invalid(e.to_string()))?,
        )?;
        assert_eq!(stored["agent_identity"], json!({"preserve":true}));
        assert_eq!(stored["tokens"]["refresh_token"], "new-refresh");
        assert_eq!(stored["tokens"]["id_token"], "new-id");
        assert_eq!(stored["tokens"]["account_id"], "account");
        assert!(
            OffsetDateTime::parse(stored["last_refresh"].as_str().unwrap_or(""), &Rfc3339).is_ok()
        );
        let requests = transport
            .sent
            .lock()
            .map_err(|_| invalid("fixture poisoned"))?;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://auth.openai.com/oauth/token");
        let body = crate::json(&requests[0].body)?;
        assert_eq!(body["client_id"], CLIENT_ID);
        assert_eq!(body["grant_type"], "refresh_token");
        assert_eq!(body["refresh_token"], "old-refresh");
    }
    Ok(())
}

#[tokio::test]
async fn shared_login_serializes_refresh_and_reloads_the_winner() -> Result<()> {
    refresh_pair(false, 200).await
}

#[tokio::test]
#[should_panic(expected = "two refreshes raced")]
async fn negative_control_detects_independent_locks_racing_the_scripted_token_endpoint() {
    refresh_pair(true, 200).await.expect("scripted refresh");
}

async fn refresh_pair(broken: bool, status: u16) -> Result<()> {
    let (_root, first, transport) = fixture(&jwt(0), status)?;
    let transport = Arc::new(transport);
    let mut second = first.fresh();
    assert!(crate::sync::Arc::ptr_eq(&first.refresh, &second.refresh));
    if broken {
        second.refresh = crate::sync::Arc::new(crate::refresh::Refresh::new());
    }
    // The scripted endpoint suspends after recording each request. Both
    // attempts therefore reach the shared guard before the first refresh returns.
    let (left, right) = futures::join!(
        first.bearer(transport.as_ref(), None, None),
        second.bearer(transport.as_ref(), None, None)
    );
    if status == 200 {
        assert_eq!(left?, right?);
    } else {
        let (left, right) = (left.unwrap_err(), right.unwrap_err());
        assert_eq!(left.code(), right.code());
        assert_eq!(left.to_string(), right.to_string());
        assert!(left.to_string().contains(&status.to_string()));
    }
    let requests = transport
        .sent
        .lock()
        .map_err(|_| invalid("fixture poisoned"))?;
    assert_eq!(requests.len(), 1, "two refreshes raced");
    assert_eq!(requests[0].url, "https://auth.openai.com/oauth/token");
    assert_eq!(
        crate::json(&requests[0].body)?["refresh_token"],
        "old-refresh"
    );
    Ok(())
}

#[path = "codex_failure_tests.rs"]
mod failures;
#[path = "codex_race_tests.rs"]
mod races;

#[tokio::test]
async fn unauthorized_bearer_refreshes_once_and_retains_omitted_tokens() -> Result<()> {
    let access = jwt(crate::codex::fixed_now().unix_timestamp() + 3600);
    let (root, login, mut transport) = fixture(&access, 200)?;
    transport.response = json!({"access_token":"updated-access"});
    let (updated, _) = login.bearer(&transport, Some(&access), None).await?;
    assert_eq!(updated, "updated-access");
    let stored = crate::json(
        &std::fs::read(root.path().join("auth.json")).map_err(|e| invalid(e.to_string()))?,
    )?;
    assert_eq!(stored["tokens"]["refresh_token"], "old-refresh");
    assert_eq!(stored["tokens"]["id_token"], "old-id");
    Ok(())
}

#[tokio::test]
async fn refresh_retains_access_token_when_only_other_tokens_are_rotated() -> Result<()> {
    let access = jwt(crate::codex::fixed_now().unix_timestamp() + 3600);
    let (root, login, mut transport) = fixture(&access, 200)?;
    transport.response = json!({"refresh_token":"rotated-refresh","id_token":"rotated-id"});
    assert_eq!(
        login.bearer(&transport, Some(&access), None).await?.0,
        access
    );
    let stored = crate::json(
        &std::fs::read(root.path().join("auth.json")).map_err(|e| invalid(e.to_string()))?,
    )?;
    assert_eq!(stored["tokens"]["refresh_token"], "rotated-refresh");
    assert_eq!(stored["tokens"]["id_token"], "rotated-id");
    Ok(())
}

#[tokio::test]
async fn revoked_refresh_and_account_changes_preserve_existing_credentials() -> Result<()> {
    for status in [400, 401, 403] {
        let (root, login, transport) = fixture(&jwt(0), status)?;
        let before =
            std::fs::read(root.path().join("auth.json")).map_err(|e| invalid(e.to_string()))?;
        let error = login
            .bearer(&transport, None, None)
            .await
            .expect_err("rejected refresh");
        assert!(matches!(
            error,
            acyclic_harness_core::Error::Unauthorized(_)
        ));
        assert!(error.to_string().contains(&status.to_string()));
        let requests = transport
            .sent
            .lock()
            .map_err(|_| invalid("fixture poisoned"))?;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url, "https://auth.openai.com/oauth/token");
        drop(requests);
        assert_eq!(
            before,
            std::fs::read(root.path().join("auth.json")).map_err(|e| invalid(e.to_string()))?
        );
        let mut changed = auth(&jwt(crate::codex::fixed_now().unix_timestamp() + 3600));
        changed["tokens"]["account_id"] = json!("another-account");
        std::fs::write(root.path().join("auth.json"), changed.to_string())
            .map_err(|e| invalid(e.to_string()))?;
        assert!(login.bearer(&transport, None, None).await.is_err());
    }
    Ok(())
}

#[test]
fn unreadable_expiry_uses_eight_day_fallback() {
    let now = OffsetDateTime::parse("2026-10-15T12:00:00Z", &Rfc3339)
        .unwrap_or(OffsetDateTime::UNIX_EPOCH);
    let mut auth = auth("opaque");
    auth["last_refresh"] = json!("2026-10-08T12:00:01Z");
    assert!(!needs_refresh(&auth, now));
    auth["last_refresh"] = json!("2026-10-07T12:00:00Z");
    assert!(needs_refresh(&auth, now));
    auth["last_refresh"] = json!("invalid");
    assert!(needs_refresh(&auth, now));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn accepted_auth_documents_have_a_lossless_canonical_codec(bytes in prop::collection::vec(any::<u8>(), 0..2048)) {
        if let Ok(auth) = decode_auth(&bytes) {
            let bytes = serde_json::to_vec(&auth).unwrap();
            prop_assert_eq!(decode_auth(&bytes).unwrap(), auth);
        }
    }

    #[test]
    fn unicode_credentials_and_unrelated_fields_survive_the_auth_codec(text in ".{0,128}") {
        let mut document = auth(&format!("key{text}"));
        document["agent_identity"] = json!({"arbitrary":text});
        prop_assert_eq!(decode_auth(&serde_json::to_vec(&document).unwrap()).unwrap(), document);
    }

    #[test]
    fn jwt_refresh_boundary_matches_the_signed_expiry(expiry in any::<i64>()) {
        prop_assert_eq!(needs_refresh(&auth(&jwt(expiry)), OffsetDateTime::UNIX_EPOCH), expiry <= 300);
    }

    #[test]
    fn jwt_claims_ignore_unrelated_unicode_payloads_without_losing_expiry(expiry in any::<i64>(), text in ".{0,128}") {
        let payload = json!({"exp":expiry,"extra":[{"text":text},[true,false,null]]});
        let access = format!("header.{}.signature", URL_SAFE_NO_PAD.encode(payload.to_string()));
        prop_assert_eq!(needs_refresh(&auth(&access), OffsetDateTime::UNIX_EPOCH), expiry <= 300);
    }

    #[test]
    fn native_auth_reads_preserve_unicode_documents_and_release_all_custody(text in ".{0,128}") {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        let mut document = auth("fixture-access");
        document["agent_identity"] = json!({"unrelated":text});
        let encoded = serde_json::to_vec(&document).unwrap();
        std::fs::write(&path, &encoded).unwrap();
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::performance::work_budget(100_000, 4_000_000),
            &acyclic_fs::CancellationToken::new(),
        ).unwrap();
        let baseline = work.live_allocation_bytes();
        {
            let mut custody = work.claim(0, 0, WorkCounters::UNCHARGED).unwrap();
            let mut io = CredentialIo::new(Some(&mut custody)).unwrap();
            let (bytes, decoded) = load(&path, &mut io).unwrap();
            prop_assert_eq!(&bytes, &encoded);
            prop_assert_eq!(decoded, document);
            prop_assert_eq!(work.receipt().source_bytes_read, bytes.len() as u64);
            prop_assert!(work.live_allocation_bytes() > baseline);
        }
        prop_assert_eq!(work.live_allocation_bytes(), baseline);
    }
}

#[test]
fn checked_in_auth_seed_exercises_an_accepted_document() {
    for seed in [
        include_bytes!("../fuzz/corpus/provider_stream/auth.json").as_slice(),
        include_bytes!("../fuzz/corpus/provider_stream/auth-jwt.json").as_slice(),
    ] {
        assert!(decode_auth(&seed[1..]).is_ok());
        crate::fuzzing::assert_stream_invariants(seed);
    }
}

#[tokio::test]
async fn failed_refreshes_share_the_typed_result_with_all_queued_callers() -> Result<()> {
    for status in [400, 401, 403, 429, 503] {
        refresh_pair(false, status).await?;
    }
    Ok(())
}

#[tokio::test]
#[should_panic(expected = "two refreshes raced")]
async fn negative_control_failed_refreshes_race_without_shared_ownership() {
    refresh_pair(true, 503).await.expect("scripted refresh");
}

#[tokio::test]
async fn a_new_caller_can_retry_a_previously_failed_attempt() -> Result<()> {
    let (_root, login, transport) = fixture(&jwt(0), 503)?;
    assert!(login.bearer(&transport, None, None).await.is_err());
    assert!(login.bearer(&transport, None, None).await.is_err());
    assert_eq!(transport.sent.lock().unwrap().len(), 2);
    Ok(())
}

#[tokio::test]
async fn injected_clock_drives_exact_expiry_boundaries_and_publication_time() -> Result<()> {
    for remaining in [299, 300, 301] {
        let (_root, login, transport) = fixture(
            &jwt(crate::codex::fixed_now().unix_timestamp() + remaining),
            200,
        )?;
        login.bearer(&transport, None, None).await?;
        assert_eq!(
            transport.sent.lock().unwrap().len(),
            usize::from(remaining <= 300)
        );
        if remaining <= 300 {
            let stored = crate::json(&std::fs::read(&login.path).unwrap())?;
            assert_eq!(
                stored["last_refresh"],
                crate::codex::fixed_now().format(&Rfc3339).unwrap()
            );
        }
    }
    Ok(())
}
