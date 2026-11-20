use super::*;
use futures::StreamExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};
use {
    acyclic_fs::{CancellationToken, NativeMaterializationWork},
    acyclic_fs_work::performance::WorkCounters,
};

pub(super) fn native_server(
    inner: Arc<Server>,
    expected_work: &NativeMaterializationWork,
) -> (Arc<crate::fixtures::ScriptedHttp>, Arc<AtomicUsize>) {
    let sent = Arc::new(AtomicUsize::new(0));
    let observed = sent.clone();
    let expected_work = expected_work.clone();
    let transport = crate::fixtures::ScriptedHttp::respond(|_| {
        panic!("native login must not fall back to ordinary HTTP");
    })
    .with_native(true, move |request, work| {
        assert!(work.shares_allocation_context(&expected_work));
        let sent = observed.clone();
        let inner = inner.clone();
        acyclic_fs::NativeFutureBundle::new(work, move || async move {
            sent.fetch_add(1, Ordering::SeqCst);
            assert!(
                work.receipt()?.source_bytes_read > 0,
                "credential reads use this ledger"
            );
            let (status, body) = inner.reply(request)?;
            HttpResponse::with_native_body(
                status,
                futures::stream::iter([Ok(Bytes::from(body))]),
                work,
            )
        })
    });
    (Arc::new(transport), sent)
}

#[tokio::test]
async fn native_login_funds_credentials_proactive_refresh_and_unauthorized_recovery() -> Result<()>
{
    let (root, login, inner, _) = fixture(vec![
        (200, json!({"access_token":"proactive-access"}).to_string()),
        (401, String::new()),
        (200, json!({"access_token":"recovered-access"}).to_string()),
        (200, RESPONSE.into()),
    ])?;
    let path = root.path().join("auth.json");
    let mut auth = acyclic_harness_provider_response::json(
        &std::fs::read(&path).expect("fixture credentials"),
    )?;
    auth["tokens"]["access_token"] = json!(format!(
        "header.{}.signature",
        URL_SAFE_NO_PAD.encode(json!({"exp":0}).to_string())
    ));
    std::fs::write(&path, auth.to_string()).expect("expired fixture credentials");
    let work = NativeMaterializationWork::new(
        acyclic_fs_work::performance::work_budget(65_536, 64 * 1024 * 1024),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes()?;
    let (server, sent) = native_server(inner.clone(), &work);
    let provider = Registry::with_transport(Credentials::explicit(BTreeMap::new()), server.clone())
        .with_codex_login(login)
        .resolve("openai/gpt-5")?;
    let (request, dispatch) = crate::tests::request(&provider)?;
    {
        let mut stream = provider
            .generate_with_native_work(&request, dispatch, &work)?
            .await?;
        let mut completed = false;
        while let Some(event) = stream.next().await {
            let (event, custody) = event?;
            assert!(custody.work().shares_allocation_context(&work));
            completed |= matches!(
                event,
                acyclic_harness_core::model::ModelEvent::Completed { .. }
            );
            drop((event, custody));
        }
        assert!(completed);
    }
    assert_eq!(sent.load(Ordering::SeqCst), 4);
    let mut sent = inner.requests.lock().expect("fixture requests");
    assert_eq!(sent[0].url, "https://auth.openai.com/oauth/token");
    assert_eq!(sent[2].url, "https://auth.openai.com/oauth/token");
    assert_eq!(
        sent[1].body, sent[3].body,
        "401 retries the same prepared body"
    );
    assert_eq!(sent[3].headers["authorization"], "Bearer recovered-access");
    sent.clear();
    assert_eq!(work.receipt()?.durability_operations, 2);
    assert!(work.receipt()?.backend_read_operations > 0);
    assert_eq!(work.live_allocation_bytes()?, baseline);
    Ok(())
}

#[tokio::test]
async fn unfunded_credential_reads_stop_before_reading_or_http() -> Result<()> {
    for exhausted_memory in [false, true] {
        let (_root, login, inner, _) = fixture(Vec::new())?;
        let mut budget = acyclic_fs_work::performance::work_budget(100_000, 4_000_000);
        if exhausted_memory {
            budget.peak_allocation_bytes =
                NativeMaterializationWork::accounting_state_bytes() + 1024;
        } else {
            budget.source_bytes_read = 0;
        }
        let work = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes()?;
        let mut custody = work.claim(0, 0, WorkCounters::UNCHARGED)?;
        let error = login
            .bearer(inner.transport().as_ref(), None, Some(&mut custody))
            .await
            .expect_err("unfunded credential IO");
        drop((error, custody));
        assert_eq!(work.receipt()?.source_bytes_read, 0);
        assert!(inner.requests.lock().expect("fixture requests").is_empty());
        assert_eq!(work.live_allocation_bytes()?, baseline);
    }
    Ok(())
}
