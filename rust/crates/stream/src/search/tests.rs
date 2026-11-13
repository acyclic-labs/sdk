use super::*;
use crate::{
    AppendOutcome, AppendRequest, IdempotencyKey, MemoryStream, StreamProviderExt as _,
    testing::Counted,
};

mod fixture {
    use crate as acyclic_stream;
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/support/session.rs"
    ));
}

async fn append<P: StreamProvider + ?Sized>(
    provider: &P,
    path: &str,
    values: Vec<crate::AppendRecord>,
) -> Result<(), StreamError> {
    for values in values.chunks(crate::MAX_ITEMS) {
        let result = provider
            .append(AppendRequest {
                path: StreamPath::new(path)?,
                records: values.to_vec(),
                if_tail: None,
                idempotency_key: IdempotencyKey::unique(),
            })
            .await?;
        assert!(matches!(result, AppendOutcome::Committed(_)));
    }
    Ok(())
}

#[tokio::test]
async fn native_search_keeps_original_cuts_custody_and_observer_failures()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::{Arc, atomic::Ordering};
    for durable in [false, true] {
        let directory = tempfile::tempdir()?;
        let provider: Arc<dyn crate::NativeStreamProvider> = if durable {
            Arc::new(
                crate::LocalStream::open(directory.path(), crate::LocalStreamLimits::default())
                    .await?,
            )
        } else {
            Arc::new(MemoryStream::default())
        };
        append(
            provider.as_ref(),
            "scope/history",
            vec!["needle original".into(), "unrelated".into()],
        )
        .await?;
        append(
            provider.as_ref(),
            "scope/worker/decisions",
            vec!["needle child".into()],
        )
        .await?;
        let cursor = Cursor::new(StreamPath::new("scope")?, b"needle".to_vec());
        let budget = NonZeroU32::new(2).ok_or(StreamError::InvalidArgument)?;
        let meter = Arc::new(crate::test_meter::Meter::default());
        let mut page = cursor
            .native_page(provider.as_ref(), budget, meter.original())
            .await??;
        append(
            provider.as_ref(),
            "scope/history",
            vec!["needle later".into()],
        )
        .await?;
        let mut found = Vec::new();
        loop {
            assert!(page.value().work <= budget.get());
            found.extend(page.value().matches.iter().map(|found| {
                (
                    found.path.as_str().to_owned(),
                    found.record.sequence,
                    found.record.value.to_vec(),
                )
            }));
            let Some(next) = page.value().next.as_ref() else {
                break;
            };
            let selected = next
                .native_page(provider.as_ref(), budget, meter.original())
                .await??;
            page = selected;
        }
        assert_eq!(
            found,
            vec![
                ("scope/history".into(), 0, b"needle original".to_vec()),
                ("scope/worker/decisions".into(), 0, b"needle child".to_vec()),
            ]
        );
        assert!(meter.allocations.load(Ordering::SeqCst) > 0);
        drop(page);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);

        let denied = Arc::new(crate::test_meter::Meter::default());
        denied.deny_allocation.store(true, Ordering::SeqCst);
        let error = cursor
            .native_page(provider.as_ref(), budget, denied.original())
            .await
            .err()
            .ok_or("allocation denial disappeared")?;
        assert!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<crate::test_meter::NativeDenial>())
                .is_some()
        );
        assert_eq!(denied.read_operations.load(Ordering::SeqCst), 0);
        assert_eq!(denied.held.load(Ordering::SeqCst), 0);

        let cancelled = Arc::new(crate::test_meter::Meter {
            cancel_after_read: 1,
            ..Default::default()
        });
        let cursor = Cursor::new(StreamPath::new("scope/history")?, b"needle".to_vec());
        let result = cursor
            .native_page(provider.as_ref(), budget, cancelled.original())
            .await;
        let error = result.err().ok_or("native read cancellation disappeared")?;
        assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
        assert!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<crate::test_meter::NativeDenial>())
                .is_some()
        );
        assert_eq!(cancelled.held.load(Ordering::SeqCst), 0);
        let replay = cursor
            .native_page(provider.as_ref(), budget, meter.original())
            .await??;
        assert_eq!(replay.value().matches[0].record.sequence, 0);
        drop(replay);
        assert_eq!(meter.held.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn history_search_is_bounded_replayable_and_excludes_concurrent_appends()
-> Result<(), StreamError> {
    let provider = MemoryStream::default();
    append(
        &provider,
        "session/history",
        (0..10_000)
            .map(|n| format!("decision {n}").into())
            .collect(),
    )
    .await?;
    let counted = Counted::new(provider.clone());
    let cursor = Cursor::new(StreamPath::new("session/history")?, b"decision".to_vec());
    let budget = NonZeroU32::new(64).ok_or(StreamError::InvalidArgument)?;
    let first = cursor.page(&counted, budget).await?;
    assert_eq!(first.work, 64);
    assert_eq!(first.matches.len(), 63);
    assert_eq!(
        counted.requests().data(),
        1,
        "first page opens exactly one bounded record read"
    );
    assert_eq!(
        counted.requests().control(),
        1,
        "one opening head pins the namespace cut"
    );
    let continuation = first.next.ok_or(StreamError::InvalidResponse)?;
    let before = counted.requests();
    let oversized = continuation
        .page(
            &counted,
            NonZeroU32::new((crate::MAX_ITEMS * 2) as u32).ok_or(StreamError::InvalidArgument)?,
        )
        .await?;
    assert_eq!(oversized.matches.len(), crate::MAX_ITEMS * 2);
    assert_eq!(counted.requests().since(before).data(), 2);
    append(
        &provider,
        "session/history",
        vec!["decision appended".into()],
    )
    .await?;
    append(
        &provider,
        "session/history/new-child",
        vec!["decision new child".into()],
    )
    .await?;
    let second = continuation.page(&provider, budget).await?;
    let replay = continuation.page(&provider, budget).await?;
    assert_eq!(second.matches, replay.matches);
    assert_eq!(second.matches.first().map(|m| m.record.sequence), Some(63));
    let mut count = first.matches.len() + second.matches.len();
    let mut next = second.next;
    while let Some(cursor) = next {
        let page = cursor.page(&provider, budget).await?;
        assert!(page.work <= budget.get());
        assert!(
            page.matches
                .iter()
                .all(|m| m.path.as_str() == "session/history" && m.record.sequence < 10_000)
        );
        count += page.matches.len();
        next = page.next;
    }
    assert_eq!(count, 10_000);
    Ok(())
}

#[tokio::test]
async fn subtree_search_finds_child_decisions_without_collecting_the_tree()
-> Result<(), StreamError> {
    let provider = MemoryStream::default();
    for path in [
        "root/history",
        "root/children/child/decisions",
        "root/logs",
        "other/decisions",
    ] {
        append(
            &provider,
            path,
            vec!["accepted patch".into(), "unrelated".into()],
        )
        .await?;
    }
    let budget = NonZeroU32::new(2).ok_or(StreamError::InvalidArgument)?;
    let mut next = Some(Cursor::new(StreamPath::new("root")?, b"accepted".to_vec()));
    let mut paths = Vec::new();
    let mut saw_empty_progress = false;
    while let Some(cursor) = next {
        let page = cursor.page(&provider, budget).await?;
        assert!(page.work <= 2);
        saw_empty_progress |= page.matches.is_empty() && page.next.is_some();
        for found in page.matches {
            assert_eq!(found.record.value.as_slice(), b"accepted patch");
            assert_eq!(found.record.sequence, 0);
            assert_eq!(found.record.origin, found.path);
            paths.push(found.path.as_str().to_owned());
        }
        next = page.next;
    }
    assert!(
        saw_empty_progress,
        "topology and nonmatches must yield bounded progress"
    );
    assert_eq!(
        paths,
        ["root/children/child/decisions", "root/history", "root/logs"]
    );
    Ok(())
}

#[tokio::test]
async fn search_uses_current_session_grants_even_for_a_retained_cursor()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use acyclic_auth::contract::{ResourceGrant, ResourcePattern};
    use std::sync::Arc;
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "root/allowed", vec!["visible decision".into()]).await?;
    append(&provider, "root/private", vec!["private decision".into()]).await?;
    let fixture = fixture::Fixture::with_options(
        provider,
        "search",
        fixture::Options {
            withhold_welcome: false,
            confirm_checked: true,
            tls: false,
        },
    )
    .await?;
    let client =
        crate::grpc::Client::from_channel(fixture._channel.clone(), "Bearer fixture".parse()?);
    let credential = |path: &str| {
        crate::session::testing::credential_with_grants(vec![ResourceGrant {
            resource: ResourcePattern {
                name: format!("streams:/{path}"),
                prefix: true,
            },
            actions: vec![
                "streams/list".into(),
                "streams/read".into(),
                "streams/follow".into(),
            ],
        }])
    };
    client
        .session_client()?
        .register_credential(0, credential("root/allowed"))?;
    let cursor = Cursor::new(StreamPath::new("root/allowed")?, b"decision".to_vec());
    let first = cursor
        .page(
            &client,
            NonZeroU32::new(1).ok_or(StreamError::InvalidArgument)?,
        )
        .await?;
    let continuation = first.next.ok_or(StreamError::InvalidResponse)?;
    let visible = continuation
        .page(
            &client,
            NonZeroU32::new(2).ok_or(StreamError::InvalidArgument)?,
        )
        .await?;
    assert_eq!(visible.matches.len(), 1);
    assert_eq!(
        visible.matches[0].record.value.as_slice(),
        b"visible decision"
    );
    assert!(matches!(
        Cursor::new(StreamPath::new("root/private")?, Vec::new())
            .page(
                &client,
                NonZeroU32::new(2).ok_or(StreamError::InvalidArgument)?
            )
            .await,
        Err(StreamError::AccessDenied)
    ));
    client
        .session_client()?
        .register_credential(0, credential("root/private"))?;
    assert!(matches!(
        continuation
            .page(
                &client,
                NonZeroU32::new(2).ok_or(StreamError::InvalidArgument)?
            )
            .await,
        Err(StreamError::AccessDenied)
    ));
    Ok(())
}
