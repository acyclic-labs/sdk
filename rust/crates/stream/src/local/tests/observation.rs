use super::*;

#[tokio::test]
async fn filtered_local_operations_preserve_caller() -> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let captured = acyclic_grpc_observability::test_support::Capture::default();
    let subscriber = tracing_subscriber::Registry::default()
        .with(captured.clone())
        .with(tracing_subscriber::filter::filter_fn(|metadata| {
            !metadata.name().starts_with("acyclic.stream")
        }));
    let _default = tracing::subscriber::set_default(subscriber);
    let parent = tracing::info_span!(
        "caller",
        outcome = "caller",
        error.kind = "caller",
        frames = 77,
        bytes = 88,
        batch_len = 99,
        rev = 66
    );
    async {
        let directory = tempfile::tempdir()?;
        let provider =
            LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let request = AppendRequest {
            path: StreamPath::new("traced")?,
            records: vec![OwnedBytes::from_static(b"record")],
            if_tail: None,
            idempotency_key: None,
        };
        provider.append(request).await?;
        provider.flush()?;
        assert!(
            provider
                .read(ReadRequest {
                    path: StreamPath::new("missing")?,
                    from: 0,
                    limit: 1
                })
                .await
                .is_err()
        );
        drop(provider);
        let reopened =
            LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(reopened.tail(StreamPath::new("traced")?).await?, 1);
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .instrument(parent)
    .await?;
    let fields = captured.fields("caller");
    for (field, value) in [
        ("outcome", "caller"),
        ("error.kind", "caller"),
        ("frames", "77"),
        ("bytes", "88"),
        ("batch_len", "99"),
        ("rev", "66"),
    ] {
        assert_eq!(fields[field], value);
    }
    Ok(())
}

#[tokio::test]
async fn local_follow_observes_lifetime_drop_and_late_error()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    for late_error in [false, true] {
        let directory = tempfile::tempdir()?;
        let provider =
            LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let path = StreamPath::new("follow")?;
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![OwnedBytes::from_static(b"record")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let captured = acyclic_grpc_observability::test_support::Capture::default();
        let mut records = tracing::subscriber::with_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
            || {
                let parent = obs::OperationSpan::new(tracing::info_span!("origin_parent"));
                parent.scope(async { provider.follow(path, 1).await })
            },
        )
        .await?;
        assert!(
            !captured
                .fields("acyclic.stream.follow")
                .contains_key("outcome")
        );
        assert_eq!(captured.closed("acyclic.stream.follow"), 0);
        let other = acyclic_grpc_observability::test_support::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let parent = tracing::info_span!("foreign_parent");
        let _entered = parent.enter();
        if late_error {
            provider.inner.poison();
            assert_eq!(
                records.next().await.unwrap().unwrap_err(),
                StreamError::Unavailable
            );
            assert!(records.next().await.is_none());
        } else {
            assert!(futures::poll!(records.next()).is_pending());
        }
        drop(records);
        let fields = captured.fields("acyclic.stream.follow");
        assert_eq!(
            fields["terminal"],
            if late_error { "error" } else { "dropped" }
        );
        if late_error {
            assert_eq!(fields["outcome"], "err");
            assert_eq!(fields["error.kind"], "unavailable");
        } else {
            assert!(!fields.contains_key("outcome"));
            assert!(!fields.contains_key("error.kind"));
        }
        assert_eq!(captured.closed("acyclic.stream.follow"), 1);
        assert_eq!(captured.closed("origin_parent"), 1);
        assert_eq!(other.closed("foreign_parent"), 0);
    }
    Ok(())
}

#[tokio::test]
async fn cancelled_local_read_records_error_and_closes_origin_parent()
-> Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::layer::SubscriberExt as _;
    let directory = tempfile::tempdir()?;
    let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
    let visibility = provider.inner.visibility.write().await;
    let captured = acyclic_grpc_observability::test_support::Capture::default();
    let future = tracing::subscriber::with_default(
        tracing_subscriber::Registry::default().with(captured.clone()),
        || {
            let parent = obs::OperationSpan::new(tracing::info_span!("origin_parent"));
            parent.scope(async {
                provider
                    .read(ReadRequest {
                        path: StreamPath::new("pending")?,
                        from: 0,
                        limit: 1,
                    })
                    .await
            })
        },
    );
    let mut future = Box::pin(future);
    assert!(futures::poll!(future.as_mut()).is_pending());
    let other = acyclic_grpc_observability::test_support::Capture::default();
    let _default = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default().with(other.clone()),
    );
    let parent = tracing::info_span!("foreign_parent");
    let _entered = parent.enter();
    drop(future);
    assert_eq!(
        captured.fields("acyclic.stream.read")["error.kind"],
        "cancelled"
    );
    assert_eq!(captured.closed("acyclic.stream.read"), 1);
    assert_eq!(captured.closed("origin_parent"), 1);
    assert_eq!(other.closed("foreign_parent"), 0);
    drop(visibility);
    Ok(())
}
