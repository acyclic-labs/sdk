use super::*;
use acyclic_stream::{AppendRecord, AppendRequest, MemoryStream};

#[tokio::test]
async fn failed_external_sources_notify_only_their_actor_without_stopping_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    for fault in [Some(StreamError::InvalidResponse), None] {
        let provider = Arc::new(MemoryStream::default());
        let source = StreamPath::new("external/source")?;
        provider
            .append(AppendRequest {
                path: source.clone(),
                records: vec![AppendRecord::from("seed")],
                if_tail: None,
                idempotency_key: IdempotencyKey::new(b"source".to_vec())?,
            })
            .await?;
        let records = acyclic_stream::records_only(
            provider
                .follow(acyclic_stream::FollowRequest::new(source.clone(), 1))
                .await?,
        );
        let expected = fault
            .clone()
            .unwrap_or(StreamError::Unavailable)
            .to_string();
        // Retain the real source selection while injecting transport failure or EOF.
        let records = records.map_cursor(|_| Box::pin(futures::stream::iter(fault.map(Err))));
        let runtime = Runtime::new(
            crate::guest::Requests::default(),
            Store::new(provider, StreamPath::new("external/actors")?),
            Arc::new(crate::conformance::Package::new()?),
            Arc::new(SystemClock),
        );
        let affected = ActorReference::new(b"account", b"agents", b"affected")?;
        let unrelated = ActorReference::new(b"account", b"agents", b"unrelated")?;
        let failed = runtime.delivery_failure(&affected)?;
        let mut healthy = runtime.delivery_failure(&unrelated)?;
        let selected = Selection {
            subscription: Subscription {
                path: source,
                cursor: 1,
                paused: false,
            },
            key: None,
        };
        let (_send, receive) = watch::channel(selected.clone());
        runtime
            .external
            .registrations
            .lock()
            .unwrap()
            .pending
            .push_back(Watch {
                actor: affected.clone(),
                receive,
                selected,
                next: 1,
                records: Some(records),
                wake: futures::task::noop_waker(),
            });
        let mut serving = Box::pin(runtime.run(|actor, _| assert_eq!(actor, &affected)));
        let cause = tokio::select! {
            result = serving.as_mut() => return Err(format!("shared runtime stopped: {result:?}").into()),
            cause = failed => cause,
        };
        assert!(cause.to_string().contains(&expected));
        assert!(futures::poll!(healthy.as_mut()).is_pending());
        tokio::select! {
            result = serving.as_mut() => return Err(format!("shared runtime stopped: {result:?}").into()),
            result = runtime.complete() => assert!(matches!(result, Err(Error::CompletionFailed(_)))),
        }
    }
    Ok(())
}
