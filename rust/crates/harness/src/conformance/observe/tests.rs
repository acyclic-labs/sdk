use super::*;
use crate::conformance::{Result, fixtures, vectors};
use acyclic_actors::guest::Handler;
use acyclic_harness_core::agent::Delivered;
use acyclic_stream::testing::Counted;

#[test]
fn counter_only_mutation_keeps_the_provider_allocation_budget() {
    fn admitted(provider: &dyn StreamProvider) -> acyclic_test_support::AllocationInfo {
        let request = WriteRequest::Append(AppendRequest {
            path: StreamPath::new("unpolled-counter-budget").unwrap(),
            records: vec![OwnedBytes::from_static(b"value").into()],
            if_tail: Some(0),
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"counter-budget"))
                .unwrap(),
        });
        let mut pending = None;
        let allocations = acyclic_test_support::measure(|| pending = Some(provider.write(request)));
        drop(pending);
        allocations
    }

    let direct = MemoryStream::default();
    let observed = Observed::counters(Arc::new(MemoryStream::default()));
    let baseline = admitted(&direct);
    assert!(baseline.count_total > 0);
    assert_eq!(
        admitted(&observed),
        baseline,
        "counter-only mutation added an allocation"
    );
}

#[tokio::test]
async fn counter_only_keeps_effects_and_dispatch_counts_without_request_retention() -> Result {
    let mut samples = Vec::new();
    for capture in [true, false] {
        let inner = Counted::new(MemoryStream::default());
        let dispatched = inner.ledger();
        let provider = Arc::new(if capture {
            Observed::capturing(Arc::new(inner))
        } else {
            Observed::counters(Arc::new(inner))
        });
        let handler: Arc<dyn Handler> = vectors::handler(&provider)?;
        let host = vectors::hosting(provider.clone(), handler, None)?;
        let root = host
            .create(&vectors::intent("counter-mode")?, fixtures::adopted())
            .await?;
        vectors::drain(&host).await?;
        assert!(matches!(
            host.send(&root, &fixtures::sender(), &fixtures::request("one")?)
                .await?,
            Delivered::Committed { .. }
        ));
        vectors::drain(&host).await?;
        assert_eq!(
            vectors::values(&*provider, root.conversation()?).await?,
            [b"one".to_vec()]
        );
        assert_eq!(host.runtime().counts(), (1, 0));
        assert_eq!(
            provider.tail(StreamPath::new("absent")?).await,
            Err(StreamError::NotFound)
        );
        let requests = provider.requests.requests();
        assert_eq!(requests, dispatched.requests(), "unreported provider call");
        assert!(requests.data() > 0);
        assert!(requests.control() > 0, "failed calls must remain counted");
        samples.push(requests);
        if capture {
            let retained = provider.take_commits().len()
                + provider.take_appends().len()
                + provider.take_follows().len();
            assert!(retained > 0);
            assert!(provider.history_reads.load(Ordering::SeqCst) > 0);
        } else {
            assert!(
                provider.capture.is_none(),
                "counter-only observation retained a request"
            );
            assert_eq!(provider.calls(), 0);
            assert_eq!(provider.history_reads.load(Ordering::SeqCst), 0);
        }
    }
    assert_eq!(samples[0], samples[1], "capture changed dispatch work");
    Ok(())
}
