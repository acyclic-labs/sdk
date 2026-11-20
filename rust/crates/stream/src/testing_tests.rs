pub use acyclic_stream_contract::testing::*;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn erased_provider_preserves_records_failures_and_request_budgets() {
        use crate::{
            AppendOutcome, AppendRequest, MemoryStream, ReadRequest, StreamCategory, StreamPath,
            StreamProvider, StreamProviderExt as _,
        };
        use futures::TryStreamExt as _;

        let counted = Counted::new(MemoryStream::default());
        let ledger = counted.ledger();
        let provider: Box<dyn StreamProvider> = Box::new(counted);
        let path = StreamPath::new("request-budget").unwrap();
        let result = crate::assert_requests!(
            ledger,
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![crate::OwnedBytes::from_static(b"value").into()],
                    if_tail: Some(0),
                    idempotency_key: crate::IdempotencyKey::new(crate::OwnedBytes::from_static(
                        b"request-budget"
                    ),)
                    .unwrap(),
                })
                .await,
            control = 0,
            data <= 1
        )
        .unwrap();
        assert!(matches!(result, AppendOutcome::Committed(_)));
        let records = crate::assert_requests!(
            ledger,
            async {
                provider
                    .read(ReadRequest {
                        key: None,
                        path,
                        from: 0,
                        limit: 2,
                        at_least: Default::default(),
                    })
                    .await?
                    .try_collect::<Vec<_>>()
                    .await
            }
            .await,
            control = 0,
            data <= 1
        )
        .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.as_ref(), b"value");
        let absent = StreamPath::new("absent").unwrap();
        let before = ledger.requests();
        assert_eq!(
            crate::assert_requests!(ledger, provider.tail(absent).await, control = 1, data = 0)
                .map_err(|error| error.category()),
            Err(StreamCategory::NotFound)
        );
        let requested = ledger.requests().since(before);
        assert_eq!(requested.class(RequestClass::Head), 1);
        assert_eq!(requested.auxiliary(Some(RequestClass::Head)), Some(0));

        // Listing is the requested data operation here. Its independent result
        // oracle stays intact; a method-class name cannot turn it into a probe.
        let before = ledger.requests();
        let list = || crate::ChildrenPageRequest {
            offset: 0,
            tail_stream: None,
            parent: None,
            limit: 8,
            after: None,
            hierarchy_version: None,
        };
        let page = provider.children_page(list()).await.unwrap();
        assert_eq!(page.children.len(), 1);
        assert_eq!(
            page.children[0].path,
            StreamPath::new("request-budget").unwrap()
        );
        let requested = ledger.requests().since(before);
        assert_eq!(requested.class(RequestClass::List), 1);
        assert_eq!(requested.auxiliary(Some(RequestClass::List)), Some(0));
        assert_eq!(requested.auxiliary(None), None);
        assert_eq!(
            requested.control(),
            1,
            "raw method classification is not requested semantics"
        );

        // Actual extra provider calls must remain visible, including one of
        // the same requested class and a failed auxiliary HEAD.
        provider.children_page(list()).await.unwrap();
        assert_eq!(
            provider
                .tail(StreamPath::new("absent").unwrap())
                .await
                .map_err(|error| error.category()),
            Err(StreamCategory::NotFound)
        );
        let extra = ledger.requests().since(before);
        assert_eq!(extra.class(RequestClass::List), 2);
        assert_eq!(extra.auxiliary(Some(RequestClass::List)), Some(1));

        // Bounded arithmetic state control, not a fabricated native fixture.
        let overflow = RequestLedger::new();
        overflow.0[RequestClass::Token as usize].store(u64::MAX - 1, Ordering::Relaxed);
        let before = overflow.requests();
        overflow.record(RequestClass::Token);
        let full = overflow.requests();
        overflow.record(RequestClass::Token);
        assert_eq!(overflow.requests().class(RequestClass::Token), u64::MAX);
        assert_eq!(full.since(before).auxiliary(Some(RequestClass::Data)), None);
        assert_eq!(
            overflow
                .requests()
                .since(full)
                .auxiliary(Some(RequestClass::Data)),
            None
        );
        assert_ne!(overflow.requests().since(full).control(), 0);
    }

    #[test]
    fn budgets_use_the_operation_delta_and_return_its_result() {
        let provider = Counted::new(());
        provider.record(RequestClass::Token);
        let value = crate::assert_requests!(
            provider,
            {
                provider.record(RequestClass::Data);
                42
            },
            control = 0,
            data <= 1
        );
        assert_eq!(value, 42);
        assert_eq!(provider.requests().class(RequestClass::Token), 1);
    }

    #[test]
    fn cloned_handles_share_all_request_classes() {
        let provider = Counted::new(());
        let clone = provider.clone();
        for class in [
            RequestClass::Metadata,
            RequestClass::List,
            RequestClass::Head,
            RequestClass::Lease,
            RequestClass::Token,
        ] {
            clone.record(class);
            assert_eq!(provider.requests().class(class), 1);
        }
        assert_eq!(provider.requests().control(), 5);
    }

    #[test]
    #[should_panic(expected = "request budget exceeded for control")]
    fn negative_control_rejects_a_hidden_head_request() {
        let provider = Counted::new(());
        crate::assert_requests!(
            provider,
            {
                provider.record(RequestClass::Data);
                provider.record(RequestClass::Head);
            },
            control = 0,
            data <= 1
        );
    }
}
