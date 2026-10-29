use super::*;
use crate::{
    AppendRequest, IdempotencyKey, MemoryStream, ReadRequest, StreamPath, StreamProvider,
    StreamProviderExt as _,
};
use futures::StreamExt;

#[test]
fn ready_records_reuse_the_registered_source_node() {
    futures::executor::block_on(async {
        let provider = MemoryStream::default();
        let path = StreamPath::new("ready/allocation").unwrap();
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![b"committed".as_slice().into(); 32],
                if_tail: Some(0),
                idempotency_key: IdempotencyKey::new(b"ready allocation".to_vec()).unwrap(),
            })
            .await
            .unwrap();
        // Capture actual committed provider output before measuring scheduling.
        // Both schedulers transfer the same immutable records, without measuring
        // provider reads or setup allocations as scheduler work.
        let records: Vec<_> = provider
            .read(ReadRequest {
                path,
                from: 0,
                limit: 32,
                key: None,
                at_least: Default::default(),
            })
            .await
            .unwrap()
            .map(Result::unwrap)
            .collect()
            .await;
        assert_eq!(records.len(), 32);
        let mut shared = ReadySources::new();
        shared.register(
            wire::SessionChannel {
                channel_id: 0,
                generation: 1,
            },
            futures::stream::iter(records.clone()),
        );
        let mut recreated = futures::stream::SelectAll::new();
        recreated.push(futures::stream::iter(records));
        let mut cx = Context::from_waker(std::task::Waker::noop());
        // Warm allocator measurement before either arm.
        acyclic_test_support::measure(|| {});
        let mut before = 0;
        let mut after = 0;
        for sequence in 0..32 {
            let mut old = Poll::Pending;
            let allocation = acyclic_test_support::measure(|| {
                old = Pin::new(&mut recreated).poll_next(&mut cx);
            });
            before += allocation.count_total;
            let mut current = Poll::Pending;
            let allocation = acyclic_test_support::measure(|| {
                current = shared.poll_next(&mut cx);
            });
            after += allocation.count_total;
            let Poll::Ready(Some(old)) = old else {
                panic!("committed record must be ready");
            };
            let Poll::Ready(Ok(Some(current))) = current else {
                panic!("committed record must be ready");
            };
            assert_eq!(current, old);
            assert_eq!(current.sequence, sequence);
            assert_eq!(allocation.count_total, 0);
        }
        assert_eq!(
            before, 32,
            "recreated task must fail the zero-allocation bound"
        );
        assert_eq!(after, 0);
        eprintln!(
            "ready_source records=32 task_allocations_before={before} task_allocations_after={after}"
        );
    });
}
