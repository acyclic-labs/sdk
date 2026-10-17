use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // The allocation hint follows the captured read cut, even when more records
    // are published before delivery. It never reflects subsequent appends.
    #[test]
    fn finite_read_hint_tracks_the_pinned_remaining_records(
        count in 1usize..140,
        offset in 0usize..140,
        maximum in 1u32..257,
        later in 1usize..8,
    ) {
        futures::executor::block_on(async {
            let provider = MemoryStream::default();
            let path = StreamPath::new("hint/read").unwrap();
            let append = |count| AppendRequest {
                path: path.clone(),
                records: vec![Bytes::from_static(b"record"); count],
                if_tail: None,
                idempotency_key: None,
            };
            provider.append(append(count)).await.unwrap();
            let from = offset.min(count);
            let expected = (count - from).min(maximum as usize);
            let mut records = provider.read(ReadRequest {
                path: path.clone(), from: from as u64, limit: maximum,
            }).await.unwrap();
            assert_eq!(records.size_hint().1, Some(expected));
            provider.append(append(later)).await.unwrap();
            for index in 0..expected {
                assert_eq!(records.size_hint().1, Some(expected - index));
                let record = records.next().await.unwrap().unwrap();
                assert_eq!(record.sequence, (from + index) as u64);
                assert_eq!(record.value.as_ref(), b"record");
            }
            assert_eq!(records.size_hint().1, Some(0));
            assert!(records.next().await.is_none());
            assert!(records.next().await.is_none());
            if expected < maximum as usize {
                // The former requested-limit hint violates this same bound.
                assert_ne!(Some(maximum as usize), Some(expected));
            }
        });
    }
}
