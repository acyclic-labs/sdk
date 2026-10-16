use super::*;
use proptest::prelude::*;
use std::sync::Arc;

fn select_cached(
    count: u8,
    queries: &[u8],
    eager: bool,
) -> Result<(Vec<Option<u64>>, u64), Box<dyn std::error::Error>> {
    let values = (1..=count).map(entry).collect::<Result<Vec<_>, _>>()?;
    let keys = queries
        .iter()
        .copied()
        .map(name)
        .collect::<Result<Vec<_>, _>>()?;
    let cancellation = CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::performance::work_budget(65_536, 16 * 1024 * 1024),
        &cancellation,
    )
    .map_err(|(error, _)| error)?;
    let baseline = work.live_allocation_bytes();
    let bytes = (values.capacity() * std::mem::size_of::<AttributeEntry>()) as u64
        + values
            .iter()
            .map(AttributeFormat::value_owned_bytes)
            .sum::<u64>()
        + 128;
    let source_custody = Arc::new(work.claim(bytes, 1, WorkCounters::UNCHARGED)?);
    let page = Arc::new(Page::<AttributeFormat>::Leaf(values));
    let original = page.clone();
    let mut accounting = KernelAccounting::native(&work, &cancellation);
    let root = ObjectId {
        kind: ObjectKind::AttributePage,
        digest: Digest::from_bytes([1; 32]),
    };
    let mut machine = Machine::<AttributeFormat>::new(
        root,
        &keys,
        keys.len() as u32,
        DecodeLimits::default(),
        &mut accounting,
    )?;
    let request = machine.requests.value.pop().ok_or("missing frontier")?;
    let decoded = persistent_io::OwnedPage {
        page: persistent_io::PageLease::Shared {
            page,
            logical_bytes: bytes,
        },
        logical_bytes: 0,
        native_custody: Some(source_custody.clone()),
    };
    let allocation = accounting.claim_owned(0, 0)?;
    let start = work.receipt().items_examined;
    // Mutation: the former path clones the complete cache hit before selecting.
    let (decoded, allocation) = if eager {
        let ((page, bytes), allocation) =
            decoded.into_owned_with_accounting(allocation, &mut accounting)?;
        (
            persistent_io::OwnedPage {
                page: persistent_io::PageLease::Owned(page),
                logical_bytes: bytes,
                native_custody: None,
            },
            allocation,
        )
    } else {
        (decoded, allocation)
    };
    machine.accept(request, decoded, allocation, &mut accounting)?;
    let output = machine.finish(&mut accounting)?;
    let examined = work.receipt().items_examined - start;
    let result = output
        .value
        .iter()
        .map(|value| value.as_ref().map(|value| value.value_bytes))
        .collect();
    output.retire(&mut accounting)?;
    assert!(matches!(original.as_ref(), Page::Leaf(values) if values.len() == count as usize));
    drop((original, source_custody));
    assert_eq!(
        work.live_allocation_bytes(),
        baseline,
        "batch lookup leaked custody"
    );
    Ok((result, examined))
}
fn bound(count: u8, queries: usize) -> u64 {
    16 + queries as u64 * (16 + 2 * (u8::BITS - count.leading_zeros()) as u64)
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn cached_selection_matches_owned_results_and_releases_every_allocation(
        count in 32u8..=128, queries in prop::collection::vec(1u8..=160, 1..8),
    ) {
        let expected: Vec<_> = queries.iter().map(|&query| (query <= count).then_some(query as u64)).collect();
        let (borrowed, items) = select_cached(count, &queries, false).unwrap();
        let (copied, _) = select_cached(count, &queries, true).unwrap();
        prop_assert_eq!(&borrowed, &expected);
        prop_assert_eq!(copied, expected);
        prop_assert!(items <= bound(count, queries.len()), "cached batch lookup copied unrelated entries");
    }
}
#[test]
#[should_panic(expected = "cached batch lookup copied unrelated entries")]
fn negative_control_eager_page_clone_violates_the_lookup_work_bound() {
    let (_, items) = select_cached(128, &[63], true).unwrap();
    assert!(
        items <= bound(128, 1),
        "cached batch lookup copied unrelated entries"
    );
}
