use super::*;
use crate::CancellationToken;
use crate::foundation::Digest;
use proptest::prelude::*;

fn binding(parent: u8, name: &[u8]) -> Result<Binding, super::super::TreePageError> {
    Ok(Binding {
        parent: FileId::from_bytes([parent; 16]),
        name: LogicalName::new(super::super::NameEncoding::Utf8, name.to_vec())?,
    })
}

fn encode(
    page: &BindingPage,
    accounting: &mut KernelAccounting<'_>,
) -> Result<(Vec<u8>, KernelOwnedAllocation), FsError> {
    let page = match page {
        Page::Leaf(items) => PageRef::Leaf(items),
        Page::Internal(items) => PageRef::Internal(items),
    };
    BindingFormat::encode_with_accounting(&page, 64, accounting)
}

#[test]
fn binding_pages_preserve_aliases_sealed_children_and_original_credit()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::default();
    let mut accounting = KernelAccounting::standalone(
        crate::performance::work_budget(u64::MAX, u64::MAX),
        &cancellation,
    );
    let leaf = Page::Leaf(vec![
        binding(1, b"a")?,
        binding(1, b"b")?,
        binding(2, b"a")?,
    ]);
    let (bytes, guard) = encode(&leaf, &mut accounting)?;
    let shape = BindingFormat::decode_shape(&bytes, DecodeLimits::default())?;
    assert_eq!(shape.items, 3);
    assert_eq!(shape.nested_bytes, 3);
    let live_before = accounting
        .standalone_parts()
        .ok_or(FsError::InvalidDiff)?
        .0
        .live_bytes();
    let (decoded, allocation) =
        super::super::file_read::decode_owned(&mut accounting, |observe| {
            decode_binding_page(&bytes, DecodeLimits::default(), &mut |step| observe(step))
        })?;
    assert_eq!(decoded, leaf);
    let logical_bytes = accounting
        .standalone_parts()
        .ok_or(FsError::InvalidDiff)?
        .0
        .live_bytes()
        .checked_sub(live_before)
        .ok_or(crate::WorkError::Overflow)?;
    super::super::persistent_point::Owned::new(decoded, allocation, logical_bytes)
        .retire(&mut accounting)?;
    let length = usize_to_u64(bytes.capacity());
    drop(bytes);
    super::super::persistent_point::retire_allocation(guard, length, &mut accounting)?;

    let child = ObjectId {
        kind: ObjectKind::BindingPage,
        digest: Digest::from_bytes([7; 32]),
        storage: Some(crate::ObjectStorage {
            locator: Digest::from_bytes([8; 32]),
            root_file_id: FileId::from_bytes([9; 16]),
            origin_generation: Digest::from_bytes([10; 32]),
        }),
    };
    let internal = Page::Internal(vec![Child {
        first: binding(1, b"a")?,
        page: child,
    }]);
    let (bytes, guard) = encode(&internal, &mut accounting)?;
    let mut references = Vec::new();
    super::super::visit_object_references(
        ObjectId {
            kind: ObjectKind::BindingPage,
            digest: Digest::from_bytes([11; 32]),
            storage: None,
        },
        &bytes,
        &mut accounting,
        |reference| {
            references.push(reference);
            Ok(())
        },
    )?;
    assert_eq!(references, [child]);
    let decoded = decode_binding_page(&bytes, DecodeLimits::default(), &mut |_| {
        Ok::<_, CanonicalDecodeError>(())
    })?;
    assert_eq!(decoded, internal);
    assert_eq!(
        BindingFormat::decode_shape(&bytes, DecodeLimits::default())?.kind,
        DecodedPageKind::Internal
    );
    let length = usize_to_u64(bytes.capacity());
    drop(bytes);
    super::super::persistent_point::retire_allocation(guard, length, &mut accounting)?;
    assert_eq!(
        accounting
            .standalone_parts()
            .ok_or(FsError::InvalidDiff)?
            .0
            .live_bytes(),
        0,
    );
    for counter in ["items_examined", "bytes_encoded", "peak_allocation_bytes"] {
        let mut budget = crate::performance::work_budget(u64::MAX, u64::MAX);
        match counter {
            "items_examined" => budget.items_examined = 0,
            "bytes_encoded" => budget.bytes_encoded = 0,
            _ => budget.peak_allocation_bytes = 0,
        }
        let mut denied = KernelAccounting::standalone(budget, &cancellation);
        assert!(matches!(
            encode(&leaf, &mut denied),
            Err(FsError::Work(crate::WorkError::BudgetExceeded { counter: actual, .. })) if actual == counter
        ));
        assert_eq!(
            denied
                .standalone_parts()
                .ok_or(FsError::InvalidDiff)?
                .0
                .live_bytes(),
            0
        );
    }
    cancellation.cancel();
    let mut canceled = KernelAccounting::standalone(
        crate::performance::work_budget(u64::MAX, u64::MAX),
        &cancellation,
    );
    assert!(matches!(
        encode(&leaf, &mut canceled),
        Err(FsError::Cancelled(_))
    ));
    assert_eq!(canceled.spent(), WorkCounters::UNCHARGED);
    Ok(())
}

proptest! {
    #[test]
    fn canonical_binding_pages_reject_prefixes_duplicates_and_wrong_children(
        values in prop::collection::vec((any::<[u8; 16]>(), prop::collection::vec(b'a'..=b'z', 1..12)), 0..32),
    ) {
        let mut entries = values.into_iter().map(|(parent, name)| Binding {
            parent: FileId::from_bytes(parent),
            name: LogicalName::new(super::super::NameEncoding::Utf8, name).unwrap(),
        }).collect::<Vec<_>>();
        entries.sort();
        entries.dedup();
        let page = Page::Leaf(entries);
        let cancellation = CancellationToken::default();
        let mut accounting = KernelAccounting::standalone(crate::performance::work_budget(u64::MAX, u64::MAX), &cancellation);
        let (bytes, guard) = encode(&page, &mut accounting).unwrap();
        let decoded = decode_binding_page(&bytes, DecodeLimits::default(), &mut |_| Ok::<_, CanonicalDecodeError>(())).unwrap();
        prop_assert_eq!(&decoded, &page);
        for end in 0..bytes.len() {
            prop_assert!(decode_binding_page(&bytes[..end], DecodeLimits::default(), &mut |_| Ok::<_, CanonicalDecodeError>(())).is_err());
        }
        let mut trailing = bytes.clone();
        trailing.push(0);
        prop_assert!(decode_binding_page(&trailing, DecodeLimits::default(), &mut |_| Ok::<_, CanonicalDecodeError>(())).is_err());
        let length = usize_to_u64(bytes.capacity());
        drop(bytes);
        super::super::persistent_point::retire_allocation(guard, length, &mut accounting).unwrap();
        if let Page::Leaf(mut entries) = page {
            if let Some(first) = entries.first().cloned() {
                entries.insert(0, first);
                prop_assert!(encode(&Page::Leaf(entries), &mut accounting).is_err());
            }
        }
        let wrong = Page::Internal(vec![Child {
            first: binding(1, b"a").unwrap(),
            page: ObjectId { kind: ObjectKind::TreePage, digest: Digest::from_bytes([1; 32]), storage: None },
        }]);
        prop_assert!(encode(&wrong, &mut accounting).is_err());
        let empty = Page::Internal(Vec::new());
        prop_assert!(encode(&empty, &mut accounting).is_err());
    }
}
