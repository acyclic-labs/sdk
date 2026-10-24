use super::*;
use proptest::prelude::*;

fn finite_budget() -> crate::WorkBudget {
    let mut budget = serde_json::to_value(crate::WorkBudget::UNBOUNDED).unwrap();
    for value in budget.as_object_mut().unwrap().values_mut() {
        *value = serde_json::Value::from(100_000_000_u64);
    }
    serde_json::from_value(budget).unwrap()
}

#[test]
fn prepared_iterator_retains_vector_custody_until_its_backing_retires() {
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(finite_budget(), &cancellation).unwrap();
    let mut change = Change::new(&work).unwrap();
    change.put((RecordKey::ChildIndex, None).into()).unwrap();
    let prepared = PreparedChange::new(change, Some(&work)).unwrap();
    let live = work.live_allocation_bytes();
    assert!(live > crate::NativeMaterializationWork::accounting_state_bytes());
    let mut records = prepared.into_records();
    assert_eq!(work.live_allocation_bytes(), live);
    drop(records.next().unwrap());
    assert_eq!(work.live_allocation_bytes(), live);
    assert!(records.next().is_none());
    assert_eq!(work.live_allocation_bytes(), live);
    drop(records);
    assert_eq!(
        work.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
}

#[test]
fn negative_control_discarding_iterator_custody_is_detected() {
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(finite_budget(), &cancellation).unwrap();
    let mut change = Change::new(&work).unwrap();
    change.put((RecordKey::ChildIndex, None).into()).unwrap();
    let mut records = PreparedChange::new(change, Some(&work))
        .unwrap()
        .into_records();
    let live = work.live_allocation_bytes();
    drop(records._allocation.take());
    assert_eq!(records.records.as_slice().len(), 1);
    assert!(
        work.live_allocation_bytes() < live,
        "dropping the ticket while the vector remains live must fail the custody check"
    );
}

#[test]
fn retained_backing_borrows_without_transfer_and_rejects_unfunded_growth() {
    let cancellation = crate::CancellationToken::new();
    let width = std::mem::size_of::<u64>() as u64;
    let make_work = |elements| {
        crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                peak_allocation_bytes: crate::NativeMaterializationWork::accounting_state_bytes()
                    + elements * width,
                ..finite_budget()
            },
            &cancellation,
        )
        .unwrap()
    };
    let original = make_work(2);
    let borrower = make_work(3);
    let mut values = Vec::new();
    let mut held = None;
    reserve_retained_slots(&mut values, 2, &original, &mut held).unwrap();
    values.extend([11_u64, 13]);
    let original_live = original.live_allocation_bytes();
    let borrowed_before = borrower.receipt();
    reserve_retained_slots(&mut values, 2, &borrower, &mut held).unwrap();
    assert_eq!(borrower.receipt(), borrowed_before);
    assert_eq!(original.live_allocation_bytes(), original_live);
    assert!(
        held.as_ref()
            .unwrap()
            .work()
            .shares_allocation_context(&original)
    );
    assert!(reserve_retained_slots(&mut values, 3, &borrower, &mut held).is_err());
    assert_eq!(values, [11, 13]);
    assert_eq!(values.capacity(), 2);
    assert_eq!(borrower.receipt(), borrowed_before);
    assert_eq!(original.live_allocation_bytes(), original_live);
    drop(values);
    drop(held);
    assert_eq!(
        original.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
}

#[test]
fn refused_payload_adoption_keeps_the_pending_record_and_queue() {
    let mut residents = Residents::default();
    let key = RecordKey::Children(WorkspaceContextId::from_bytes([7; 16]));
    let children = (0_u128..128)
        .map(|id| WorkspaceContextId::from_bytes(id.to_le_bytes()))
        .collect();
    residents
        .install_loaded(
            (key, Some(RecordValue::Children(ContextChildren(children)))).into(),
            None,
        )
        .unwrap();
    let index = residents.position(key, None).unwrap().unwrap();
    let pending = residents.pending;
    let backing = residents.entries.capacity() * std::mem::size_of::<Entry>()
        + residents.index.nodes.capacity() * std::mem::size_of::<IndexNode>();
    let cancellation = crate::CancellationToken::new();
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            peak_allocation_bytes: crate::NativeMaterializationWork::accounting_state_bytes()
                + backing as u64,
            ..finite_budget()
        },
        &cancellation,
    )
    .unwrap();
    assert!(residents.adopt_native(&work).is_err());
    assert_eq!(residents.pending, pending);
    assert!(residents.entries[index].pending);
    assert!(residents.entries[index].record.allocation.is_none());
    assert!(
        matches!(&residents.get(index).unwrap().value, Some(RecordValue::Children(children)) if children.0.len() == 128)
    );
    let funded = crate::NativeMaterializationWork::new(finite_budget(), &cancellation).unwrap();
    residents.adopt_native(&funded).unwrap();
    assert_eq!(residents.pending, None);
    let before = funded.receipt();
    residents.adopt_native(&funded).unwrap();
    assert_eq!(funded.receipt(), before);
    drop(residents);
    assert_eq!(
        work.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    assert_eq!(
        funded.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
}

fn key(tag: u8, id: u128) -> RecordKey {
    let bytes = id.to_le_bytes();
    match tag {
        0 => RecordKey::Context(WorkspaceContextId::from_bytes(bytes)),
        1 => RecordKey::Children(WorkspaceContextId::from_bytes(bytes)),
        2 => RecordKey::ChildCount(WorkspaceContextId::from_bytes(bytes)),
        3 => RecordKey::ChildIndex,
        4 => RecordKey::Lineage(WorkspaceId::from_bytes(bytes)),
        _ => RecordKey::LazyWorkspace(WorkspaceId::from_bytes(bytes)),
    }
}

fn check_shape(index: &RecordIndex) {
    for node in &index.nodes {
        if let IndexNode::Branch { bit, children } = node {
            assert!(usize::from(*bit) < KEY_BITS);
            assert_ne!(children[0], children[1]);
            for child in children {
                if let IndexNode::Branch { bit: next, .. } = index.nodes[*child] {
                    assert!(
                        next > *bit,
                        "a path must consume a previously unexamined identity bit"
                    );
                }
            }
        }
    }
}

#[test]
fn every_identity_bit_can_separate_two_leaves() {
    for bit in 0..KEY_BITS {
        let mut index = RecordIndex::default();
        index.reserve(2, None).unwrap();
        let first = [0_u8; KEY_BYTES];
        let mut second = first;
        second[bit / 8] |= 1 << (7 - bit % 8);
        let mut examined = 0;
        index.insert_key(first, 0, &mut examined).unwrap();
        index.insert_key(second, 1, &mut examined).unwrap();
        assert_eq!(index.leaf(&first, &mut 0).unwrap(), Some((first, 0)));
        assert_eq!(index.leaf(&second, &mut 0).unwrap(), Some((second, 1)));
        check_shape(&index);
        assert_eq!(index.nodes.len(), 3);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn production_index_matches_a_complete_key_oracle(
        inputs in prop::collection::vec((0_u8..6, any::<u128>()), 1..128),
        absent in (0_u8..6, any::<u128>()),
    ) {
        let mut index = RecordIndex::default();
        let mut expected = BTreeMap::new();
        for (tag, id) in inputs {
            let record = key(tag, id);
            if !expected.contains_key(&record) {
                let position = expected.len();
                index.reserve(1, None).unwrap();
                let mut examined = 0;
                index.insert(record, position, &mut examined).unwrap();
                prop_assert!(examined <= 2 * (KEY_BITS + 1) as u64);
                expected.insert(record, position);
            }
            for (record, position) in &expected {
                let mut examined = 0;
                prop_assert_eq!(index.position(*record, &mut examined).unwrap(), Some(*position));
                prop_assert!(examined <= (KEY_BITS + 1) as u64);
            }
            check_shape(&index);
            prop_assert_eq!(index.nodes.len(), expected.len() * 2 - 1);
        }
        let record = key(absent.0, absent.1);
        prop_assert_eq!(index.position(record, &mut 0).unwrap(), expected.get(&record).copied());
    }

    #[test]
    fn dirty_epochs_and_pending_links_match_complete_identity(
        inputs in prop::collection::vec((0_u8..6, any::<u128>(), any::<bool>()), 1..128),
    ) {
        let mut residents = Residents::default();
        let mut epoch = 1_u128;
        let mut dirty = BTreeSet::new();
        let mut keys = BTreeSet::new();
        for (tag, id, checkpoint) in inputs {
            if checkpoint {
                epoch += 1;
                dirty.clear();
            }
            let record = key(tag, id);
            residents.reserve(1, None).unwrap();
            let actual = residents.install(PreparedResident::Ordinary((record, None).into()), Some(Uuid::from_u128(epoch)), &mut 0).unwrap();
            prop_assert_eq!(actual, dirty.insert(record));
            keys.insert(record);
            prop_assert_eq!(residents.len(), keys.len());
            let mut linked = BTreeSet::new();
            let mut next = residents.pending;
            while let Some(index) = next {
                prop_assert!(linked.insert(index), "a resident is linked at most once");
                prop_assert!(residents.entries[index].pending);
                next = residents.entries[index].next_pending;
            }
            prop_assert_eq!(linked.len(), keys.len());
        }
    }
}
