use super::*;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn fragmented_unicode_growth_is_bounded_amortized_and_failure_atomic(
        maximum in 1usize..4096,
        fragments in prop::collection::vec(prop::collection::vec(any::<char>(), 0..16), 0..128),
    ) {
        let mut budget = Budget::new(maximum);
        let mut text = String::new();
        let mut expected = String::new();
        let mut relocated = 0;
        for fragment in fragments {
            let fragment: String = fragment.into_iter().collect();
            let before = (text.len(), text.capacity(), budget.used());
            let result = budget.append(&mut text, Some(&Value::String(fragment.clone())));
            if result.is_ok() {
                expected.push_str(&fragment);
                if text.capacity() != before.1 { relocated += before.0; }
            } else {
                prop_assert_eq!((text.len(), text.capacity(), budget.used()), before);
            }
            prop_assert_eq!(text.as_str(), expected.as_str());
            prop_assert_eq!(budget.used(), text.capacity());
            prop_assert!(text.capacity() <= maximum);
            prop_assert!(relocated <= 2 * text.capacity());
        }
    }

    #[test]
    fn arbitrary_replacements_preserve_bounds_and_fail_without_changing_the_balance(
        maximum in 1usize..4096,
        replacements in prop::collection::vec((any::<u16>(), any::<u16>(), any::<bool>()), 0..64),
    ) {
        let mut budget = Budget::new(maximum);
        for (remove_bytes, add_bytes, overflow) in replacements {
            let before = budget.used();
            let old = usize::from(remove_bytes) % (before + 2);
            let new = if overflow { usize::MAX } else { usize::from(add_bytes) % (maximum + 1) };
            let result = budget.replace(old, new);
            let after = budget.used();
            let bytes = before as i128 - old as i128 + new as i128;
            let admissible = old <= before && bytes <= maximum as i128;
            prop_assert_eq!(result.is_ok(), admissible);
            prop_assert!(after <= maximum);
            if result.is_err() {
                prop_assert_eq!(after, before);
            } else {
                prop_assert_eq!(after as i128 + old as i128, before as i128 + new as i128);
            }
        }
    }
}

#[test]
fn native_growth_charges_only_prefix_relocations() -> Result<()> {
    let cancel = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000, 1_000_000),
        &cancel,
    )
    .map_err(|(cause, work)| acyclic_harness_core::Error::NativeConstruction { cause, work })?;
    let mut budget = Budget::native(4096, &work);
    let mut text = String::new();
    let mut copied = 0;
    for _ in 0..1024 {
        let before = (text.len(), text.capacity());
        budget.append(&mut text, Some(&Value::String("x".into())))?;
        if text.capacity() != before.1 {
            copied += before.0;
        }
        assert_eq!(work.receipt().bytes_copied, copied as u64);
    }
    assert!(copied < 2 * text.len());
    let before = (text.len(), text.capacity(), budget.used());
    assert!(
        budget
            .append(&mut text, Some(&Value::String("x".repeat(4096))))
            .is_err()
    );
    assert_eq!((text.len(), text.capacity(), budget.used()), before);
    Ok(())
}
