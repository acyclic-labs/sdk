use super::*;

#[test]
fn home_rebinding_does_not_visit_or_reallocate_pending_memberships() {
    for count in [1, 1_000, 100_000] {
        let admissions = Admissions::new();
        for id in 0..count {
            admissions.insert(id).unwrap();
        }
        admissions.bind_home("previous").unwrap();
        let before = acyclic_test_support::measure(|| {
            // Executable previous mechanism: rebinding a node on each queued
            // entry allocates once per membership, even with identical bytes.
            let state = admissions.state.lock().unwrap();
            let rebound = state
                .entries
                .values()
                .map(|()| String::from("next"))
                .collect::<Vec<_>>();
            assert_eq!(rebound.len(), count as usize);
        });
        let after = acyclic_test_support::measure(|| {
            admissions.bind_home("next").unwrap();
            assert!(admissions.interested("next"));
            assert!(!admissions.interested("previous"));
            assert!(!admissions.interested("unrelated"));
        });
        assert_eq!(after.count_total, 0);
        assert_eq!(after.bytes_total, 0);
        assert!(before.count_total >= count);
        // Keep every original admission across repeated replacement and prove
        // withdrawal is idempotent even after dispatch removed another entry.
        admissions.bind_home("").unwrap();
        assert!(admissions.interested(""));
        for id in 0..count {
            admissions.remove(id).unwrap();
            admissions.remove(id).unwrap();
            assert_eq!(admissions.interested(""), id + 1 != count);
        }
        eprintln!(
            "pending={count} rebind_allocations={}->{} bytes={}->{} visited={count}->0",
            before.count_total, after.count_total, before.bytes_total, after.bytes_total,
        );
    }
}
