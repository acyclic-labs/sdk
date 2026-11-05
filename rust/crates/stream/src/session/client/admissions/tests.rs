use super::*;

#[test]
fn home_rebinding_preserves_pending_memberships_without_allocating() {
    for count in [1, 1_000, 100_000] {
        let admissions = Admissions::new();
        for id in 0..count {
            admissions.insert(id).unwrap();
        }
        admissions.bind_home("previous").unwrap();
        let after = acyclic_test_support::measure(|| {
            admissions.bind_home("next").unwrap();
            assert!(admissions.interested("next"));
            assert!(!admissions.interested("previous"));
            assert!(!admissions.interested("unrelated"));
        });
        assert_eq!(
            after.count_total, 0,
            "home rebinding allocated per pending membership"
        );
        assert_eq!(after.bytes_total, 0);
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
            "pending={count} rebind_allocations={} bytes={}",
            after.count_total, after.bytes_total,
        );
    }
}
