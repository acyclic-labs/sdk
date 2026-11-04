use super::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn only_a_complete_consistent_zero_page_proves_the_job_empty(
        assigned in 0_usize..64, listed in 0_usize..64, complete in any::<bool>(), pid in any::<u32>()
    ) {
        // Retain the exact two termination regressions alongside every generated page.
        prop_assert!(first_reported_member(
            Page { assigned: 0, listed: 1, complete: true }, 17
        ).is_err(), "inconsistent member counts must not acknowledge termination");
        prop_assert!(first_reported_member(
            Page { assigned: 0, listed: 0, complete: false }, 0
        ).is_err(), "ERROR_MORE_DATA cannot witness an empty Job");
        let result = first_reported_member(Page { assigned, listed, complete }, pid as usize);
        if let Ok(member) = result {
            match member {
                None => prop_assert!(complete && assigned == 0 && listed == 0),
                Some(observed) => {
                    prop_assert!(assigned >= listed && listed == 1);
                    prop_assert!(observed != 0 && observed == pid);
                }
            }
        }
    }
}
