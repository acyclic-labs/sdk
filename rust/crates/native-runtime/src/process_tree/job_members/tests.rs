use super::*;
use proptest::prelude::*;

#[test]
fn a_zero_assigned_prefix_with_a_listed_pid_cannot_prove_empty() {
    let result = first_reported_member(
        Page {
            assigned: 0,
            listed: 1,
            complete: true,
        },
        17,
    );
    eprintln!("JOB_FIRST_INCONSISTENT result={result:?}");
    assert!(
        result.is_err(),
        "inconsistent member counts must not acknowledge termination"
    );
}

#[test]
fn an_incomplete_zero_member_page_cannot_prove_empty() {
    let result = first_reported_member(
        Page {
            assigned: 0,
            listed: 0,
            complete: false,
        },
        0,
    );
    eprintln!("JOB_FIRST_INCOMPLETE result={result:?}");
    assert!(
        result.is_err(),
        "ERROR_MORE_DATA cannot witness an empty Job"
    );
}

proptest! {
    #[test]
    fn only_a_complete_consistent_zero_page_proves_the_job_empty(
        assigned in 0_usize..64, listed in 0_usize..64, complete in any::<bool>(), pid in any::<u32>()
    ) {
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
