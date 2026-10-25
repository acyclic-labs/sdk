use super::*;
use proptest::prelude::*;

fn page(storage: &mut [usize], pids: &[u32]) -> Page {
    let listed = pids.len().min(storage.len().saturating_sub(HEADER));
    for (slot, pid) in storage.iter_mut().skip(HEADER).zip(pids) {
        *slot = *pid as usize;
    }
    Page {
        assigned: pids.len(),
        listed,
        complete: listed == pids.len(),
    }
}

#[test]
fn a_kernel_prefix_cannot_stand_in_for_the_complete_job() -> io::Result<()> {
    let pids: Vec<_> = (1..=1025).collect();
    let observed = collect_members(|storage| Ok(page(storage, &pids)))?;
    eprintln!(
        "JOB_MEMBER_PAGE expected={} observed={}",
        pids.len(),
        observed.len()
    );
    assert_eq!(
        observed, pids,
        "all live members must reach custody, without a fixed prefix"
    );
    Ok(())
}

#[test]
fn inconsistent_counts_cannot_acknowledge_an_empty_job() {
    let result = collect_members(|_| {
        Ok(Page {
            assigned: 2,
            listed: 0,
            complete: true,
        })
    });
    // A short page must first negotiate its full allocation. A second page
    // still claiming no entries cannot serve as an empty-Job witness.
    eprintln!("JOB_MEMBER_INCONSISTENT result={result:?}");
    assert!(result.is_err());
}

#[test]
fn native_query_failure_is_never_an_empty_job() {
    let result = collect_members(|_| Err(io::Error::from_raw_os_error(5)));
    assert_eq!(
        result.expect_err("query must fail closed").raw_os_error(),
        Some(5)
    );
}

#[test]
fn concurrent_admission_requires_a_new_complete_page() -> io::Result<()> {
    let pids: Vec<_> = (1..=16).collect();
    let mut calls = 0;
    let result = collect_members(|storage| {
        calls += 1;
        let count = match calls {
            1 => 2,
            2 => 4,
            _ => 16,
        };
        Ok(page(
            storage,
            pids.get(..count).ok_or(io::ErrorKind::InvalidData)?,
        ))
    })?;
    assert_eq!(result, pids);
    assert_eq!(calls, 4);
    Ok(())
}

proptest! {
    #[test]
    fn every_accepted_page_contains_every_reported_member(pids in prop::collection::vec(1_u32..=u32::MAX, 0..8192)) {
        let observed = collect_members(|storage| Ok(page(storage, &pids)))?;
        prop_assert_eq!(observed, pids);
    }
}

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
