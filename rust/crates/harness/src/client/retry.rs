//! Error-triggered submission backoff; durability and ordinary updates use push follows.
use super::FileClientError;

pub(crate) fn submission_retry(error: Option<&FileClientError>, failures: &mut u32) -> Option<u32> {
    if !matches!(error, Some(FileClientError::Unavailable { .. })) {
        return None;
    }
    let delay = (10_u32 << (*failures).min(7)).min(1_000);
    *failures = failures.saturating_add(1);
    Some(delay)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn check(failures: u32, unavailable: bool, omit_retry: bool) -> bool {
        let error = if unavailable {
            FileClientError::Unavailable {
                reason: "injected".into(),
            }
        } else {
            FileClientError::Harness(crate::Error::Conflict("awaiting durability".into()))
        };
        let mut observed = failures;
        let actual = if omit_retry {
            None
        } else {
            submission_retry(Some(&error), &mut observed)
        };
        let mut expected = 10_u32;
        for _ in 0..failures.min(7) {
            expected = (expected * 2).min(1_000);
        }
        actual == unavailable.then_some(expected)
            && observed
                == if unavailable {
                    failures.saturating_add(1)
                } else {
                    failures
                }
    }
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn only_transport_failure_schedules_bounded_exponential_retries(failures in any::<u32>(), unavailable in any::<bool>()) {
            prop_assert!(check(failures, unavailable, false));
        }
    }
    #[test]
    fn negative_control_missing_failure_wakeup_is_shrunk_by_the_same_property() {
        use proptest::test_runner::{Config, TestError, TestRunner};
        let mut runner = TestRunner::new(Config {
            cases: 32,
            failure_persistence: None,
            ..Config::default()
        });
        let result = runner.run(&any::<u32>(), |failures| {
            prop_assert!(check(failures, true, true));
            Ok(())
        });
        let Err(TestError::Fail(_, failures)) = result else {
            panic!("omitted retry escaped");
        };
        assert_eq!(failures, 0);
        assert!(check(failures, true, false));
    }
}
