use super::*;
use proptest::prelude::*;

proptest! {
    // Every backend/control cause remains recoverable beneath an Interrupted
    // classification, so ownership loss follows the control-stop path.
    #[test]
    fn control_failure_preserves_its_concrete_cause(code in 1i32..4096) {
        let failure = control_error(std::io::Error::from_raw_os_error(code));
        prop_assert_eq!(failure.kind(), std::io::ErrorKind::Interrupted);
        prop_assert_eq!(failure.get_ref()
            .and_then(|cause| cause.downcast_ref::<std::io::Error>())
            .and_then(std::io::Error::raw_os_error), Some(code));
    }
}
