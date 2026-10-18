use proptest::prelude::*;

use acyclic_fuzz_support as oracle;

type Check = fn(&i16, Result<i16, ()>) -> Option<i16>;

fn contract(check: Check, expected: i16, decoded: Result<i16, ()>) -> bool {
    match std::panic::catch_unwind(|| check(&expected, decoded)) {
        Ok(Some(value)) => decoded == Ok(expected) && value == expected,
        Ok(None) => false,
        Err(_) => decoded != Ok(expected),
    }
}

proptest! {
    #[test]
    fn fuzz_round_trip_checks_the_complete_result(
        expected in any::<i16>(),
        decoded in prop_oneof![any::<i16>().prop_map(Ok), Just(Err(()))],
    ) {
        prop_assert!(contract(|expected, decoded| Some(oracle::round_trip(expected, decoded)), expected, decoded));
    }
}

#[test]
fn fuzz_round_trip_negative_controls() {
    let success_only: Check = |expected, decoded| {
        decoded
            .inspect(|value| {
                assert_eq!(value, expected);
            })
            .ok()
    };
    assert!(!contract(success_only, 1, Err(())));
    assert!(!contract(|_, decoded| decoded.ok(), 1, Ok(2)));
}

proptest! {
    #[test]
    fn complete_result_variants_preserve_their_contracts(
        expected in any::<i16>(),
        decoded in prop_oneof![any::<i16>().prop_map(Ok), Just(Err(()))],
        projection in prop_oneof![any::<i16>().prop_map(Ok), Just(Err(()))],
        accepted in any::<bool>(),
    ) {
        let required = std::panic::catch_unwind(|| oracle::must_decode(decoded));
        prop_assert_eq!(required.is_ok(), decoded.is_ok());
        if let Ok(value) = required { prop_assert_eq!(Ok(value), decoded); }
        let rejected = std::panic::catch_unwind(|| oracle::must_reject(decoded));
        prop_assert_eq!(rejected.is_ok(), decoded.is_err());
        let acceptance = std::panic::catch_unwind(|| oracle::accepts(accepted, decoded));
        prop_assert_eq!(acceptance.is_ok(), accepted == decoded.is_ok());
        if let Ok(value) = acceptance { prop_assert_eq!(value, decoded.ok()); }
        let exact = std::panic::catch_unwind(|| oracle::same_result(&Ok(expected), decoded));
        prop_assert_eq!(exact.is_ok(), decoded == Ok(expected));
        let exact_error = std::panic::catch_unwind(|| oracle::same_result(&Err(()), decoded));
        prop_assert_eq!(exact_error.is_ok(), decoded.is_err());
        let optional = std::panic::catch_unwind(|| oracle::round_trip_option(&expected, decoded.ok()));
        prop_assert_eq!(optional.is_ok(), decoded == Ok(expected));
        let projected = std::panic::catch_unwind(|| {
            oracle::round_trip_by(&expected, decoded, |_| projection)
        });
        prop_assert_eq!(projected.is_ok(), decoded.is_ok() && projection == Ok(expected));
        if let Ok(value) = projected { prop_assert_eq!(Ok(value), decoded); }
    }

    #[test]
    fn raw_input_views_cannot_admit_foreign_buffers(
        bytes in prop::collection::vec(any::<u8>(), 0..65),
        a in any::<usize>(), b in any::<usize>(),
    ) {
        let input = oracle::test_input(&bytes);
        let (a, b) = (a % (bytes.len() + 1), b % (bytes.len() + 1));
        let (start, end) = (a.min(b), a.max(b));
        let view = input.slice(start..end).expect("bounded subslice");
        prop_assert_eq!(view.bytes(), bytes.get(start..end).expect("bounded subrange"));
        prop_assert_eq!(input.view(view.bytes()).map(oracle::FuzzInput::bytes), Some(view.bytes()));
        let foreign = if bytes.is_empty() { vec![0] } else { bytes.clone() };
        prop_assert!(input.view(&foreign).is_none(), "foreign allocation became raw fuzzer input");
    }
}
