use super::*;
use acyclic_test_support::differential::oracle::Oracle;
use proptest::prelude::*;

#[path = "isolation_fixture.rs"]
mod fixture;
use fixture::*;

#[derive(Clone, Copy, Debug)]
struct Case {
    child: u8,
    readers: u16,
    revision: u8,
    result: bool,
    violation: Option<Violation>,
}

#[derive(Debug)]
enum Fault {
    CheckOriginal,
    RejectAll,
}

fn oracle() -> Oracle<Case, Fault, (bool, bool)> {
    let mut builder = Oracle::declare(|input: &Case, fault, see| {
        // Only the input decides acceptance, before the production call runs.
        let expected = input.violation.is_none();
        let original = valid_seed(input.child, input.readers, input.revision, input.result);
        let mut candidate = original.clone();
        if let Some(violation) = input.violation {
            violate(&mut candidate, violation);
        }
        see.compare(
            expected,
            || {
                let submitted = if matches!(fault, Some(Fault::CheckOriginal)) {
                    &original
                } else {
                    &candidate
                };
                submitted.validate(&mut None).is_ok() && !matches!(fault, Some(Fault::RejectAll))
            },
            |expected, actual| (*actual, expected),
        )?;
        Ok(())
    })
    .invariant("isolation", |(actual, expected)| {
        if actual == expected {
            Ok(())
        } else {
            Err("fork isolation acceptance differs from input".into())
        }
    })
    .control(
        "isolation",
        Case {
            child: 7,
            readers: 0,
            revision: 9,
            result: false,
            violation: None,
        },
        Fault::RejectAll,
    );
    for readers in [0, u16::MAX] {
        for result in [false, true] {
            for violation in VIOLATIONS {
                builder = builder.control(
                    "isolation",
                    Case {
                        child: 7,
                        readers,
                        revision: 9,
                        result,
                        violation: Some(violation),
                    },
                    Fault::CheckOriginal,
                );
            }
        }
    }
    builder
        .control(
            "isolation",
            Case {
                child: 7,
                readers: 0xaaaa,
                revision: 9,
                result: true,
                violation: None,
            },
            Fault::RejectAll,
        )
        .build()
}

#[test]
fn fork_seed_preserves_child_and_parent_isolation() {
    let oracle = oracle();
    proptest!(ProptestConfig::with_cases(64), |(
        child in 0..16_u8, readers in any::<u16>(), revision in 1..64_u8
    )| {
        for result in [false, true] {
            let input = Case { child, readers, revision, result, violation: None };
            oracle.check(&input);
            for violation in VIOLATIONS {
                oracle.check(&Case { violation: Some(violation), ..input });
            }
        }
    });
}
