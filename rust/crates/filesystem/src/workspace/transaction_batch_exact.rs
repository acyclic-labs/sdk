use super::*;

fn check(invalid: bool, fault: Fault) -> TestCaseResult {
    let [(old, old_trace), (new, new_trace)] = pair(&[], invalid, fault);
    let expected: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("src/workspace/transaction_batch_expected.json"),
        )
        .unwrap(),
    )
    .unwrap();
    let expected = &expected[format!("staging.{invalid}")];
    let actual =
        serde_json::json!({ "work": new.2, "allocations": differential::allocation_counts(new.3) });
    prop_assert!(
        old.0 == new.0 && old.1 == new.1 && old_trace == new_trace && actual == *expected,
        "exact staging work/allocation differs: actual={actual}, expected={expected}"
    );
    Ok(())
}

#[test]
fn staging_has_exact_work_and_physical_allocations() {
    for invalid in [false, true] {
        check(invalid, Fault::None).unwrap();
    }
}

#[test]
fn restoring_serial_staging_fails_the_exact_guards() {
    for invalid in [false, true] {
        let error = check(invalid, Fault::Serial).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("exact staging work/allocation differs")
        );
    }
}
