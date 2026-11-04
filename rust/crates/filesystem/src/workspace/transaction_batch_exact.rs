use super::*;

fn check(invalid: bool, fault: Fault) -> TestCaseResult {
    let [(old, old_trace), (new, new_trace)] = pair(&[], invalid, fault);
    equivalent(&old, &old_trace, &new, &new_trace, invalid)?;
    prop_assert!(
        new.2.backend_read_operations <= old.2.backend_read_operations
            && new.2.page_reads < old.2.page_reads
            && new.2.allocation_operations < old.2.allocation_operations
            && new.3.count_total < old.3.count_total,
        "batch staging repeated serial work: batch={:?}/{:?}, serial={:?}/{:?}",
        new.2,
        new.3,
        old.2,
        old.3
    );
    Ok(())
}

#[test]
fn staging_reuses_reads_and_physical_allocations() {
    let results = [false, true].map(|invalid| check(invalid, Fault::None));
    for result in &results {
        if let Err(error) = result {
            eprintln!("{error}");
        }
    }
    assert!(
        results.iter().all(Result::is_ok),
        "batch staging repeated serial work"
    );
}

#[test]
fn restoring_serial_staging_fails_the_same_resource_guards() {
    for invalid in [false, true] {
        let error = check(invalid, Fault::Serial).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("batch staging repeated serial work")
        );
    }
}
