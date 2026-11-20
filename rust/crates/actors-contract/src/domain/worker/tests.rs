use super::*;

#[test]
fn worker_deletion_admission_borrows_original_fields() -> Result<(), DomainError> {
    let request =
        DeleteWorkerRequest::new(ActorNamespace::new("worker".into())?, "delete".repeat(32))?;
    let cost = acyclic_test_support::measure(|| request.validate_from_proto().unwrap());
    assert_eq!(cost.count_total, 0);
    for (length, admitted) in [(0, false), (1, true), (256, true), (257, false)] {
        let request =
            DeleteWorkerRequest::new(ActorNamespace::new("worker".into())?, "k".repeat(length));
        assert_eq!(request.is_ok(), admitted);
    }
    Ok(())
}
