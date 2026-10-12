use super::*;

#[test]
fn native_attribute_rejects_deep_source_before_schema_compilation_or_copy() {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let cancellation = CancellationToken::new();
    let budget = WorkBudget {
        allocation_operations: 1024,
        peak_allocation_bytes: 1_048_576,
        bytes_copied: 1_048_576,
        bytes_encoded: 1_048_576,
        items_examined: 8192,
        ..WorkBudget::default()
    };
    let mut value = Value::Null;
    for _ in 0..257 {
        value = Value::Array(vec![value]);
    }
    let mut attribute = ContextAttribute {
        type_name: "application:state".into(),
        type_revision: "1".into(),
        schema: Value::Bool(true),
        state_revision: "1".into(),
        value,
    };
    attribute
        .validate(&mut None)
        .expect("ordinary deep attribute remains legal");
    let work = NativeMaterializationWork::new(budget, &cancellation).expect("original ledger");
    let mut observer = crate::store::history::native_work_io(&work).expect("original observer");
    assert!(attribute.validate(&mut Some(&mut observer)).is_err());
    attribute.value = Value::Array(vec![Value::Null]);
    let control = NativeMaterializationWork::new(budget, &cancellation).expect("control ledger");
    let mut observer = crate::store::history::native_work_io(&control).expect("control observer");
    attribute
        .validate(&mut Some(&mut observer))
        .expect("bounded source is valid");
}
