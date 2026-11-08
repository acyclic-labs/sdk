//! Shrinking stateful FIFO/reference ownership property over the actual JS queue.
use super::super::field;
use super::driver;
use proptest::prelude::*;
use serde_json::{Value, json};
use std::{collections::VecDeque, path::Path};

fn driver(fault: &str) -> std::io::Result<driver::Driver> {
    driver::Driver::configured(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."),
        "queue-work",
        fault,
    )
}
fn expected(capacity: usize, operations: &[(u8, u32)]) -> Value {
    let mut values = VecDeque::new();
    let mut ending = None;
    let mut reader_done = false;
    Value::Array(
        operations
            .iter()
            .map(|&(action, value)| {
                let outcome = match action % 5 {
                    0 if ending.is_some() || values.len() == capacity => json!("capacity"),
                    0 => {
                        values.push_back(value);
                        json!("ok")
                    }
                    1 if reader_done => json!("end"),
                    1 => match values.pop_front() {
                        Some(value) => json!(value),
                        None => match ending {
                            Some(error) => {
                                reader_done = true;
                                json!(if error { "terminal" } else { "end" })
                            }
                            None => json!("empty"),
                        },
                    },
                    2 => json!(values.drain(..).collect::<Vec<_>>()),
                    action => {
                        ending.get_or_insert(action == 4);
                        json!("ok")
                    }
                };
                json!({"outcome":outcome,"available":capacity-values.len(),"retained":values.len()})
            })
            .collect(),
    )
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Buffer capacity, FIFO values, end/error ordering and references remaining
    // after pop/discard agree with an independent deque across every operation.
    #[test]
    fn fifo_capacity_and_ownership_match_a_stateful_reference(
        capacity in 1usize..17,
        operations in prop::collection::vec((any::<u8>(), any::<u32>()), 0..60),
    ) {
        static DRIVER: driver::Shared = driver::Shared::new();
        let actual = driver::Driver::shared(&DRIVER, &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../.."), "queue-work", "")?
            .run()?.call(&json!({"action":"trace","capacity":capacity,"operations":operations}))?;
        prop_assert_eq!(actual, expected(capacity, &operations));
    }
}
#[test]
fn negative_control_wrong_ring_head_fails_the_same_reference() -> std::io::Result<()> {
    let operations = [(0, 10), (0, 20), (1, 0), (1, 0), (0, 30), (1, 0)];
    let actual =
        driver("head")?.call(&json!({"action":"trace","capacity":3,"operations":operations}))?;
    assert_ne!(
        actual,
        expected(3, &operations),
        "wrong head escaped the FIFO/ownership reference"
    );
    Ok(())
}
#[test]
fn dequeue_work_is_one_slot_write_per_record_and_shift_fails_the_same_bound() -> std::io::Result<()>
{
    let mut shared = driver("")?;
    let mut shift = driver("shift")?;
    for capacity in [128, 512, 1024] {
        let request = json!({"action":"work","capacity":capacity});
        let before = shift.call(&request)?;
        let after = shared.call(&request)?;
        let writes = |value: &Value| -> std::io::Result<u64> {
            field(value, "writes")?.as_u64().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("queue slot-write count is not unsigned: {value}"),
                )
            })
        };
        let before_writes = writes(&before)?;
        let after_writes = writes(&after)?;
        assert!(
            after_writes <= capacity,
            "dequeue must write only the consumed slot"
        );
        assert!(
            before_writes > capacity,
            "shift control passed the same slot-write bound"
        );
        assert_eq!(after_writes, capacity);
        assert_eq!(before_writes, capacity * (capacity - 1) / 2);
        assert_eq!(*field(&after, "retained")?, 0);
        assert_eq!(*field(&before, "retained")?, 0);
        eprintln!("queue_dequeue capacity={capacity} shift={before} ring={after}");
    }
    Ok(())
}
