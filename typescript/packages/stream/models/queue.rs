//! Shrinking stateful FIFO/reference ownership property over the actual JS queue.
use super::driver;
use proptest::prelude::*;
use serde_json::{Value, json};
use std::{collections::VecDeque, path::Path};

fn driver(fault: &str) -> driver::Driver {
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
        static DRIVER: std::sync::OnceLock<std::sync::Mutex<driver::Driver>> = std::sync::OnceLock::new();
        let driver = DRIVER.get_or_init(|| std::sync::Mutex::new(driver("")));
        let actual = driver.lock().unwrap().run().call(json!({"action":"trace","capacity":capacity,"operations":operations}));
        prop_assert_eq!(actual, expected(capacity, &operations));
    }
}
#[test]
fn negative_control_wrong_ring_head_fails_the_same_reference() {
    let operations = [(0, 10), (0, 20), (1, 0), (1, 0), (0, 30), (1, 0)];
    let actual =
        driver("head").call(json!({"action":"trace","capacity":3,"operations":operations}));
    assert_ne!(
        actual,
        expected(3, &operations),
        "wrong head escaped the FIFO/ownership reference"
    );
}
#[test]
fn dequeue_work_is_one_slot_write_per_record_and_shift_fails_the_same_bound() {
    let mut shared = driver("");
    let mut shift = driver("shift");
    for capacity in [128, 512, 1024] {
        let request = json!({"action":"work","capacity":capacity});
        let before = shift.call(request.clone());
        let after = shared.call(request);
        let bound = |value: &Value| value["writes"].as_u64().unwrap() <= capacity;
        assert!(bound(&after), "dequeue must write only the consumed slot");
        assert!(
            !bound(&before),
            "shift control passed the same slot-write bound"
        );
        assert_eq!(after["writes"], capacity);
        assert_eq!(before["writes"], capacity * (capacity - 1) / 2);
        assert_eq!(after["retained"], 0);
        assert_eq!(before["retained"], 0);
        eprintln!("queue_dequeue capacity={capacity} shift={before} ring={after}");
    }
}
