use super::*;

fn crash_latch(forget_crash: bool) {
    loom::model(move || {
        let (schedule, _) = Schedule::new();
        schedule.cut_at(0, DurableFault::Before);
        schedule.0.state().writes.forget_crash = forget_crash;
        let other = schedule.clone();
        let writer =
            loom::thread::spawn(move || begin(other, "write", 1).and_then(|call| call.before()));
        let result = begin(schedule.clone(), "write", 1).and_then(|call| call.before());
        for result in [result, writer.join().unwrap()] {
            let error = result.expect_err("durable write admitted after crash");
            assert_eq!(
                error
                    .get_ref()
                    .unwrap()
                    .downcast_ref::<InjectedStorageFault>()
                    .unwrap()
                    .ordinal,
                0
            );
        }
        assert_eq!(schedule.durable_writes().len(), 1);
    });
}

#[test]
fn concurrent_writers_cannot_clear_the_crash_boundary() {
    crash_latch(false);
}

#[test]
#[should_panic(expected = "durable write admitted after crash")]
fn negative_control_forgetting_the_crash_admits_another_write() {
    crash_latch(true);
}
