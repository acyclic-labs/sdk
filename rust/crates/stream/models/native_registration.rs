use super::*;

struct Registered(Arc<AtomicU64>);
impl Drop for Registered {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn registration(broken: bool) {
    loom::model(move || {
        let active = Arc::new(AtomicU64::new(1)); // A is waiting before Welcome.
        let welcome = Arc::new(gate::Gate::new());
        let registering = Arc::clone(&active);
        let pending_welcome = Arc::clone(&welcome);
        let work = async move {
            registering.fetch_add(1, Ordering::SeqCst);
            let _registered = Registered(registering);
            pending_welcome.changed(false).await.unwrap();
        };
        let following = if broken {
            // The former N-API async export leaves the first poll to its worker.
            futures::future::Either::Left(work)
        } else {
            futures::future::Either::Right(eager::start(work))
        };
        let abandoning = Arc::clone(&active);
        let first = loom::thread::spawn(move || {
            let observed = abandoning.fetch_sub(1, Ordering::SeqCst);
            // Welcome follows A's cancellation. The worker can poll before,
            // between or after these actions; no third participant is needed.
            welcome.set(true).unwrap();
            observed
        });
        // Welcome can arrive before or after the worker replaces the temporary
        // waker. Both orders must complete without losing the notification.
        loom::future::block_on(following);
        assert_eq!(
            first.join().unwrap(),
            2,
            "a pending sibling must register before cancellation"
        );
        assert_eq!(active.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn native_registration_precedes_dispatch_and_preserves_welcome_wakes() {
    registration(false);
}
#[test]
#[should_panic(expected = "a pending sibling must register before cancellation")]
fn negative_control_leaves_native_registration_to_the_worker() {
    registration(true);
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn an_immediately_ready_operation_is_completed_exactly_once(value in proptest::num::u64::ANY) {
        let calls = std::cell::Cell::new(0);
        let future = eager::start(async {
            calls.set(calls.get() + 1);
            value
        });
        proptest::prop_assert_eq!(calls.get(), 1);
        proptest::prop_assert_eq!(futures::executor::block_on(future), value);
        proptest::prop_assert_eq!(calls.get(), 1);
    }
}
