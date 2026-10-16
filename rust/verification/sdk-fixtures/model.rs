use loom::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use sdk_fixture_verification::get_or_try_init;

fn model(broken: bool) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(move || {
        let slot = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let jobs = [0, 1].map(|_| {
            let slot = Arc::clone(&slot);
            let calls = Arc::clone(&calls);
            loom::thread::spawn(move || {
                let initialize = || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, ()>(vec![1, 2])
                };
                if broken {
                    // Deliberately release the slot while constructing the package.
                    if let Some(value) = slot.lock().unwrap().as_ref() {
                        return Arc::clone(value);
                    }
                    let value = Arc::new(initialize().unwrap());
                    *slot.lock().unwrap() = Some(Arc::clone(&value));
                    value
                } else {
                    get_or_try_init(&slot, initialize).unwrap()
                }
            })
        });
        let [a, b] = jobs.map(|job| job.join().unwrap());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "package initialized twice");
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(a.as_slice(), &[1, 2]);
    });
}

#[test]
fn package_is_initialized_once_and_published_whole() {
    model(false);
}

#[test]
#[should_panic(expected = "package initialized twice")]
fn model_detects_initialization_outside_the_lock() {
    model(true);
}
#[test]
fn failed_initialization_leaves_the_slot_retryable() {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let slot = Arc::new(Mutex::new(None));
        let calls = Arc::new(AtomicUsize::new(0));
        let jobs = [0, 1].map(|_| {
            let slot = Arc::clone(&slot);
            let calls = Arc::clone(&calls);
            loom::thread::spawn(move || {
                get_or_try_init(&slot, || {
                    if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(())
                    } else {
                        Ok(vec![1, 2])
                    }
                })
            })
        });
        let results = jobs.map(|job| job.join().unwrap());
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(slot.lock().unwrap().as_ref().unwrap().as_slice(), &[1, 2]);
    });
}
