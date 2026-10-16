#[cfg(not(acyclic_stream_loom))]
compile_error!(
    "stream models require Loom synchronization: run RUSTFLAGS='--cfg acyclic_stream_loom' cargo test --manifest-path rust/crates/stream/models/Cargo.toml --test session_model"
);

mod error {
    use thiserror::Error;
    include!("../src/lib/error.rs");
}
pub use error::StreamError;
#[path = "../src/session/sync.rs"]
mod sync;
use sync::{Arc, AtomicU64, Mutex, Ordering};

mod cancellation;
mod checked;
#[path = "../../stream-napi/src/eager.rs"]
mod eager;
mod native_registration;
#[path = "../src/session/policy.rs"]
mod policy;
#[path = "../src/session/client/progress.rs"]
mod progress;
mod outbound {
    include!("../src/session/client/outbound.rs");
    include!("outbound.rs");
}
mod gate {
    include!("../src/session/gate.rs");
    include!("gate.rs");
}
mod signal {
    include!("../src/session/signal.rs");
    include!("signal.rs");
}
type ReleaseSender = Arc<signal::Signal<credit::Released>>;
mod credit {
    include!("../src/session/client/credit.rs");
    include!("credit.rs");
}

struct WakeCount(AtomicU64, Box<dyn Fn() + Send + Sync>);
impl std::task::Wake for WakeCount {
    fn wake(self: std::sync::Arc<Self>) {
        (self.1)();
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn wake_count() -> (std::sync::Arc<WakeCount>, std::task::Waker) {
    wake_probe(|| {})
}
fn wake_probe(
    probe: impl Fn() + Send + Sync + 'static,
) -> (std::sync::Arc<WakeCount>, std::task::Waker) {
    let count = std::sync::Arc::new(WakeCount(AtomicU64::new(0), Box::new(probe)));
    let wake = std::task::Waker::from(std::sync::Arc::clone(&count));
    (count, wake)
}

#[test]
fn cursor_consumption_and_movement_are_atomic() {
    cursor(false);
}
#[test]
#[should_panic(expected = "resume must follow the fenced consumption")]
fn cursor_negative_control_reads_before_fencing() {
    cursor(true);
}
fn cursor(broken: bool) {
    loom::model(move || {
        let cursor = Arc::new(progress::Progress::new(0));
        let old = Arc::clone(&cursor);
        let old = loom::thread::spawn(move || old.consume_record(1, 0, [0; 32]));
        let moved = Arc::clone(&cursor);
        let moved = loom::thread::spawn(move || {
            let early = if broken { Some(moved.next()?) } else { None };
            let generation = moved.fence()?;
            let resume = early.unwrap_or(moved.next()?);
            assert_eq!(generation, 2);
            Ok::<_, StreamError>(resume)
        });
        let old_applied = old.join().unwrap().unwrap();
        let resume = moved.join().unwrap().unwrap();
        assert_eq!(
            resume,
            u64::from(old_applied),
            "resume must follow the fenced consumption"
        );
        assert!(!cursor.consume_record(1, resume, [0; 32]).unwrap());
        assert!(cursor.consume_record(2, resume, [0; 32]).unwrap());
        assert_eq!(cursor.next().unwrap(), resume + 1);
    });
}

fn opaque_cursor(broken: bool) {
    loom::model(move || {
        let cursor = Arc::new(progress::Progress::new(0));
        let consuming = Arc::clone(&cursor);
        let consuming = loom::thread::spawn(move || consuming.consume_opaque(1, 0, b"child"));
        let moving = Arc::clone(&cursor);
        let moving = loom::thread::spawn(move || {
            let early = if broken {
                Some(moving.position()?.1)
            } else {
                None
            };
            moving.fence()?;
            let (next, opaque) = moving.position()?;
            Ok::<_, StreamError>((next, early.unwrap_or(opaque)))
        });
        let consumed = consuming.join().unwrap().unwrap();
        let position = moving.join().unwrap().unwrap();
        assert_eq!(
            position,
            if consumed {
                (1, b"child".to_vec())
            } else {
                (0, Vec::new())
            },
            "resume must atomically retain the opaque cursor"
        );
        assert!(!cursor.consume_opaque(1, position.0, b"stale").unwrap());
        assert_eq!(cursor.position().unwrap(), position);
    });
}
#[test]
fn child_consumption_and_movement_keep_one_cursor() {
    opaque_cursor(false);
}
#[test]
#[should_panic(expected = "resume must atomically retain the opaque cursor")]
fn opaque_cursor_negative_control_splits_the_resume_cut() {
    opaque_cursor(true);
}

#[test]
fn channel_allocation_is_unique_and_never_wraps() {
    allocate(false);
}
#[test]
#[should_panic(expected = "IDs must be distinct")]
fn allocation_negative_control_splits_the_increment() {
    allocate(true);
}
fn allocate(broken: bool) {
    for start in [1, u64::MAX - 2] {
        loom::model(move || {
            let counter = Arc::new(AtomicU64::new(start));
            let run = |counter: Arc<AtomicU64>| {
                loom::thread::spawn(move || {
                    if broken {
                        let value = counter.load(Ordering::Relaxed);
                        counter.store(
                            value.checked_add(2).ok_or(StreamError::Capacity)?,
                            Ordering::Relaxed,
                        );
                        Ok(value)
                    } else {
                        progress::allocate(&counter)
                    }
                })
            };
            let first = run(Arc::clone(&counter));
            let second = run(Arc::clone(&counter));
            let ids: Vec<_> = [first.join().unwrap(), second.join().unwrap()]
                .into_iter()
                .flatten()
                .collect();
            assert!(ids.iter().all(|id| *id != 0 && id % 2 == 1));
            if ids.len() == 2 {
                assert_ne!(ids[0], ids[1], "IDs must be distinct");
            }
            assert_eq!(
                counter.load(Ordering::Relaxed),
                start + 2 * ids.len() as u64
            );
            assert_eq!(ids.len(), if start == 1 { 2 } else { 1 });
        });
    }
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(128))]
    // Reserving a maximum-record slot keeps one independently credited channel
    // admissible while every uncredited/latest channel is backpressured.
    #[test]
    fn reserved_capacity_is_available_only_to_credited_channels(
        frames in 2_usize..1024, largest in 1_usize..65536,
    ) {
        let room = policy::room(frames, 4 * largest, frames - 1, 0, largest, true);
        proptest::prop_assert!(!policy::enabled(true, 0, room, false));
        proptest::prop_assert!(policy::enabled(false, 0, room, true));
        let full = policy::room(frames, 4 * largest, frames, 0, largest, true);
        proptest::prop_assert!(!policy::enabled(false, 0, full, true));
    }
}

#[test]
fn latest_consumption_and_movement_share_the_production_cursor_model() {
    progress::verify_latest_progress(false);
}

#[test]
#[should_panic(expected = "a stale delivery cannot advance the fenced resume cut")]
fn latest_negative_control_advances_after_its_generation_was_fenced() {
    progress::verify_latest_progress(true);
}
