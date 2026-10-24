use super::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc,
};
use tracing::{Metadata, subscriber::Interest};
use tracing_subscriber::{Layer, layer::SubscriberExt as _};

struct Blocker {
    first: AtomicBool,
    gate: Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>,
}
impl tracing::Subscriber for Blocker {
    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if metadata.name() == "registration_race" && self.first.swap(false, Ordering::SeqCst) {
            let (entered, release) = self.gate.lock().unwrap().take().unwrap();
            entered.send(()).unwrap();
            release.recv().unwrap();
        }
        Interest::never()
    }
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        false
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}
struct Count(Arc<AtomicUsize>);
impl<S: tracing::Subscriber> Layer<S> for Count {
    fn on_event(&self, _: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn emit() {
    tracing::info!(name: "registration_race", "captured");
}

// Separate processes start with an untouched interest cache. The control uses
// the reverted per-capture dispatcher pair, not an imitation of tracing's cache.
#[test]
fn slow_registration_cannot_overwrite_capture_interest() {
    for pinned in [false, true] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "capture::tests::registration_race_child",
                "--nocapture",
            ])
            .env(
                "ACYCLIC_CAPTURE_RACE_CHILD",
                if pinned { "pinned" } else { "reverted" },
            )
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains(if pinned {
            "captured=1"
        } else {
            "captured=0"
        }));
    }
}

#[test]
fn registration_race_child() {
    let Ok(mode) = std::env::var("ACYCLIC_CAPTURE_RACE_CHILD") else {
        return;
    };
    let pinned = mode == "pinned";
    if pinned {
        initialize();
    }
    let (entered, waiting) = mpsc::channel();
    let (release, gate) = mpsc::channel();
    let peer = std::thread::spawn(move || {
        let blocker = Dispatch::new(Blocker {
            first: AtomicBool::new(true),
            gate: Mutex::new(Some((entered, gate))),
        });
        tracing::dispatcher::with_default(&blocker, emit);
    });
    waiting.recv().unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let capture = std::thread::spawn({
        let seen = seen.clone();
        move || {
            if pinned {
                dispatch(tracing_subscriber::registry().with(Count(seen)))
            } else {
                let _registration = Dispatch::new(tracing_subscriber::registry());
                Dispatch::new(tracing_subscriber::registry().with(Count(seen)))
            }
        }
    });
    let capture = if pinned {
        // Registration holds tracing-core's dispatcher read lock. Whether the
        // writer has started or is pending, it must rebuild after this reader.
        release.send(()).unwrap();
        capture.join().unwrap()
    } else {
        // JustOne holds no lock: force the writer to finish before the late
        // reader caches never. This is the actual cross-test loss schedule.
        let capture = capture.join().unwrap();
        release.send(()).unwrap();
        capture
    };
    peer.join().unwrap();
    tracing::dispatcher::with_default(&capture, emit);
    let captured = seen.load(Ordering::SeqCst);
    assert_eq!(captured, usize::from(pinned));
    println!("captured={captured}");
}
