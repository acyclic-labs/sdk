use super::*;
use loom::{sync::Mutex, thread};
use std::task::{Context, Wake, Waker};

fn complete<F: Future>(future: F) -> F::Output {
    crate::sync::owned::block_on(future)
}

fn joined<T>(worker: thread::JoinHandle<T>) -> T {
    worker
        .join()
        .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}

fn concurrent_initializers() {
    let slot = StateArc::new(AsyncMutex::new(None));
    // A filesystem operation is a leaf; it never reacquires the projection.
    let count = StateArc::new(Mutex::new(0));
    let workers: Vec<_> = (0..2)
        .map(|_| {
            let slot = slot.clone();
            let count = count.clone();
            thread::spawn(move || {
                let value = complete(initialize(&slot, async || {
                    *count.lock().unwrap() += 1;
                    thread::yield_now();
                    Ok(7)
                }))
                .unwrap();
                assert_eq!(*value, Some(7));
            })
        })
        .collect();
    for worker in workers {
        joined(worker);
    }
    assert_eq!(*count.lock().unwrap(), 1, "projection initialized twice");
}

#[test]
fn first_native_users_share_one_initialization() {
    loom::model(concurrent_initializers);
}

#[test]
fn failed_or_cancelled_initialization_wakes_waiters_without_partial_state() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    for cancelled in [false, true] {
        loom::model(move || {
            let slot = StateArc::new(AsyncMutex::new(None::<u8>));
            let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
            let waker = Waker::from(wakes.clone());
            let mut context = Context::from_waker(&waker);
            let (release, released) = tokio::sync::oneshot::channel::<()>();
            let mut creation = Box::pin(initialize(&slot, async move || {
                released
                    .await
                    .map_err(|_| Error::Invalid("creation source closed".into()))?;
                Err(Error::Invalid("creation failed".into()))
            }));
            assert!(creation.as_mut().poll(&mut context).is_pending());
            let mut waiter = Box::pin(initialize(&slot, async || Ok(7)));
            // Registration is observed before the original creator can exit.
            assert!(waiter.as_mut().poll(&mut context).is_pending());
            if !cancelled {
                release.send(()).unwrap();
                wakes.0.store(0, Ordering::Relaxed);
                assert!(matches!(
                    creation.as_mut().poll(&mut context),
                    std::task::Poll::Ready(Err(_))
                ));
            } else {
                wakes.0.store(0, Ordering::Relaxed);
            }
            drop(creation);
            assert!(
                wakes.0.load(Ordering::Relaxed) > 0,
                "initialization did not wake its registered successor"
            );
            let std::task::Poll::Ready(value) = waiter.as_mut().poll(&mut context) else {
                panic!("initialization wake did not release its registered successor");
            };
            assert_eq!(
                *value.unwrap(),
                Some(7),
                "{}",
                if cancelled {
                    "cancelled initialization published partial state"
                } else {
                    "failed initialization published partial state"
                }
            );
        });
    }
}

fn lock_order(broken: bool) {
    let state = StateArc::new(AsyncMutex::new(()));
    let projection = StateArc::new(AsyncMutex::new(None));
    let (other_state, other_projection) = (state.clone(), projection.clone());
    let other = thread::spawn(move || {
        let _state = complete(lock_owned(other_state));
        complete(initialize(&other_projection, async || {
            thread::yield_now();
            Ok(7)
        }))
        .unwrap();
    });
    if broken {
        let _projection = complete(initialize(&projection, async || Ok(7))).unwrap();
        thread::yield_now();
        let _state = complete(lock_owned(state));
    } else {
        let _state = complete(lock_owned(state));
        complete(initialize(&projection, async || {
            thread::yield_now();
            Ok(7)
        }))
        .unwrap();
    }
    joined(other);
}

#[test]
fn checkout_then_projection_order_cannot_deadlock() {
    const CONTROL: &str = "ACYCLIC_PROJECTION_LOCK_ORDER_CONTROL";
    if crate::native_environment::variable(CONTROL).is_some() {
        loom::model(|| lock_order(true));
        panic!("inverted lock order was not detected");
    }
    loom::model(|| lock_order(false));
    // Loom's intentional deadlock can also panic while unwinding owned guards.
    // Isolate that abort, retaining both the failing status and exact diagnostic.
    let name = concat!(
        module_path!(),
        "::checkout_then_projection_order_cannot_deadlock"
    );
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([name.split_once("::").unwrap().1, "--exact", "--nocapture"])
        .env(CONTROL, "1")
        .env("RUST_BACKTRACE", "0")
        .output()
        .unwrap();
    assert!(!output.status.success(), "inverted lock order passed");
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(diagnostic.contains("deadlock; threads ="), "{diagnostic}");
}

// The command hand-back protocol, through production `compare_and_swap`, over
// a model volume whose head is one generation and one file map. Each command
// forks the head, writes only its own view, and either hands that view back
// or discards it. A three-way merge stands in for the filesystem join: a value
// both sides changed differently becomes a git conflict marker.
type Files = std::collections::BTreeMap<&'static str, String>;

#[derive(Clone, Default)]
struct Head {
    generation: u64,
    files: Files,
}

fn merge(base: &Files, ours: &Files, theirs: &Files) -> (Files, Vec<&'static str>) {
    let mut merged = Files::new();
    let mut conflicts = Vec::new();
    let keys: std::collections::BTreeSet<_> = ours.keys().chain(theirs.keys()).copied().collect();
    for key in keys {
        let (base, ours, theirs) = (base.get(key), ours.get(key), theirs.get(key));
        let value = if theirs == base || ours == theirs {
            ours
        } else if ours == base {
            theirs
        } else {
            conflicts.push(key);
            merged.insert(
                key,
                format!(
                    "<<<<<<< ours
{}
=======
{}
>>>>>>> theirs
",
                    ours.map_or("", String::as_str),
                    theirs.map_or("", String::as_str)
                ),
            );
            continue;
        };
        if let Some(value) = value {
            merged.insert(key, value.clone());
        }
    }
    (merged, conflicts)
}

// A verified command's hand-back. Returns its acknowledged generation and the
// paths that now carry conflict markers.
fn hand_back(volume: &Mutex<Head>, fork: &Head, view: &Files) -> (u64, Vec<&'static str>) {
    complete(compare_and_swap(move || async move {
        let target = volume.lock().unwrap().clone();
        let (files, conflicts) = merge(&fork.files, &target.files, view);
        let mut head = volume.lock().unwrap();
        if head.generation != target.generation {
            return Ok(None);
        }
        *head = Head {
            generation: target.generation + 1,
            files,
        };
        Ok(Some((head.generation, conflicts)))
    }))
    .unwrap()
}

fn command(
    volume: &Mutex<Head>,
    writes: &[(&'static str, &'static str)],
) -> (u64, Vec<&'static str>, Files) {
    let fork = volume.lock().unwrap().clone();
    let mut view = fork.files.clone();
    for (path, value) in writes {
        view.insert(*path, (*value).to_owned());
    }
    let (generation, conflicts) = hand_back(volume, &fork, &view);
    (generation, conflicts, view)
}

fn concurrent_hand_backs() {
    let volume = StateArc::new(Mutex::new(Head::default()));
    // A command whose caller is lost holds its own view across both other
    // commands' whole lifetimes. Neither waits for it.
    let lost = volume.lock().unwrap().clone();
    let mut lost_view = lost.files.clone();
    lost_view.insert("lost", "unverified".to_owned());
    let other = {
        let volume = volume.clone();
        thread::spawn(move || command(&volume, &[("b", "b"), ("shared", "b")]))
    };
    let a = command(&volume, &[("a", "a"), ("shared", "a")]);
    let b = joined(other);
    // Its discard is the drop of its own view: all or nothing, never published.
    drop(lost_view);
    let head = volume.lock().unwrap().clone();
    assert_eq!(head.generation, 2, "a hand-back was lost or duplicated");
    assert_eq!(
        head.files.get("a").map(String::as_str),
        Some("a"),
        "disjoint write lost"
    );
    assert_eq!(
        head.files.get("b").map(String::as_str),
        Some("b"),
        "disjoint write lost"
    );
    assert!(
        !head.files.contains_key("lost"),
        "unverified view was published"
    );
    // Exactly the later hand-back sees the overlap, and reports it with git
    // markers holding both sides; neither side silently overwrites the other.
    let (first, second) = if a.0 < b.0 { (&a, &b) } else { (&b, &a) };
    assert!(first.1.is_empty());
    assert_eq!(second.1, ["shared"]);
    let shared = &head.files["shared"];
    assert!(
        shared.starts_with(
            "<<<<<<< ours
"
        ) && shared.contains(
            "
a
"
        ) && shared.contains(
            "
b
"
        )
    );
    // Every acknowledged hand-back's own writes remain in the final head.
    for (_, _, view) in [&a, &b] {
        for (path, value) in view {
            if *path != "shared" {
                assert_eq!(head.files.get(path), Some(value), "acknowledged write lost");
            }
        }
    }
}

#[test]
fn hand_backs_never_wait_and_never_overwrite() {
    loom::model(concurrent_hand_backs);
}
