//! The fence cookie state machine that the macOS `FSEvents` fence drives: where
//! a cookie can be placed, and when its arrival confirms delivery.
use super::*;
use crate::source_watch::{HostChange, HostChangeSink as _};
use std::os::unix::fs::PermissionsExt as _;

struct Fixture {
    _root: tempfile::TempDir,
    watch: NativeWatch,
    hints: RootHints,
}

/// A watch excluding `names`, each created as a directory with `mode`, and
/// the sink its source would deliver into. The watch observes only its root
/// directory, so the test alone delivers what happens beneath the excluded
/// directories.
fn fixture(
    names: &[(&str, u32)],
    permit_fence_writes: bool,
) -> Result<Fixture, Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut options = NativeWatchOptions::new(VolumeLimits::default());
    options.recursive = false;
    options.permit_fence_writes = permit_fence_writes;
    let mut watch = NativeWatch::open(root.path(), options)?;
    let mut excluded = Vec::new();
    for (name, _) in names {
        let host = watch.root.join(name);
        std::fs::create_dir(&host)?;
        excluded.push(relative_namespace_path(
            &watch.root,
            &host,
            watch.profile,
            VolumeLimits::default(),
        )?);
    }
    watch.exclude_subtrees(&excluded)?;
    for (name, mode) in names {
        std::fs::set_permissions(
            watch.root.join(name),
            std::fs::Permissions::from_mode(*mode),
        )?;
    }
    let rescan = watch.begin_rescan()?;
    watch.finish_rescan(rescan)?;
    let hints = RootHints {
        root: watch.root.clone(),
        profile: watch.profile,
        limits: VolumeLimits::default(),
        sender: watch
            .receiver
            .get_mut()
            .map_err(|_| "watch queue poisoned")?
            .sender_for_test(),
        queued: Arc::clone(&watch.queued),
        shared: Arc::clone(&watch.shared),
        fenced: Arc::clone(&watch.fenced),
        _root_allocation: None,
    };
    Ok(Fixture {
        _root: root,
        watch,
        hints,
    })
}

/// Arms a fence and returns its cookie's notification, as the source
/// reports it.
fn cookie(watch: &mut NativeWatch) -> Result<HostChange, Box<dyn std::error::Error>> {
    let placed = watch
        .place_cookie(|_, _| Ok(()))?
        .ok_or("no directory took the cookie")?;
    assert_eq!(armed(watch)?.as_ref(), Some(&placed));
    Ok(HostChange::Altered(
        placed.strip_prefix(&watch.root)?.to_path_buf(),
    ))
}

fn armed(watch: &NativeWatch) -> Result<Option<PathBuf>, Box<dyn std::error::Error>> {
    Ok(watch
        .shared
        .state
        .lock()
        .map_err(|_| "watch state poisoned")?
        .fence
        .clone())
}

fn deliver(
    hints: &RootHints,
    changes: &[HostChange],
    items: u64,
) -> Result<(), crate::demand::DemandError> {
    let cancellation = CancellationToken::new();
    let mut work = crate::demand::DemandWork::new(
        WorkBudget {
            items_examined: items,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    hints.host_changed(changes, &mut work)
}

/// Located changes retain their original paths and funding; a continuity
/// loss or a root rename still refuses every later ordinary hint.
#[test]
fn located_hints_keep_original_custody_and_loss_stays_sticky()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, _)| cause)?;
    let (sender, receiver) =
        crate::watch_queue::channel(Some(original.claim(0, 0, WorkCounters::UNCHARGED)?));
    let shared = Arc::new(WatchState::new(SharedState::new(None)));
    let queued = Arc::new(AtomicUsize::new(0));
    let hints = RootHints {
        root: PathBuf::from("/source"),
        profile: FilesystemProfile::Portable,
        limits: VolumeLimits::default(),
        sender,
        queued: Arc::clone(&queued),
        shared: Arc::clone(&shared),
        fenced: Arc::new(Condvar::new()),
        _root_allocation: None,
    };
    let mut work = original.clone();
    hints.host_changed_with_native_work(
        &[
            HostChange::Altered(PathBuf::new()),
            HostChange::Rebound("value.txt".into()),
        ],
        &mut work,
    )?;
    assert!(
        shared
            .state
            .lock()
            .map_err(|_| "watch state poisoned")?
            .invalidation
            .is_none()
    );
    assert_eq!(queued.load(Ordering::Acquire), 2);
    let root = receiver.try_recv()?;
    let file = receiver.try_recv()?;
    assert!(matches!(root.change, WatchChange::MetadataChanged(path) if path.is_root()));
    let expected = relative_namespace_path(
        &hints.root,
        &hints.root.join("value.txt"),
        hints.profile,
        hints.limits,
    )?;
    assert_eq!(file.change, WatchChange::Arrived(expected));
    assert!(
        file.allocations[0]
            .as_ref()
            .is_some_and(|allocation| allocation.bytes() > 0)
    );
    queued.fetch_sub(2, Ordering::AcqRel);
    hints.host_changed_with_native_work(&[HostChange::Everything], &mut work)?;
    hints.host_changed_with_native_work(&[HostChange::Altered("later".into())], &mut work)?;
    assert_eq!(
        shared
            .state
            .lock()
            .map_err(|_| "watch state poisoned")?
            .invalidation,
        Some(WatchInvalidationReason::NativeRescanRequired),
    );
    assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
    for root_change in [
        HostChange::Rebound(PathBuf::new()),
        HostChange::Departed(PathBuf::new()),
    ] {
        shared
            .state
            .lock()
            .map_err(|_| "watch state poisoned")?
            .invalidation = None;
        hints.host_changed_with_native_work(&[root_change], &mut work)?;
        assert_eq!(
            shared
                .state
                .lock()
                .map_err(|_| "watch state poisoned")?
                .invalidation,
            Some(WatchInvalidationReason::RootChanged),
        );
    }
    Ok(())
}

/// A read-only excluded directory cannot hold a cookie, so the next one
/// does; with none writable, the fence confirms nothing instead of failing.
#[test]
fn a_fence_places_its_cookie_where_the_host_takes_one() -> Result<(), Box<dyn std::error::Error>> {
    let mut writable = fixture(&[(".git", 0o555), (".volumes", 0o755)], true)?;
    let arrived = cookie(&mut writable.watch)?;
    assert!(matches!(&arrived, HostChange::Altered(path) if path.starts_with(".volumes")));
    deliver(&writable.hints, &[arrived], u64::MAX)?;
    assert!(writable.watch.await_cookie()?);
    assert!(writable.watch.confirmable()?);

    // Negative control: the only excluded directory refuses the cookie.
    let mut refused = fixture(&[(".git", 0o555)], true)?;
    assert!(refused.watch.place_cookie(|_, _| Ok(()))?.is_none());
    assert!(
        armed(&refused.watch)?.is_none(),
        "a refused cookie arms nothing"
    );
    assert_eq!(
        std::fs::read_dir(refused.watch.root.join(".git"))?.count(),
        0
    );
    Ok(())
}

/// Observation exclusions never imply permission to write a fence cookie.
#[test]
fn read_only_observation_filters_events_without_touching_excluded_subtrees()
-> Result<(), Box<dyn std::error::Error>> {
    let mut observation = fixture(&[(".git", 0o755)], false)?;
    let missing = observation.watch.root.join("missing");
    let excluded = [&observation.watch.root.join(".git"), &missing]
        .into_iter()
        .map(|path| {
            relative_namespace_path(
                &observation.watch.root,
                path,
                observation.watch.profile,
                VolumeLimits::default(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    observation.watch.exclude_subtrees(&excluded)?;
    deliver(
        &observation.hints,
        &[HostChange::Altered(".git/objects/data".into())],
        u64::MAX,
    )?;
    assert!(observation.watch.confirmable()?);
    assert!(observation.watch.place_cookie(|_, _| Ok(()))?.is_none());
    assert!(!missing.exists());
    assert_eq!(
        std::fs::read_dir(observation.watch.root.join(".git"))?.count(),
        0
    );
    #[cfg(target_os = "macos")]
    {
        let cancellation = CancellationToken::new();
        let work = crate::NativeMaterializationWork::new(
            WorkBudget {
                backend_write_operations: 0,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .map_err(|(cause, _)| cause)?;
        observation.watch.fence_with_native_work(&work)?;
        assert_eq!(work.receipt().backend_write_operations, 0);
        assert!(!observation.watch.confirmable()?);
    }
    Ok(())
}

/// A cookie confirms delivery only when no batch before it was lost, and
/// only once the batch carrying it is accepted whole.
#[test]
fn a_cookie_after_a_lost_batch_never_confirms() -> Result<(), Box<dyn std::error::Error>> {
    // Negative control: a cookie with nothing lost confirms.
    let mut quiet = fixture(&[(".volumes", 0o755)], true)?;
    let arrived = cookie(&mut quiet.watch)?;
    deliver(&quiet.hints, &[arrived], u64::MAX)?;
    assert!(quiet.watch.await_cookie()?);

    // A batch is lost while the fence waits, then the cookie arrives.
    let mut lost = fixture(&[(".volumes", 0o755)], true)?;
    let arrived = cookie(&mut lost.watch)?;
    lost.hints.delivery_lost();
    deliver(&lost.hints, &[arrived], u64::MAX)?;
    assert!(!lost.watch.await_cookie()?);
    assert!(!lost.watch.confirmable()?);
    assert!(matches!(
        lost.watch
            .poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?
            .value,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::BackendError,
            ..
        }
    ));

    // The batch carrying the cookie fails after it: the fence stays armed.
    let mut torn = fixture(&[(".volumes", 0o755)], true)?;
    let batch = [
        cookie(&mut torn.watch)?,
        HostChange::Altered("authored.txt".into()),
    ];
    assert!(deliver(&torn.hints, &batch, 1).is_err());
    assert!(
        armed(&torn.watch)?.is_some(),
        "a torn batch confirms nothing"
    );
    torn.hints.delivery_lost();
    assert!(!torn.watch.await_cookie()?);

    // Producer loss settles a cookie that can no longer arrive. The join
    // covers termination before or after the waiter acquires the state lock.
    let mut stopped = fixture(&[(".volumes", 0o755)], true)?;
    let _undelivered = cookie(&mut stopped.watch)?;
    assert!(stopped.watch.confirmable()?);
    let producer = AdapterDelivery {
        shared: Arc::clone(&stopped.watch.shared),
        fenced: Arc::clone(&stopped.watch.fenced),
    };
    let stopping = std::thread::spawn(move || drop(producer));
    let delivered = stopped.watch.await_cookie()?;
    stopping.join().map_err(|_| "cookie producer panicked")?;
    assert!(!delivered);
    assert!(matches!(
        stopped
            .watch
            .poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?
            .value,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::BackendError,
            ..
        }
    ));

    // A consumer that loses continuity publishes the same rescan obligation
    // directly, without manufacturing an expired deadline or host changes.
    let mut invalidated = fixture(&[(".volumes", 0o755)], true)?;
    let _undelivered = cookie(&mut invalidated.watch)?;
    invalidated.watch.invalidate()?;
    assert!(!invalidated.watch.await_cookie()?);
    assert!(matches!(
        invalidated
            .watch
            .poll(8, WorkBudget::UNBOUNDED, &CancellationToken::new())?
            .value,
        WatchBatch::RescanRequired {
            reason: WatchInvalidationReason::NativeRescanRequired,
            ..
        }
    ));
    Ok(())
}

/// A held native cookie cannot make cancellation depend on another delivery.
#[test]
fn cancellation_ends_a_cookie_wait_before_or_after_registration()
-> Result<(), Box<dyn std::error::Error>> {
    for cancelled_first in [false, true] {
        let mut fixture = fixture(&[(".volumes", 0o755)], true)?;
        let arrived = cookie(&mut fixture.watch)?;
        let stop = CancellationToken::new();
        if cancelled_first {
            stop.cancel();
        }
        std::thread::scope(|scope| {
            let (entered, registered) = std::sync::mpsc::sync_channel(1);
            let token = &stop;
            let cancelling = scope.spawn(move || {
                registered
                    .recv()
                    .expect("cookie waiter ended before its registration");
                token.cancel();
            });
            let mut entered = Some(entered);
            let result = fixture.watch.await_cookie_cancellable(&stop, None, || {
                if let Some(entered) = entered.take() {
                    let _ = entered.send(());
                }
                stop.check().map_err(|_| {
                    NativeWatchError::NativeWork(crate::NativeTreeMaterializationError::Cancelled)
                })
            });
            cancelling
                .join()
                .expect("cookie cancellation producer panicked");
            assert!(matches!(
                result,
                Err(NativeWatchError::NativeWork(
                    crate::NativeTreeMaterializationError::Cancelled
                ))
            ));
        });
        // Cancellation leaves continuity and the original cookie intact.
        assert!(armed(&fixture.watch)?.is_some());
        deliver(&fixture.hints, &[arrived], u64::MAX)?;
        assert!(fixture.watch.await_cookie()?);
    }
    Ok(())
}
