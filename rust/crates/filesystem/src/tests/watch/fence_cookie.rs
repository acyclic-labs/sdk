//! The fence cookie state machine that the macOS FSEvents fence drives: where
//! a cookie can be placed, and when its arrival confirms delivery.
use super::*;
use crate::source_watch::{HostChange, HostChangeSink as _};
use std::os::unix::fs::PermissionsExt as _;
use std::time::Duration;

const FENCE: Duration = Duration::from_secs(5);

struct Fixture {
    _root: tempfile::TempDir,
    watch: NativeWatch,
    hints: RootHints,
}

/// A watch excluding `names`, each created as a directory with `mode`, and
/// the sink its source would deliver into. The watch observes only its root
/// directory, so the test alone delivers what happens beneath the excluded
/// directories.
fn fixture(names: &[(&str, u32)]) -> Result<Fixture, Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let mut options = NativeWatchOptions::new(VolumeLimits::default());
    options.recursive = false;
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
    watch.begin_rescan()?;
    watch.finish_rescan()?;
    let hints = RootHints {
        root: watch.root.clone(),
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

/// A read-only excluded directory cannot hold a cookie, so the next one
/// does; with none writable, the fence confirms nothing instead of failing.
#[test]
fn a_fence_places_its_cookie_where_the_host_takes_one() -> Result<(), Box<dyn std::error::Error>> {
    let mut writable = fixture(&[(".git", 0o555), (".volumes", 0o755)])?;
    let arrived = cookie(&mut writable.watch)?;
    assert!(matches!(&arrived, HostChange::Altered(path) if path.starts_with(".volumes")));
    deliver(&writable.hints, &[arrived], u64::MAX)?;
    assert!(writable.watch.await_cookie(FENCE)?);
    assert!(writable.watch.confirmable()?);

    // Negative control: the only excluded directory refuses the cookie.
    let mut refused = fixture(&[(".git", 0o555)])?;
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

/// A cookie confirms delivery only when no batch before it was lost, and
/// only once the batch carrying it is accepted whole.
#[test]
fn a_cookie_after_a_lost_batch_never_confirms() -> Result<(), Box<dyn std::error::Error>> {
    // Negative control: a cookie with nothing lost confirms.
    let mut quiet = fixture(&[(".volumes", 0o755)])?;
    let arrived = cookie(&mut quiet.watch)?;
    deliver(&quiet.hints, &[arrived], u64::MAX)?;
    assert!(quiet.watch.await_cookie(FENCE)?);

    // A batch is lost while the fence waits, then the cookie arrives.
    let mut lost = fixture(&[(".volumes", 0o755)])?;
    let arrived = cookie(&mut lost.watch)?;
    lost.hints.delivery_lost();
    deliver(&lost.hints, &[arrived], u64::MAX)?;
    assert!(!lost.watch.await_cookie(FENCE)?);
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
    let mut torn = fixture(&[(".volumes", 0o755)])?;
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
    assert!(!torn.watch.await_cookie(FENCE)?);
    Ok(())
}
