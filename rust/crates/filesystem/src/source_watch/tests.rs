use super::{HostChange, HostChangeSink, NativeSourceWatch};
use crate::native_host::HostRoot;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Every change delivered, in order.
#[derive(Default)]
struct Recorded(Mutex<Vec<HostChange>>);

impl HostChangeSink for Recorded {
    fn host_changed(&self, changes: &[HostChange]) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(changes);
    }
}

impl Recorded {
    fn take(&self) -> Vec<HostChange> {
        std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner))
    }

    fn reported(changes: &[HostChange], path: &Path) -> bool {
        changes.iter().any(|change| match change {
            HostChange::Rebound(reported) | HostChange::Altered(reported) => reported == path,
            _ => false,
        })
    }

    fn rebound(changes: &[HostChange], path: &Path) -> bool {
        changes.contains(&HostChange::Rebound(path.to_path_buf()))
    }

    fn everything(changes: &[HostChange]) -> bool {
        changes.contains(&HostChange::Everything)
    }
}

/// Everything reported until `reported` holds of it, or ten seconds pass. A
/// fence proves delivery on Linux and Windows; on macOS, where it reports
/// everything instead, the reports themselves are waited for.
fn reports_until(
    watch: &NativeSourceWatch,
    recorded: &Recorded,
    reported: impl Fn(&[HostChange]) -> bool,
) -> std::io::Result<Vec<HostChange>> {
    watch.fence()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let mut changes = recorded.take();
    while !reported(&changes) && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
        changes.extend(recorded.take());
    }
    Ok(changes)
}

fn watch(root: &Path) -> Result<(NativeSourceWatch, Arc<Recorded>), Box<dyn std::error::Error>> {
    let recorded = Arc::new(Recorded::default());
    let watch = NativeSourceWatch::start(&HostRoot::open(root)?, recorded.clone())?;
    Ok((watch, recorded))
}

/// Watches `directory` where the source would, before reading beneath it.
fn admit(watch: &NativeSourceWatch, directory: &str) {
    #[cfg(target_os = "linux")]
    watch.admit(Path::new(directory));
    #[cfg(not(target_os = "linux"))]
    let _ = (watch, directory);
}

/// Every kind of change beneath the root is reported under the names it
/// touched.
#[test]
fn changes_beneath_the_root_are_reported_by_name() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("d"))?;
    std::fs::write(root.path().join("d").join("f"), b"one")?;
    std::fs::write(root.path().join("g"), b"one")?;
    let (watch, recorded) = watch(root.path())?;
    admit(&watch, "d");

    std::fs::write(root.path().join("d").join("f"), b"changed")?;
    std::fs::write(root.path().join("new"), b"new")?;
    std::fs::rename(root.path().join("g"), root.path().join("h"))?;
    std::fs::remove_file(root.path().join("new"))?;
    std::fs::create_dir(root.path().join("e"))?;
    let expected = ["d/f", "new", "g", "h", "e"];
    let changes = reports_until(&watch, &recorded, |changes| {
        expected
            .iter()
            .all(|path| Recorded::reported(changes, Path::new(path)))
    })?;
    for path in expected {
        assert!(
            Recorded::reported(&changes, &PathBuf::from(path)),
            "{path} was not reported: {changes:?}"
        );
    }
    // A write changes the entry in place (which `FSEvents`, coalescing a
    // recent creation into it, may report as the rebinding it could be);
    // every name operation rebinds.
    #[cfg(not(target_os = "macos"))]
    assert!(changes.contains(&HostChange::Altered(PathBuf::from("d").join("f"))));
    for path in ["new", "g", "h", "e"] {
        assert!(
            Recorded::rebound(&changes, Path::new(path)),
            "{path}: {changes:?}"
        );
    }
    // Nothing was lost; a macOS fence reports everything by design.
    #[cfg(not(target_os = "macos"))]
    assert!(
        !Recorded::everything(&changes),
        "nothing was lost: {changes:?}"
    );
    assert!(watch.is_exact());
    Ok(())
}

/// A fence returns only once every change that completed before it has
/// been delivered: creations, writes in place, and renames alike.
#[test]
fn a_fence_delivers_every_change_completed_before_it() -> TestResult {
    let root = tempfile::tempdir()?;
    let (watch, recorded) = watch(root.path())?;
    for index in 0..500 {
        let created = format!("f{index}");
        std::fs::write(root.path().join(&created), b"x")?;
        let mut expected = vec![PathBuf::from(created)];
        if index >= 1 {
            let written = format!("f{}", index - 1);
            std::fs::write(root.path().join(&written), b"written")?;
            expected.push(PathBuf::from(written));
        }
        if index >= 2 {
            let (from, to) = (format!("f{}", index - 2), format!("g{}", index - 2));
            std::fs::rename(root.path().join(&from), root.path().join(&to))?;
            expected.extend([PathBuf::from(from), PathBuf::from(to)]);
        }
        watch.fence()?;
        let changes = recorded.take();
        for path in expected {
            assert!(
                Recorded::reported(&changes, &path) || Recorded::everything(&changes),
                "{path:?} changed before fence {index} but was not delivered by it: {changes:?}"
            );
        }
    }
    Ok(())
}

/// Changes the host could not queue are reported as everything, and the
/// watch keeps reporting afterwards.
#[cfg(any(target_os = "linux", windows))]
#[test]
fn lost_changes_are_reported_as_everything() -> TestResult {
    let root = tempfile::tempdir()?;
    let (watch, recorded) = watch(root.path())?;
    {
        let _paused = watch.platform.paused();
        // More than the host queues: inotify's default queue holds 16384
        // events; a directory read buffer holds 64 KiB of reports.
        let flood = if cfg!(windows) { 3_000 } else { 20_000 };
        for index in 0..flood {
            std::fs::write(root.path().join(format!("flood-{index:05}")), b"")?;
        }
    }
    watch.fence()?;
    assert!(Recorded::everything(&recorded.take()));
    std::fs::write(root.path().join("after"), b"x")?;
    watch.fence()?;
    assert!(Recorded::reported(&recorded.take(), Path::new("after")));
    assert!(watch.is_exact());
    Ok(())
}

/// `FSEvents` reports lost events with flags, which report everything.
#[cfg(target_os = "macos")]
#[test]
fn lost_changes_are_reported_as_everything() -> TestResult {
    let root = tempfile::tempdir()?;
    let (watch, recorded) = watch(root.path())?;
    for flags in [0x02, 0x04, 0x40, 0x80] {
        watch.platform.inject(root.path(), flags);
        assert!(Recorded::everything(&recorded.take()), "flags {flags:#x}");
    }
    assert!(watch.is_exact());
    Ok(())
}

/// A directory that moves is watched under the name it moved to once the
/// source reads beneath that name, and changes there are reported by it.
#[cfg(target_os = "linux")]
#[test]
fn a_moved_directory_reports_changes_under_its_new_name() -> TestResult {
    let root = tempfile::tempdir()?;
    std::fs::create_dir_all(root.path().join("a").join("b"))?;
    let (watch, recorded) = watch(root.path())?;
    admit(&watch, "a/b");
    std::fs::rename(root.path().join("a"), root.path().join("c"))?;
    watch.fence()?;
    let changes = recorded.take();
    assert!(Recorded::reported(&changes, Path::new("a")));
    assert!(Recorded::reported(&changes, Path::new("c")));
    admit(&watch, "c/b");
    std::fs::write(root.path().join("c").join("b").join("x"), b"x")?;
    watch.fence()?;
    let changes = recorded.take();
    assert!(
        Recorded::reported(&changes, Path::new("c/b/x")),
        "{changes:?}"
    );
    assert!(!Recorded::reported(&changes, Path::new("a/b/x")));
    Ok(())
}

/// A root that stops being reported makes the watch inexact for good,
/// after it reports everything once more.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn a_removed_root_makes_the_watch_inexact() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("root");
    std::fs::create_dir(&root)?;
    let (watch, recorded) = watch(&root)?;
    std::fs::remove_dir(&root)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while watch.is_exact() {
        assert!(
            std::time::Instant::now() < deadline,
            "the watch stayed exact"
        );
        let _ = watch.fence();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(Recorded::everything(&recorded.take()));
    Ok(())
}

/// While a Windows watch lives, its root's path keeps naming the directory
/// the source holds: the root cannot be renamed or removed.
#[cfg(windows)]
#[test]
fn a_watched_root_keeps_its_name() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("root");
    std::fs::create_dir(&root)?;
    let (watch, _recorded) = watch(&root)?;
    assert!(std::fs::rename(&root, parent.path().join("moved")).is_err());
    assert!(std::fs::remove_dir(&root).is_err());
    drop(watch);
    std::fs::rename(&root, parent.path().join("moved"))?;
    Ok(())
}

/// A root whose path stops naming it, because a directory on the way moved,
/// makes the watch inexact, as it makes the source fail closed.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn a_root_whose_path_moves_makes_the_watch_inexact() -> TestResult {
    let parent = tempfile::tempdir()?;
    let root = parent.path().join("a").join("root");
    std::fs::create_dir_all(&root)?;
    let (watch, recorded) = watch(&root)?;
    std::fs::write(root.join("before"), b"x")?;
    watch.fence()?;
    assert!(watch.is_exact());
    std::fs::rename(parent.path().join("a"), parent.path().join("b"))?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while watch.is_exact() {
        assert!(
            std::time::Instant::now() < deadline,
            "the watch stayed exact"
        );
        let _ = watch.fence();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(Recorded::everything(&recorded.take()));
    Ok(())
}
