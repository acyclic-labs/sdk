//! Callback admission for an already held native directory.
use super::*;
use std::path::Path;

/// A registration on the original directory, using the shared native driver.
/// Dropping it suppresses further callbacks and retires its native resources.
pub struct HeldRootWatch {
    observer: Arc<dyn ObserverControl>,
    _source: NativeSourceWatch,
}

trait ObserverControl: Send + Sync {
    fn retire(&self);
}

struct Observer<F> {
    changed: F,
    retired: AtomicBool,
    failed: AtomicBool,
}

impl HeldRootWatch {
    pub(crate) fn open(
        root: &crate::native_host::HostRoot,
        changed: impl Fn(notify::Result<notify::Event>) + Send + Sync + 'static,
    ) -> Result<Self, DemandError> {
        let mut work = crate::native_host::ordinary_work();
        let observer = Arc::new(Observer {
            changed,
            retired: AtomicBool::new(false),
            failed: AtomicBool::new(false),
        });
        let sink: Arc<dyn HostChangeSink> = observer.clone();
        #[cfg(windows)]
        let source = Delivery::new(sink, &mut work)?.start(&mut work, |delivery, work| {
            NativeSourceWatch::open_watch(
                root.directory_handle(),
                delivery,
                false,
                Some(true),
                false,
                work,
            )
        })?;
        #[cfg(target_os = "linux")]
        let source = Delivery::new(sink, &mut work)?.start(&mut work, |delivery, work| {
            NativeSourceWatch::start_bound_driver(root, delivery, false, work)
        })?;
        #[cfg(target_os = "macos")]
        let source = NativeSourceWatch::start(root, sink, &mut work)?;
        Ok(Self {
            observer,
            _source: source,
        })
    }
}

impl Drop for HeldRootWatch {
    fn drop(&mut self) {
        // Intentional last-waiter retirement is not producer loss.
        self.observer.retire();
    }
}

impl<F: Send + Sync> ObserverControl for Observer<F> {
    fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }
}

use notify::{
    EventKind,
    event::{ModifyKind, RemoveKind, RenameMode},
};

/// The change kind of one ReadDirectoryChangesW action, so consumers can tell
/// names from in-place changes.
#[cfg(windows)]
fn windows_kind(action: u32) -> EventKind {
    use notify::event::CreateKind;
    // FILE_ACTION_* values of FILE_NOTIFY_INFORMATION.
    const ADDED: u32 = 1;
    const REMOVED: u32 = 2;
    const MODIFIED: u32 = 3;
    const RENAMED_OLD_NAME: u32 = 4;
    const RENAMED_NEW_NAME: u32 = 5;
    match action {
        ADDED => EventKind::Create(CreateKind::Any),
        REMOVED => EventKind::Remove(RemoveKind::Any),
        MODIFIED => EventKind::Modify(ModifyKind::Any),
        RENAMED_OLD_NAME => EventKind::Modify(ModifyKind::Name(RenameMode::From)),
        RENAMED_NEW_NAME => EventKind::Modify(ModifyKind::Name(RenameMode::To)),
        _ => EventKind::Any,
    }
}

impl<F: Fn(notify::Result<notify::Event>) + Send + Sync> Observer<F> {
    fn report(&self, event: notify::Result<notify::Event>) {
        if !self.retired.load(Ordering::Acquire) && !self.failed.load(Ordering::Acquire) {
            (self.changed)(event);
        }
    }

    fn fail(&self, error: notify::Error) {
        if !self.retired.load(Ordering::Acquire) && !self.failed.swap(true, Ordering::AcqRel) {
            (self.changed)(Err(error));
        }
    }

    fn path(
        &self,
        relative: &Path,
        kind: notify::EventKind,
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        let bytes = relative.as_os_str().len() as u64;
        let slots = std::mem::size_of::<PathBuf>() as u64;
        let allocation = bytes
            .checked_add(slots)
            .ok_or(crate::performance::WorkError::Overflow)?;
        work.claim_allocation(
            allocation,
            1 + u64::from(bytes != 0),
            WorkCounters {
                bytes_copied: bytes,
                ..WorkCounters::UNCHARGED
            },
        )?;
        let mut event = notify::Event::new(kind);
        event.paths = vec![relative.to_path_buf()];
        let delivered =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.report(Ok(event))));
        work.release(allocation)?;
        if let Err(panic) = delivered {
            std::panic::resume_unwind(panic);
        }
        Ok(())
    }

    fn rescan(&self) {
        self.report(Ok(
            notify::Event::new(notify::EventKind::Any).set_flag(notify::event::Flag::Rescan)
        ));
    }
}

impl<F: Fn(notify::Result<notify::Event>) + Send + Sync> HostChangeSink for Observer<F> {
    fn logical_owned_bytes(&self) -> u64 {
        super::arc_control_bytes::<Self>()
    }

    fn host_changed(
        &self,
        changes: &[HostChange],
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        work.check()?;
        for change in changes {
            match change {
                HostChange::Departed(path) => {
                    self.path(path, EventKind::Remove(RemoveKind::Any), work)?;
                }
                HostChange::Rebound(path) => {
                    self.path(
                        path,
                        EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
                        work,
                    )?;
                }
                HostChange::Altered(path) => {
                    self.path(path, EventKind::Modify(ModifyKind::Any), work)?;
                }
                #[cfg(windows)]
                HostChange::Observed { action, path, .. } => {
                    self.path(path, windows_kind(*action), work)?;
                }
                HostChange::Everything => self.rescan(),
                #[cfg(any(windows, target_os = "macos"))]
                HostChange::Unconfirmed => self.rescan(),
            }
        }
        Ok(())
    }

    fn delivery_failed(&self, cause: &DemandError) {
        self.fail(notify::Error::generic(&cause.to_string()));
    }

    fn delivery_lost(&self) {
        self.fail(notify::Error::io(std::io::ErrorKind::BrokenPipe.into()));
    }

    #[cfg(windows)]
    fn host_changed_wide(
        &self,
        changes: &[HostChangeWide<'_>],
        work: &mut DemandWork,
    ) -> Result<(), DemandError> {
        work.check()?;
        for change in changes {
            match change {
                HostChangeWide::Path { action, path, .. } => {
                    let (path, allocation) = crate::native_host::decode_windows_name(path, work)?;
                    let delivered = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        self.path(Path::new(&path), windows_kind(*action), work)
                    }));
                    drop(path);
                    work.release(allocation)?;
                    match delivered {
                        Ok(result) => result?,
                        Err(panic) => std::panic::resume_unwind(panic),
                    }
                }
                HostChangeWide::Everything | HostChangeWide::Unconfirmed => self.rescan(),
            }
        }
        Ok(())
    }
}

#[cfg(all(test, target_os = "macos"))]
#[test]
fn moved_original_root_ends_callback_delivery_without_a_fence()
-> Result<(), Box<dyn std::error::Error>> {
    for move_parent in [false, true] {
        let directory = tempfile::tempdir()?;
        let parent = directory.path().canonicalize()?.join("parent");
        let path = parent.join("root");
        std::fs::create_dir_all(&path)?;
        let root =
            crate::native_host::HostRoot::open(&path, &mut crate::native_host::ordinary_work())?;
        let (send, receive) = std::sync::mpsc::channel();
        let watch = HeldRootWatch::open(&root, move |event| {
            if let Err(error) = event {
                let _ = send.send(error);
            }
        })?;
        let original = if move_parent { &parent } else { &path };
        std::fs::rename(original, directory.path().join("moved"))?;
        let error = receive.recv_timeout(std::time::Duration::from_secs(2))?;
        assert!(
            matches!(error.kind, notify::ErrorKind::Io(ref error) if error.kind() == std::io::ErrorKind::BrokenPipe),
            "{error}"
        );
        drop(watch);
        assert!(receive.try_recv().is_err(), "one original terminal cause");
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux"))]
#[test]
fn original_callback_failure_and_producer_panic_settle_once()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let root = crate::native_host::HostRoot::open(
        directory.path(),
        &mut crate::native_host::ordinary_work(),
    )?;
    for producer_panics in [false, true] {
        let (send, receive) = std::sync::mpsc::channel();
        let watch = HeldRootWatch::open(&root, move |event| match event {
            Ok(_) if producer_panics => panic!("original callback producer lost"),
            Ok(_) => {}
            Err(error) => {
                let _ = send.send(error);
            }
        })?;
        if producer_panics {
            std::fs::write(directory.path().join("change"), b"actual native event")?;
        } else {
            watch
                ._source
                .inject_delivery_failure(DemandError::InvalidRequest);
        }
        let error = receive.recv_timeout(std::time::Duration::from_secs(2))?;
        if producer_panics {
            assert!(
                matches!(error.kind, notify::ErrorKind::Io(ref error) if error.kind() == std::io::ErrorKind::BrokenPipe)
            );
        } else {
            assert!(
                error
                    .to_string()
                    .contains(&DemandError::InvalidRequest.to_string()),
                "{error}"
            );
        }
        drop(watch);
        assert!(
            receive.try_recv().is_err(),
            "one producer has one terminal cause"
        );
    }
    Ok(())
}

#[cfg(all(test, windows))]
#[test]
fn windows_held_root_watch_tells_names_from_in_place_changes()
-> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;
    crate::watch::initialize_native_watch_services()?;
    let directory = tempfile::tempdir()?;
    let (a, b) = (directory.path().join("a"), directory.path().join("b"));
    std::fs::write(&a, b"x")?;
    let (sender, kinds) = std::sync::mpsc::channel();
    let _watch = crate::hold_root(directory.path())?.watch_changes(move |event| {
        let _ = sender.send(event.map(|event| event.kind));
    })?;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&a)?
        .write_all(b"y")?;
    assert_eq!(kinds.recv()??, EventKind::Modify(ModifyKind::Any));
    std::fs::rename(&a, &b)?;
    // Further in-place reports of the append may precede the rename.
    let mut kind = kinds.recv()??;
    while kind == EventKind::Modify(ModifyKind::Any) {
        kind = kinds.recv()??;
    }
    assert!(
        matches!(kind, EventKind::Modify(ModifyKind::Name(_))),
        "{kind:?}"
    );
    Ok(())
}
