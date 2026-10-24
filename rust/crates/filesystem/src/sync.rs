#[cfg(acyclic_fs_loom)]
pub(crate) use loom::sync::*;
#[cfg(not(acyclic_fs_loom))]
pub(crate) use std::sync::*;

// Native namespace and helper coordination has a dedicated model domain.
// The unchanged ledger uses the separate filesystem accounting proofs.
#[cfg(feature = "native-mount")]
pub(crate) mod native {
    #[cfg(any(acyclic_fs_loom, acyclic_fs_native_loom))]
    pub(crate) use loom::sync::*;
    #[cfg(not(any(acyclic_fs_loom, acyclic_fs_native_loom)))]
    pub(crate) use std::sync::*;

    // Keep backend selection private: callers cannot pass a modeled builder to
    // a std-only spawn helper, or accidentally run modeled locks on an OS thread.
    #[cfg(any(acyclic_fs_loom, acyclic_fs_native_loom))]
    use loom::thread;
    #[cfg(not(any(acyclic_fs_loom, acyclic_fs_native_loom)))]
    use std::thread;

    pub(crate) fn spawn<F, T>(work: F) -> std::io::Result<thread::JoinHandle<T>>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        #[cfg(any(acyclic_fs_loom, acyclic_fs_native_loom))]
        return thread::Builder::new().spawn(acyclic_grpc_observability::task::blocking(work));
        #[cfg(not(any(acyclic_fs_loom, acyclic_fs_native_loom)))]
        acyclic_grpc_observability::task::named_thread(thread::Builder::new(), work)
    }

    #[cfg(any(acyclic_fs_loom, acyclic_fs_native_loom))]
    pub(crate) type RetirementSender = loom::sync::mpsc::Sender<()>;
    #[cfg(not(any(acyclic_fs_loom, acyclic_fs_native_loom)))]
    pub(crate) type RetirementSender = std::sync::mpsc::SyncSender<()>;

    // Retirement consumes its sender, including constructor rejection. There
    // is exactly one notification for the one worker receive.
    pub(crate) fn retirement_channel() -> (RetirementSender, mpsc::Receiver<()>) {
        #[cfg(any(acyclic_fs_loom, acyclic_fs_native_loom))]
        return loom::sync::mpsc::channel();
        #[cfg(not(any(acyclic_fs_loom, acyclic_fs_native_loom)))]
        std::sync::mpsc::sync_channel(1)
    }

    #[cfg(all(test, any(acyclic_fs_loom, acyclic_fs_native_loom)))]
    #[test]
    fn modeled_native_spawn_keeps_worker_in_scheduler() {
        loom::model(|| {
            let seen = Arc::new(atomic::AtomicBool::new(false));
            let worker_seen = Arc::clone(&seen);
            let (retirement, receiver) = retirement_channel();
            let worker = spawn(move || {
                receiver.recv().unwrap();
                worker_seen.store(true, atomic::Ordering::Release);
            })
            .unwrap();
            retirement.send(()).unwrap();
            worker.join().unwrap();
            assert!(seen.load(atomic::Ordering::Acquire));
        });
    }
}

// Type identity checks compile in ordinary builds, including --all-features.
#[cfg(not(acyclic_fs_loom))]
const _: fn(Mutex<()>) -> std::sync::Mutex<()> = |mutex| mutex;
#[cfg(acyclic_fs_loom)]
const _: fn(Mutex<()>) -> loom::sync::Mutex<()> = |mutex| mutex;

#[cfg(not(acyclic_fs_loom))]
#[test]
fn ordinary_build_keeps_standard_synchronization() {
    assert!(
        !cfg!(acyclic_fs_loom),
        "ordinary builds must not enable the dedicated model cfg"
    );
}
