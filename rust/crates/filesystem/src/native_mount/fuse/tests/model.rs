//! A loom model of the real invalidator. It runs `run_invalidator` against
//! queued kernel notifications while another thread queues them and waits
//! for them to settle, with the kernel refusing notifications transiently in
//! every pattern over the first few it receives. In every interleaving each
//! notification must reach the kernel, in order, without a second waiter or
//! a later change prompting a retry.
//!
//! Run with `RUSTFLAGS="--cfg acyclic_loom" cargo test -p acyclic-fs --lib
//! native_mount::fuse::tests::model`.
use super::*;

/// Kernel notifications the model refuses, by their position in the order
/// the kernel receives them.
const REFUSABLE: u32 = 4;
const QUEUED: [u64; 2] = [ROOT_INODE, ROOT_INODE + 1];

struct Kernel {
    refused: u32,
    received: std::sync::Mutex<Vec<(u64, bool)>>,
}

impl crate::native_mount::fuse::KernelCacheNotifier for Kernel {
    fn inval_inode(&self, inode: u64) -> std::io::Result<()> {
        let mut received = self
            .received
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let position = u32::try_from(received.len()).unwrap_or(u32::MAX);
        let accepted = position >= REFUSABLE || self.refused & (1 << position) == 0;
        received.push((inode, accepted));
        if accepted {
            Ok(())
        } else {
            Err(std::io::Error::from_raw_os_error(libc::EINTR))
        }
    }

    fn inval_entry(&self, _: u64, _: &[u8]) -> std::io::Result<()> {
        Err(std::io::Error::from_raw_os_error(libc::EINVAL))
    }
}

#[test]
fn every_transiently_refused_notification_eventually_reaches_the_kernel()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
    let source: Arc<dyn MountFilesystem> = Arc::new(source);
    for refused in 0..1_u32 << REFUSABLE {
        let source = Arc::clone(&source);
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(2);
        model.check(move || {
            // The model's own closure runs on loom's small default stack, so
            // the scenario runs on a thread with room for the real path.
            let source = Arc::clone(&source);
            let scenario = spawn_with_room(move || observe(&source, refused));
            assert!(scenario.is_some(), "spawning the scenario failed");
            let Some(scenario) = scenario else { return };
            assert!(scenario.join().is_ok(), "the scenario panicked");
        });
    }
    Ok(())
}

/// Spawns a model thread whose stack fits the real invalidation path.
fn spawn_with_room<T: Send + 'static>(
    body: impl FnOnce() -> T + Send + 'static,
) -> Option<loom::thread::JoinHandle<T>> {
    loom::thread::Builder::new()
        .stack_size(16 << 20)
        .spawn(body)
        .ok()
}

/// Queues notifications for a running invalidator and waits for them to
/// settle as a mount's reader would, without asking for a retry.
fn observe(source: &Arc<dyn MountFilesystem>, refused: u32) {
    let core = projection_core(
        Arc::clone(source),
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    );
    assert!(core.is_ok(), "projection core: {:?}", core.as_ref().err());
    let Ok(core) = core else { return };
    let kernel = Arc::new(Kernel {
        refused,
        received: std::sync::Mutex::new(Vec::new()),
    });
    let invalidator = spawn_with_room({
        let (core, kernel) = (Arc::clone(&core), Arc::clone(&kernel));
        move || crate::native_mount::fuse::run_invalidator(&core, &*kernel)
    });
    assert!(invalidator.is_some(), "spawning the invalidator failed");
    let Some(invalidator) = invalidator else {
        return;
    };
    core.state_to_finish()
        .invalidation
        .deferred
        .extend(QUEUED.map(KernelCacheItem::Inode));
    let mut state = core
        .state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    while state.invalidation.is_pending(None) {
        state = core
            .invalidation
            .wait(state)
            .unwrap_or_else(std::sync::PoisonError::into_inner);
    }
    drop(state);
    core.stop();
    assert!(invalidator.join().is_ok(), "the invalidator panicked");
    let received = kernel
        .received
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let accepted = received
        .iter()
        .filter(|(_, accepted)| *accepted)
        .map(|(inode, _)| *inode)
        .collect::<Vec<_>>();
    assert_eq!(accepted, QUEUED, "refusals {refused:#06b}: {received:?}");
}
