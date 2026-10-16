use super::*;

// The immutable checkout is input, not a participant in invalidation. Every
// schedule creates a fresh private candidate, source, native meter and core.
pub(in crate::native_mount::fuse) fn seed() -> Box<MemoryCheckout> {
    Box::new(
        memory_checkout(
            posix_config(),
            crate::model::CheckoutMode::read_only_pinned(),
            1,
        )
        .unwrap()
        .pop()
        .unwrap(),
    )
}

pub(in crate::native_mount::fuse) fn fixture(
    checkout: &MemoryCheckout,
) -> (Arc<ProjectionCore>, crate::NativeMaterializationWork) {
    core(source(checkout))
}

#[inline(never)]
fn source(checkout: &MemoryCheckout) -> Arc<dyn MountFilesystem> {
    let cancellation = crate::CancellationToken::new();
    let mut accounting = crate::kernel::allocation::KernelAccounting::standalone(
        crate::WorkBudget::UNBOUNDED,
        &cancellation,
    );
    let checkout = checkout
        .private_candidate(&mut accounting, &cancellation)
        .unwrap();
    let shared = Arc::new(crate::native_mount::SharedCheckout::new(checkout));
    Arc::new(MemorySource::new(shared, posix_config()).unwrap())
}

#[inline(never)]
fn core(
    source: Arc<dyn MountFilesystem>,
) -> (Arc<ProjectionCore>, crate::NativeMaterializationWork) {
    let work = crate::NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .unwrap();
    let core =
        ProjectionCore::new(source, directory(FileId::new()), None, false, 0, 0, &work).unwrap();
    (core, work)
}
