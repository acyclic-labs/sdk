use super::*;

type Volume = crate::facade::Volume<
    crate::facade::MemoryAuthorityBackend,
    crate::facade::MemoryObjectBackend,
>;

// Immutable backend input is seeded before the schedules. Source and core
// synchronization are constructed afresh inside each Loom execution.
pub(in crate::native_mount::fuse) fn volume() -> (Volume, crate::GenerationId) {
    let fs = crate::Fs::memory();
    let cancellation = crate::CancellationToken::new();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let volume = runtime
        .block_on(Box::pin(fs.create_volume(
            posix_config(),
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )))
        .unwrap()
        .value;
    let creation = runtime
        .block_on(Box::pin(fs.read_creation_record(
            volume.id(),
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )))
        .unwrap()
        .value
        .0;
    (
        volume,
        crate::GenerationId::new(creation.initial_generation_root.digest),
    )
}

pub(in crate::native_mount::fuse) fn fixture(
    (volume, generation): &(Volume, crate::GenerationId),
) -> (Arc<ProjectionCore>, crate::NativeMaterializationWork) {
    let checkout = checkout(volume, *generation);
    let source = source(checkout);
    core(source)
}

#[inline(never)]
fn boxed<F: std::future::Future>(make: impl FnOnce() -> F) -> std::pin::Pin<Box<F>> {
    Box::pin(make())
}

#[inline(never)]
fn checkout(volume: &Volume, generation: crate::GenerationId) -> Box<MemoryCheckout> {
    let cancellation = crate::CancellationToken::new();
    let future = boxed(|| {
        volume.checkout(
            crate::model::GenerationSelector::Exact(generation),
            crate::model::CheckoutMode::read_only_pinned(),
            crate::WorkBudget::UNBOUNDED,
            &cancellation,
        )
    });
    static REPORTED: std::sync::Once = std::sync::Once::new();
    REPORTED.call_once(|| {
        eprintln!(
            "modeled checkout future: {} bytes backing, {}-byte boxed handle",
            std::mem::size_of_val(future.as_ref().get_ref()),
            std::mem::size_of_val(&future),
        )
    });
    // Box the output before the executor carries it through its own frames.
    let future = Box::pin(async move { future.await.map(|receipt| Box::new(receipt.value)) });
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
        .unwrap()
}

#[inline(never)]
fn source(checkout: Box<MemoryCheckout>) -> Arc<dyn MountFilesystem> {
    let shared = Arc::new(crate::native_mount::SharedCheckout::new(*checkout));
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
