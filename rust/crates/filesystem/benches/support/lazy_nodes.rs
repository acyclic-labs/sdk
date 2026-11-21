use acyclic_fs::facade::FsWork;
use acyclic_fs::kernel::{KernelAccounting, KernelOwnedAllocation};
use acyclic_fs::lazy_workspace::{
    LazyOverlay, LazyOverlayChange, LazyOverlayId, LazyShadow, LazyShadowId, LazyWorkspaceStore,
    MemoryLazyNode, MemoryLazyWorkspaceStore,
};
use divan::Bencher;
use std::future::Future;
use std::task::{Context, Poll, Waker};
use {
    acyclic_fs::{CancellationToken, NativeMaterializationWork},
    acyclic_fs_work::performance::WorkBudget,
};

fn fail<T>(error: impl std::fmt::Debug) -> T {
    eprintln!("lazy-node benchmark failed: {error:?}");
    std::process::exit(1);
}

fn benchmark_marker(
    name: &str,
    size: usize,
    context: u8,
    begin: bool,
    work: &Option<NativeMaterializationWork>,
    store: &MemoryLazyWorkspaceStore,
) {
    if std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some() {
        let meter = work.as_ref().map(|work| {
            let meter = work.runtime_meter();
            meter.allocation_owner().unwrap_or(meter)
        });
        let (leaf, leaf_bytes) = meter.as_ref().map_or((0, 0), |meter| {
            (
                std::sync::Arc::as_ptr(meter) as *const () as usize,
                std::mem::size_of_val(meter.as_ref()),
            )
        });
        let store_bytes = std::mem::size_of_val(store);
        let store = std::ptr::from_ref(store) as usize;
        eprintln!(
            "ACYCLIC_BENCH_BOUNDARY name={name} size={size} context={context} begin={begin} leaf={leaf:#x} leaf_bytes={leaf_bytes} store={store:#x} store_bytes={store_bytes}"
        );
    }
}

struct Fragments(usize);
impl std::io::Write for Fragments {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += 1;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

const BUDGET: WorkBudget = acyclic_fs_work::performance::BOUNDARY_BUDGET;

fn complete<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => fail("the synchronous memory store unexpectedly suspended"),
    }
}

fn work(context: u8, cancellation: &CancellationToken) -> Option<NativeMaterializationWork> {
    if context == 0 {
        return None;
    }
    let original = NativeMaterializationWork::new(BUDGET, cancellation).unwrap_or_else(fail);
    Some(match context {
        1 => original,
        2 => original.native_volume_work(BUDGET).unwrap_or_else(fail),
        _ => original.invocation_view().unwrap_or_else(fail).0,
    })
}

fn accounting<'a>(
    work: &'a Option<NativeMaterializationWork>,
    cancellation: &'a CancellationToken,
) -> KernelAccounting<'a> {
    match work {
        Some(work) => KernelAccounting::native(work, cancellation),
        None => BUDGET.accounting(cancellation),
    }
}

fn overlay(
    size: usize,
    _: &mut KernelAccounting<'_>,
    _: &mut KernelOwnedAllocation,
    _: &mut u64,
) -> LazyOverlay {
    let empty = LazyOverlayId::from_bytes(*blake3::hash(br#""v2.Empty""#).as_bytes());
    LazyOverlay::Node {
        path: format!("/{}", "a".repeat(size)),
        priority: 1,
        change: LazyOverlayChange::Tombstone { removed: None },
        left: empty,
        right: empty,
    }
}
fn shadow(
    size: usize,
    ctx: &mut KernelAccounting<'_>,
    allocation: &mut KernelOwnedAllocation,
    bytes: &mut u64,
) -> LazyShadow {
    let empty: LazyShadowId = serde_json::from_slice(
        &serde_json::to_vec(blake3::hash(br#""Empty""#).as_bytes()).unwrap_or_else(fail),
    )
    .unwrap_or_else(fail);
    LazyShadow::Node {
        file_id: acyclic_fs_format::identity::FileId::from_bytes([7; 16]),
        priority: 1,
        authored: Some(
            acyclic_fs::LazyBox::fund(
                acyclic_fs::LazyStoredRecord {
                    record: vec![7; size],
                    metadata: acyclic_fs::WorkspaceMetadata {
                        posix_mode: None,
                        posix_uid: None,
                        posix_gid: None,
                        posix_flags: None,
                        windows_attributes: None,
                        created_ns: None,
                        modified_ns: None,
                        accessed_ns: None,
                        changed_ns: None,
                        has_named_attributes: false,
                        has_acl: false,
                        has_security_descriptor: false,
                    },
                },
                allocation,
                bytes,
                ctx,
            )
            .unwrap_or_else(fail),
        ),
        observed: None,
        left: empty,
        right: empty,
    }
}
macro_rules! bench_node {
    ($name:ident, $node:ident, $id:ty, $put:ident) => {
        #[divan::bench(args = [(0, 0), (0, 1), (0, 2), (0, 3), (64, 0), (64, 1), (64, 2), (64, 3), (4096, 0), (4096, 1), (4096, 2), (4096, 3)], threads = [1, 4], sample_count = 64, sample_size = 8)]
        fn $name(bencher: Bencher, (size, context): (usize, u8)) {
            let cancellation = CancellationToken::new();
            let work = work(context, &cancellation);
            let store = MemoryLazyWorkspaceStore::default();
            let mut setup = WorkBudget::UNBOUNDED.accounting(&cancellation);
            let mut setup_allocation = setup.claim_owned(0, 0).unwrap_or_else(fail);
            let mut setup_bytes = 0;
            let node = $node(size, &mut setup, &mut setup_allocation, &mut setup_bytes);
            let encoded = serde_json::to_vec(&node).unwrap_or_else(fail);
            let digest = *blake3::hash(&encoded).as_bytes();
            let id: $id = serde_json::from_slice(&serde_json::to_vec(&digest).unwrap_or_else(fail)).unwrap_or_else(fail);
            let make_input = || {
                let mut ctx = accounting(&work, &cancellation);
                let (value, allocation) = node.copy(&mut ctx).unwrap_or_else(fail);
                (value, allocation, ctx)
            };
            // Measure the repeated immutable-node admission, including validation
            // and retirement of the incoming argument. Fund setup copies first.
            let (value, allocation, mut ctx) = make_input();
            complete(store.$put(id, (value, allocation), &mut ctx)).unwrap_or_else(fail);
            if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
                let mut fragments = Fragments(0);
                serde_json::to_writer(&mut fragments, &node).unwrap_or_else(fail);
                eprintln!("{} size={size} context={context} json_bytes={} fragments={} before={:?}",
                    stringify!($name), encoded.len(), fragments.0, work.as_ref().map(NativeMaterializationWork::receipt));
            }
            benchmark_marker(stringify!($name), size, context, true, &work, &store);
            bencher.with_inputs(make_input).bench_values(|(value, allocation, mut ctx)| {
                complete(store.$put(id, (value, allocation), &mut ctx)).unwrap_or_else(fail);
            });
            benchmark_marker(stringify!($name), size, context, false, &work, &store);
            if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
                eprintln!("{} size={size} context={context} after={:?}", stringify!($name),
                    work.as_ref().map(NativeMaterializationWork::receipt));
            }
            drop(node);
            setup.release_owned(&mut setup_allocation, setup_bytes).unwrap_or_else(fail);
        }
    };
}
bench_node!(lazy_overlay_store, overlay, LazyOverlayId, put_lazy_overlay);
bench_node!(lazy_shadow_store, shadow, LazyShadowId, put_lazy_shadow);
