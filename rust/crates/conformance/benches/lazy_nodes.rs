use acyclic_fs::kernel::{KernelAccounting, KernelOwnedAllocation};
use acyclic_fs::lazy_workspace::{
    LazyOverlay, LazyOverlayChange, LazyOverlayId, LazyShadow, LazyShadowId, LazyWorkspaceStore,
    MemoryLazyWorkspaceStore,
};
use acyclic_fs::{CancellationToken, FsWork, NativeMaterializationWork, WorkBudget, WorkCounters};
use divan::Bencher;
use std::future::Future;
use std::task::{Context, Poll, Waker};

const BUDGET: WorkBudget = acyclic_fs::performance::BOUNDARY_BUDGET;

fn complete<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("the synchronous memory store unexpectedly suspended"),
    }
}

fn work(context: u8, cancellation: &CancellationToken) -> Option<NativeMaterializationWork> {
    if context == 0 {
        return None;
    }
    let original = NativeMaterializationWork::new(BUDGET, cancellation).unwrap();
    Some(match context {
        1 => original,
        2 => original.native_volume_work(BUDGET).unwrap(),
        _ => original.invocation_view().unwrap().0,
    })
}

fn accounting<'a>(
    work: &'a Option<NativeMaterializationWork>,
    cancellation: &'a CancellationToken,
) -> KernelAccounting<'a> {
    match work {
        Some(work) => KernelAccounting::NativeOriginal { work, cancellation },
        None => BUDGET.accounting(cancellation),
    }
}

fn input<'a, T: Clone>(
    value: &T,
    bytes: u64,
    actual: impl FnOnce(&T) -> u64,
    mut accounting: KernelAccounting<'a>,
) -> (T, KernelOwnedAllocation, KernelAccounting<'a>) {
    let mut allocation = accounting
        .claim_owned(bytes, u64::from(bytes != 0))
        .unwrap();
    let copied = WorkCounters {
        bytes_copied: bytes,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied).unwrap();
    let value = value.clone();
    accounting.record_completed(copied).unwrap();
    accounting
        .reconcile_owned(&mut allocation, bytes, actual(&value))
        .unwrap();
    (value, allocation, accounting)
}

fn overlay(size: usize) -> LazyOverlay {
    let empty = LazyOverlayId::from_bytes(*blake3::hash(br#""Empty""#).as_bytes());
    LazyOverlay::Node {
        path: format!("/{}", "a".repeat(size)),
        priority: 1,
        change: LazyOverlayChange::Tombstone { removed: None },
        left: empty,
        right: empty,
    }
}
fn overlay_bytes(node: &LazyOverlay) -> u64 {
    match node {
        LazyOverlay::Empty => 0,
        LazyOverlay::Node { path, .. } => path.capacity() as u64,
    }
}

fn shadow(size: usize) -> LazyShadow {
    let empty: LazyShadowId = serde_json::from_slice(
        &serde_json::to_vec(blake3::hash(br#""Empty""#).as_bytes()).unwrap(),
    )
    .unwrap();
    LazyShadow::Node {
        file_id: acyclic_fs::FileId::from_bytes([7; 16]),
        priority: 1,
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
        left: empty,
        right: empty,
    }
}
fn shadow_bytes(node: &LazyShadow) -> u64 {
    match node {
        LazyShadow::Empty => 0,
        LazyShadow::Node { record, .. } => record.capacity() as u64,
    }
}

macro_rules! bench_node {
    ($name:ident, $node:ident, $bytes:ident, $id:ty, $put:ident) => {
        #[divan::bench(args = [(0, 0), (0, 1), (0, 2), (0, 3), (64, 0), (64, 1), (64, 2), (64, 3), (4096, 0), (4096, 1), (4096, 2), (4096, 3)], threads = [1, 4], sample_count = 64, sample_size = 8)]
        fn $name(bencher: Bencher, (size, context): (usize, u8)) {
            let cancellation = CancellationToken::new();
            let work = work(context, &cancellation);
            let store = MemoryLazyWorkspaceStore::default();
            let node = $node(size);
            let encoded = serde_json::to_vec(&node).unwrap();
            let digest = *blake3::hash(&encoded).as_bytes();
            let id: $id = serde_json::from_slice(&serde_json::to_vec(&digest).unwrap()).unwrap();
            let make_input = || input(&node, $bytes(&node), $bytes, accounting(&work, &cancellation));
            // Measure the repeated immutable-node admission, including validation
            // and retirement of the incoming argument. Fund setup copies first.
            let (value, allocation, mut ctx) = make_input();
            complete(store.$put(id, (value, allocation), &mut ctx)).unwrap();
            if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
                eprintln!("{} size={size} context={context} json_bytes={} before={:?}",
                    stringify!($name), encoded.len(), work.as_ref().map(NativeMaterializationWork::receipt));
            }
            super::benchmark_marker(stringify!($name), context != 0, true);
            bencher.with_inputs(make_input).bench_values(|(value, allocation, mut ctx)| {
                complete(store.$put(id, (value, allocation), &mut ctx)).unwrap();
            });
            super::benchmark_marker(stringify!($name), context != 0, false);
            if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
                eprintln!("{} size={size} context={context} after={:?}", stringify!($name),
                    work.as_ref().map(NativeMaterializationWork::receipt));
            }
        }
    };
}
bench_node!(
    lazy_overlay_store,
    overlay,
    overlay_bytes,
    LazyOverlayId,
    put_lazy_overlay
);
bench_node!(
    lazy_shadow_store,
    shadow,
    shadow_bytes,
    LazyShadowId,
    put_lazy_shadow
);
