use acyclic_fs::facade::FsWork;
use acyclic_fs::kernel::{KernelAccounting, NamespacePath};
use acyclic_fs::{CancellationToken, NativeMaterializationWork, VolumeConfig};
use divan::Bencher;

fn fail<T>(error: impl std::fmt::Debug) -> T {
    eprintln!("read-admission benchmark failed: {error:?}");
    std::process::exit(1);
}

#[divan::bench(
    args = [(8, 0), (8, 1), (8, 2), (8, 3), (64, 0), (64, 1), (64, 2), (64, 3)],
    threads = [1, 4], sample_count = 64, sample_size = 8
)]
fn namespace_parse(bencher: Bencher, (depth, context): (usize, u8)) {
    let path = "/segment".repeat(depth);
    let cancellation = CancellationToken::new();
    let config = VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral);
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    let markers = std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some();
    let operation = || {
        let original = (context != 0).then(|| {
            let original = NativeMaterializationWork::new(budget, &cancellation)
                .unwrap_or_else(|(error, _)| fail(error));
            match context {
                2 => original.native_volume_work(budget).unwrap_or_else(fail),
                3 => original.recheck_admitted(),
                _ => original,
            }
        });
        let mut accounting = match &original {
            Some(work) => KernelAccounting::NativeOriginal {
                work,
                cancellation: &cancellation,
            },
            None => budget.accounting(&cancellation),
        };
        let (parsed, mut allocation) = NamespacePath::parse_in_profile(
            &path,
            config.profile,
            config.limits,
            &mut accounting,
            &cancellation,
        )
        .unwrap_or_else(fail);
        assert_eq!(parsed.depth(), depth);
        let bytes = parsed.logical_owned_bytes();
        let receipt = match &original {
            Some(work) => work.receipt(),
            None => match &accounting {
                KernelAccounting::Standalone { work, .. } => *work,
                _ => fail("expected standalone accounting"),
            },
        };
        std::hint::black_box(&parsed);
        drop(parsed);
        accounting
            .release_owned(&mut allocation, bytes)
            .unwrap_or_else(fail);
        receipt
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!(
            "namespace_parse depth={depth} context={context} work={:?}",
            operation()
        );
    }
    if markers {
        eprintln!("ACYCLIC_READ_ADMISSION depth={depth} context={context} begin=true");
    }
    bencher.bench(|| std::hint::black_box(operation()));
    if markers {
        eprintln!("ACYCLIC_READ_ADMISSION depth={depth} context={context} begin=false");
    }
}
