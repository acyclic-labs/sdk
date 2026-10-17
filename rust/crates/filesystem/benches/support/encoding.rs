use acyclic_fs::{CancellationToken, Fs, MemoryFs, NativeMaterializationWork, WorkCounters};
use divan::Bencher;
use std::future::Future;
use std::task::{Context, Poll, Waker};

fn fail<T>(error: impl std::fmt::Debug) -> T {
    eprintln!("canonical-write benchmark failed: {error:?}");
    std::process::exit(1);
}
fn complete<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => fail("in-process canonical publication unexpectedly suspended"),
    }
}

#[divan::bench(args = [false, true], threads = [1, 4], sample_count = 64, sample_size = 8)]
fn workspace_create(bencher: Bencher, native: bool) {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    let fixture = || {
        (
            Fs::memory(),
            native.then(|| {
                NativeMaterializationWork::new(budget, &cancellation).unwrap_or_else(fail)
            }),
        )
    };
    let operation = |(fs, work): &(MemoryFs, Option<NativeMaterializationWork>)| {
        if let Some(work) = work {
            let workspace = complete(
                fs.create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                    "bench",
                    acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
                    acyclic_fs::OperationId::from_bytes([4; 16]),
                    work,
                    &cancellation,
                    None,
                ),
            )
            .unwrap_or_else(fail);
            drop(workspace);
            work.receipt()
        } else {
            let created = complete(
                fs.create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                    "bench",
                    acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
                    acyclic_fs::OperationId::from_bytes([4; 16]),
                    budget,
                    &cancellation,
                    None,
                ),
            )
            .unwrap_or_else(fail);
            let receipt = created.work;
            drop(created.value);
            receipt
        }
    };
    let markers = std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some();
    if markers || std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        let input = fixture();
        if markers {
            eprintln!("ACYCLIC_ENCODER_BOUNDARY native={native} begin=true");
        }
        let receipt: WorkCounters = operation(&input);
        if markers {
            eprintln!("ACYCLIC_ENCODER_BOUNDARY native={native} begin=false");
        }
        eprintln!("workspace_create native={native} work={receipt:?}");
    }
    // Provider setup, original-ledger allocation and retirement are outside the
    // timed call; publication performs every real canonical writer and charge.
    bencher
        .with_inputs(fixture)
        .bench_refs(|input| std::hint::black_box(operation(input)));
}
