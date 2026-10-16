//! Complete rescans of a published native tree, including unchanged files.
use acyclic_fs::model::{CheckoutMode, GenerationSelector};
use acyclic_fs::{
    CancellationToken, CaptureOptions, CapturePolicy, Fs, OperationId, WorkBudget,
    capture_baseline, capture_root_identity, seal_checkout,
};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    if std::env::var_os("ACYCLIC_BENCH_TRACE").is_some() {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(std::io::sink)
            .init();
    }
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    divan::main();
}

#[divan::bench(args = [1048576, 16777216], sample_count = 3, sample_size = 1)]
fn native_range_read(bencher: divan::Bencher, bytes: usize) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let source = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(source.path(), vec![b'x'; bytes]).unwrap();
    let cancellation = CancellationToken::new();
    let original =
        acyclic_fs::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let meter = original.runtime_meter();
    let file = acyclic_native_runtime::NativeFile::from_file(
        std::fs::File::open(source.path()).unwrap(),
        Some(acyclic_native_runtime::NativeFileMeter::Owned(
            meter.clone(),
        )),
    )
    .map_err(|(error, _)| error)
    .unwrap();
    bencher
        .counter(divan::counter::BytesCount::new(bytes as u64))
        .bench_local(|| {
            let chunk = runtime
                .block_on(
                    file.read_bounded_async(0, bytes, meter.clone(), true)
                        .unwrap(),
                )
                .unwrap();
            assert_eq!(chunk.0.len(), bytes);
            assert_eq!(chunk.0.first(), Some(&b'x'));
            assert_eq!(chunk.0.last(), Some(&b'x'));
            chunk
        });
}

#[divan::bench(args = [(128, 0), (128, 80), (1024, 0)], sample_count = 3, sample_size = 1)]
fn rescan(bencher: divan::Bencher, (count, bytes): (u32, usize)) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let cancellation = CancellationToken::new();
    let workspace = runtime
        .block_on(Fs::memory().create_workspace("capture", WorkBudget::UNBOUNDED, &cancellation))
        .unwrap()
        .value;
    let source = tempfile::tempdir().unwrap();
    for index in 0..count {
        std::fs::write(
            source.path().join(format!("f{index:05}")),
            vec![b'x'; bytes],
        )
        .unwrap();
    }
    let options = CaptureOptions {
        source_root: source.path().to_owned(),
        expected_root_identity: capture_root_identity(source.path()).unwrap(),
        maximum_paths: count,
        maximum_extent_spans: 8,
    };
    runtime.block_on(async {
        let mut initial = workspace
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .unwrap()
            .value;
        capture_baseline(
            &mut initial,
            &options,
            &CapturePolicy::default(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap();
        seal_checkout(
            &mut initial,
            OperationId::new(),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .unwrap();
    });
    let report_work = std::env::var_os("ACYCLIC_BENCH_WORK").is_some();
    bencher
        .counter(divan::counter::ItemsCount::new(u64::from(count)))
        .with_inputs(|| {
            runtime
                .block_on(workspace.checkout(
                    GenerationSelector::Head,
                    CheckoutMode::tracking_transaction(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                ))
                .unwrap()
                .value
        })
        .bench_local_values(|mut checkout| {
            let captured = runtime
                .block_on(capture_baseline(
                    &mut checkout,
                    &options,
                    &CapturePolicy::default(),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                ))
                .unwrap();
            assert_eq!(captured.value.examined_paths, u64::from(count));
            if report_work {
                eprintln!("capture={:?} work={:?}", captured.value, captured.work);
            }
            (checkout, captured)
        });
}
