use super::*;

async fn capture(
    checkout: &mut crate::facade::MemoryCheckout,
    options: &CaptureOptions,
    path: &NamespacePath,
    work: &NativeMaterializationWork,
    baseline: Option<&NativeViewBaseline>,
) -> CaptureReceipt {
    capture_subtrees_with_native_work(
        checkout,
        std::slice::from_ref(path),
        options,
        &CapturePolicy::default(),
        work.cancellation(),
        baseline,
        work,
    )
    .await
    .unwrap()
    .value
}

async fn check(body: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    let file = source.path().join("file");
    std::fs::write(&file, body)?;
    // Keep access time ahead of modification/change time so reading cannot
    // itself edit the metadata being compared (including Linux relatime).
    let now = std::time::SystemTime::now();
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&file)?
        .set_times(
            std::fs::FileTimes::new()
                .set_accessed(now + std::time::Duration::from_secs(60))
                .set_modified(now - std::time::Duration::from_secs(60)),
        )?;
    let before = std::fs::metadata(&file)?;
    let times = std::fs::FileTimes::new()
        .set_accessed(before.accessed()?)
        .set_modified(before.modified()?);
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let mut checkout = capture_policy_tests::native_checkout(&work).await?;
    let limits = checkout.volume_config().limits;
    let path =
        NamespacePath::from_portable(&crate::path::PortablePath::parse("/file", limits)?, limits)?;
    let options = CaptureOptions {
        source_root: source.path().to_owned(),
        expected_root_identity: capture_root_identity(source.path())?,
        maximum_paths: 8,
        maximum_extent_spans: 8,
    };
    capture(&mut checkout, &options, &path, &work, None).await;
    let baseline = NativeViewBaseline::new(options.expected_root_identity, true);
    let prior = checkout.root().clone();
    let receipt = capture(&mut checkout, &options, &path, &work, Some(&baseline)).await;
    assert_eq!(receipt.changed_paths, 0, "unchanged body {body:?}");
    assert_eq!(checkout.root(), &prior);
    if !body.is_empty() {
        let mut edited = body.to_vec();
        edited[0] ^= 1;
        std::fs::write(&file, &edited)?;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file)?
            .set_times(times)?;
        let receipt = capture(&mut checkout, &options, &path, &work, Some(&baseline)).await;
        assert_eq!(
            receipt.changed_paths, 1,
            "same length edit with restored timestamps"
        );
        let read = checkout
            .read_file_range(
                &path,
                crate::ByteRange {
                    offset: 0,
                    length: edited.len() as u64,
                },
                &work,
                &cancellation,
            )
            .await?;
        assert_eq!(read.bytes.as_ref(), edited);
    }
    Ok(())
}

#[test]
fn inline_capture_reuses_only_identical_staged_bytes() -> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let _inline = acyclic_native_runtime::InlineBlocking::enter();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    runtime.block_on(async {
        for length in [0, 1, 7, 63, 64, 65] {
            for byte in [0, b'x'] {
                check(&vec![byte; length]).await?;
            }
        }
        Ok(())
    })
}
