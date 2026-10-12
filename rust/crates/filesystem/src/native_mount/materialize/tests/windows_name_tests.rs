use super::*;

#[tokio::test]
async fn overlapped_sparse_control_finishes_before_native_writes()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let directory = tempfile::tempdir()?;
    let cancellation = CancellationToken::new();
    let mut receipt = GenerationReceipt::new(
        super::restore_recovery_tests::FINITE_MATERIALIZATION_BUDGET,
        &cancellation,
    )
    .map_err(|failure| failure.0)?;
    let root = receipt.run(|work| HostRoot::open(directory.path(), work))?;
    let root = receipt.owned_arc(root, 0)?;
    let limits = crate::model::VolumeLimits::default();
    let namespace = NamespacePath::new(
        vec![LogicalName::new(
            NameEncoding::Utf8,
            b"sparse-control.bin".to_vec(),
            limits.maximum_component_bytes,
        )?],
        limits,
    )?;
    let host_path = materialize_host_path(None, namespace.components(), &mut receipt)?;
    let native = create_file(&root.0, &host_path.0, &mut receipt)?;
    let meter = receipt.custody.work().runtime_meter();
    let control = native
        .control_async(
            |file, meter| mark_sparse_measured(file, meter.expect("metered control")),
            Some(meter),
        )
        .map_err(materialize_runtime_error)?;
    control.await.map_err(materialize_runtime_error)?;
    let meter = receipt.custody.work().runtime_meter();
    let resize = native
        .set_len_async(1024 * 1024, Some(meter))
        .map_err(materialize_runtime_error)?;
    resize.await.map_err(materialize_runtime_error)?;
    let mut ordinary_control = None::<
        &mut fn(
            NativeMaterializationActionPhase,
        ) -> std::future::Ready<Result<(), MaterializeError>>,
    >;
    write_generation_bytes(
        &native,
        1024 * 1024 - 4,
        acyclic_stream::OwnedBytes::from_static(b"tail"),
        0,
        &mut receipt,
        &mut ordinary_control,
    )
    .await?;
    sync_generation_file(&native, &mut receipt, &mut ordinary_control).await?;
    let path = Path::new("sparse-control.bin");
    let bytes = std::fs::read(directory.path().join(path))?;
    assert_eq!(bytes.len(), 1024 * 1024);
    assert_eq!(&bytes[bytes.len() - 4..], b"tail");
    Ok(())
}

#[test]
fn posix_profile_names_materialize_through_utf8_on_windows()
-> Result<(), Box<dyn std::error::Error>> {
    let valid = LogicalName::new(
        NameEncoding::PosixBytes,
        "uni-é中.txt".as_bytes().to_vec(),
        255,
    )?;
    let limits = crate::model::VolumeLimits::default();
    let mut receipt = GenerationReceipt::new(
        super::restore_recovery_tests::FINITE_MATERIALIZATION_BUDGET,
        &CancellationToken::new(),
    )
    .map_err(|failure| failure.0)?;
    let namespace = NamespacePath::new(vec![valid], limits)?;
    let path = materialize_host_path(None, namespace.components(), &mut receipt)?;
    assert_eq!(path.0, "uni-é中.txt".encode_utf16().collect::<Vec<_>>());

    let invalid = LogicalName::new(NameEncoding::PosixBytes, vec![0xff], 255)?;
    assert!(matches!(
        materialize_host_path(
            None,
            NamespacePath::new(vec![invalid], limits)?.components(),
            &mut receipt
        ),
        Err(MaterializeError::UnrepresentableName)
    ));
    Ok(())
}
