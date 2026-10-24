use super::*;

#[tokio::test]
async fn native_request_denials_precede_real_io_and_buffer_allocation() -> TestResult {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("file"), b"actual source bytes")?;
    let source = open_source(directory.path()).await?;
    let path = requested("file")?;
    let token = CancellationToken::new();
    let budget = FINITE_BUDGET;
    let node = source
        .lookup(source.reference(), &path, budget, &token)
        .await?
        .value
        .ok_or("file absent")?;

    let mut no_io = budget;
    no_io.backend_read_operations = 0;
    let failure = source
        .lookup(source.reference(), &path, no_io, &token)
        .await
        .err()
        .ok_or("zero IO was not enforced")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.backend_read_operations, 0);
    failure.work.verify(no_io)?;

    let mut no_memory = budget;
    no_memory.allocation_operations = 0;
    let failure = source
        .lookup(source.reference(), &path, no_memory, &token)
        .await
        .err()
        .ok_or("path clone exceeded allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.allocation_operations, 0);
    assert_eq!(failure.work.backend_read_operations, 0);
    failure.work.verify(no_memory)?;

    let mut no_output = budget;
    no_output.output_bytes = 0;
    let failure = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            no_output,
            &token,
        )
        .await
        .err()
        .ok_or("range output allowance was ignored")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_bytes_read, 0);
    assert_eq!(failure.work.output_bytes, 0);
    failure.work.verify(no_output)?;

    token.cancel();
    let lookup = source
        .lookup(source.reference(), &path, budget, &token)
        .await
        .err()
        .ok_or("cancelled lookup succeeded")?;
    let read = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            budget,
            &token,
        )
        .await
        .err()
        .ok_or("cancelled range succeeded")?;
    let opened = source
        .open_file(source.reference(), &path, node.version, budget, &token)
        .await
        .err()
        .ok_or("cancelled open succeeded")?;
    let page = source
        .list_page(source.reference(), &requested("")?, None, 1, budget, &token)
        .await
        .err()
        .ok_or("cancelled page succeeded")?;
    for failure in [lookup, read, opened, page] {
        assert!(matches!(failure.error, DemandError::Cancelled));
        assert_eq!(*failure.work, WorkCounters::default());
    }
    Ok(())
}

#[tokio::test]
async fn native_read_failure_retains_bytes_spent_before_current_fence_denial() -> TestResult {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("file"), b"native-read")?;
    let source = open_source(directory.path()).await?;
    let path = requested("file")?;
    let token = CancellationToken::new();
    let budget = FINITE_BUDGET;
    let node = source
        .lookup(source.reference(), &path, budget, &token)
        .await?
        .value
        .ok_or("file absent")?;
    let baseline = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            budget,
            &token,
        )
        .await?;
    assert_eq!(baseline.value.bytes.as_ref(), b"native");
    baseline.work.verify(budget)?;
    let mut limited = budget;
    limited.backend_read_operations = baseline
        .work
        .backend_read_operations
        .checked_sub(1)
        .ok_or("no native IO")?;
    let failure = source
        .read_range(
            source.reference(),
            &path,
            node.version,
            0,
            6,
            limited,
            &token,
        )
        .await
        .err()
        .ok_or("current-proof IO exceeded allowance")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_bytes_read, 6);
    assert_eq!(failure.work.output_bytes, 0);
    failure.work.verify(limited)?;

    let file = source
        .open_file(source.reference(), &path, node.version, budget, &token)
        .await?
        .value;
    let baseline = {
        let allowance = budget;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = (*file).read_range(0, 6, &mut accounting);
        allowance.finish(result, &accounting)
    }?;
    limited.backend_read_operations = baseline
        .work
        .backend_read_operations
        .checked_sub(1)
        .ok_or("no held native IO")?;
    let failure = {
        let allowance = limited;
        let cancellation = &token;
        let mut accounting = allowance.accounting(cancellation);
        let result = (*file).read_range(0, 6, &mut accounting);
        allowance.finish(result, &accounting)
    }
    .err()
    .ok_or("held file ignored native IO limit")?;
    assert!(matches!(failure.error, DemandError::Work(_)));
    assert_eq!(failure.work.source_bytes_read, 6);
    assert_eq!(failure.work.output_bytes, 0);
    failure.work.verify(limited)?;
    Ok(())
}
