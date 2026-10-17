use super::*;

#[tokio::test]
async fn historical_positions_reuse_verified_replay_without_io_or_allocation() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut writer) = fixture(config(), &cancel).await?;
    let old = open_checkout(&volume, pinned(), &cancel).await?;
    let generation = old.generation_id();
    for index in 0..4 {
        create(
            &mut writer,
            &format!("file-{index}"),
            OwnedBytes::from_static(b"content"),
            &cancel,
        )
        .await?;
        commit(&mut writer, &cancel).await?;
    }
    let current = open_checkout(&volume, pinned(), &cancel).await?;
    let cold = current.generation_position(generation, B, &cancel).await?;
    assert_eq!(cold.value, (Sequence::new(1), Sequence::new(5)));
    assert!(cold.work.authority_records_read > 0);
    let budget = WorkBudget {
        items_examined: super::super::generation_cache::CAPACITY as u64,
        bytes_copied: size_of::<Sequence>() as u64,
        ..WorkBudget::UNCHARGED
    };
    let hot = current
        .generation_position(generation, budget, &cancel)
        .await?;
    assert_eq!(hot.value, cold.value);
    assert_eq!(hot.work.backend_read_operations, 0);
    assert_eq!(hot.work.authority_records_read, 0);
    assert_eq!(hot.work.allocation_operations, 0);
    hot.work.verify(budget)?;

    // A newer observation cannot inject a generation into an older prefix.
    volume
        .fs
        .inner
        .generation_positions
        .record((volume.id, current.generation_id()), Sequence::new(5));
    assert!(matches!(
        old.generation_position(current.generation_id(), B, &cancel)
            .await,
        Err(OperationFailure {
            error: FsError::NotFound,
            ..
        })
    ));
    // The current head keeps its latest position even if this root is already
    // known as historical; the historical cache never replaces head evidence.
    volume
        .fs
        .inner
        .generation_positions
        .record((volume.id, current.generation_id()), Sequence::new(2));
    assert_eq!(
        current
            .generation_position(current.generation_id(), B, &cancel)
            .await?
            .value,
        (Sequence::new(5), Sequence::new(5))
    );
    Ok(())
}
