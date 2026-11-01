use super::*;

#[tokio::test]
async fn historical_positions_use_the_index_within_the_observed_prefix() -> TestResult {
    let cancel = CancellationToken::new();
    let (volume, mut writer) = fixture(config(), &cancel).await?;
    let old = open_checkout(&volume, pinned(), &cancel).await?;
    let generation = old.generation_id();
    let genesis = old
        .generation_position(generation, WorkBudget::UNCHARGED, &cancel)
        .await?;
    assert_eq!(genesis.value, (Sequence::new(1), Sequence::new(1)));
    assert_eq!(genesis.work.backend_read_operations, 0);
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
    for _ in 0..2 {
        let lookup = current.generation_position(generation, B, &cancel).await?;
        assert_eq!(lookup.value, (Sequence::new(1), Sequence::new(5)));
        assert_eq!(lookup.work.authority_records_read, 1);
        assert_eq!(lookup.work.backend_read_operations, 3);
    }
    assert!(matches!(
        old.generation_position(current.generation_id(), B, &cancel)
            .await,
        Err(OperationFailure {
            error: FsError::NotFound,
            ..
        })
    ));
    Ok(())
}
