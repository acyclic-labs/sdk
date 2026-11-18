use super::*;

#[tokio::test]
async fn workspace_forks_an_unpublished_checkpoint_without_mutating_its_source()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let source = unbounded!(fs, create_workspace("checkpoint-source"))?;
    let mut transaction = unbounded!(source, begin_transaction(IdempotencyKey::new()))?;
    transaction
        .write_text("/candidate-only", "candidate")
        .await?;
    let checkpoint = transaction
        .checkout
        .checkpoint(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await?
        .value;
    let unpublished = unbounded!(source, generation(checkpoint))?;
    let child = fork_workspace(
        &source,
        "checkpoint-child",
        ForkOptions::from_generation(unpublished, IdempotencyKey::from_bytes([0x44; 16])),
    )
    .await?;

    assert_eq!(
        unbounded!(child, read("/candidate-only", 64))?,
        OwnedBytes::from_static(b"candidate")
    );
    assert!(unbounded!(source, read("/candidate-only", 64)).is_err());
    Ok(())
}
