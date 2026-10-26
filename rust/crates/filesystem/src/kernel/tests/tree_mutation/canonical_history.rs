use super::*;

/// Both forks start at the same empty tree and install the same stable
/// identities. Only publication boundaries differ; metadata is not regenerated.
#[tokio::test]
async fn identical_fork_contents_have_identical_directory_roots()
-> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryObjectStore::default();
    let cancellation = crate::CancellationToken::new();
    let limits = DecodeLimits::default();
    let base = put(&store, &TreePage::Leaf(Vec::new()), 1_024).await?;
    let entries = (0..192_u16)
        .map(|value| wide_entry(value, FileKind::Regular))
        .collect::<Result<Vec<_>, _>>()?;
    let one_batch = apply_tree_mutations(
        &store,
        base,
        entries.iter().cloned().map(TreeMutation::Insert).collect(),
        192,
        limits,
        WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .await?
    .value;
    let mut incremental = base;
    for batch in entries.chunks(17) {
        incremental = apply_tree_mutations(
            &store,
            incremental,
            batch.iter().cloned().map(TreeMutation::Insert).collect(),
            17,
            limits,
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    }
    assert_eq!(flatten_tree(&store, one_batch).await?, entries);
    assert_eq!(flatten_tree(&store, incremental).await?, entries);
    assert_eq!(
        one_batch, incremental,
        "identical fork contents must have history-independent B+tree roots"
    );
    Ok(())
}
