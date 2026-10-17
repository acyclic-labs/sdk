use super::*;

#[tokio::test]
async fn pinned_directory_discovery_does_not_scan_ten_thousand_retained_workspaces()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("discovered-agent"))?;
    workspace.write_text("/visible.txt", "visible").await?;
    let pinned = unbounded!(workspace, head())?;
    let cancellation = CancellationToken::new();
    let before = pinned
        .list_directory("/", None, 1, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    let mut retained = Vec::with_capacity(10_000);
    for index in 0..10_000 {
        let other = unbounded!(fs, create_workspace(&format!("retained-agent-{index}")))?;
        retained.push(unbounded!(other, head())?);
    }
    workspace
        .write_text("/late.txt", "not in pinned view")
        .await?;
    let after = pinned
        .list_directory("/", None, 1, WorkBudget::UNBOUNDED, &cancellation)
        .await?;
    assert_eq!(retained.len(), 10_000);
    // The listing is identical; only its own work may fall, because decoded objects are cached after the first
    // listing (#788). Retained workspaces never add work to a pinned listing.
    let (first, second) = (&before.value.0, &after.value.0);
    assert_eq!(
        (&first.entries, first.has_more, &first.next_residency),
        (&second.entries, second.has_more, &second.next_residency)
    );
    assert!(second.work.backend_read_operations <= first.work.backend_read_operations);
    assert!(second.work.object_bytes_read <= first.work.object_bytes_read);
    assert_eq!(second.work.source_entries_visited, 0);
    assert_eq!(after.value.0.entries.len(), 1);
    assert!(after.work.backend_read_operations <= before.work.backend_read_operations);
    assert!(after.work.page_reads <= before.work.page_reads);
    assert_eq!(after.work.source_entries_visited, 0);
    assert_eq!(after.work.materializations, 0);
    Ok(())
}

#[tokio::test]
async fn bounded_path_index_hit_never_substitutes_a_false_empty_result()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("bounded-path-index"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    transaction.write_text("/a", "body").await?;
    unbounded!(transaction, hard_link("/a", "/b"))?;
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    let generation = unbounded!(workspace, head())?;
    let id = unbounded!(generation, stat("/a"))?.file_id;
    let complete = generation.paths_for_file_ids([id], 16).await?;
    assert_eq!(complete[&id], vec!["/a", "/b"]);
    let bounded = generation.paths_for_file_ids([id], 1).await?;
    assert_eq!(bounded[&id].len(), 1);
    Ok(())
}
