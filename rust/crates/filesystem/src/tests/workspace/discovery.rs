use super::*;

/// One measured listing of the pinned root: its page and the operation's total work.
type Listing = (crate::kernel::DirectoryPage, crate::WorkCounters);

/// The discovery oracle, independent of the implementation's own output: the fixture wrote exactly one file into the
/// pinned generation, so the page is exactly `visible.txt` with nothing after it. Retained workspaces and later writes
/// must not change a warm listing at all, including every work counter (reads, bytes, copies, allocations, items).
fn check_discovery(before: &Listing, after: &Listing) -> Result<(), String> {
    for (label, (page, _)) in [("before", before), ("after", after)] {
        let names = page
            .entries
            .iter()
            .map(|entry| (entry.name.as_bytes(), entry.kind))
            .collect::<Vec<_>>();
        if names != [(b"visible.txt".as_slice(), crate::kernel::FileKind::Regular)]
            || page.has_more
            || page.next_residency.is_some()
        {
            return Err(format!(
                "{label}: listing differs from the fixture: {page:?}"
            ));
        }
    }
    if before != after {
        return Err(format!("warm listings differ: {before:?} != {after:?}"));
    }
    let (page, work) = after;
    if page.work.source_entries_visited != 0
        || work.source_entries_visited != 0
        || work.materializations != 0
    {
        return Err(format!(
            "a pinned listing visited source entries or materialized: {work:?}"
        ));
    }
    Ok(())
}

#[tokio::test]
async fn pinned_directory_discovery_does_not_scan_ten_thousand_retained_workspaces()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("discovered-agent"))?;
    workspace.write_text("/visible.txt", "visible").await?;
    let pinned = unbounded!(workspace, head())?;
    let cancellation = CancellationToken::new();
    let list = || async {
        let listed = pinned
            .list_directory("/", None, 1, WorkBudget::UNBOUNDED, &cancellation)
            .await?;
        Ok::<Listing, Box<dyn Error>>((listed.value.0, listed.work))
    };
    // The first listing decodes and caches the root's objects (#788); both measured listings start from that same
    // warm cache state, so their work must be equal, not merely no larger.
    let cold = list().await?;
    let before = list().await?;
    let mut retained = Vec::with_capacity(10_000);
    for index in 0..10_000 {
        let other = unbounded!(fs, create_workspace(&format!("retained-agent-{index}")))?;
        retained.push(unbounded!(other, head())?);
    }
    workspace
        .write_text("/late.txt", "not in pinned view")
        .await?;
    let after = list().await?;
    assert_eq!(retained.len(), 10_000);
    check_discovery(&before, &after)?;
    // The cold listing reads the same page; the cache only ever removes work from it.
    assert_eq!(cold.0.entries, after.0.entries);
    assert!(after.1.backend_read_operations <= cold.1.backend_read_operations);
    // Negative controls: each fault the oracle targets is detected.
    let mut more = after.clone();
    more.0.has_more = true;
    let mut wrong = after.clone();
    wrong.0.entries.clear();
    let mut extra_read = after.clone();
    extra_read.1.backend_read_operations += 1;
    let mut extra_copy = after.clone();
    extra_copy.0.work.bytes_copied += 1;
    for (fault, observed) in [
        ("has_more on both listings", (more.clone(), more)),
        ("constant empty listing", (wrong.clone(), wrong)),
        ("an added backend read", (before.clone(), extra_read)),
        ("an added copy", (before.clone(), extra_copy)),
    ] {
        assert!(
            check_discovery(&observed.0, &observed.1).is_err(),
            "oracle missed {fault}"
        );
    }
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
