use super::*;

/// A due-index read must observe staged moves without reopening head. Its
/// original selected cut still refuses a later concurrent edit at publication.
#[tokio::test]
async fn staged_reads_keep_the_original_generation_and_publication_fences()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let cancel = CancellationToken::new();
    let workspace = value!(
        fs.create_workspace("staged-index", WorkBudget::UNBOUNDED, &cancel)
            .await
    )?;
    let mut initial = value!(
        workspace
            .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancel)
            .await
    )?;
    value!(
        initial
            .create_dir_all("schedules", WorkBudget::UNBOUNDED, &cancel)
            .await
    )?;
    initial.write_text("schedules/0001", "first").await?;
    commit(&mut initial).await?;
    let mut candidate = value!(
        workspace
            .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancel)
            .await
    )?;
    candidate.write_text("schedules/0000", "staged").await?;
    let (page, _allocation) = value!(
        candidate
            .list_directory("schedules", None, 1, WorkBudget::UNBOUNDED, &cancel)
            .await
    )?;
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].name.as_bytes(), b"0000");
    assert_eq!(
        value!(
            candidate
                .read("schedules/0000", 6, WorkBudget::UNBOUNDED, &cancel)
                .await
        )?
        .as_ref(),
        b"staged"
    );
    assert!(matches!(
        value!(
            workspace
                .read("schedules/0000", 6, WorkBudget::UNBOUNDED, &cancel)
                .await
        ),
        Err(WorkspaceError::NotFound)
    ));
    assert!(matches!(
        value!(
            candidate
                .read("schedules/0000", 5, WorkBudget::UNBOUNDED, &cancel)
                .await
        ),
        Err(WorkspaceError::ReadLimitExceeded)
    ));
    let mut concurrent = value!(
        workspace
            .begin_transaction(IdempotencyKey::new(), WorkBudget::UNBOUNDED, &cancel)
            .await
    )?;
    concurrent
        .write_text("schedules/0000", "concurrent")
        .await?;
    commit(&mut concurrent).await?;
    // The existing candidate remains stable after another publisher advances head.
    assert_eq!(
        value!(
            candidate
                .read("schedules/0000", 6, WorkBudget::UNBOUNDED, &cancel)
                .await
        )?
        .as_ref(),
        b"staged"
    );
    assert!(matches!(
        commit(&mut candidate).await?,
        TransactionCommit::Conflict { .. }
    ));
    assert_eq!(
        value!(
            workspace
                .read("schedules/0000", 10, WorkBudget::UNBOUNDED, &cancel)
                .await
        )?
        .as_ref(),
        b"concurrent"
    );
    Ok(())
}
