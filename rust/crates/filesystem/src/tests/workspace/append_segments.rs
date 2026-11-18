use super::*;

#[path = "../../../tests/support/append.rs"]
mod fixture;
use fixture::stage;

#[tokio::test]
async fn one_empty_append_seals_its_transaction_without_changing_file_bytes()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-one-empty"))?;
    workspace.write_text("/log", "prefix").await?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let empty = stage(&transaction, b"").await?;
    assert_eq!(
        unbounded!(transaction, append_staged("/log", &[empty]))?,
        crate::ByteRange {
            offset: 6,
            length: 0
        }
    );
    assert!(!transaction.checkout.has_pending_mutations());
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::AlreadyCommitted(_)
    ));
    assert_eq!(unbounded!(workspace, read("/log", 16))?.as_ref(), b"prefix");
    Ok(())
}

#[tokio::test]
async fn append_segments_empty_parts_and_repeated_appends_have_explicit_ranges()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-empty"))?;
    workspace.write_text("/existing", "base").await?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let empty = stage(&transaction, b"").await?;
    assert_eq!(
        unbounded!(transaction, append_staged("/existing", &[empty, empty]))?,
        crate::ByteRange {
            offset: 4,
            length: 0
        }
    );
    // Empty segments leave the authored tree unchanged.
    assert!(!transaction.checkout.has_pending_mutations());
    assert_eq!(
        unbounded!(transaction, append_staged("/new", &[empty]))?,
        crate::ByteRange {
            offset: 0,
            length: 0
        }
    );
    let a = stage(&transaction, b"A").await?;
    let b = stage(&transaction, b"B").await?;
    assert_eq!(
        unbounded!(transaction, append_staged("/existing", &[a]))?,
        crate::ByteRange {
            offset: 4,
            length: 1
        }
    );
    assert_eq!(
        unbounded!(transaction, append_staged("/existing", &[empty, b]))?,
        crate::ByteRange {
            offset: 5,
            length: 1
        }
    );
    assert!(matches!(
        commit(&mut transaction).await?,
        TransactionCommit::Committed(_)
    ));
    assert_eq!(
        unbounded!(workspace, read("/existing", 16))?.as_ref(),
        b"baseAB"
    );
    assert!(unbounded!(workspace, read("/new", 1))?.is_empty());
    Ok(())
}

#[tokio::test]
async fn append_segments_budget_cuts_leave_the_candidate_and_identity_unpublished()
-> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("append-budget-cuts"))?;
    workspace.write_text("/log", "base").await?;
    let head = unbounded!(workspace, head())?.id();
    let mut probe = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let parts = [
        stage(&probe, b"one").await?,
        stage(&probe, b"two").await?,
        stage(&probe, b"three").await?,
    ];
    let full = probe
        .append_staged(
            "/log",
            &parts,
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await?;
    assert!(full.work.items_examined > parts.len() as u64);
    let mut refused = 0;
    for limit in 0..full.work.items_examined {
        let key = IdempotencyKey::new();
        let mut transaction = unbounded!(workspace, begin_transaction(key))?;
        let root = transaction.checkout.root().clone();
        let result = transaction
            .append_staged(
                "/log",
                &parts,
                WorkBudget {
                    items_examined: limit,
                    ..WorkBudget::UNBOUNDED
                },
                &CancellationToken::new(),
            )
            .await;
        if result.is_err() {
            refused += 1;
            assert_eq!(
                transaction.checkout.root(),
                &root,
                "partial append at item budget {limit}"
            );
            assert!(!transaction.checkout.has_pending_mutations());
            assert!(unbounded!(workspace, operation_generation(key))?.is_none());
        }
        assert_eq!(unbounded!(workspace, head())?.id(), head);
    }
    assert!(refused > parts.len());
    assert_eq!(unbounded!(workspace, read("/log", 16))?.as_ref(), b"base");
    Ok(())
}
