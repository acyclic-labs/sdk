use super::*;

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[tokio::test]
async fn reopened_sibling_promotions_return_conflict_without_missing_objects()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let main = unbounded!(fs, create_workspace("promote-main"))?;
        main.write_text("/README.md", "base\n").await?;
        let mut transaction = unbounded!(main, begin_transaction(IdempotencyKey::new()))?;
        unbounded!(transaction, create_dir_all("/src"))?;
        commit(&mut transaction).await?;
        main.write_text("/src/main.rs", "fn main() {}\n").await?;
        let base = unbounded!(main, head())?;
        fork_workspace(
            &main,
            "promote-first",
            ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
        )
        .await?;
        fork_workspace(
            &main,
            "promote-second",
            ForkOptions::from_generation(base, IdempotencyKey::new()),
        )
        .await?;
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        unbounded!(fs, open_workspace("promote-first"))?
            .write_text("/README.md", "first\n")
            .await?;
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let main = unbounded!(fs, open_workspace("promote-main"))?;
        let first = unbounded!(fs, open_workspace("promote-first"))?;
        let plan = first
            .join_into(&main)
            .history(JoinHistory::Merge)
            .plan()
            .await?;
        assert!(matches!(
            plan.apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new()
                },
                crate::PublicationPermit::Unrestricted
            )
            .await?,
            JoinOutcome::Applied(_)
        ));
        assert_eq!(
            unbounded!(main, read("/README.md", 64))?,
            OwnedBytes::from_static(b"first\n")
        );
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        unbounded!(fs, open_workspace("promote-second"))?
            .write_text("/README.md", "second\n")
            .await?;
    }
    {
        let fs = Fs::local(crate::LocalOptions::new(directory.path())).await?;
        let main = unbounded!(fs, open_workspace("promote-main"))?;
        let second = unbounded!(fs, open_workspace("promote-second"))?;
        let plan = second
            .join_into(&main)
            .history(JoinHistory::Merge)
            .plan()
            .await?;
        assert!(matches!(
            plan.apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new()
                },
                crate::PublicationPermit::Unrestricted
            )
            .await?,
            JoinOutcome::Conflicted { .. }
        ));
        assert_eq!(
            unbounded!(main, read("/README.md", 64))?,
            OwnedBytes::from_static(b"first\n")
        );
    }
    Ok(())
}
