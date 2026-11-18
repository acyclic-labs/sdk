use acyclic_fs::{ApplyOptions, ForkOptions, Fs, IdempotencyKey, JoinOutcome, PublicationPermit};
use acyclic_stream::OwnedBytes;
use std::error::Error;

use crate::fixtures::fork_workspace;

#[tokio::test]
async fn sibling_forks_adding_distinct_files_join_in_sequence() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let main = unbounded!(fs, create_workspace("siblings-main"))?;
    main.write_text("/README.md", "base\n").await?;
    let base = unbounded!(main, head())?;
    let first = fork_workspace(
        &main,
        "siblings-first",
        ForkOptions::from_generation(base.clone(), IdempotencyKey::new()),
    )
    .await?;
    let second = fork_workspace(
        &main,
        "siblings-second",
        ForkOptions::from_generation(base, IdempotencyKey::new()),
    )
    .await?;
    first.write_text("/a.rs", "a\n").await?;
    second.write_text("/b.rs", "b\n").await?;

    for (label, fork) in [("first", &first), ("second", &second)] {
        let plan = unbounded!(fork.join_into(&main), plan())?;
        let outcome = plan
            .apply(
                ApplyOptions {
                    if_target: plan.target_head(),
                    idempotency_key: IdempotencyKey::new(),
                },
                std::collections::BTreeMap::new(),
                PublicationPermit::Unrestricted,
            )
            .await?;
        let applied = match outcome {
            JoinOutcome::Applied(_) => true,
            JoinOutcome::Conflicted { conflicts, .. } => {
                let described = plan.describe_conflicts(conflicts, false).await?;
                return Err(format!("{label} join conflicted: {:?}", described.conflicts).into());
            }
            _ => false,
        };
        assert!(applied, "{label} join did not apply");
    }
    assert_eq!(
        unbounded!(main, read("/a.rs", 16))?,
        OwnedBytes::from_static(b"a\n")
    );
    assert_eq!(
        unbounded!(main, read("/b.rs", 16))?,
        OwnedBytes::from_static(b"b\n")
    );
    assert_eq!(
        unbounded!(main, read("/README.md", 16))?,
        OwnedBytes::from_static(b"base\n")
    );
    Ok(())
}
