use acyclic_fs::{CancellationToken, Fs};
use acyclic_fs_work::performance::WorkBudget;
use std::error::Error;

/// An immutable generation's cached absence is exact: asking again for
/// identities that have no paths answers empty rather than validating an
/// empty path batch.
#[tokio::test]
async fn repeated_lookups_of_absent_identities_answer_empty() -> Result<(), Box<dyn Error>> {
    let fs = Fs::memory();
    let workspace = unbounded!(fs, create_workspace("absent-identities"))?;
    workspace.write_text("/kept", "kept").await?;
    workspace.write_text("/gone", "gone").await?;
    let gone = unbounded!(workspace, stat("/gone"))?.file_id;
    workspace.remove("/gone").await?;
    let head = unbounded!(workspace, head())?;
    for _ in 0..2 {
        assert!(
            head.paths_for_file_ids(
                [gone],
                Some(32),
                WorkBudget::UNBOUNDED,
                &CancellationToken::new()
            )
            .await?
            .value
            .entries()
            .is_empty()
        );
    }
    Ok(())
}
