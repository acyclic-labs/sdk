//! Validate resolution against the original durable join publication.
use super::*;

/// Checks marker-bearing files from a published join against the current view.
/// Unchanged files and native metadata changes require no payload reads.
pub async fn git_merge_is_resolved<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    before: &Generation<A, O>,
    applied: &Generation<A, O>,
    current: &Generation<A, O>,
) -> Result<bool, GitCaptureError> {
    let cancellation = CancellationToken::new();
    let changes = before
        .diff_to(applied, 100_000, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value
        .0;
    let paths = changes
        .changed_paths(100_000, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(|failure| failure.error)?
        .value
        .0;
    for change in paths {
        let Some(after) = change.after else { continue };
        if after.kind != FileKind::Regular
            || change
                .before
                .is_some_and(|before| before.payload == after.payload)
        {
            continue;
        }
        let path = format!("/{}", capture::portable_changed_path(&change.path)?);
        let bytes = applied
            .read(
                &path,
                MAXIMUM_PATCH_FILE_BYTES,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| failure.error)?
            .value;
        if !std::str::from_utf8(&bytes).is_ok_and(crate::text_merge::has_conflict_markers) {
            continue;
        }
        drop(bytes);
        let bytes = match current
            .read(
                &path,
                MAXIMUM_PATCH_FILE_BYTES,
                WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
        {
            Ok(receipt) => receipt.value,
            Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => continue,
            Err(failure) => return Err(failure.error.into()),
        };
        if merge_markers::contains(&String::from_utf8_lossy(&bytes)) {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        // Only replacing the projected marker file resolves it. Unrelated
        // writes and a failed resolution leave the exact same entry unresolved.
        #[test]
        fn continuation_checks_the_projected_file(mutation in 0u8..8, text in "[a-z ]{0,32}") {
            futures::executor::block_on(async {
                let fs = crate::Fs::memory();
                let stop = CancellationToken::new();
                let workspace = fs.create_workspace("resolution", WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                workspace.write_text("/note", "base\n").await.unwrap();
                let before = workspace.head(WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                workspace.write_text("/note", &format!("<<<<<<< ours\nours\n=======\n{text}\n>>>>>>> theirs\n")).await.unwrap();
                let applied = workspace.head(WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                match mutation {
                    0 => {},
                    1 => { workspace.write_text("/note", &text).await.unwrap(); },
                    2 => { workspace.write_text("/unrelated", &text).await.unwrap(); },
                    3 => { workspace.remove("/note").await.unwrap(); },
                    value => {
                        let marker = ["<<<<<<< ours", "||||||| base", "=======", ">>>>>>> theirs"][usize::from(value - 4)];
                        workspace.write_text("/note", marker).await.unwrap();
                    },
                }
                let current = workspace.head(WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                prop_assert_eq!(git_merge_is_resolved(&before, &applied, &current).await.unwrap(), matches!(mutation, 1 | 3));
                Ok(())
            })?;
        }
    }
}
