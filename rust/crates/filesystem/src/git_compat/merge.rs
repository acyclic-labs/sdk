//! Validate only unresolved identities retained in the pending Git join.
use super::*;

/// Checks only the actual conflicted paths in the current immutable view.
/// Marker recognition streams bounded ranges; clean files are never read.
/// Removing a conflicted path is a valid explicit deletion resolution.
pub async fn git_merge_is_resolved<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    current: &Generation<A, O>,
    paths: &BTreeSet<String>,
) -> Result<bool, GitCaptureError> {
    let cancellation = CancellationToken::new();
    for path in paths {
        let stat = match current
            .stat(path, WorkBudget::UNBOUNDED, &cancellation)
            .await
        {
            Ok(receipt) => receipt.value,
            Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => continue,
            Err(failure) => return Err(failure.error.into()),
        };
        if stat.kind != FileKind::Regular {
            continue;
        }
        let length = stat
            .logical_bytes
            .ok_or(WorkspaceError::InvalidMergeResolution)?;
        let mut scanner = merge_markers::Scanner::new();
        let mut offset = 0;
        while offset < length {
            let bytes = current
                .read_range(
                    path,
                    offset,
                    (length - offset).min(64 * 1024),
                    WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map_err(|failure| failure.error)?
                .value;
            if bytes.bytes.is_empty() {
                return Err(WorkspaceError::InvalidMergeResolution.into());
            }
            offset += bytes.bytes.len() as u64;
            if scanner.push(&bytes.bytes) {
                return Ok(false);
            }
        }
        if scanner.finish() {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(all(test, not(acyclic_fs_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn a_large_clean_file_cannot_block_small_conflict_resolution() {
        futures::executor::block_on(async {
            let fs = crate::Fs::memory();
            let stop = CancellationToken::new();
            let workspace = fs
                .create_workspace("large-clean", WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap()
                .value;
            let mut large = vec![b'x'; 17 * 1024 * 1024];
            large[..14].copy_from_slice(b">>>>>>> clean\n");
            let mut transaction = workspace
                .begin_transaction(crate::IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap()
                .value;
            let content = transaction
                .stage_content(
                    &mut std::io::Cursor::new(large),
                    17 * 1024 * 1024,
                    WorkBudget::UNBOUNDED,
                    &stop,
                )
                .await
                .unwrap()
                .value;
            transaction
                .write_staged("/large", &[content], WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap();
            transaction
                .commit::<WorkspaceError, _, _>(
                    None,
                    crate::PublicationPermit::Unrestricted,
                    WorkBudget::UNBOUNDED,
                    &stop,
                    |_| async {
                        Ok(crate::OperationReceipt {
                            value: (),
                            work: crate::WorkCounters::UNCHARGED,
                        })
                    },
                )
                .await
                .unwrap();
            workspace
                .write_text(
                    "/note",
                    "<<<<<<< ours\nours\n=======\ntheirs\n>>>>>>> theirs\n",
                )
                .await
                .unwrap();
            let paths = BTreeSet::from(["/note".into()]);
            let head = workspace
                .head(WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap()
                .value;
            assert!(!git_merge_is_resolved(&head, &paths).await.unwrap());
            // Negative control: the former all-changed-files full read fails.
            assert!(
                head.read("/large", 64 * 1024 * 1024, WorkBudget::UNBOUNDED, &stop)
                    .await
                    .is_err()
            );
            workspace.write_text("/note", "resolved\n").await.unwrap();
            let head = workspace
                .head(WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap()
                .value;
            assert!(git_merge_is_resolved(&head, &paths).await.unwrap());
        });
    }

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
                workspace.write_text("/note", &format!("<<<<<<< ours\nours\n=======\n{text}\n>>>>>>> theirs\n")).await.unwrap();
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
                prop_assert_eq!(git_merge_is_resolved(&current, &BTreeSet::from(["/note".into()])).await.unwrap(), matches!(mutation, 1 | 3));
                Ok(())
            })?;
        }
    }
}
