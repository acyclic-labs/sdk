//! Validate only unresolved identities retained in the pending Git join.
use super::*;

/// The current projection of a retained Git join's conflicted identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GitMergeResolution {
    /// All conflict identities were repaired or explicitly deleted.
    Resolved(BTreeSet<String>),
    /// Current absolute paths that still contain unresolved markers.
    Conflicts(Vec<String>),
}

/// Returns live tracked paths after repair, or the live unresolved marker paths.
/// Retained identities follow renames. Only conflicted regular files are
/// streamed; removing a conflict is a valid explicit deletion resolution.
pub async fn git_merge_resolution<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    current: &Generation<A, O>,
    conflicts: &BTreeMap<String, Option<crate::FileId>>,
    tracked: &BTreeSet<String>,
) -> Result<GitMergeResolution, GitCaptureError> {
    let cancellation = CancellationToken::new();
    let mut paths: BTreeMap<_, _> = tracked
        .iter()
        .map(|path| (format!("/{path}"), false))
        .collect();
    for path in conflicts.keys() {
        paths.insert(path.clone(), true);
    }
    let aliases = current
        .paths_for_file_ids(
            conflicts.values().filter_map(|id| *id),
            Some(u32::MAX),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    for (_, renamed) in aliases.entries() {
        for path in renamed {
            paths.insert(path.clone(), true);
        }
    }
    let mut resolved = BTreeSet::new();
    let mut unresolved = Vec::new();
    for (mut path, conflicted) in paths {
        let stat = match current
            .stat(&path, WorkBudget::UNBOUNDED, &cancellation)
            .await
        {
            Ok(receipt) => receipt.value,
            Err(failure) if failure.error.is_absent() => continue,
            Err(failure) => return Err(failure.error.into()),
        };
        if stat.kind == FileKind::Directory {
            continue;
        }
        if conflicted && stat.kind == FileKind::Regular {
            let length = stat
                .logical_bytes
                .ok_or(WorkspaceError::InvalidMergeResolution)?;
            let mut scanner = merge_markers::Scanner::new();
            let mut offset = 0;
            while offset < length {
                let bytes = current
                    .read_range(
                        &path,
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
                    break;
                }
            }
            if scanner.finish() {
                unresolved.push(path);
                continue;
            }
        }
        if path.starts_with('/') {
            path.remove(0);
        }
        resolved.insert(path);
    }
    if unresolved.is_empty() {
        Ok(GitMergeResolution::Resolved(resolved))
    } else {
        Ok(GitMergeResolution::Conflicts(unresolved))
    }
}

#[cfg(test)]
#[cfg(not(loom))]
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
            let paths = BTreeMap::from([("/note".into(), None)]);
            let tracked = BTreeSet::from(["note".into(), "large".into()]);
            let head = workspace
                .head(WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap()
                .value;
            assert_eq!(
                git_merge_resolution(&head, &paths, &tracked).await.unwrap(),
                GitMergeResolution::Conflicts(vec!["/note".into()])
            );
            workspace.write_text("/note", "resolved\n").await.unwrap();
            let head = workspace
                .head(WorkBudget::UNBOUNDED, &stop)
                .await
                .unwrap()
                .value;
            assert_eq!(
                git_merge_resolution(&head, &paths, &tracked).await.unwrap(),
                GitMergeResolution::Resolved(tracked)
            );
        });
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        // Resolution follows retained identities through renames and deletions;
        // unrelated files never enter the resulting tracked set.
        #[test]
        fn continuation_checks_the_projected_file(mutation in 0u8..10, text in "[a-z ]{0,32}", suffix in "[a-z]{1,16}") {
            futures::executor::block_on(async {
                let fs = crate::Fs::memory();
                let stop = CancellationToken::new();
                let workspace = fs.create_workspace("resolution", WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                workspace.write_text("/note", "base\n").await.unwrap();
                workspace.write_text("/note", &format!("<<<<<<< ours\nours\n=======\n{text}\n>>>>>>> theirs\n")).await.unwrap();
                let before = workspace.head(WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                let id = before.stat("/note", WorkBudget::UNBOUNDED, &stop).await.unwrap().value.file_id;
                workspace.write_text("/untracked", &text).await.unwrap();
                let renamed = format!("/renamed-{suffix}");
                match mutation {
                    0 => {},
                    1 => { workspace.write_text("/note", &text).await.unwrap(); },
                    2 => { workspace.write_text("/unrelated", &text).await.unwrap(); },
                    3 => { workspace.remove("/note").await.unwrap(); },
                    8 | 9 => {
                        let mut transaction = workspace.begin_transaction(crate::IdempotencyKey::new(), WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                        transaction.rename("/note", &renamed, false, WorkBudget::UNBOUNDED, &stop).await.unwrap();
                        transaction.commit::<WorkspaceError, _, _>(None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &stop,
                            |_| async { Ok(crate::OperationReceipt { value: (), work: crate::WorkCounters::UNCHARGED }) }).await.unwrap();
                        if mutation == 9 { workspace.write_text(&renamed, &text).await.unwrap(); }
                    },
                    value => {
                        let marker = ["<<<<<<< ours", "||||||| base", "=======", ">>>>>>> theirs"][usize::from(value - 4)];
                        workspace.write_text("/note", marker).await.unwrap();
                    },
                }
                let current = workspace.head(WorkBudget::UNBOUNDED, &stop).await.unwrap().value;
                let resolved = git_merge_resolution(&current, &BTreeMap::from([("/note".into(), Some(id))]), &BTreeSet::from(["note".into()])).await.unwrap();
                let expected = match mutation {
                    1 => GitMergeResolution::Resolved(BTreeSet::from(["note".into()])),
                    3 => GitMergeResolution::Resolved(BTreeSet::new()),
                    9 => GitMergeResolution::Resolved(BTreeSet::from([format!("renamed-{suffix}")])),
                    8 => GitMergeResolution::Conflicts(vec![renamed]),
                    _ => GitMergeResolution::Conflicts(vec!["/note".into()]),
                };
                prop_assert_eq!(resolved, expected);
                Ok(())
            })?;
        }
    }
}
