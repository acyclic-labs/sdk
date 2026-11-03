use super::*;
use crate::WorkCounters;
use acyclic_test_support::differential;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

type MemoryWorkspace = Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;
type MemoryTransaction = Transaction<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;
type Output = (
    Result<BTreeMap<String, FileKind>, String>,
    BTreeMap<String, Result<Vec<u8>, String>>,
    WorkCounters,
    acyclic_test_support::AllocationInfo,
);
#[derive(Clone, Copy)]
enum Fault {
    None,
    Serial,
}
macro_rules! value {
    ($e:expr) => {
        $e.await.map(|r| r.value).map_err(|f| f.error)
    };
}

async fn commit(transaction: &mut MemoryTransaction) -> Result<GenerationId, String> {
    match value!(transaction.commit::<WorkspaceError, _, _>(
        None,
        crate::PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        |_| async {
            differential::record(("publish", 1u64));
            Ok(crate::OperationReceipt {
                value: (),
                work: Default::default(),
            })
        }
    ))
    .map_err(|error| format!("{error:?}"))?
    {
        crate::TransactionCommit::Committed(generation)
        | crate::TransactionCommit::AlreadyCommitted(generation) => Ok(generation.id()),
        _ => Err("unexpected publication outcome".to_owned()),
    }
}

/// Every name and kind the published head binds. File identities are not
/// compared: an authored creation's identity includes its working file table
/// and its ordinal in the compiled batch (#1951), so it depends on whether the
/// same files were staged serially or as one batch.
async fn namespace(workspace: &MemoryWorkspace) -> Result<BTreeMap<String, FileKind>, String> {
    let cancellation = CancellationToken::new();
    let mut names = BTreeMap::new();
    let mut pending = vec!["/".to_owned()];
    while let Some(directory) = pending.pop() {
        let mut after = None;
        loop {
            let (page, _allocation) = value!(workspace.list_directory(
                &directory,
                after.as_ref(),
                64,
                WorkBudget::UNBOUNDED,
                &cancellation
            ))
            .map_err(|error| format!("{error:?}"))?;
            for entry in &page.entries {
                let name = String::from_utf8(entry.name.as_bytes().to_vec())
                    .map_err(|error| error.to_string())?;
                let path = format!("{}/{name}", directory.trim_end_matches('/'));
                if entry.kind == FileKind::Directory {
                    pending.push(path.clone());
                }
                names.insert(path, entry.kind);
            }
            if !page.has_more {
                break;
            }
            after = page.entries.last().map(|entry| entry.name.clone());
        }
    }
    Ok(names)
}

async fn initialize() -> (MemoryWorkspace, MemoryTransaction) {
    let cancellation = CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;
    let workspace =
        value!(crate::Fs::memory().create_workspace("staging-differential", budget, &cancellation))
            .unwrap();
    let mut setup = value!(workspace.begin_transaction(
        crate::IdempotencyKey::from_bytes([1; 16]),
        budget,
        &cancellation
    ))
    .unwrap();
    value!(setup.create_dir_all("/out", budget, &cancellation)).unwrap();
    setup
        .write_text("/out/existing", "original bytes with a tail")
        .await
        .unwrap();
    setup.write_text("/keep", "untouched").await.unwrap();
    commit(&mut setup).await.unwrap();
    let transaction = value!(workspace.begin_transaction(
        crate::IdempotencyKey::from_bytes([2; 16]),
        budget,
        &cancellation
    ))
    .unwrap();
    (workspace, transaction)
}

fn pair(
    history: &[(u8, Vec<u8>)],
    invalid: bool,
    fault: Fault,
) -> [(Output, Vec<(&'static str, u64)>); 2] {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    differential::pair::<_, _, (&'static str, u64)>(
        || runtime.block_on(initialize()),
        |(workspace, mut transaction), old| {
            let cancellation = CancellationToken::new();
            let mut files = vec![
                (
                    "/out/existing".to_owned(),
                    b"a replacement with a long tail".to_vec(),
                ),
                ("/out/nested/empty".to_owned(), Vec::new()),
                ("/out/blob".to_owned(), vec![42; 512]),
            ];
            files.extend(
                history
                    .iter()
                    .map(|(file, bytes)| (format!("/out/f{}", file % 3), bytes.clone())),
            );
            files.push(("/out/existing".to_owned(), b"exact".to_vec()));
            if invalid {
                files.push(("/out".to_owned(), b"denied".to_vec()));
            }
            let mut observed: BTreeSet<_> = files.iter().map(|(path, _)| path.clone()).collect();
            observed.insert("/keep".to_owned());
            let files: Vec<_> = files
                .iter()
                .map(|(path, bytes)| {
                    (
                        path.as_str(),
                        acyclic_stream::OwnedBytes::copy_from_slice(bytes),
                    )
                })
                .collect();
            let directories = ["/out/nested", "/out"];
            let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
            let mut result = Ok(());
            let measured = acyclic_test_support::measure(|| {
                result = runtime.block_on(async {
                    if old || matches!(fault, Fault::Serial) {
                        for directory in directories {
                            transaction
                                .create_dirs(directory, &mut accounting, &cancellation)
                                .await?;
                        }
                        for (path, bytes) in &files {
                            super::batch_original::write(
                                &mut transaction,
                                path,
                                bytes.clone(),
                                &mut accounting,
                                &cancellation,
                            )
                            .await?;
                        }
                        Ok(())
                    } else {
                        transaction
                            .stage_file_batch::<true, ReplaceFiles>(
                                &directories,
                                &files,
                                &mut accounting,
                                &cancellation,
                            )
                            .await
                    }
                });
            });
            let result = match result {
                Ok(()) => runtime.block_on(async {
                    commit(&mut transaction).await?;
                    namespace(&workspace).await
                }),
                Err(error) => Err(format!("{error:?}")),
            };
            if let Ok(id) = &result {
                let head = runtime
                    .block_on(workspace.head(WorkBudget::UNBOUNDED, &cancellation))
                    .unwrap();
                assert_eq!(head.value.id(), *id);
            }
            let mut bytes = BTreeMap::new();
            for path in observed {
                let read = runtime
                    .block_on(workspace.read(&path, 4096, WorkBudget::UNBOUNDED, &cancellation))
                    .map(|r| r.value.to_vec())
                    .map_err(|f| format!("{:?}", f.error));
                bytes.insert(path, read);
            }
            (result, bytes, accounting.spent(), measured)
        },
    )
}

fn check(history: &[(u8, Vec<u8>)], invalid: bool) -> TestCaseResult {
    let [(old, old_trace), (new, new_trace)] = pair(history, invalid, Fault::None);
    equivalent(&old, &old_trace, &new, &new_trace, invalid)
}

fn equivalent(
    old: &Output,
    old_trace: &[(&'static str, u64)],
    new: &Output,
    new_trace: &[(&'static str, u64)],
    invalid: bool,
) -> TestCaseResult {
    // authored_file_id includes the working table and compiled-batch ordinal:
    // serial and batch creations need not have the same opaque identities.
    // Each successful result is checked against its own published head above.
    // Require the intended outcome, rather than accepting two equal failures.
    let expected_error = invalid.then(|| format!("{:?}", WorkspaceError::NotRegularFile));
    prop_assert_eq!(old.0.as_ref().err(), expected_error.as_ref());
    prop_assert_eq!(new.0.as_ref().err(), expected_error.as_ref());
    prop_assert_eq!(&old.1, &new.1);
    prop_assert_eq!(old_trace, new_trace);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn batching_keeps_complete_generation_file_bytes_errors_and_publication_trace(
        history in prop::collection::vec((any::<u8>(), prop::collection::vec(any::<u8>(), 0..32)), 0..6),
    ) {
        for invalid in [false, true] { check(&history, invalid)?; }
    }
}

#[path = "transaction_batch_exact.rs"]
mod exact;
