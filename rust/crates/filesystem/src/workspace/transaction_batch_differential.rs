use super::*;
use crate::WorkCounters;
use acyclic_test_support::differential;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

type MemoryWorkspace = Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;
type MemoryTransaction = Transaction<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>;
type Output = (
    Result<GenerationId, String>,
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
            differential::record("publish", 1);
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
    differential::pair(
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
                            .write_batch_with_accounting::<true>(
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
                Ok(()) => runtime.block_on(commit(&mut transaction)),
                Err(error) => Err(format!("{error:?}")),
            };
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
    prop_assert!(
        old.0 == new.0 && old.1 == new.1 && old_trace == new_trace,
        "independent staging differs: old={old:?}, new={new:?}, old_trace={old_trace:?}, new_trace={new_trace:?}"
    );
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

#[test]
fn staging_accounting_measurements() {
    for invalid in [false, true] {
        let [(old, old_trace), (new, new_trace)] = pair(&[], invalid, Fault::None);
        assert!(old.0 == new.0 && old.1 == new.1 && old_trace == new_trace);
        eprintln!(
            "PERF_ACCOUNTING {}",
            serde_json::json!({
                "case": format!("staging.{invalid}"), "work": new.2,
                "allocations": differential::allocation_counts(new.3),
            })
        );
    }
}
