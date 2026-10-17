use super::*;
use crate::{Fs, OperationReceipt, PublicationPermit};
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;
use std::collections::BTreeMap;
macro_rules! value {
    ($expression:expr) => {
        $expression
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    };
}
#[derive(Clone, Copy)]
enum Fault {
    None,
    NoReset,
    OmitFinal,
    AcceptDirectory,
    MutationOnRejection,
    RejectAll,
}
async fn check(
    history: &[(u8, Vec<u8>)],
    maximum: u32,
    pending: bool,
    fault: Fault,
) -> TestCaseResult {
    let cancellation = CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;
    let fs = Fs::memory();
    let mut config = crate::model::VolumeConfig::portable(crate::model::Lifecycle::Ephemeral);
    config.limits.maximum_mutations_per_batch = maximum;
    config.limits.maximum_paths_per_batch = maximum;
    let workspace =
        value!(fs.create_workspace_with_config("file-batch", config, budget, &cancellation))
            .unwrap();
    let mut setup =
        value!(workspace.begin_transaction(crate::IdempotencyKey::new(), budget, &cancellation))
            .unwrap();
    value!(setup.create_dir_all("/out", budget, &cancellation)).unwrap();
    setup
        .write_text("/out/existing", "original bytes with a tail")
        .await
        .unwrap();
    setup.write_text("/keep", "untouched").await.unwrap();
    commit(&mut setup).await;
    let mut transaction =
        value!(workspace.begin_transaction(crate::IdempotencyKey::new(), budget, &cancellation))
            .unwrap();
    if pending {
        transaction.write_text("/pending", "earlier").await.unwrap();
    }
    // Exercise rejection before any healthy operation makes the checkout an
    // overlay, then check again after the batch through the serial fallback.
    let denied_before = value!(transaction.write_batch(
        &["/bad"],
        &[
            ("/bad/leak", Bytes::from_static(b"bad").into()),
            ("/out", Bytes::from_static(b"bad").into()),
        ],
        budget,
        &cancellation
    ));
    if matches!(fault, Fault::MutationOnRejection) {
        value!(transaction.create_dir_all("/bad", budget, &cancellation)).unwrap();
    }
    let mut files = vec![
        (
            "/out/existing".to_owned(),
            b"replacement with a longer tail".to_vec(),
        ),
        ("/out/nested/empty".to_owned(), Vec::new()),
        ("/out/blob".to_owned(), vec![42; 512]),
    ];
    files.extend(
        history
            .iter()
            .map(|(id, bytes)| (format!("/out/f{}", id % 3), bytes.clone())),
    );
    files.push(("/out/existing".to_owned(), b"exact".to_vec()));
    let mut expected: BTreeMap<_, _> = files.iter().cloned().collect();
    expected.insert("/keep".to_owned(), b"untouched".to_vec());
    if pending {
        expected.insert("/pending".to_owned(), b"earlier".to_vec());
    }
    if matches!(fault, Fault::OmitFinal) {
        files.pop();
    }
    let files: Vec<_> = files
        .iter()
        .map(|(path, bytes)| (path.as_str(), bytes.clone().into()))
        .collect();
    let directories = ["/out/nested", "/out", "/out/nested"];
    let written = match fault {
        Fault::NoReset => {
            let mut accounting = KernelAccounting::standalone(budget, &cancellation);
            transaction
                .write_batch_with_accounting::<false>(
                    &directories,
                    &files,
                    &mut accounting,
                    &cancellation,
                )
                .await
        }
        Fault::RejectAll => Err(WorkspaceError::NotFound),
        _ => value!(transaction.write_batch(&directories, &files, budget, &cancellation)),
    };
    let denied = value!(transaction.write_batch(
        &[],
        &[("/out", Bytes::from_static(b"bad").into())],
        budget,
        &cancellation
    ));
    let rejected = matches!(denied_before, Err(WorkspaceError::NotRegularFile))
        && matches!(denied, Err(WorkspaceError::NotRegularFile))
        && !matches!(fault, Fault::AcceptDirectory);
    if written.is_ok() {
        commit(&mut transaction).await;
    }
    let mut actual = BTreeMap::new();
    for path in expected.keys() {
        let bytes = value!(workspace.read(path, 4096, budget, &cancellation))
            .ok()
            .map(|bytes| bytes.to_vec());
        actual.insert(path.clone(), bytes);
    }
    let expected: BTreeMap<_, _> = expected
        .into_iter()
        .map(|(path, bytes)| (path, Some(bytes)))
        .collect();
    let directories_exist = value!(workspace.stat("/out/nested", budget, &cancellation))
        .is_ok_and(|stat| stat.kind == FileKind::Directory);
    let no_rejected_mutation = value!(workspace.stat("/bad", budget, &cancellation))
        .is_err_and(|error| matches!(error, WorkspaceError::NotFound));
    prop_assert!(
        written.is_ok()
            && rejected
            && no_rejected_mutation
            && directories_exist
            && actual == expected,
        "file batch differs from ordered input bytes or accepts a directory overwrite"
    );
    Ok(())
}
async fn commit<A: AsyncAuthorityStore, O: AsyncObjectStore>(transaction: &mut Transaction<A, O>) {
    value!(transaction.commit::<WorkspaceError, _, _>(
        None,
        PublicationPermit::Unrestricted,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
        |_| async {
            Ok(OperationReceipt {
                value: (),
                work: Default::default(),
            })
        }
    ))
    .unwrap();
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn file_batches_preserve_ordered_replacements_and_unrelated_files(
        history in prop::collection::vec((any::<u8>(), prop::collection::vec(any::<u8>(), 0..32)), 0..6),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        for maximum in [2, 8] {
            runtime.block_on(check(&history, maximum, false, Fault::None))?;
        }
        runtime.block_on(check_native(&history, Fault::None))?;
    }
}
#[tokio::test]
async fn file_batch_pending_overlay_keeps_the_original_write_path() {
    check(&[(0, vec![1, 2, 3])], 8, true, Fault::None)
        .await
        .unwrap();
}
#[tokio::test]
async fn file_batch_negative_controls() {
    check(&[], 8, false, Fault::None).await.unwrap();
    for fault in [
        Fault::NoReset,
        Fault::OmitFinal,
        Fault::AcceptDirectory,
        Fault::MutationOnRejection,
        Fault::RejectAll,
    ] {
        let error = check(&[], 8, false, fault).await.unwrap_err();
        assert!(error.to_string().contains(
            "file batch differs from ordered input bytes or accepts a directory overwrite"
        ));
    }
}

async fn check_native(history: &[(u8, Vec<u8>)], fault: Fault) -> TestCaseResult {
    let cancellation = CancellationToken::new();
    let budget = WorkBudget::UNBOUNDED;
    let workspace =
        value!(Fs::memory().create_workspace("native-batch", budget, &cancellation)).unwrap();
    let mut setup =
        value!(workspace.begin_transaction(crate::IdempotencyKey::new(), budget, &cancellation))
            .unwrap();
    value!(setup.create_dir_all("/out", budget, &cancellation)).unwrap();
    setup
        .write_text("/out/item", "original bytes with a tail")
        .await
        .unwrap();
    commit(&mut setup).await;
    let original = crate::NativeMaterializationWork::new(
        crate::performance::work_budget(1_000_000, 64 * 1024 * 1024),
        &cancellation,
    )
    .unwrap();
    let mut transaction = workspace
        .begin_transaction(crate::IdempotencyKey::new(), &original, &cancellation)
        .await
        .unwrap();
    let mut input = vec![("/out/blob".to_owned(), vec![42; 512])];
    input.extend(
        history
            .iter()
            .map(|(id, bytes)| (format!("/out/f{}", id % 3), bytes.clone())),
    );
    input.push(("/out/item".to_owned(), b"exact".to_vec()));
    let expected: BTreeMap<_, _> = input.iter().cloned().collect();
    let files: Vec<_> = input
        .iter()
        .map(|(path, data)| {
            let (bytes, custody) = original.copy_bytes(data).unwrap();
            (
                path.as_str(),
                acyclic_stream::OwnedBytes::from_vec_with_guard(
                    bytes,
                    custody,
                    original.runtime_meter(),
                )
                .unwrap(),
            )
        })
        .collect();
    let result = match fault {
        Fault::NoReset => {
            let mut accounting = KernelAccounting::NativeOriginal {
                work: &original,
                cancellation: &cancellation,
            };
            transaction
                .write_batch_with_accounting::<false>(
                    &["/out"],
                    &files,
                    &mut accounting,
                    &cancellation,
                )
                .await
        }
        Fault::RejectAll => Err(WorkspaceError::NotFound),
        _ => {
            transaction
                .write_batch(&["/out"], &files, &original, &cancellation)
                .await
        }
    };
    let published = if result.is_ok() {
        let committed = transaction
            .commit_with_native_work::<WorkspaceError, _, _>(
                None,
                PublicationPermit::Unrestricted,
                &original,
                &cancellation,
                &mut |_| std::future::ready(Ok(())),
            )
            .await;
        matches!(
            committed,
            Ok(TransactionCommit::Committed(_) | TransactionCommit::AlreadyCommitted(_))
        )
    } else {
        false
    };
    let mut actual = BTreeMap::new();
    for path in expected.keys() {
        let bytes = value!(workspace.read(path, 4096, budget, &cancellation))
            .ok()
            .map(|bytes| bytes.to_vec());
        actual.insert(path.clone(), bytes);
    }
    let expected: BTreeMap<_, _> = expected
        .into_iter()
        .map(|(path, bytes)| (path, Some(bytes)))
        .collect();
    prop_assert!(
        result.is_ok() && published && actual == expected,
        "native file batch differs from input bytes or loses original custody"
    );
    Ok(())
}

#[tokio::test]
async fn native_file_batch_negative_controls() {
    check_native(&[], Fault::None).await.unwrap();
    for fault in [Fault::NoReset, Fault::RejectAll] {
        let error = check_native(&[], fault).await.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("native file batch differs from input bytes or loses original custody")
        );
    }
}
