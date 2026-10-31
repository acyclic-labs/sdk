//! A fork's proof extends its source's published proof instead of walking it.

use super::*;
use crate::WorkCounters;
use crate::kernel::allocation::KernelAccounting;

#[derive(Clone, Copy)]
enum ForkSource {
    Head,
    Unheld,
    Held,
    Pinned,
    WrongGeneration,
    ObservedHead,
    ObservedExact,
    ObservedForgotten,
    ObservedLate,
    #[cfg(not(target_arch = "wasm32"))]
    NativePinned,
    #[cfg(not(target_arch = "wasm32"))]
    NativeObservedHead,
}

/// The work to prove a fork of `files` settled files plus one more commit,
/// using the latest slot, the original held proof, or a full closure walk.
async fn fork_proof_work(
    files: u8,
    source_kind: ForkSource,
) -> Result<WorkCounters, Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs.create_workspace("fork-proof", B, &cancel).await?.value;
    for file in 0..files {
        workspace
            .write_text(&format!("/{file}"), &"x".repeat(5_000 + usize::from(file)))
            .await?;
    }
    #[cfg(not(target_arch = "wasm32"))]
    let original = matches!(
        source_kind,
        ForkSource::NativePinned | ForkSource::NativeObservedHead
    )
    .then(|| crate::NativeMaterializationWork::new(B, &cancel))
    .transpose()
    .map_err(|(error, _)| error)?;
    #[cfg(not(target_arch = "wasm32"))]
    let baseline = original.as_ref().map(|work| work.live_allocation_bytes());
    #[cfg(not(target_arch = "wasm32"))]
    let mut transaction = if let Some(original) = &original {
        let mut transaction = workspace
            .begin_transaction(crate::IdempotencyKey::new(), original, &cancel)
            .await?;
        let (bytes, allocation) = original.copy_bytes(b"original")?;
        let bytes = acyclic_stream::OwnedBytes::from_vec_with_guard(
            bytes,
            allocation,
            original.runtime_meter(),
        )
        .map_err(|failure| format!("{:?}", failure.into_error()))?;
        transaction
            .write("/retained", bytes, original, &cancel)
            .await?;
        transaction
    } else {
        let mut transaction = workspace
            .begin_transaction(crate::IdempotencyKey::new(), B, &cancel)
            .await?
            .value;
        transaction.write_text("/retained", "original").await?;
        transaction
    };
    #[cfg(target_arch = "wasm32")]
    let mut transaction = {
        let mut transaction = workspace
            .begin_transaction(crate::IdempotencyKey::new(), B, &cancel)
            .await?
            .value;
        transaction.write_text("/retained", "original").await?;
        transaction
    };
    let commit_ordinary = async |transaction: &mut crate::Transaction<_, _>| {
        transaction
            .commit(
                None,
                crate::PublicationPermit::Unrestricted,
                B,
                &cancel,
                |_| async {
                    Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(
                        crate::OperationReceipt {
                            value: (),
                            work: WorkCounters::UNCHARGED,
                        },
                    )
                },
            )
            .await
    };
    #[cfg(not(target_arch = "wasm32"))]
    let committed = if let Some(original) = &original {
        transaction
            .commit_with_native_work::<crate::WorkspaceError, _, _>(
                None,
                crate::PublicationPermit::Unrestricted,
                original,
                &cancel,
                &mut |_| std::future::ready(Ok(())),
            )
            .await?
    } else {
        commit_ordinary(&mut transaction).await?.value
    };
    #[cfg(target_arch = "wasm32")]
    let committed = commit_ordinary(&mut transaction).await?.value;
    let crate::TransactionCommit::Committed(mut older) = committed else {
        return Err("the source must be published".into());
    };
    let observed = match source_kind {
        ForkSource::ObservedHead | ForkSource::ObservedForgotten => {
            Some(workspace.head(B, &cancel).await?.value)
        }
        ForkSource::ObservedExact => Some(workspace.generation(older.id, B, &cancel).await?.value),
        #[cfg(not(target_arch = "wasm32"))]
        ForkSource::NativeObservedHead => {
            Some(workspace.head(original.as_ref().unwrap(), &cancel).await?)
        }
        _ => None,
    };
    workspace.write_text("/tip", "tip").await?;
    let source = match source_kind {
        ForkSource::Head => workspace.head(B, &cancel).await?.value,
        ForkSource::Unheld => {
            // Original implementation: only the mutable latest slot survived.
            older.publication_closure = None;
            older
        }
        ForkSource::Pinned => {
            older.pin("source-proof-pin", B, &cancel).await?;
            let Some(crate::facade::GenerationClosure::Publication(custody)) =
                older.publication_closure.as_ref()
            else {
                return Err("committed generation must retain its publication handback".into());
            };
            let custody = custody.lock().unwrap();
            assert!(custody.pin.is_none());
            assert!(custody.published.is_some());
            drop(custody);
            older
        }
        ForkSource::WrongGeneration => {
            let mut tip = workspace.head(B, &cancel).await?.value;
            tip.publication_closure = older.publication_closure.clone();
            // Remove the valid latest proof so only a wrongly bound witness remains.
            let discarded = std::mem::take(&mut *fs.inner.publication_proof.inner.lock().unwrap());
            drop(discarded);
            tip
        }
        ForkSource::Held => older,
        ForkSource::ObservedHead | ForkSource::ObservedExact | ForkSource::ObservedForgotten => {
            drop(older);
            let mut observed = observed.unwrap();
            assert!(
                observed.publication_closure.is_some(),
                "observation lost the exact original proof"
            );
            if matches!(source_kind, ForkSource::ObservedForgotten) {
                observed.publication_closure = None;
            }
            observed
        }
        ForkSource::ObservedLate => {
            let observed = workspace.generation(older.id, B, &cancel).await?.value;
            drop(older);
            assert!(
                observed.publication_closure.is_none(),
                "latest proof cannot certify an older generation"
            );
            observed
        }
        #[cfg(not(target_arch = "wasm32"))]
        ForkSource::NativePinned => {
            let original = original.as_ref().unwrap();
            older.pin("source-proof-pin", original, &cancel).await?;
            older
        }
        #[cfg(not(target_arch = "wasm32"))]
        ForkSource::NativeObservedHead => {
            drop(older);
            observed.unwrap()
        }
    };
    if matches!(
        source_kind,
        ForkSource::ObservedHead
            | ForkSource::ObservedExact
            | ForkSource::ObservedForgotten
            | ForkSource::ObservedLate
    ) || {
        #[cfg(not(target_arch = "wasm32"))]
        {
            matches!(source_kind, ForkSource::NativeObservedHead)
        }
        #[cfg(target_arch = "wasm32")]
        {
            false
        }
    } {
        // The handle must retain its proof after both replacement and eviction.
        let discarded = std::mem::take(&mut *fs.inner.publication_proof.inner.lock().unwrap());
        drop(discarded);
    }
    let name = crate::WorkspaceName::new("fork-proof-child", &B, &cancel)?.value;
    let mut accounting = KernelAccounting::standalone(B, &cancel);
    let destination = fs.workspace_id_for_name(&name, &mut accounting, &cancel)?;
    let (allocation, admitted) = accounting.record_actual_owned(0)?;
    admitted?;
    let start = accounting.spent();
    let (proof, source_object, pending, filtered) = fs
        .materialize_forked_generation_root(
            &source,
            destination,
            crate::kernel::Owned::new(None, allocation, 0),
            &mut accounting,
            &cancel,
        )
        .await?;
    assert_eq!(source_object, source.id.object_id());
    assert!(!filtered);
    assert!(
        pending.is_some(),
        "the fork's proof must yield a ticket for its own volume"
    );
    drop((proof, pending));
    let end = accounting.spent();
    drop(source);
    drop(transaction);
    drop(workspace);
    drop(fs);
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(original) = &original {
        // The final receipt retires the ledger's sole remaining proof counter.
        original.receipt().verify(B)?;
        assert_eq!(Some(original.live_allocation_bytes()), baseline);
    }
    Ok(WorkCounters {
        object_probes: end.object_probes - start.object_probes,
        backend_read_operations: end.backend_read_operations - start.backend_read_operations,
        object_bytes_read: end.object_bytes_read - start.object_bytes_read,
        items_examined: end.items_examined - start.items_examined,
        ..WorkCounters::UNCHARGED
    })
}

#[tokio::test]
async fn observed_generations_keep_exact_proofs_after_replacement_and_eviction()
-> Result<(), Box<dyn std::error::Error>> {
    let few = fork_proof_work(4, ForkSource::ObservedHead).await?;
    let many = fork_proof_work(32, ForkSource::ObservedHead).await?;
    let exact = fork_proof_work(32, ForkSource::ObservedExact).await?;
    let forgotten = fork_proof_work(32, ForkSource::ObservedForgotten).await?;
    let late = fork_proof_work(32, ForkSource::ObservedLate).await?;
    #[cfg(not(target_arch = "wasm32"))]
    assert_eq!(
        many,
        fork_proof_work(32, ForkSource::NativeObservedHead).await?
    );
    assert_eq!(
        few, many,
        "observed proof work grew with the source closure"
    );
    assert_eq!(many, exact);
    assert!(forgotten.object_bytes_read > many.object_bytes_read);
    assert!(late.object_bytes_read > many.object_bytes_read);
    eprintln!(
        "observed4={few:?} observed32={many:?} exact32={exact:?} forgotten32={forgotten:?} late32={late:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_fork_extends_its_published_source_proof() -> Result<(), Box<dyn std::error::Error>> {
    let (few, many) = (
        fork_proof_work(4, ForkSource::Head).await?,
        fork_proof_work(32, ForkSource::Head).await?,
    );
    // Negative control: a source whose proof is not cached walks its closure.
    let (few_walked, many_walked) = (
        fork_proof_work(4, ForkSource::Unheld).await?,
        fork_proof_work(32, ForkSource::Unheld).await?,
    );
    assert_eq!(few, many, "the fork's proof grew with its source's closure");
    assert!(
        many_walked.object_bytes_read > few_walked.object_bytes_read,
        "the walk must grow with the closure: {few_walked:?} vs {many_walked:?}"
    );
    Ok(())
}

#[tokio::test]
async fn a_held_generation_keeps_its_original_proof_after_publication_and_pin()
-> Result<(), Box<dyn std::error::Error>> {
    let few = fork_proof_work(4, ForkSource::Held).await?;
    let many = fork_proof_work(32, ForkSource::Held).await?;
    let pinned = fork_proof_work(32, ForkSource::Pinned).await?;
    let few_walked = fork_proof_work(4, ForkSource::Unheld).await?;
    let many_walked = fork_proof_work(32, ForkSource::Unheld).await?;
    let wrong = fork_proof_work(32, ForkSource::WrongGeneration).await?;
    #[cfg(not(target_arch = "wasm32"))]
    assert_eq!(many, fork_proof_work(32, ForkSource::NativePinned).await?);
    assert_eq!(few, many, "held proof work grew with the source closure");
    assert_eq!(many, pinned, "pin consumed the original reusable summary");
    assert!(many_walked.object_bytes_read > few_walked.object_bytes_read);
    assert!(many_walked.object_bytes_read > many.object_bytes_read);
    assert!(
        wrong.object_bytes_read > many.object_bytes_read,
        "a witness of another generation must not vouch for the source"
    );
    eprintln!(
        "held4={few:?} held32={many:?} unheld4={few_walked:?} unheld32={many_walked:?} wrong={wrong:?}"
    );
    Ok(())
}

async fn first_publication_work(
    files: u8,
    discard_proof: bool,
) -> Result<WorkCounters, Box<dyn std::error::Error>> {
    let cancel = CancellationToken::new();
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace("first-publish-source", B, &cancel)
        .await?
        .value;
    for file in 0..files {
        workspace
            .write_text(&format!("/{file}"), &"x".repeat(5_000 + usize::from(file)))
            .await?;
    }
    let source = workspace.head(B, &cancel).await?.value;
    let child = workspace
        .fork::<_, _, crate::WorkspaceError>(
            "first-publish-child",
            crate::workspace::ForkOptions::from_generation(source, crate::IdempotencyKey::new()),
            crate::PublicationPermit::Unrestricted,
            B,
            &cancel,
            &mut |_| {
                std::future::ready(Ok(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                }))
            },
        )
        .await?
        .value;
    if discard_proof {
        // Negative control: the same public fork without its installed proof
        // has to prove its inherited closure at the first publication.
        let discarded = std::mem::take(&mut *fs.inner.publication_proof.inner.lock().unwrap());
        drop(discarded);
    }
    let mut transaction = child
        .begin_transaction(crate::IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction.write_text("/new", "child only").await?;
    let receipt = transaction
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            B,
            &cancel,
            |_| async {
                Ok::<_, crate::OperationFailure<crate::WorkspaceError>>(crate::OperationReceipt {
                    value: (),
                    work: WorkCounters::UNCHARGED,
                })
            },
        )
        .await?;
    assert!(matches!(
        receipt.value,
        crate::TransactionCommit::Committed(_)
    ));
    assert_eq!(
        child.read("/new", 32, B, &cancel).await?.value.as_ref(),
        b"child only"
    );
    assert!(workspace.read("/new", 32, B, &cancel).await.is_err());
    Ok(receipt.work)
}

#[tokio::test]
async fn first_publication_reuses_the_public_forks_installed_proof()
-> Result<(), Box<dyn std::error::Error>> {
    let few = first_publication_work(4, false).await?;
    let many = first_publication_work(32, false).await?;
    let few_walked = first_publication_work(4, true).await?;
    let many_walked = first_publication_work(32, true).await?;
    eprintln!(
        "first publication cached4={few:?} cached32={many:?} uncached4={few_walked:?} uncached32={many_walked:?}"
    );
    // The public commit also reads changed authenticated index paths, whose
    // height can grow. Compare the SAME publication with its installed proof
    // removed: forgetting pending.created() makes these savings disappear.
    assert!(few.object_probes < few_walked.object_probes);
    assert!(few.object_bytes_read < few_walked.object_bytes_read);
    assert!(
        many_walked.object_probes - many.object_probes
            > few_walked.object_probes - few.object_probes
    );
    assert!(
        many_walked.object_bytes_read - many.object_bytes_read
            > few_walked.object_bytes_read - few.object_bytes_read
    );
    assert!(many_walked.object_probes > few_walked.object_probes);
    assert!(many_walked.object_bytes_read > few_walked.object_bytes_read);
    assert!(many_walked.object_probes > many.object_probes);
    Ok(())
}

async fn composed_publication_work(
    files: u8,
    discard_proof: bool,
) -> Result<WorkCounters, Box<dyn std::error::Error>> {
    use acyclic_stream::StreamProviderExt as _;
    let cancel = CancellationToken::new();
    let stream = std::sync::Arc::new(acyclic_stream::MemoryStream::default());
    let fs = Fs::new(
        crate::StreamAuthorityStore::new(stream.clone()),
        crate::MemoryObjectStore::default(),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let workspace = fs
        .create_workspace("composed-proof", B, &cancel)
        .await?
        .value;
    for file in 0..files {
        workspace
            .write_text(&format!("/{file}"), &"x".repeat(5_000 + usize::from(file)))
            .await?;
    }
    let source = workspace.head(B, &cancel).await?.value;
    let forks = [crate::ForkSpec {
        destination: crate::WorkspaceName::new("composed-child", &B, &cancel)?.value,
        source: &source,
        paths: None,
        idempotency_key: crate::IdempotencyKey::new(),
    }];
    let mut commit = crate::FilesystemCommit::new(acyclic_stream::CommitRequest {
        deadline_unix_millis: None,
        conditions: Vec::new(),
        mutations: Vec::new(),
        idempotency_key: acyclic_stream::IdempotencyKey::new(OwnedBytes::from_static(
            b"composed-proof",
        ))?,
    });
    fs.prepare_fork_many(&forks, &mut commit, B, &cancel)
        .await?;
    fs.seal_commit(&mut commit, B, &cancel)?;
    let outcome = stream.commit(commit.request().clone()).await?;
    // The original physical outcome is decided. Cancelled cache-refresh work
    // must neither lose its receipt nor discard the original proof ticket.
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        fs.settle_commit(commit, outcome, B, &cancelled)
            .await?
            .value,
        crate::FilesystemCommitOutcome::Committed(_)
    ));
    if discard_proof {
        let discarded = std::mem::take(&mut *fs.inner.publication_proof.inner.lock().unwrap());
        drop(discarded);
    }
    let child = fs.open_workspace("composed-child", B, &cancel).await?.value;
    let mut transaction = child
        .begin_transaction(crate::IdempotencyKey::new(), B, &cancel)
        .await?
        .value;
    transaction.write_text("/new", "child only").await?;
    let receipt = transaction
        .commit(
            None,
            crate::PublicationPermit::Unrestricted,
            B,
            &cancel,
            allow::<crate::WorkspaceError>,
        )
        .await?;
    assert!(matches!(
        receipt.value,
        crate::TransactionCommit::Committed(_)
    ));
    assert_eq!(
        child.read("/new", 32, B, &cancel).await?.value.as_ref(),
        b"child only"
    );
    assert_eq!(
        child.read("/0", 6_000, B, &cancel).await?.value.as_ref(),
        "x".repeat(5_000).as_bytes()
    );
    assert!(workspace.read("/new", 32, B, &cancel).await.is_err());
    Ok(receipt.work)
}

#[tokio::test]
async fn composed_fork_keeps_its_original_proof_through_durable_settlement()
-> Result<(), Box<dyn std::error::Error>> {
    let few = composed_publication_work(4, false).await?;
    let many = composed_publication_work(32, false).await?;
    // Same real publication with the authentic proof discarded: omitting
    // committed handback must erase these savings when cache refresh is cancelled.
    let few_walked = composed_publication_work(4, true).await?;
    let many_walked = composed_publication_work(32, true).await?;
    eprintln!(
        "composed publication retained4={few:?} retained32={many:?} discarded4={few_walked:?} discarded32={many_walked:?}"
    );
    assert!(few.object_probes < few_walked.object_probes);
    assert!(few.object_bytes_read < few_walked.object_bytes_read);
    assert!(
        many_walked.object_probes - many.object_probes
            > few_walked.object_probes - few.object_probes
    );
    assert!(
        many_walked.object_bytes_read - many.object_bytes_read
            > few_walked.object_bytes_read - few.object_bytes_read
    );
    Ok(())
}
