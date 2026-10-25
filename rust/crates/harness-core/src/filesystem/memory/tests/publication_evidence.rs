//! Local publication evidence must not survive reconstruction or bypass authority.
use super::*;

#[test]
fn publication_evidence_is_local_to_its_host_and_complete_identity() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let storage = runtime.block_on(MemoryHarnessStorage::new(AgentId::new(), 4096))?;
    let file =
        runtime.block_on(storage.stage(OperationId::new(), "one", b"data", "text/plain", "one"))?;
    let wire: FileRef = serde_json::from_slice(
        &serde_json::to_vec(&file).map_err(|e| Error::Invalid(e.to_string()))?,
    )
    .map_err(|e| Error::Invalid(e.to_string()))?;
    assert_eq!(wire, file);
    let mut reset_version = file.clone();
    reset_version.pin_version(file.version().to_owned(), &mut None)?;
    #[cfg(feature = "objects")]
    let object_reset_version = file.clone().with_version(file.version().to_owned())?;
    let reconstructed = FileRef::new(
        file.volume().clone(),
        file.path().to_owned(),
        file.version().to_owned(),
        file.descriptor().clone(),
        file.display_name().to_owned(),
        &mut None,
    )?;
    let other_host = FilesystemHost::new(
        storage.host.filesystem.clone(),
        file.volume().provider(),
        None,
    )?;
    let other_reader = FilesystemContentVerifier::new(
        other_host,
        &storage.verifier(),
        &storage.scope,
        4096,
        None,
    )?;
    for (label, reference, reader, expected_reads) in [
        ("fresh", &file, storage.content_verifier.as_ref(), 0),
        ("wire", &wire, storage.content_verifier.as_ref(), 1),
        (
            "reset_version",
            &reset_version,
            storage.content_verifier.as_ref(),
            1,
        ),
        (
            "reconstructed",
            &reconstructed,
            storage.content_verifier.as_ref(),
            1,
        ),
        ("different_host", &file, other_reader.as_ref(), 1),
        #[cfg(feature = "objects")]
        (
            "object_reset_version",
            &object_reset_version,
            storage.content_verifier.as_ref(),
            1,
        ),
    ] {
        let (result, trace) = acyclic_test_support::differential::observe::<
            _,
            _,
            (&'static str, u64),
        >(
            || (),
            |_| runtime.block_on(async { reader.verify_publication(reference, 4096, None)?.await }),
        );
        result?;
        let reads = trace
            .iter()
            .filter(|(kind, _)| *kind == "resolved content body")
            .count();
        eprintln!("PUBLICATION_EVIDENCE mode={label} body_reads={reads}");
        assert_eq!(
            reads, expected_reads,
            "only an unchanged local publication may reuse evidence"
        );
    }
    let changed =
        runtime.block_on(storage.stage(OperationId::new(), "one", b"edit", "text/plain", "one"))?;
    let mut wrong_version = file.clone();
    wrong_version.pin_version(changed.version().to_owned(), &mut None)?;
    assert!(
        runtime
            .block_on(async {
                storage
                    .content_verifier
                    .verify_publication(&wrong_version, 4096, None)?
                    .await
            })
            .is_err()
    );
    assert!(
        runtime
            .block_on(async {
                storage
                    .content_verifier
                    .verify_publication(&file, 3, None)?
                    .await
            })
            .is_err()
    );
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = crate::filesystem::PreparationIo::from_budget(FINITE_CONTENT_BUDGET, &cancellation)?;
    // Evidence is used on measured calls too; its identity codec must retain
    // the same finite bound as the ordinary file capability codec.
    let measured = storage
        .host
        .seal_publication(file.copy(&mut None)?, Some(&io))?;
    assert!(storage.host.published_here(&measured, Some(&io))?);
    runtime.block_on(async {
        storage
            .content_verifier
            .verify_publication(&measured, 4096, Some(&io))?
            .await
    })?;
    cancellation.cancel();
    assert!(
        runtime
            .block_on(async {
                storage
                    .content_verifier
                    .verify_publication(&file, 4096, Some(&io))?
                    .await
            })
            .is_err()
    );
    let denied = storage.delegate_directory_read(AgentId::new(), "other-directory", "elsewhere")?;
    let reader = FilesystemContentVerifier::new(
        storage.host.clone(),
        &storage.verifier(),
        &denied,
        4096,
        None,
    )?;
    assert!(
        runtime
            .block_on(async { reader.verify_publication(&file, 4096, None)?.await })
            .is_err()
    );
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_publication_witness_retires_independently_of_host_metadata() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
    let file = storage
        .stage(
            OperationId::new(),
            "custody",
            b"data",
            "text/plain",
            "custody",
        )
        .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(FINITE_CONTENT_BUDGET, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::filesystem::PreparationIo::from_native_work(&work)?;
    let baseline = work.live_allocation_bytes();
    let host = FilesystemHost::new(
        storage.host.filesystem.clone(),
        file.volume().provider(),
        Some(&io),
    )?;
    let reference = host.seal_publication(file, Some(&io))?;
    let with_host = work.live_allocation_bytes();
    assert!(with_host > baseline);
    assert_eq!(
        io.owned_bytes()?,
        0,
        "each output owns its transferred storage"
    );
    drop(io);
    assert_eq!(work.live_allocation_bytes(), with_host);
    drop(host);
    let with_witness = work.live_allocation_bytes();
    assert!(
        with_witness > baseline,
        "the witness keeps its domain funded"
    );
    assert!(
        with_witness < with_host,
        "host metadata retires independently"
    );
    let copy = reference.clone();
    assert_eq!(work.live_allocation_bytes(), with_witness);
    drop(reference);
    assert_eq!(work.live_allocation_bytes(), with_witness);
    drop(copy);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn publication_witness_failure_releases_every_allocation_boundary() -> Result<()> {
    let storage = MemoryHarnessStorage::new(AgentId::new(), 4096).await?;
    let file = storage
        .stage(OperationId::new(), "proof", b"data", "text/plain", "proof")
        .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let budget = FINITE_CONTENT_BUDGET;
    for native in [false, true] {
        let ledger = |budget| {
            if native {
                let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                crate::filesystem::PreparationIo::from_native_work(&work)
            } else {
                crate::filesystem::PreparationIo::from_budget(budget, &cancellation)
            }
        };
        let probe = ledger(budget)?;
        probe.retain_owned(17)?;
        let setup = budget.allocation_operations - probe.remaining()?.allocation_operations;
        let setup_peak = probe.receipt().peak_allocation_bytes;
        drop(storage.host.seal_publication(file.clone(), Some(&probe))?);
        let required =
            budget.allocation_operations - setup - probe.remaining()?.allocation_operations;
        assert!(required > 0);
        let peak = probe.receipt().peak_allocation_bytes;
        assert!(peak > setup_peak);
        let mut cuts = Vec::new();
        for operations in 0..required {
            let mut limited = budget;
            limited.allocation_operations = setup + operations;
            cuts.push(limited);
        }
        for bytes in [setup_peak, setup_peak + (peak - setup_peak) / 2, peak - 1] {
            let mut limited = budget;
            limited.peak_allocation_bytes = bytes;
            cuts.push(limited);
        }
        for limited in cuts {
            let io = ledger(limited)?;
            io.retain_owned(17)?;
            assert!(
                storage
                    .host
                    .seal_publication(file.clone(), Some(&io))
                    .is_err()
            );
            assert_eq!(io.owned_bytes()?, 17, "native={native}");
            io.release(17)?;
        }
    }
    Ok(())
}
