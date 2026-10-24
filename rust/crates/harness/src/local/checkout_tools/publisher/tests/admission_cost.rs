//! Count final runtime admission after a checkout write has been captured.
use super::*;
use crate::conversation::ContentResidencyVerifier as _;

#[test]
fn captured_checkout_publications_keep_their_verified_payload_evidence() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let mut observed = Vec::new();
    for once in [false, true] {
        let (_directory, _state, harness, publisher) =
            runtime.block_on(super::observed_cost::cost_fixture())?;
        let bytes = vec![b'r'; 1024 * 1024];
        let operation = OperationId::new();
        let started = std::time::Instant::now();
        let (file, _) = runtime.block_on(async {
            if once {
                publisher
                    .stage_once(
                        operation,
                        "captured-runtime-cost",
                        &bytes,
                        "text/plain",
                        "captured-runtime-cost",
                        Default::default(),
                    )?
                    .await
            } else {
                publisher
                    .stage(
                        operation,
                        "captured-runtime-cost",
                        &bytes,
                        "text/plain",
                        "captured-runtime-cost",
                        Default::default(),
                    )?
                    .await
            }
        })?;
        let reader = crate::filesystem::FilesystemContentVerifier::new(
            harness.files.for_workspace(
                &harness.project,
                publisher.tools.workspace.view().workspace().clone(),
            )?,
            &harness.verifier,
            &harness.signed,
            harness.limits.file_bytes,
            None,
        )?;
        let wire: FileRef = serde_json::from_value(serde_json::to_value(&file).map_err(storage)?)
            .map_err(storage)?;
        let mut counts = Vec::new();
        for reference in [&file, &wire] {
            let admitted = std::time::Instant::now();
            let (result, trace) = acyclic_test_support::differential::observe(
                || (),
                |()| {
                    runtime.block_on(async {
                        reader
                            .verify_publication(reference, bytes.len() as u64, None)?
                            .await
                    })
                },
            );
            result?;
            let reads: Vec<(&'static str, u64)> = trace
                .into_iter()
                .filter(|(operation, _)| *operation == "resolved content body")
                .collect();
            eprintln!(
                "CAPTURED_PUBLICATION once={once} wire={} read_calls={} requested_bytes={} admission_us={}",
                counts.len() == 1,
                reads.len(),
                reads.iter().map(|(_, n)| n).sum::<u64>(),
                admitted.elapsed().as_micros()
            );
            counts.push(reads.len());
        }
        eprintln!(
            "CAPTURED_PUBLICATION_TOTAL once={once} elapsed_us={}",
            started.elapsed().as_micros()
        );
        observed.push(counts);
        runtime.block_on(harness.close())?;
    }
    assert_eq!(
        observed,
        [vec![0, 1], vec![0, 1]],
        "local captured publications retain their evidence; serialized references require verification"
    );
    Ok(())
}

#[test]
fn captured_publication_evidence_cannot_cross_a_same_length_rewrite() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let (_directory, _state, harness, publisher) =
        runtime.block_on(super::observed_cost::cost_fixture())?;
    let mut files = Vec::new();
    for byte in *b"ab" {
        files.push(runtime.block_on(async {
            publisher
                .stage_once(
                    OperationId::new(),
                    "rebind-rewrite",
                    &vec![byte; 1024 * 1024],
                    "text/plain",
                    "rebind-rewrite",
                    Default::default(),
                )?
                .await
                .map(|(file, _)| file)
        })?);
    }
    let [first, second] = files.as_slice() else {
        return Err(Error::Invalid("two publication receipts required".into()));
    };
    let writer = publisher.writer_for(
        &harness.project,
        publisher.tools.workspace.view().workspace(),
    )?;
    let forged = FileRef::new(
        first.volume().clone(),
        first.path().to_owned(),
        second.version().to_owned(),
        first.descriptor().clone(),
        first.display_name().to_owned(),
        &mut None,
    )?;
    let rejected = runtime.block_on(writer.rebind_verified_publication(first, forged, None));
    assert!(
        rejected.is_err(),
        "equal lengths cannot authenticate different immutable payloads"
    );
    let wire: FileRef =
        serde_json::from_value(serde_json::to_value(second).map_err(storage)?).map_err(storage)?;
    let rebound =
        runtime.block_on(writer.rebind_verified_publication(&wire, wire.clone(), None))?;
    let reader = crate::filesystem::FilesystemContentVerifier::new(
        harness.files.for_workspace(
            &harness.project,
            publisher.tools.workspace.view().workspace().clone(),
        )?,
        &harness.verifier,
        &harness.signed,
        harness.limits.file_bytes,
        None,
    )?;
    let (result, trace) = acyclic_test_support::differential::observe(
        || (),
        |()| {
            runtime.block_on(async {
                reader
                    .verify_publication(&rebound, 1024 * 1024, None)?
                    .await
            })
        },
    );
    result?;
    let reads: Vec<(&'static str, u64)> = trace
        .into_iter()
        .filter(|(operation, _)| *operation == "resolved content body")
        .collect();
    assert_eq!(
        reads.len(),
        1,
        "untrusted source metadata cannot mint publication evidence"
    );
    eprintln!(
        "CAPTURED_REBIND_ADVERSARIAL changed_payload_rejected=true untrusted_source_reads={}",
        reads.len()
    );
    runtime.block_on(harness.close())?;
    Ok(())
}
