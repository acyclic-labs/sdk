//! Count final runtime admission after a checkout write has been captured.
use super::*;
use crate::conversation::ContentResidencyVerifier as _;

#[test]
fn captured_checkout_publications_keep_their_verified_payload_evidence() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(storage)?;
    let (_directory, _state, harness, publisher) =
        runtime.block_on(super::observed_cost::cost_fixture())?;
    let mut observed = Vec::new();
    for once in [false, true] {
        let path = if once {
            "captured-once"
        } else {
            "captured-path"
        };
        let bytes = vec![b'r'; 1024 * 1024];
        let operation = OperationId::new();
        let (file, _) = runtime.block_on(async {
            if once {
                publisher
                    .stage_once(
                        operation,
                        path,
                        &bytes,
                        "text/plain",
                        path,
                        Default::default(),
                    )?
                    .await
            } else {
                publisher
                    .stage(
                        operation,
                        path,
                        &bytes,
                        "text/plain",
                        path,
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
            counts.push(reads.len());
        }
        observed.push(counts);
    }
    assert_eq!(
        observed,
        [vec![0, 1], vec![0, 1]],
        "local captured publications retain their evidence; serialized references require verification"
    );
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
    runtime.block_on(harness.disconnect())?;
    Ok(())
}
