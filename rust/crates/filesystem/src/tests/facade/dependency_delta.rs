use super::*;
use acyclic_test_support::measure;

#[test]
fn content_mutation_preserves_history_without_copying_it() -> TestResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut measured = Vec::new();
    for history in [8, 24] {
        let cancel = CancellationToken::new();
        let (mut checkout, file) = runtime.block_on(async {
            let (volume, mut source) = fixture(config(), &cancel).await?;
            let file = create(
                &mut source,
                "data",
                OwnedBytes::from_static(b"before"),
                &cancel,
            )
            .await?;
            commit(&mut source, &cancel).await?;
            let mut checkout = volume
                .checkout(GenerationSelector::Head, writable_tracking(), B, &cancel)
                .await?
                .value;
            for index in 0..history {
                assert!(
                    checkout
                        .lookup_no_follow(&path(&format!("absent-{index:02}"))?, B, &cancel)
                        .await?
                        .value
                        .record
                        .is_none()
                );
            }
            checkout.read_file_record_by_id(file, B, &cancel).await?;
            Ok::<_, Box<dyn std::error::Error>>((checkout, file))
        })?;
        let previous_count = checkout.dependencies.proof().len();
        assert!(previous_count >= history);
        // The independently authored output is fixed; history consists solely
        // of read observations and cannot affect content or mutation work.
        let mut outcome = None;
        let allocation = measure(|| {
            outcome = Some(runtime.block_on(checkout.change_content(
                file,
                ContentChange::Resize { logical_bytes: 3 },
                ContentTimes::Preserve,
                B,
                &cancel,
            )));
        });
        let receipt = outcome.ok_or("mutation did not run")??;
        assert!(checkout.dependencies.proof().len() >= previous_count);
        for index in 0..history {
            assert!(
                checkout
                    .dependencies
                    .proof()
                    .observes(&DependencyRegion::DirectoryName {
                        directory_id: checkout.root.root_file_id,
                        name: path(&format!("absent-{index:02}"))?.components()[0].clone(),
                    })
            );
        }
        assert_eq!(
            runtime
                .block_on(read(&mut checkout, file, 0, 3, &cancel))?
                .as_ref(),
            b"bef"
        );
        measured.push((
            allocation,
            receipt.work.bytes_copied,
            receipt.work.items_examined,
        ));
    }
    assert_eq!(
        measured[0], measured[1],
        "read history cannot add mutation copies or allocations"
    );
    eprintln!("FACADE_DEPENDENCY_DELTA {measured:?}");
    Ok(())
}
