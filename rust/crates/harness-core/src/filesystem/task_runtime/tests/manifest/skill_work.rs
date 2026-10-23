use super::*;

// Property: an invocation cannot return a body larger than the original read
// allowance, or return any body after cancellation. The immutable fixture is
// shared across generated cases; every case has its own original work ledger.
#[test]
fn selected_skill_reads_obey_original_allowance_and_cancellation() -> Result<()> {
    let executor = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Error::Storage(error.to_string()))?;
    let fixture = executor.block_on(Fixture::new())?;
    let reader = reader(&fixture)?;
    let skill = executor.block_on(async {
        let reference = recovery::publish_sources(&fixture).await?;
        let bytes = reader.read(&reference, 65_536, None, None)?.await?;
        Ok::<_, Error>(
            ContextManifest::decode(&bytes, Limits::default())?
                .discovery
                .skills
                .remove(0),
        )
    })?;
    let body = executor.block_on(skill.invoke(reader.as_ref(), 65_536))?;
    let ModelContent::Text(body) = body.content else {
        panic!("skill must be text")
    };
    let strategy = (0..body.len() as u64, any::<bool>());
    proptest::test_runner::TestRunner::default()
        .run(&strategy, |(bytes, cancelled)| {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::WorkBudget {
                source_bytes_read: bytes,
                ..super::super::native_mail::BUDGET
            };
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
            let io = PreparationIo::from_native_work(&work)
                .map_err(|error| TestCaseError::fail(format!("{error:?}")))?;
            if cancelled {
                cancellation.cancel();
            }
            let result =
                executor.block_on(skill.invoke_observed(reader.as_ref(), 65_536, Some(&io)));
            prop_assert!(result.as_ref().is_err_and(Error::is_native_failure));
            prop_assert!(work.receipt().source_bytes_read <= bytes);
            Ok(())
        })
        .unwrap();
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        super::super::native_mail::BUDGET,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = PreparationIo::from_native_work(&work)?;
    assert_eq!(
        executor
            .block_on(skill.invoke_observed(reader.as_ref(), 65_536, Some(&io)))?
            .content,
        ModelContent::Text(body.clone())
    );
    assert!(work.receipt().source_bytes_read >= body.len() as u64);
    assert!(work.receipt().backend_read_operations > 0);
    Ok(())
}
