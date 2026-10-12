use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn native_stock_startup_denies_preallocation_and_precancel_before_sdk_io() -> Result<()> {
    let fixture =
        NativeCustomerFixture::new_with_grants(&[], crate::tool::ToolRegistry::default()).await?;
    let volume = VolumeRef::new(
        fixture.creation.filesystem_provider.clone(),
        "scheduler",
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(AgentId::from_bytes([80; 16])),
    )?;
    let resumable = crate::durable_tool::ResumableToolRegistry::default();
    let original = &fixture.creation.host;
    let limits = original
        .session_limits
        .expect("the original fixture configures session limits");
    for cancelled in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut budget = metadata_control_budget();
        if !cancelled {
            budget.allocation_operations = 0;
        }
        let work = test_work(budget, &cancellation)?;
        if cancelled {
            cancellation.cancel();
        }
        let result = NativeCustomerRuntime::open_with_creation_providers_with_native_work(
            fixture.creation.stream.clone(),
            fixture.creation.filesystem.clone(),
            &volume,
            &original.verifier,
            &original.owner_scope,
            &fixture.creation.root_scope,
            &original.tasks,
            &original.machines,
            fixture.runtime.tool_registry(),
            &resumable,
            limits,
            16,
            fixture.creation.root_scope.limits().file_bytes,
            original.clock.clone(),
            original.policy.clone(),
            &original.resource_creation,
            &work,
        )
        .await;
        assert!(matches!(&result, Err(error) if error.is_native_failure()));
        assert_eq!(work.receipt().backend_read_operations, 0);
        assert_eq!(work.receipt().backend_write_operations, 0);
        drop(result);
        assert_eq!(work.live_allocation_bytes(), 0);
    }
    Ok(())
}
