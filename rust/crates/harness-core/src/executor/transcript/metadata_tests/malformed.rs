use super::*;

fn reject_with_funded_diagnostics<T: PreparationRecord>(bytes: &[u8]) -> Result<()> {
    for native in [false, true] {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::performance::work_budget(16 * 1024 * 1024, 16 * 1024 * 1024);
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let io = if native {
            PreparationIo::from_native_work(&work)?
        } else {
            PreparationIo::from_budget(budget, &cancellation)?
        };
        let mut result = None;
        let physical = acyclic_test_support::measure(|| {
            result = Some(io.decode(
                std::marker::PhantomData::<T>,
                bytes,
                crate::wire::ErrorCode::Storage,
            ));
        });
        assert!(result.ok_or_else(|| invalid(None))?.is_err());
        assert!(physical.count_total > 0);
        assert!(
            physical.bytes_max <= io.receipt().peak_allocation_bytes,
            "unfunded malformed scalar collection: {physical:?}"
        );
        assert_eq!(io.owned_bytes()?, 0);
        drop(io);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    Ok(())
}

#[test]
fn malformed_scalar_collections_cannot_escape_context_or_projection_funding() -> Result<()> {
    // Untagged serde buffers content before decoding typed collections. Even
    // invalid primitive elements must be funded before their rejection.
    for count in [1, 64, 512] {
        for context in [
            serde_json::json!({
                "messages": vec![true; count], "metadata": {}, "current_input_index": null,
            }),
            serde_json::json!({
                "messages": [{"role": "user", "content": vec![true; count]}],
                "metadata": {}, "current_input_index": 0,
            }),
            serde_json::json!({
                "messages": [], "metadata": {"file:0": vec![true; count]},
                "current_input_index": null,
            }),
        ] {
            let bytes = crate::contract::canonical_json_bytes(&context)?;
            reject_with_funded_diagnostics::<crate::context::Context>(&bytes)?;
            let projection = serde_json::json!({
                "context": context, "canonical": null, "checkpoint": null,
            });
            let bytes = crate::contract::canonical_json_bytes(&projection)?;
            reject_with_funded_diagnostics::<ResponseProjection>(&bytes)?;
        }
    }
    Ok(())
}
