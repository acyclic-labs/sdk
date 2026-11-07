use super::*;

#[test]
fn model_step_callers_keep_only_control_state() -> Result<()> {
    let provider = crate::test_support::ScriptedModelProvider::new([]);
    let executor = StockExecutor::new(
        Model::new("test".into(), "layout".into(), "1".into())?,
        Arc::new(provider),
        ContextPipeline::default(),
        ToolRegistry::default(),
    );
    let execution = StockExecution::ordinary(&executor);
    let journal = crate::executor::tests::Journal::default();
    let input = TurnInput {
        max_steps: None,
        operation_id: OperationId::new(),
        input: ModelContent::Text("input".into()),
        selected_context: None,
    };
    let scope = ModelStepScope {
        purpose: ModelPurpose::Response,
        boundary: None,
    };
    let mut phase = DispatchPhase::new();
    let preparation =
        execution.prepare_dispatch_step(&journal, &input, 0, (&[], None), scope, &mut phase);
    let preparation_bytes = std::mem::size_of_val(&preparation);
    drop(preparation);
    let dispatch = execution.dispatch_model_step(
        &journal,
        &input,
        0,
        request::ModelStepRequest {
            history: request::ModelStepHistory::Selected(&[]),
            prepared: None,
        },
        ModelPurpose::Response,
        &mut phase,
    );
    let dispatch_bytes = std::mem::size_of_val(&dispatch);
    drop(dispatch);
    let cancellable = execution.cancellable_model_step(
        &journal,
        &input,
        0,
        request::ModelStepRequest {
            history: request::ModelStepHistory::Selected(&[]),
            prepared: None,
        },
        ModelPurpose::Response,
        &mut phase,
    );
    let cancellable_bytes = std::mem::size_of_val(&cancellable);
    drop(cancellable);
    let preparation_phase = super::super::phase::run(&journal, None, || {
        execution.resolve_model_request(&journal, &input, (0, &[], scope, None), None)
    });
    let phase_bytes = std::mem::size_of_val(&preparation_phase);
    // Two 4 KiB pages hold bounded coordination state. Request resolution must
    // own its frame instead of being copied into each enclosing state machine.
    let sizes = [
        ("preparation", preparation_bytes),
        ("dispatch", dispatch_bytes),
        ("phase", phase_bytes),
        ("cancellable", cancellable_bytes),
    ];
    for (name, bytes) in sizes {
        eprintln!("{name} future: {bytes} bytes");
    }
    for (name, bytes) in sizes {
        assert!(bytes <= 8 * 1024, "{name} future retains {bytes} bytes");
    }
    Ok(())
}
