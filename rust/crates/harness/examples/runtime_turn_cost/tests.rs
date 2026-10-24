//! Measurements include both summary and response dispatches in either order.
use super::*;
use futures::StreamExt;

#[tokio::test]
async fn maximum_request_includes_summary_dispatches_in_either_order() -> Result<()> {
    for summary_first in [false, true] {
        let timing = Arc::new(Mutex::new(Timing::default()));
        let provider = Provider {
            turns: 0,
            compact: true,
            checkout_tools: false,
            timing: Arc::clone(&timing),
        };
        let mut expected = 0;
        for summary in [summary_first, !summary_first] {
            let text = if summary {
                format!(
                    "Summarize the preceding context for continued work.{}",
                    " context".repeat(256)
                )
            } else {
                "response".into()
            };
            let request = PreparedModelRequest::prepare(
                acyclic_harness::model::ModelRequest {
                    model: Model::new(
                        "fixture".into(),
                        "measurement".into(),
                        "1".into(),
                        Value::Null,
                    )?,
                    messages: vec![acyclic_harness::model::ModelMessage {
                        role: acyclic_harness::model::ModelRole::User,
                        content: acyclic_harness::model::ModelContent::Text(text),
                    }],
                    tools: Vec::new(),
                    max_output_tokens: None,
                },
                acyclic_harness::conversation::Limits::default(),
                None,
            )?;
            expected = expected.max(request.bytes().len());
            let dispatch = ModelDispatch {
                operation_id: acyclic_harness::OperationId::new(),
                step: 0,
                request_digest: request.request_digest(),
            };
            let events = provider
                .generate(&request, dispatch)
                .collect::<Vec<_>>()
                .await;
            assert!(events.iter().all(Result::is_ok));
            let timing = timing.lock().unwrap_or_else(PoisonError::into_inner);
            assert_eq!(
                timing.maximum_request_bytes, expected,
                "maximum dispatched request omitted the summary branch"
            );
        }
        assert_eq!(
            timing
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .summaries,
            1
        );
    }
    Ok(())
}

#[tokio::test]
async fn checkout_measurement_uses_the_stock_tool_and_requires_its_return_boundary()
-> std::result::Result<(), Box<dyn std::error::Error>> {
    use tracing_subscriber::prelude::*;
    let timing = Arc::new(Mutex::new(Timing::default()));
    let subscriber = measurement_subscriber(timing.clone(), true, profile::Profile::default());
    assert!(
        timing.try_lock().is_ok(),
        "measurement subscriber retained timing mutex"
    );
    subscriber.try_init()?;
    let root = tempfile::tempdir()?;
    run(2, false, true, root.path(), timing.clone()).await?;
    timing
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .journal_work
        .require_stock_events()?;
    assert_eq!(
        timing
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .intervals
            .len(),
        2,
        "stock checkout return boundary was not measured"
    );
    Ok(())
}
