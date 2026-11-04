use crate::{
    OperationId, Result,
    conversation::Limits,
    model::{
        Divergence, DivergenceSink, Model, ModelContent, ModelDelivery, ModelDispatch, ModelEvent,
        ModelMessage, ModelProvider, ModelRequest, ModelRole, PreparedModelRequest, RecordedCall,
        RecordedModel, RecordedToolOutcome, Recording, replay_key,
    },
    tool::ToolResult,
};
use acyclic_fs::{CancellationToken, NativeMaterializationWork, performance::BOUNDARY_BUDGET};
use futures::StreamExt;
use serde_json::json;
use std::sync::Arc;

struct NoLive;
impl DivergenceSink for NoLive {
    fn diverged(&self, _: Divergence) {
        panic!("a retained replay selection entered the live provider");
    }
}

fn events(context: usize, occurrence: usize) -> Vec<ModelEvent> {
    let text = format!("{context}:{occurrence}");
    vec![
        ModelEvent::Content {
            delta: text.clone(),
        },
        ModelEvent::MessageCompleted { text },
        ModelEvent::Completed {
            metadata: json!([context, occurrence]),
        },
    ]
}

/// Checks the actual replay entrances against source occurrence order.
/// Bytes select two interleaved requests, four dispatches per request,
/// returned-object allocation credit, cancellation and observed prefixes.
/// These are generated SDK operands and schedules, not provider recordings.
pub fn assert_replay_admission(schedule: &[u8]) {
    futures::executor::block_on(check(schedule)).expect("replay schedule admission");
}

async fn check(schedule: &[u8]) -> Result<()> {
    let mut requests = Vec::new();
    let mut source = Recording::default();
    let operation = OperationId::new();
    for context in 0..2 {
        let request = PreparedModelRequest::prepare(
            ModelRequest {
                compose_tools: false,
                model: Model::new("recorded-control".into(), "schedule".into(), "1".into())?,
                messages: vec![ModelMessage {
                    role: ModelRole::User,
                    content: ModelContent::Text(context.to_string()),
                }],
                tools: Vec::new(),
                sampling: Default::default(),
                max_output_tokens: None,
            },
            Limits::default(),
            None,
        )?;
        let key = replay_key(request.bytes())?;
        for occurrence in 0..4 {
            source.insert(
                key,
                RecordedCall {
                    source: Some(ModelDispatch {
                        operation_id: operation,
                        step: (context * 4 + occurrence) as u32,
                        request_digest: request.request_digest(),
                    }),
                    events: events(context, occurrence)
                        .iter()
                        .map(crate::contract::canonical_json_bytes)
                        .collect::<Result<_>>()?,
                    tools: vec![(
                        format!("{context}:{occurrence}"),
                        RecordedToolOutcome::Completed(ToolResult {
                            value: json!([context, occurrence]),
                        }),
                    )],
                },
            );
        }
        requests.push(request);
    }
    let trial = RecordedModel::new(
        Arc::new(super::ScriptedModelProvider::new([])),
        Arc::new(source),
        Arc::new(NoLive),
    );
    let mut selected = [[None; 4]; 2];
    let mut next = [0; 2];
    let mut tool_taken = [[false; 4]; 2];
    for &action in schedule {
        let context = usize::from(action & 1);
        let slot = usize::from((action >> 1) & 3);
        let request = &requests[context];
        let dispatch = ModelDispatch {
            operation_id: operation,
            step: (context * 4 + slot) as u32,
            request_digest: request.request_digest(),
        };
        let cancellation = CancellationToken::new();
        let mut budget = BOUNDARY_BUDGET;
        let credit = (action >> 3) & 7;
        if credit != 7 {
            budget.allocation_operations = 1 + u64::from(credit);
        }
        let work =
            NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
        let prior_unused = trial.unused();
        let retained = work.live_allocation_bytes();
        match trial.generate_with_native_work(request, dispatch, &work) {
            Err(crate::Error::NativeWork { .. }) => {
                assert_eq!(
                    work.live_allocation_bytes(),
                    retained,
                    "refused replay retained index funding"
                );
                assert_eq!(
                    trial.unused(),
                    prior_unused,
                    "funding refusal consumed source"
                );
                if selected[context][slot].is_none() {
                    assert!(
                        trial
                            .recorded_tool_result(
                                operation,
                                &format!("{context}:{}", next[context]),
                                None,
                            )?
                            .is_none(),
                        "funding refusal published tool coordinates"
                    );
                }
                continue;
            }
            Err(error) => return Err(error),
            Ok(future) => {
                if selected[context][slot].is_none() {
                    selected[context][slot] = Some(next[context]);
                    next[context] += 1;
                }
                if action & 64 != 0 {
                    cancellation.cancel();
                }
                let mut cancelled = false;
                let mut failure = |error| match error {
                    crate::Error::NativeWork { cause, .. } => {
                        cancelled =
                            matches!(cause, acyclic_fs::NativeTreeMaterializationError::Cancelled);
                    }
                    other => panic!("unexpected replay admission failure: {other}"),
                };
                match future.await {
                    Err(error) => failure(error),
                    Ok(mut stream) => {
                        while let Some(event) = stream.next().await {
                            if let Err(error) = event {
                                failure(error);
                                break;
                            }
                        }
                    }
                }
                if action & 64 != 0 {
                    assert!(
                        cancelled,
                        "accepted cancellation did not terminate with its typed cause"
                    );
                }
            }
        }
        let occurrence = selected[context][slot].expect("accepted source coordinate");
        let expected = events(context, occurrence);
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(BOUNDARY_BUDGET, &cancellation)
            .map_err(|(error, _)| error)?;
        let mut stream = trial
            .generate_with_native_work(request, dispatch, &work)?
            .await?;
        assert_eq!(
            stream.delivery(),
            ModelDelivery::Recorded {
                source: Some(ModelDispatch {
                    step: (context * 4 + occurrence) as u32,
                    ..dispatch
                })
            }
        );
        let mut actual = Vec::new();
        while let Some(event) = stream.next().await {
            let (event, custody) = event?;
            actual.push(event);
            drop(custody);
        }
        assert_eq!(
            actual, expected,
            "accepted dispatch lost its original events"
        );
        assert_eq!(trial.unused(), 8 - next.iter().sum::<usize>());
        let observed = if action & 128 == 0 {
            0
        } else {
            slot.min(expected.len())
        };
        let (suffix, custody) = trial
            .reconcile_with_native_work(dispatch, &expected[..observed], &work)?
            .await?;
        assert_eq!(suffix.as_deref(), Some(&expected[observed..]));
        drop(custody);
        let tool = trial.recorded_tool_result(
            operation,
            &format!("{context}:{occurrence}"),
            Some(&work),
        )?;
        assert_eq!(tool.is_none(), tool_taken[context][occurrence]);
        if let Some((tool, custody)) = tool {
            assert_eq!(
                tool,
                RecordedToolOutcome::Completed(ToolResult {
                    value: json!([context, occurrence])
                })
            );
            drop(custody);
        }
        tool_taken[context][occurrence] = true;
    }
    assert!(!trial.diverged());
    Ok(())
}

#[cfg(test)]
#[test]
fn retained_replay_schedules_use_the_production_admission_oracle() {
    for schedule in [
        &include_bytes!("../../../harness-fuzz/corpus/recorded_replay/refusal-retry")[..],
        &include_bytes!("../../../harness-fuzz/corpus/recorded_replay/interleaved-cancellation")[..],
        &include_bytes!("../../../harness-fuzz/corpus/recorded_replay/accepted-reconciliation")[..],
    ] {
        assert_replay_admission(schedule);
    }
}
