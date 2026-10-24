use super::*;
use crate::{
    context::{Context, ModelContextCapacity, SummaryBoundary, SummarySource, ThresholdCompaction},
    conversation::ContentPublisher,
    filesystem::ModelTaskCommand,
    local::tests::{volume, volume_config},
    model::{ModelContent, ModelMessage, ModelRole},
};

#[derive(Clone, Copy, Debug)]
enum Case {
    OutputZero,
    Projection,
    MandatorySource,
    MandatoryCapacity,
    RequestLimit,
    SummaryOutput,
    SummaryInput,
    SummaryNoBudget,
    Accounting,
    Policy,
}

#[tokio::test]
async fn every_pre_dispatch_site_is_a_failed_closable_turn() -> Result<()> {
    for case in [
        Case::OutputZero,
        Case::Projection,
        Case::MandatorySource,
        Case::MandatoryCapacity,
        Case::RequestLimit,
        Case::SummaryOutput,
        Case::SummaryInput,
        Case::SummaryNoBudget,
        Case::Accounting,
        Case::Policy,
    ] {
        let directory = tempfile::tempdir().map_err(crate::local::storage)?;
        let project = volume(directory.path()).await?;
        let capacity = ModelContextCapacity {
            context_tokens: if matches!(case, Case::SummaryInput) {
                8192
            } else {
                65_536
            },
            output_tokens: Some(4096),
        };
        let provider = ScriptedModelProvider::new([]).with_context_capacity(capacity);
        let provider = Arc::new(if matches!(case, Case::Accounting) {
            provider.with_token_counter(|request| {
                let mut count = request.serialized_token_upper_bound(None)?.0;
                count.request_digest[0] ^= 1;
                Ok(count)
            })
        } else {
            provider
        });
        let mut limits = Limits::default();
        if matches!(case, Case::RequestLimit) {
            limits.context_messages = 1;
        }
        let (_conversation_directory, mut harness) = if matches!(case, Case::MandatoryCapacity) {
            let (directory, mut harness) = super::super::poll_sweep::conversation_harness().await?;
            harness.provider = provider.clone();
            (Some(directory), harness)
        } else {
            (
                None,
                LocalHarness::open(
                    volume_config(directory.path(), project, provider.clone())?
                        .without_default_tools()
                        .limits(limits)
                        .compaction(CompactionPolicy::Disabled),
                )
                .await?,
            )
        };
        if matches!(
            case,
            Case::Projection
                | Case::MandatorySource
                | Case::MandatoryCapacity
                | Case::Accounting
                | Case::Policy
        ) {
            harness.compaction = CompactionPolicy::Threshold(ThresholdCompaction {
                response_reserve_tokens: 16_384,
                recent_tokens: if matches!(case, Case::Policy) { 0 } else { 100 },
                ..Default::default()
            });
        }
        let operation = OperationId::new();
        let admission = if matches!(case, Case::MandatoryCapacity) {
            OperationId::new()
        } else {
            operation
        };
        let publisher = harness.input_publisher()?;
        let mut manifest = crate::context::ContextManifest::default();
        if matches!(case, Case::RequestLimit) {
            manifest.conventions.push("instruction".into());
        }
        if matches!(case, Case::MandatorySource) {
            manifest.conventions.push("s".repeat(50_000));
        }
        let reference = manifest
            .publish(
                publisher.as_ref(),
                operation,
                &format!("inputs/{operation}.manifest.json"),
                harness.limits,
            )
            .await?;
        crate::local::context::binding::retain(
            &harness.task_files.inner,
            crate::runtime::task_id_for_admission(admission),
            &reference,
            harness.limits.file_bytes,
        )
        .await?;
        let mut command = ModelTaskCommand {
            context_manifest: reference.clone(),
            summary_source: None,
            input: ModelContent::Text(if matches!(case, Case::Projection) {
                "x".repeat(70_000)
            } else if matches!(case, Case::MandatorySource) {
                "c".repeat(5_000)
            } else {
                "answer".into()
            }),
            selected_context: None,
            max_steps: 1,
            max_output_tokens: None,
            mailbox_operation: None,
            selected_output_volume: None,
        };
        if matches!(case, Case::OutputZero) {
            command.max_output_tokens = Some(0);
        }
        if matches!(case, Case::MandatoryCapacity) {
            let current = "c".repeat(50_000);
            command.selected_context = Some(
                super::super::poll_sweep::selected_history::initialize_messages(
                    &harness,
                    operation,
                    [
                        (crate::conversation::MessageKind::User, &"o".repeat(28_000)),
                        (crate::conversation::MessageKind::Assistant, "older answer"),
                        (crate::conversation::MessageKind::User, &current),
                    ],
                )
                .await?,
            );
            command.input = command
                .selected_context
                .as_ref()
                .unwrap()
                .messages
                .last()
                .unwrap()
                .content
                .clone();
        }
        if matches!(
            case,
            Case::SummaryOutput | Case::SummaryInput | Case::SummaryNoBudget
        ) {
            let source = SummarySource {
                context: Context {
                    messages: vec![ModelMessage {
                        role: ModelRole::User,
                        content: ModelContent::Text("x".repeat(
                            if matches!(case, Case::SummaryInput) {
                                10_000
                            } else {
                                1
                            },
                        )),
                    }],
                    ..Default::default()
                },
                boundary: SummaryBoundary {
                    task: crate::runtime::task_id_for_admission(admission),
                    through: crate::task_volume::Cursor {
                        volume: [0; 32],
                        epoch: uuid::Uuid::nil(),
                        schema: crate::task_volume::SCHEMA,
                        query: [0; 32],
                        position: 1,
                        accepted: 1,
                        durable: 1,
                        commit: [0; 32],
                    },
                    captured_at: 1,
                    previous: None,
                    outcome_included: false,
                    frozen: false,
                    manifest: reference,
                    model: "test".into(),
                },
            };
            command.summary_source = Some(Box::new(
                publisher
                    .stage_once(
                        OperationId::new(),
                        &format!("inputs/{operation}.summary-source.json"),
                        &serde_json::to_vec(&source).map_err(crate::local::storage)?,
                        "application/json",
                        "summary source",
                        crate::conversation::ContentAccess { ceiling: None, io: None },
                    )?
                    .await?
                    .0,
            ));
            command.max_output_tokens = match case {
                Case::SummaryOutput => Some(8192),
                Case::SummaryInput => Some(1024),
                _ => None,
            };
        }
        let bytes = serde_json::to_vec(&command).map_err(crate::local::storage)?;
        let payload = publisher
            .stage_once(
                operation,
                &format!("inputs/{operation}.json"),
                &bytes,
                "application/json",
                "model input",
                crate::conversation::ContentAccess { ceiling: None, io: None },
            )?
            .await?
            .0;
        let (receive, model) = harness
            .finish_turn_inputs(operation, payload, publisher.as_ref())
            .await?;
        drop(publisher);
        let task = admitted_task(
            harness
                .publish_turn(
                    Turn {
                        operation: admission,
                        root: admission,
                        automatic_turns: 0,
                        goal_revision: None,
                        receive,
                        model,
                    },
                    None,
                )
                .await?,
        )?;
        let result = harness.resume(task).await;
        assert!(
            matches!(&result, Err(Error::TaskFailed { task: failed, .. }) if *failed == task),
            "{case:?}: {result:?}; requests: {}",
            provider.requests().len()
        );
        let expected = match case {
            Case::OutputZero => "model output token budget is zero",
            Case::Projection => "source",
            Case::MandatorySource => "mandatory content leaves no compaction source",
            Case::MandatoryCapacity => "mandatory content exceeds selected model capacity",
            Case::RequestLimit => "context",
            Case::SummaryOutput => "summary output exceeds selected model capacity",
            Case::SummaryInput => "summary input exceeds selected model capacity",
            Case::SummaryNoBudget => "summary requires a caller-selected output token budget",
            Case::Accounting => "token count does not bind",
            Case::Policy => "compaction budgets exceed selected model capacity",
        };
        assert!(
            matches!(&result, Err(Error::TaskFailed { message, .. }) if message.contains(expected)),
            "{case:?}: wrong preparation site: {result:?}"
        );
        assert!(
            provider.requests().is_empty(),
            "{case:?}: provider dispatched"
        );
        harness.close().await?;
    }
    Ok(())
}
