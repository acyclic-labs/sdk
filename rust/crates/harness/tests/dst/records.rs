//! Record codecs: every record kind a simulated agent tree emits decodes in
//! every public reader with the same meaning. One world per record kind runs a
//! real turn, then checks the TUI, inspect, JSON and recovery readers.
use super::*;
use acyclic_harness::{
    agent::{AgentRecord as _, StatusRecord},
    executor::{ContextUsage, ModelUsageTotals, TurnOutput},
    local::agent_conversation::Conversation,
    model::{ModelContentPart, ModelEvent, ToolCall, ToolResultContent},
    test_support::{PromptOutcome, local_harness_config, prompt_settled},
    tool::{RecordedInvocation, ToolInvocation},
};
use acyclic_harness_tui::{BlockKind, TranscriptRows};
use family::{Generator, Variant};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;

/// The record kinds this class names, in seed order.
const RECORDS: [&str; 7] = [
    "accounting_header",
    "context_header",
    "text_delta",
    "tool_invocation",
    "recorded_invocation",
    "settlement",
    "status",
];

/// The public readers this class names.
const READERS: [&str; 4] = ["tui", "inspect", "json_output", "recovery"];

/// One world per record kind; every world checks every reader.
pub(super) const GENERATOR: Generator = Generator {
    class: "S1",
    seeds: every_record_kind_decodes_in_every_public_reader::VALUES,
    variants,
    world,
};

acyclic_test_support::cases! {
    module = every_record_kind_decodes_in_every_public_reader,
    values: u64,
    runner = super::run,
    cases = [
        seed_0 => 0, seed_1 => 1, seed_2 => 2, seed_3 => 3,
        seed_4 => 4, seed_5 => 5, seed_6 => 6,
    ],
}

fn run(seed: u64) -> std::result::Result<(), String> {
    GENERATOR.run(seed)
}

fn record(seed: u64) -> &'static str {
    RECORDS
        .get(usize::try_from(seed).unwrap_or(0) % RECORDS.len())
        .copied()
        .unwrap_or("status")
}

fn variants(seed: u64) -> Vec<Variant> {
    READERS
        .iter()
        .map(|reader| Variant::from([("record", record(seed)), ("reader", *reader)]))
        .collect()
}

/// The records one real turn emitted, in the shape each reader consumes.
struct Emitted {
    output: TurnOutput,
    status: StatusRecord,
    usage: ModelUsageTotals,
    context: Option<ContextUsage>,
    conversation: Arc<Conversation>,
}

fn json_round_trip<T>(value: &T) -> std::result::Result<(), String>
where
    T: Serialize + DeserializeOwned,
{
    let bytes = serde_json::to_vec(value).map_err(oracle::failed)?;
    let restored: T = serde_json::from_slice(&bytes).map_err(oracle::failed)?;
    let again = serde_json::to_vec(&restored).map_err(oracle::failed)?;
    if again != bytes {
        return Err("JSON codec did not round-trip its writer's value".into());
    }
    Ok(())
}

fn same_json<T: Serialize>(left: &T, right: &T) -> bool {
    serde_json::to_vec(left).ok() == serde_json::to_vec(right).ok()
}

fn rows_text(rows: &mut TranscriptRows) -> String {
    rows.rows(80)
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(
            "
",
        )
}

fn tui_text(text: &str) -> std::result::Result<(), String> {
    let mut rows = TranscriptRows::default();
    rows.push(BlockKind::Answer, text);
    let rendered = rows_text(&mut rows);
    if !rendered.contains(text) {
        return Err(format!("TUI lost answer text {text:?}"));
    }
    Ok(())
}

fn tui_tool(
    card: &acyclic_harness::executor::ToolCard,
    failed: bool,
) -> std::result::Result<(), String> {
    if card.failed != failed {
        return Err(format!(
            "tool card failure state {} differs from {failed}",
            card.failed
        ));
    }
    let mut rows = TranscriptRows::default();
    rows.push(
        BlockKind::Tool {
            failed: card.failed,
        },
        &card.text(),
    );
    let rendered = rows_text(&mut rows);
    if !rendered.contains(&card.line()) {
        return Err(format!("TUI lost tool line {:?}", card.line()));
    }
    Ok(())
}

fn tool_call(output: &TurnOutput) -> std::result::Result<ToolCall, String> {
    output
        .transcript
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            ModelContentPart::ToolCall(call) => Some(call.clone()),
            _ => None,
        })
        .ok_or_else(|| "the turn emitted no tool invocation".into())
}

fn settlement(output: &TurnOutput) -> std::result::Result<ToolResultContent, String> {
    output
        .transcript
        .iter()
        .flat_map(|message| message.content.parts())
        .find_map(|part| match part {
            ModelContentPart::ToolResult { content, .. } => Some(content.clone()),
            _ => None,
        })
        .ok_or_else(|| "the turn emitted no settlement".into())
}

fn text_delta(output: &TurnOutput) -> ModelEvent {
    ModelEvent::Content {
        delta: output.text.clone(),
    }
}

fn invocation(call: &ToolCall) -> ToolInvocation {
    ToolInvocation {
        operation_id: acyclic_harness::OperationId::from_bytes([7; 16]),
        call_id: call.call_id.clone(),
        name: call.name.clone(),
        arguments: call.arguments.clone(),
    }
}

fn recorded(call: &ToolCall) -> RecordedInvocation {
    RecordedInvocation {
        invocation: invocation(call),
        metadata: call.metadata.clone(),
        target: None,
        filesystem: None,
    }
}

fn reread_output(emitted: &Emitted) -> EntryFuture<'_, Result<TurnOutput>> {
    entry(async move {
        let conversation = &emitted.conversation;
        let tail = entry(conversation.settled()).await?;
        if tail == 0 {
            return Err(Error::NotFound("no settled conversation entry".into()));
        }
        let entries = entry(conversation.entries(0, tail)).await?;
        let last = entries
            .last()
            .ok_or_else(|| Error::NotFound("no settled conversation entry".into()))?;
        entry(conversation.output_at(tail - 1, last)).await
    })
}

fn reread_inspection(
    emitted: &Emitted,
) -> EntryFuture<'_, Result<(ModelUsageTotals, Option<ContextUsage>)>> {
    entry(async move {
        let conversation = &emitted.conversation;
        let mut inspect = entry(conversation.inspect()).await?;
        while !inspect.snapshot().synchronized {
            entry(inspect.advance()).await?;
        }
        let snapshot = inspect.snapshot();
        Ok((snapshot.usage.clone(), snapshot.context))
    })
}

fn check_record<'a>(
    kind: &'a str,
    reader: &'a str,
    emitted: &'a Emitted,
) -> EntryFuture<'a, std::result::Result<(), String>> {
    entry(async move {
        match kind {
            "accounting_header" => {
                let usage = &emitted.usage;
                match reader {
                    "tui" => {
                        let line = acyclic_harness::tui::usage_summary(usage).to_string();
                        if let Some(input) = usage.input.value()
                            && !line.contains(&format!("in {input}"))
                        {
                            return Err(format!("TUI lost accounting input {input}: {line}"));
                        }
                    }
                    "inspect" => json_round_trip(usage)?,
                    "json_output" => json_round_trip(usage)?,
                    _ => {
                        let (restored, _) = entry(reread_inspection(emitted))
                            .await
                            .map_err(oracle::failed)?;
                        if restored != *usage {
                            return Err("recovery lost the accounting header".into());
                        }
                    }
                }
            }
            "context_header" => {
                let context = emitted
                    .context
                    .ok_or("the turn emitted no context header")?;
                match reader {
                    "tui" => {
                        let line = acyclic_harness::tui::context_summary(
                            Some(("scripted", "dst")),
                            Some(context),
                            Some(0),
                        )
                        .to_string();
                        if !line.contains("context") {
                            return Err(format!("TUI lost the context header: {line}"));
                        }
                    }
                    "inspect" => json_round_trip(&context)?,
                    "json_output" => json_round_trip(&context)?,
                    _ => {
                        let (_, restored) = entry(reread_inspection(emitted))
                            .await
                            .map_err(oracle::failed)?;
                        if restored != emitted.context {
                            return Err("recovery lost the context header".into());
                        }
                    }
                }
            }
            "text_delta" => {
                let event = text_delta(&emitted.output);
                match reader {
                    "tui" => tui_text(&emitted.output.text)?,
                    "inspect" => json_round_trip(&event)?,
                    "json_output" => json_round_trip(&event)?,
                    _ => {
                        let restored = entry(reread_output(emitted))
                            .await
                            .map_err(oracle::failed)?;
                        if restored.text != emitted.output.text {
                            return Err("recovery lost the text delta".into());
                        }
                    }
                }
            }
            "tool_invocation" => {
                let call = tool_call(&emitted.output)?;
                match reader {
                    "tui" => {
                        let card = emitted
                            .output
                            .tool_cards()
                            .into_iter()
                            .next()
                            .ok_or("the turn emitted no tool card")?;
                        tui_tool(&card, false)?;
                    }
                    "inspect" => json_round_trip(&call)?,
                    "json_output" => json_round_trip(&call)?,
                    _ => {
                        let restored = entry(reread_output(emitted))
                            .await
                            .map_err(oracle::failed)?;
                        if tool_call(&restored)? != call {
                            return Err("recovery lost the tool invocation".into());
                        }
                    }
                }
            }
            "recorded_invocation" => {
                let call = tool_call(&emitted.output)?;
                let value = recorded(&call);
                match reader {
                    "tui" => {
                        let card = emitted
                            .output
                            .tool_cards()
                            .into_iter()
                            .next()
                            .ok_or("the turn emitted no tool card")?;
                        tui_tool(&card, false)?;
                    }
                    "inspect" => json_round_trip(&value)?,
                    "json_output" => json_round_trip(&value)?,
                    _ => {
                        let restored = entry(reread_output(emitted))
                            .await
                            .map_err(oracle::failed)?;
                        if !same_json(&recorded(&tool_call(&restored)?), &value) {
                            return Err("recovery lost the recorded invocation".into());
                        }
                    }
                }
            }
            "settlement" => {
                let value = settlement(&emitted.output)?;
                match reader {
                    "tui" => {
                        let card = emitted
                            .output
                            .tool_cards()
                            .into_iter()
                            .next()
                            .ok_or("the turn emitted no tool card")?;
                        tui_tool(&card, false)?;
                    }
                    "inspect" => json_round_trip(&value)?,
                    "json_output" => json_round_trip(&value)?,
                    _ => {
                        let restored = entry(reread_output(emitted))
                            .await
                            .map_err(oracle::failed)?;
                        if settlement(&restored)? != value {
                            return Err("recovery lost the settlement".into());
                        }
                    }
                }
            }
            _ => {
                let status = &emitted.status;
                match reader {
                    "tui" => tui_text(&format!("{:?}", status.phase))?,
                    "inspect" => json_round_trip(status)?,
                    "json_output" => json_round_trip(status)?,
                    _ => {
                        let bytes = status.encode().map_err(oracle::failed)?;
                        let restored = StatusRecord::decode(&bytes).map_err(oracle::failed)?;
                        if restored != *status {
                            return Err("status record lost its writer's value".into());
                        }
                    }
                }
            }
        }
        Ok(())
    })
}

fn status_record(harness: &Arc<LocalHarness>) -> EntryFuture<'_, Result<StatusRecord>> {
    use acyclic_stream::{ReadRequest, StreamProvider as _, StreamProviderExt as _};
    use futures::TryStreamExt as _;
    entry(async move {
        let (agents, root) = entry(harness.agents()).await?;
        let path = root.status().map_err(storage)?;
        let tail = agents
            .provider()
            .tail(path.clone())
            .await
            .map_err(storage)?;
        let read = agents
            .provider()
            .read(ReadRequest {
                key: None,
                path,
                from: 0,
                limit: u32::try_from(tail.max(1)).map_err(storage)?,
                at_least: Default::default(),
            })
            .await
            .map_err(storage)?;
        let read: Vec<_> = read.try_collect().await.map_err(storage)?;
        let last = read.last().ok_or_else(|| storage("no status record"))?;
        StatusRecord::decode(&last.value).map_err(storage)
    })
}

fn emitted(harness: &Arc<LocalHarness>, output: TurnOutput) -> EntryFuture<'_, Result<Emitted>> {
    entry(async move {
        let status = entry(status_record(harness)).await?;
        let conversation = Conversation::open(entry(harness.connect()).await?);
        let mut inspect = entry(conversation.inspect()).await?;
        while !inspect.snapshot().synchronized {
            entry(inspect.advance()).await?;
        }
        let snapshot = inspect.snapshot();
        Ok(Emitted {
            output,
            status,
            usage: snapshot.usage.clone(),
            context: snapshot.context,
            conversation,
        })
    })
}

fn world(seed: u64) -> std::result::Result<bool, String> {
    let kind = record(seed);
    oracle::simulate(seed, move || {
        entry(async move {
            let directory = tempfile::tempdir().map_err(oracle::failed)?;
            let checkout = directory.path().join("checkout");
            std::fs::create_dir_all(&checkout).map_err(oracle::failed)?;
            std::fs::write(checkout.join("a.txt"), "alpha\n").map_err(oracle::failed)?;
            let model =
                Model::new("scripted".into(), "dst".into(), "1".into()).map_err(oracle::failed)?;
            let call = ToolCall::new("s1-read".into(), "read".into(), json!({"path": "a.txt"}));
            let provider = Arc::new(oracle::service(vec![oracle::Plan {
                tag: "s1",
                steps: vec![vec![call]],
                fail_once: None,
            }]));
            let config = local_harness_config(checkout, (model, provider))
                .map_err(oracle::failed)?
                .without_user_settings()
                .workspace_mounting(acyclic_harness::local::WorkspaceMounting::Physical)
                .state_directory(directory.path().join("state"));
            let harness = Arc::new(
                entry(LocalHarness::open(config))
                    .await
                    .map_err(oracle::failed)?,
            );
            let output = match entry(prompt_settled(&harness, "s1", "[dst:s1]"))
                .await
                .map_err(oracle::failed)?
            {
                PromptOutcome::Settled(_, output) => output,
                PromptOutcome::Failed(message) => {
                    return Err(format!("the record turn failed: {message}"));
                }
            };
            let emitted = entry(emitted(&harness, output))
                .await
                .map_err(oracle::failed)?;
            for reader in READERS {
                entry(check_record(kind, reader, &emitted, &harness)).await?;
            }
            Ok(true)
        })
    })
}
