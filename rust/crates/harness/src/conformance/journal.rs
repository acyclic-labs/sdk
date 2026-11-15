//! Every agent journal refuses every model file tool, on every host. A root
//! whose model aims each stock file operation at the journal record holding
//! its own message (an exact read, a directory read, a new write, an exact
//! write, an edit and a removal) gets the journal refusal for each, and the
//! record's committed bytes are unchanged: the owning host still reads them
//! exactly, under its journal custody.
use crate::conformance::{
    Result,
    approvals::{Hosted, hosted, send},
    vectors,
};
use acyclic_harness_core::{
    agent::{ConversationEntry, TurnInput},
    approvals::ApprovalPolicy,
    context::CompactionPolicy,
    contract::capability::tool_call,
    conversation::{JOURNAL_VOLUME, Limits},
    filesystem::{Grants, StockExecutors},
    model::{
        Model, ModelContentPart, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest,
        ToolCall, ToolResultContent,
    },
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

/// The journal guard's refusal; any other failure is not the guard.
const REFUSAL: &str = "the agent journal is not a model file volume";
const TOOLS: [&str; 4] = ["read", "write", "edit", "remove"];

/// Each stock file operation, aimed at the journal record `path`.
fn calls(path: &str) -> [(&'static str, &'static str, Value); 6] {
    let directory = path
        .rsplit_once('/')
        .map_or(path, |(directory, _)| directory);
    [
        ("read-exact", "read", json!({"path": path})),
        (
            "read-directory",
            "read",
            json!({"path": directory, "selection": {"kind": "directory", "limit": 1}}),
        ),
        (
            "write-new",
            "write",
            json!({"path": format!("{directory}/forged.txt"), "content": "forged", "ifVersion": "absent"}),
        ),
        (
            "write-exact",
            "write",
            json!({"path": path, "content": "forged", "ifVersion": "any"}),
        ),
        (
            "edit-exact",
            "edit",
            json!({"path": path, "old_text": "journal", "new_text": "forged"}),
        ),
        (
            "remove-exact",
            "remove",
            json!({"path": path, "ifVersion": "any"}),
        ),
    ]
}

/// Calls every operation on the target once, then records what each returned.
#[derive(Default)]
struct Reaching {
    target: Mutex<Option<String>>,
    results: Mutex<Vec<(String, String)>>,
}

impl ModelProvider for Reaching {
    fn generate<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
        _: ModelDispatch,
    ) -> crate::model::ModelStream<'a> {
        let results: Vec<_> = request
            .request()
            .messages
            .iter()
            .flat_map(|message| message.content.parts())
            .filter_map(|part| match part {
                ModelContentPart::ToolResult {
                    call_id, content, ..
                } => Some((
                    call_id.clone(),
                    match content {
                        ToolResultContent::Error { message } => message.clone(),
                        other => format!("succeeded: {other:?}"),
                    },
                )),
                _ => None,
            })
            .collect();
        let target = self.target.lock().unwrap().clone();
        let mut events: Vec<_> = match target {
            Some(path) if results.is_empty() => calls(&path)
                .into_iter()
                .map(|(id, name, arguments)| {
                    Ok(ModelEvent::ToolCall(ToolCall::new(
                        id.into(),
                        name.into(),
                        arguments,
                    )))
                })
                .collect(),
            _ => {
                *self.results.lock().unwrap() = results;
                vec![Ok(ModelEvent::Content {
                    delta: "done".into(),
                })]
            }
        };
        events.push(Ok(ModelEvent::Completed {
            metadata: Value::Null,
        }));
        crate::model::ModelStream::new(Box::pin(futures::stream::iter(events)))
    }
}

pub(crate) async fn verify(backend: &dyn crate::conformance::Backend) -> Result {
    let model = Arc::new(Reaching::default());
    let executors = StockExecutors::new(
        (
            Model::new("conformance".into(), "scripted".into(), "1".into())?,
            Arc::clone(&model) as Arc<dyn ModelProvider>,
        ),
        acyclic_harness_core::tools::registry(&TOOLS.map(str::to_owned), |_| true, Vec::new())?,
        (Limits::default(), CompactionPolicy::Disabled),
        Arc::new(Grants(TOOLS.map(tool_call).to_vec())),
    );
    // Every action is accepted, so only the journal guard can refuse a call.
    let Hosted {
        bound,
        root,
        _storage,
        stream,
    } = hosted(
        backend,
        Arc::new(executors),
        ApprovalPolicy::accept_all("conformance"),
        false,
    )
    .await?;
    // The first turn commits a message record to the journal; the second aims
    // every file operation at that record.
    let file = send(&bound, &root, "first", "a journal record").await?;
    if file.volume().id() != JOURNAL_VOLUME {
        return Err("a sent message is not a journal record".into());
    }
    *model.target.lock().unwrap() = Some(format!("volume:///{JOURNAL_VOLUME}/{}", file.path()));
    send(&bound, &root, "reach", "reach the journal").await?;
    let results = model.results.lock().unwrap().clone();
    let expected = calls("");
    if results.len() != expected.len() {
        return Err(format!("the journal calls returned {results:?}").into());
    }
    for ((id, name, _), (call, result)) in expected.iter().zip(&results) {
        if call != id || !result.contains(REFUSAL) {
            return Err(
                format!("{name} {id} was not refused by the journal guard: {result}").into(),
            );
        }
    }
    // The record is intact: its owning host reads exactly its committed bytes.
    let first = vectors::intent("first")?;
    let record = vectors::records(stream.as_ref(), root.conversation()?)
        .await?
        .into_iter()
        .find(|record| {
            ConversationEntry::decode(&record.value).is_ok_and(
                |entry| matches!(&entry.input, TurnInput::Message { id, .. } if *id == first),
            )
        })
        .ok_or("the first message has no committed turn")?;
    let length = file.descriptor().byte_length();
    let (bytes, total) = bound
        .read_conversation_file(&root, &file, record.sequence, 0, length)
        .await?;
    file.descriptor().verify(&bytes, &mut None)?;
    if total != length {
        return Err("the owner read a different journal record length".into());
    }
    Ok(())
}
