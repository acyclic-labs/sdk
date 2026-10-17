//! Browser task projection from the same borrowed native file view.
use super::{
    json::{from_js, to_js},
    query::QueryFileInfo,
    swarm::WasmClientViews,
};
use crate::{
    client::TaskQueryView,
    task_volume::{Channel, Component, Cursor},
};
use acyclic_stream::StreamPath;
use serde::Serialize;
use tsify::Tsify;
use wasm_bindgen::prelude::*;

#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
struct TaskTextInfo<'a> {
    turn: u64,
    channel: Channel,
    file: QueryFileInfo<'a>,
}
#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
struct ToolQueryInfo<'a> {
    turn: u64,
    call: &'a Component,
    request: Option<QueryFileInfo<'a>>,
    result: Option<QueryFileInfo<'a>>,
    arguments: Vec<QueryFileInfo<'a>>,
    stdout: Vec<QueryFileInfo<'a>>,
    stderr: Vec<QueryFileInfo<'a>>,
}
#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
struct TurnCompletionInfo<'a> {
    turn: u64,
    file: QueryFileInfo<'a>,
}
#[derive(Serialize, Tsify)]
struct TaskQueryInfo<'a> {
    task: &'a Component,
    cursor: &'a Cursor,
    text: Vec<TaskTextInfo<'a>>,
    tools: Vec<ToolQueryInfo<'a>>,
    completions: Vec<TurnCompletionInfo<'a>>,
}
impl<'a> From<TaskQueryView<'a>> for TaskQueryInfo<'a> {
    fn from(task: TaskQueryView<'a>) -> Self {
        Self {
            task: task.task,
            cursor: task.cursor,
            text: task
                .text()
                .map(|text| TaskTextInfo {
                    turn: text.turn,
                    channel: text.channel,
                    file: text.file.into(),
                })
                .collect(),
            tools: task
                .tools()
                .map(|tool| ToolQueryInfo {
                    turn: tool.turn,
                    call: tool.call,
                    request: tool.request().map(Into::into),
                    result: tool.result().map(Into::into),
                    arguments: tool.arguments().map(Into::into).collect(),
                    stdout: tool.stdout().map(Into::into).collect(),
                    stderr: tool.stderr().map(Into::into).collect(),
                })
                .collect(),
            completions: task
                .completions()
                .map(|(turn, file)| TurnCompletionInfo {
                    turn,
                    file: file.into(),
                })
                .collect(),
        }
    }
}
#[wasm_bindgen]
impl WasmClientViews {
    /// Borrow ordered immutable outcomes from the same followed selection.
    #[wasm_bindgen(unchecked_return_type = "[FileKey, CommandOutputView][]")]
    pub fn outcomes(
        &self,
        agent: &str,
        #[wasm_bindgen(unchecked_param_type = "OutputOwner")] owner: JsValue,
        after: u64,
    ) -> Result<JsValue, JsValue> {
        let agent =
            StreamPath::new(agent).map_err(|error| JsValue::from_str(&error.to_string()))?;
        let owner: crate::client::OutputOwner = from_js(owner)?;
        self.views
            .read(|views| {
                let Some(cursor) = views.cursor(&agent) else {
                    return to_js(&Vec::<u8>::new());
                };
                let Some(files) = views.selection(&(cursor.volume, cursor.query)) else {
                    return to_js(&Vec::<u8>::new());
                };
                to_js(&files.outputs(&owner, after).collect::<Vec<_>>())
            })
            .map_err(|error| JsValue::from_str(&error.to_string()))?
    }
    /// Task text/tool folders at the exact followed cut. Read payloads with
    /// `segments(agent, file.key)`; metadata reads never concatenate output.
    #[wasm_bindgen(unchecked_return_type = "TaskQueryInfo | null")]
    pub fn task(&self, agent: &str, task: &str) -> Result<JsValue, JsValue> {
        let error = |error: &dyn std::fmt::Display| JsValue::from_str(&error.to_string());
        let agent = StreamPath::new(agent).map_err(|e| error(&e))?;
        let task = Component::new(task).map_err(|e| error(&e))?;
        self.views
            .read(|views| {
                let Some(cursor) = views.cursor(&agent) else {
                    return Ok(JsValue::NULL);
                };
                to_js(
                    &views
                        .task(&(cursor.volume, cursor.query), &task)
                        .map(TaskQueryInfo::from),
                )
            })
            .map_err(|e| error(&e))?
    }
}
