//! The one stdio MCP tool server for harness tool providers.
//!
//! Newline-delimited JSON-RPC over blocking pipes, read through the shared
//! [`JsonLineReader`] codec. A provider answers each call at once or parks it
//! under a key until its work settles; the server owns the protocol, version
//! negotiation, request validation, the parked calls and client
//! cancellation. Between passes it blocks on input, the provider's armed
//! wake-up, writer room and output failure together, so idle work costs no
//! timer. Replies are written on their own thread, so a client that is slow
//! to read never stops the server reading its cancellations or EOF; the
//! server stops reading new requests only while a bounded reply backlog
//! waits for that client.
use super::JsonLineReader;
use crate::mcp::PROTOCOL_VERSION;
use crate::{Error, Result};
use futures::channel::{mpsc, oneshot};
use futures::executor::block_on;
use futures::future;
use futures::{FutureExt as _, SinkExt as _, StreamExt as _};
use serde_json::{Value, json};
use std::collections::{BTreeSet, VecDeque};
use std::future::Future;
use std::io::{self, Read, Write};
use std::pin::Pin;

/// Replies awaiting a client that is not reading, beyond which the server
/// reads no new request until the writer takes one.
const QUEUED_REPLIES: usize = 32;

/// What one wait of the serving loop observed.
enum Event {
    Message(Option<io::Result<Value>>),
    Woken(Result<()>),
    Room,
    Failed,
}

/// Protocol revisions answered as requested; any other gets the latest.
const SUPPORTED: [&str; 4] = [PROTOCOL_VERSION, "2025-06-18", "2025-03-26", "2024-11-05"];

/// A tool call's disposition.
pub enum ToolCall {
    /// The call's result.
    Answer(Value),
    /// The tool ran and failed with this result.
    Failed(Value),
    /// Parked under this key until [`ToolProvider::progress`] settles it.
    Park(String),
    /// Arguments the tool refuses: a JSON-RPC invalid-params error.
    Invalid(String),
    /// The provider cannot take the call now: a JSON-RPC server error.
    Busy(String),
}

/// A provider's armed wake-up: it completes once its work can progress, such
/// as when a child's pipe has data or the child exits.
pub type Wake<'a> = Pin<Box<dyn Future<Output = Result<()>> + 'a>>;

/// A parked call's settled result: its answer, or a tool failure.
pub type Settled = std::result::Result<Value, Value>;

/// A tool provider served by [`ToolServer`].
pub trait ToolProvider {
    /// The `tools/list` result: `{"tools": [...]}`. A call naming no listed
    /// tool is refused before it reaches [`Self::call`].
    fn tools(&self) -> Value;

    /// Runs one `tools/call`; an error answers it as a tool failure.
    fn call(&mut self, name: &str, arguments: &Value) -> Result<ToolCall>;

    /// The client cancelled the last call parked under `key`; its answer is
    /// no longer read.
    fn cancel(&mut self, key: &str) {
        let _ = key;
    }

    /// Input ended or the server failed: no further answer is read. Runs
    /// before the server waits for its queued replies to be written.
    fn closed(&mut self) {}

    /// Advances the provider's work and pushes each `parked` key it settles
    /// with its result. Returns the wake-up that schedules the next pass
    /// alongside input, or `None` when nothing runs and input alone can make
    /// progress. An error ends the server.
    fn progress(
        &mut self,
        parked: &BTreeSet<String>,
        settled: &mut Vec<(String, Settled)>,
    ) -> Result<Option<Wake<'_>>>;
}

/// The server's identity and its bounds.
pub struct ToolServer {
    /// `serverInfo.name`.
    pub name: &'static str,
    /// `serverInfo.version`.
    pub version: &'static str,
    /// Calls parked at once; further parks answer as tool errors.
    pub parked: usize,
    /// Bytes of one request line, excluding its delimiter.
    pub frame_bytes: u32,
}

impl ToolServer {
    /// Serves `provider` until `input` ends, then writes the queued replies.
    /// A framing, decoding or I/O failure answers a parse error and ends the
    /// server with that error; so does an output failure.
    pub fn serve(
        &self,
        provider: &mut impl ToolProvider,
        input: impl Read + Send + 'static,
        output: impl Write + Send + 'static,
    ) -> Result<()> {
        let (mut replies, mut failed, writer) = writer(output);
        let mut backlog = VecDeque::new();
        let served = self.run(provider, input, &mut replies, &mut backlog, &mut failed);
        provider.closed();
        for line in backlog {
            // A closed queue means the writer failed; joining reports it.
            if block_on(replies.send(line)).is_err() {
                break;
            }
        }
        drop(replies);
        let written = writer
            .join()
            .map_err(|_| Error::Storage("MCP output writer stopped".into()))?;
        // An output failure outranks the serving result it ended.
        written.map_err(|error| Error::Storage(error.to_string()))?;
        served
    }

    fn run(
        &self,
        provider: &mut impl ToolProvider,
        input: impl Read + Send + 'static,
        replies: &mut mpsc::Sender<Vec<u8>>,
        backlog: &mut VecDeque<Vec<u8>>,
        failed: &mut oneshot::Receiver<()>,
    ) -> Result<()> {
        let mut messages = lines(input, self.frame_bytes);
        let mut parked: Vec<(Value, String)> = Vec::new();
        let mut initialized = false;
        let mut settled = Vec::new();
        loop {
            let keys = parked.iter().map(|(_, key)| key.clone()).collect();
            let event = {
                let wake = provider.progress(&keys, &mut settled)?;
                for (key, value) in settled.drain(..) {
                    for (id, _) in parked.extract_if(.., |(_, parked)| *parked == key) {
                        queue(backlog, &result(&id, value.clone()))?;
                    }
                }
                while let Some(line) = backlog.pop_front() {
                    match replies.try_send(line) {
                        Ok(()) => {}
                        Err(error) if error.is_full() => {
                            backlog.push_front(error.into_inner());
                            break;
                        }
                        // The writer returns its own failure once joined.
                        Err(_) => return Ok(()),
                    }
                }
                let mut events: Vec<Pin<Box<dyn Future<Output = Event> + '_>>> = Vec::new();
                if backlog.len() < QUEUED_REPLIES {
                    events.push(Box::pin(messages.next().map(Event::Message)));
                }
                if let Some(wake) = wake {
                    events.push(Box::pin(wake.map(Event::Woken)));
                }
                if !backlog.is_empty() {
                    let replies = &mut *replies;
                    events.push(Box::pin(
                        future::poll_fn(move |context| replies.poll_ready(context))
                            .map(|_| Event::Room),
                    ));
                }
                events.push(Box::pin((&mut *failed).map(|_| Event::Failed)));
                let (event, _, _) = block_on(future::select_all(events));
                event
            };
            let message = match event {
                Event::Message(message) => message,
                Event::Woken(result) => {
                    result?;
                    continue;
                }
                Event::Room => continue,
                // The writer returns its own failure once joined.
                Event::Failed => return Ok(()),
            };
            let message = match message {
                Some(Ok(message)) => message,
                Some(Err(error)) => {
                    queue(backlog, &failure(&Value::Null, -32700, "Parse error"))?;
                    return Err(Error::Storage(error.to_string()));
                }
                None => return Ok(()),
            };
            let method = message.get("method").and_then(Value::as_str);
            if message.get("jsonrpc").and_then(Value::as_str) != Some("2.0") || method.is_none() {
                let id = message.get("id").unwrap_or(&Value::Null);
                queue(backlog, &failure(id, -32600, "Invalid Request"))?;
                continue;
            }
            let params = message.get("params").unwrap_or(&Value::Null);
            let Some(id) = message.get("id") else {
                if method == Some("notifications/cancelled")
                    && let Some(request) = params.get("requestId")
                {
                    for (id, key) in parked.extract_if(.., |(id, _)| id == request) {
                        provider.cancel(&key);
                        queue(backlog, &failure(&id, -32800, "Request cancelled"))?;
                    }
                }
                continue;
            };
            if !id.is_string() && !id.is_number() {
                queue(
                    backlog,
                    &failure(&Value::Null, -32600, "Invalid request ID"),
                )?;
                continue;
            }
            let reply = match method {
                Some("initialize") => match params.get("protocolVersion").and_then(Value::as_str) {
                    None => failure(id, -32602, "Missing protocol version"),
                    Some(requested) => {
                        initialized = true;
                        let version = SUPPORTED
                            .into_iter()
                            .find(|supported| *supported == requested)
                            .unwrap_or(PROTOCOL_VERSION);
                        json!({"jsonrpc":"2.0","id":id,"result":{
                            "protocolVersion":version,
                            "capabilities":{"tools":{}},
                            "serverInfo":{"name":self.name,"version":self.version}}})
                    }
                },
                Some("ping") => json!({"jsonrpc":"2.0","id":id,"result":{}}),
                _ if !initialized => failure(id, -32000, "Initialize first"),
                Some("tools/list") => json!({"jsonrpc":"2.0","id":id,"result":provider.tools()}),
                Some("tools/call") => {
                    let name = params
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let tools = provider.tools();
                    let listed =
                        tools
                            .get("tools")
                            .and_then(Value::as_array)
                            .is_some_and(|tools| {
                                tools.iter().any(|tool| {
                                    tool.get("name").and_then(Value::as_str) == Some(name)
                                })
                            });
                    let arguments = params.get("arguments").unwrap_or(&Value::Null);
                    match listed.then(|| provider.call(name, arguments)) {
                        None => failure(id, -32602, "Unknown tool"),
                        Some(Ok(ToolCall::Answer(value))) => result(id, Ok(value)),
                        Some(Ok(ToolCall::Failed(value))) => result(id, Err(value)),
                        Some(Ok(ToolCall::Invalid(message))) => failure(id, -32602, &message),
                        Some(Ok(ToolCall::Busy(message))) => failure(id, -32000, &message),
                        Some(Ok(ToolCall::Park(key))) if parked.len() >= self.parked => {
                            provider.cancel(&key);
                            result(id, Err(json!("parked call bound reached")))
                        }
                        Some(Ok(ToolCall::Park(key))) => {
                            parked.push((id.clone(), key));
                            continue;
                        }
                        Some(Err(error)) => result(id, Err(json!(error.to_string()))),
                    }
                }
                _ => failure(id, -32601, "Method not found"),
            };
            queue(backlog, &reply)?;
        }
    }
}

/// A `tools/call` result: its text, its structured content and whether the
/// tool failed.
fn result(id: &Value, settled: Settled) -> Value {
    let (value, failed) = match settled {
        Ok(value) => (value, false),
        Err(value) => (value, true),
    };
    let text = value.to_string();
    let structured = if value.is_object() {
        value
    } else {
        json!({ "result": value })
    };
    json!({"jsonrpc":"2.0","id":id,"result":{
        "content":[{"type":"text","text":text}],
        "structuredContent":structured,
        "isError":failed}})
}

fn failure(id: &Value, code: i32, message: &str) -> Value {
    json!({"jsonrpc":"2.0","id":id,"error":{"code":code,"message":message}})
}

/// Queues one reply line for the writer.
fn queue(backlog: &mut VecDeque<Vec<u8>>, value: &Value) -> Result<()> {
    let mut line = serde_json::to_vec(value).map_err(|error| Error::Storage(error.to_string()))?;
    line.push(b'\n');
    backlog.push_back(line);
    Ok(())
}

/// Writes queued replies to `output` on their own thread. The receiver
/// resolves when a write fails, and the thread returns that failure.
fn writer(
    mut output: impl Write + Send + 'static,
) -> (
    mpsc::Sender<Vec<u8>>,
    oneshot::Receiver<()>,
    std::thread::JoinHandle<io::Result<()>>,
) {
    let (replies, queued) = mpsc::channel::<Vec<u8>>(QUEUED_REPLIES);
    let (fail, failed) = oneshot::channel();
    let writer = std::thread::spawn(move || {
        for line in futures::executor::block_on_stream(queued) {
            if let Err(error) = output.write_all(&line).and_then(|()| output.flush()) {
                let _ = fail.send(());
                return Err(error);
            }
        }
        // Held until the queue closes, so the receiver resolves only on a
        // write failure.
        drop(fail);
        Ok(())
    });
    (replies, failed, writer)
}

/// Requests from `input`, read on their own thread so the provider's work
/// proceeds while the client is quiet.
fn lines(input: impl Read + Send + 'static, frame_bytes: u32) -> mpsc::Receiver<io::Result<Value>> {
    let (mut send, receive) = mpsc::channel(16);
    std::thread::spawn(move || {
        for message in JsonLineReader::new(input, frame_bytes) {
            if block_on(send.send(message)).is_err() {
                break;
            }
        }
    });
    receive
}

#[cfg(test)]
mod tests;
