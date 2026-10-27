//! The one stdio MCP tool server for harness tool providers.
//!
//! Newline-delimited JSON-RPC over blocking pipes, read through the shared
//! [`JsonLineReader`] codec. A provider answers each call at once or parks it
//! under a key until its work settles; the server owns the protocol, the
//! parked calls and client cancellation, and blocks on input whenever the
//! provider has nothing running.
use super::JsonLineReader;
use crate::{Error, Result};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::io::{self, Read, Write};
use std::sync::mpsc;
use std::time::Duration;

/// Bound of one request line.
const FRAME_BYTES: u32 = 65_536;
/// Protocol revision answered when the client names none.
const PROTOCOL: &str = "2025-06-18";

/// A tool call's disposition.
pub enum ToolCall {
    /// The call's result.
    Answer(Value),
    /// Parked under this key until [`ToolProvider::progress`] settles it.
    Park(String),
}

/// A tool provider served by [`serve_tools`].
pub trait ToolProvider {
    /// The `tools/list` result.
    fn tools(&self) -> Value;

    /// Runs one `tools/call`; an error answers it as a tool error.
    fn call(&mut self, name: &str, arguments: &Value) -> Result<ToolCall>;

    /// Advances the provider's work and pushes each `parked` key it settles
    /// with its result. Returns how long input may be awaited before the next
    /// pass, or `None` when nothing runs and input alone can make progress.
    /// An error ends the server.
    fn progress(
        &mut self,
        parked: &BTreeSet<String>,
        settled: &mut Vec<(String, Value)>,
    ) -> Result<Option<Duration>>;
}

/// The server's identity and the bound of calls parked at once.
pub struct ToolServer {
    /// `serverInfo.name`.
    pub name: &'static str,
    /// `serverInfo.version`.
    pub version: &'static str,
    /// Calls parked at once; further parks answer as tool errors.
    pub parked: usize,
}

impl ToolServer {
    /// Serves `provider` until `input` ends.
    pub fn serve(
        &self,
        provider: &mut impl ToolProvider,
        input: impl Read + Send + 'static,
        mut output: impl Write,
    ) -> Result<()> {
        let messages = lines(input);
        let mut parked: Vec<(Value, String)> = Vec::new();
        let mut keys = BTreeSet::new();
        let mut settled = Vec::new();
        loop {
            let wait = provider.progress(&keys, &mut settled)?;
            for (key, value) in settled.drain(..) {
                for (id, _) in parked.extract_if(.., |(_, parked)| *parked == key) {
                    emit(&mut output, &answer(&id, Ok(value.clone())))?;
                }
                keys.remove(&key);
            }
            let message = match wait {
                Some(wait) => messages.recv_timeout(wait),
                None => messages
                    .recv()
                    .map_err(|_| mpsc::RecvTimeoutError::Disconnected),
            };
            let message = match message {
                Ok(Ok(message)) => message,
                Ok(Err(_)) => {
                    emit(
                        &mut output,
                        &json!({"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"invalid JSON"}}),
                    )?;
                    continue;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
            };
            let params = message.get("params").unwrap_or(&Value::Null);
            let method = message.get("method").and_then(Value::as_str);
            if method == Some("notifications/cancelled") {
                parked.retain(|(id, _)| Some(id) != params.get("requestId"));
                keys = parked.iter().map(|(_, key)| key.clone()).collect();
                continue;
            }
            let Some(id) = message.get("id") else {
                continue;
            };
            let reply = match method {
                Some("initialize") => json!({"jsonrpc":"2.0","id":id,"result":{
                    "protocolVersion":params.get("protocolVersion").and_then(Value::as_str).unwrap_or(PROTOCOL),
                    "capabilities":{"tools":{}},
                    "serverInfo":{"name":self.name,"version":self.version}}}),
                Some("ping") => json!({"jsonrpc":"2.0","id":id,"result":{}}),
                Some("tools/list") => json!({"jsonrpc":"2.0","id":id,"result":provider.tools()}),
                Some("tools/call") => {
                    let name = params
                        .get("name")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let arguments = params.get("arguments").unwrap_or(&Value::Null);
                    match provider.call(name, arguments) {
                        Ok(ToolCall::Answer(value)) => answer(id, Ok(value)),
                        Ok(ToolCall::Park(_)) if parked.len() >= self.parked => {
                            answer(id, Err("parked call bound reached".into()))
                        }
                        Ok(ToolCall::Park(key)) => {
                            keys.insert(key.clone());
                            parked.push((id.clone(), key));
                            continue;
                        }
                        Err(error) => answer(id, Err(error.to_string())),
                    }
                }
                _ => {
                    json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":"method not found"}})
                }
            };
            emit(&mut output, &reply)?;
        }
    }
}

fn answer(id: &Value, result: std::result::Result<Value, String>) -> Value {
    let result = match result {
        Ok(value) => json!({"content":[{"type":"text","text":value.to_string()}]}),
        Err(error) => json!({"isError":true,"content":[{"type":"text","text":error}]}),
    };
    json!({"jsonrpc":"2.0","id":id,"result":result})
}

fn emit(output: &mut impl Write, value: &Value) -> Result<()> {
    let mut line = serde_json::to_vec(value).map_err(|error| Error::Storage(error.to_string()))?;
    line.push(b'\n');
    output
        .write_all(&line)
        .and_then(|()| output.flush())
        .map_err(|error| Error::Storage(error.to_string()))
}

/// Requests from `input`, read on their own thread so the provider's work
/// proceeds while the client is quiet.
fn lines(input: impl Read + Send + 'static) -> mpsc::Receiver<io::Result<Value>> {
    let (send, receive) = mpsc::sync_channel(16);
    std::thread::spawn(move || {
        for message in JsonLineReader::new(input, FRAME_BYTES) {
            if send.send(message).is_err() {
                break;
            }
        }
    });
    receive
}
