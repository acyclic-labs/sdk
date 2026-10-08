//! Bounded newline protocol for an already approved native process exchange.
//!
//! This codec never launches a process or retries an operation. The existing
//! native process owner must retain writes, observations, deadlines and cleanup.

use super::{
    McpInitializeResult, PROTOCOL_VERSION,
    rpc::{notification, rpc_result},
};
use crate::{Error, OperationId, Result};
use bytes::BytesMut;
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use serde_json::{Value, json};
use tokio_util::codec::{Decoder as _, Encoder as _};

#[derive(Debug, PartialEq, Eq)]
enum Phase {
    Initialize,
    Request,
    Complete,
    Failed,
}

/// Transient parser state for one initialized, one-shot stdio request.
/// It is not a durable receipt, process owner or recovery mechanism.
pub struct McpStdioExchange {
    initialization: OperationId,
    operation: OperationId,
    request: Vec<u8>,
    phase: Phase,
    line: BytesMut,
    codec: JsonRpcMessageCodec<Value>,
    received: usize,
    maximum_bytes: usize,
    result: Option<Value>,
}

impl McpStdioExchange {
    /// Creates the codec and exact first write without performing any I/O.
    /// Only discovery and tool calls are supported; callback features belong to
    /// their own authority owner. Both IDs and the complete transcript are bound
    /// by the consumer's ordinary native process admission.
    pub fn new(
        initialization: OperationId,
        operation: OperationId,
        method: &str,
        params: &Value,
        maximum_bytes: u32,
    ) -> Result<(Self, Vec<u8>)> {
        if initialization == operation
            || maximum_bytes == 0
            || !matches!(method, "tools/list" | "tools/call")
            || !params.is_object()
        {
            return Err(Error::Invalid("invalid MCP stdio exchange contract".into()));
        }
        let request = line(&json!({"jsonrpc":"2.0", "id":operation.to_string(),
            "method":method,"params":params}))?;
        let first = line(&json!({"jsonrpc":"2.0", "id":initialization.to_string(),
            "method":"initialize", "params":{"protocolVersion":PROTOCOL_VERSION,
            "capabilities":{}, "clientInfo":{"name":"acyclic-harness","version":env!("CARGO_PKG_VERSION")}}}))?;
        let ready = line(&json!({"jsonrpc":"2.0","method":"notifications/initialized"}))?;
        let maximum_bytes = maximum_bytes as usize;
        if first
            .len()
            .saturating_add(ready.len())
            .saturating_add(request.len())
            > maximum_bytes
        {
            return Err(Error::Invalid(
                "MCP stdio writes exceed admitted bound".into(),
            ));
        }
        Ok((
            Self {
                initialization,
                operation,
                request,
                phase: Phase::Initialize,
                line: BytesMut::new(),
                codec: JsonRpcMessageCodec::new_with_max_length(maximum_bytes),
                received: 0,
                maximum_bytes,
                result: None,
            },
            first,
        ))
    }

    /// Consumes stdout bytes and returns exact follow-up writes after successful
    /// negotiation. Stderr must be captured separately by the process owner.
    /// Any invalid input permanently closes this codec to further processing.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        if self.phase == Phase::Failed {
            return Err(Error::Invalid("MCP stdio exchange already failed".into()));
        }
        let result = self.consume(bytes);
        if result.is_err() {
            self.phase = Phase::Failed;
            self.result = None;
        }
        result
    }

    fn consume(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        self.received = self
            .received
            .checked_add(bytes.len())
            .filter(|total| *total <= self.maximum_bytes)
            .ok_or_else(|| Error::Invalid("MCP stdio stdout exceeds admitted bound".into()))?;
        let mut writes = Vec::new();
        self.line.extend_from_slice(bytes);
        while let Some(message) = self
            .codec
            .decode(&mut self.line)
            .map_err(|error| Error::Invalid(format!("invalid MCP stdio message: {error}")))?
        {
            if message.get("id").is_none() {
                notification(&message)?;
                // Catalog notifications are hints, never implicit reloads.
                continue;
            }
            match self.phase {
                Phase::Initialize => {
                    let result = rpc_result(message, self.initialization)?;
                    let negotiated: McpInitializeResult = serde_json::from_value(result)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    negotiated.validate()?;
                    writes.extend(line(
                        &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
                    )?);
                    writes.append(&mut self.request);
                    self.phase = Phase::Request;
                }
                Phase::Request => {
                    self.result = Some(rpc_result(message, self.operation)?);
                    self.phase = Phase::Complete;
                }
                Phase::Complete | Phase::Failed => {
                    return Err(Error::Invalid(
                        "unexpected MCP stdio response after completion".into(),
                    ));
                }
            }
        }
        Ok(writes)
    }

    /// Requires a complete newline-terminated exchange at process EOF. Incomplete
    /// output is uncertain and must be reconciled by the native receipt owner.
    pub fn finish(mut self) -> Result<Value> {
        if self.phase != Phase::Complete || !self.line.is_empty() {
            return Err(Error::Indeterminate(self.operation));
        }
        self.result
            .take()
            .ok_or(Error::Indeterminate(self.operation))
    }
}

fn line(value: &Value) -> Result<Vec<u8>> {
    let mut bytes = BytesMut::new();
    JsonRpcMessageCodec::new()
        .encode(value, &mut bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(bytes.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (OperationId, OperationId) {
        (
            OperationId::from_bytes([1; 16]),
            OperationId::from_bytes([2; 16]),
        )
    }

    fn initialized(id: OperationId) -> Vec<u8> {
        line(&json!({"jsonrpc":"2.0","id":id.to_string(),"result":{
            "protocolVersion":PROTOCOL_VERSION,"capabilities":{},"serverInfo":{"name":"fixture","version":"1"}}})).unwrap()
    }

    #[test]
    fn every_stream_cut_preserves_order_and_exact_result() {
        let (init, op) = ids();
        let expected = json!({"content":[{"type":"text","text":"héllo"}]});
        let mut transcript = initialized(init);
        transcript.extend(
            line(&json!({"jsonrpc":"2.0","method":"notifications/tools/list_changed"})).unwrap(),
        );
        transcript
            .extend(line(&json!({"jsonrpc":"2.0","id":op.to_string(),"result":expected})).unwrap());
        for width in 1..=transcript.len() {
            let (mut codec, first) = McpStdioExchange::new(
                init,
                op,
                "tools/call",
                &json!({"name":"echo","arguments":{}}),
                4096,
            )
            .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&first).unwrap()["method"],
                "initialize"
            );
            let mut writes = Vec::new();
            for chunk in transcript.chunks(width) {
                writes.extend(codec.push(chunk).unwrap());
            }
            let messages: Vec<Value> = writes
                .split(|byte| *byte == b'\n')
                .filter(|part| !part.is_empty())
                .map(|part| serde_json::from_slice(part).unwrap())
                .collect();
            assert_eq!(messages.len(), 2);
            assert_eq!(messages[0]["method"], "notifications/initialized");
            assert_eq!(messages[1]["id"], op.to_string());
            assert_eq!(codec.finish().unwrap(), expected);
        }
    }

    #[test]
    fn malformed_foreign_callback_and_truncated_inputs_cannot_replay() {
        let (init, op) = ids();
        for bytes in [b"not json\n".to_vec(), line(&json!({"jsonrpc":"2.0","id":op.to_string(),"result":{}})).unwrap(),
            line(&json!({"jsonrpc":"2.0","id":init.to_string(),"method":"sampling/createMessage","params":{}})).unwrap()] {
            let (mut codec,_) = McpStdioExchange::new(init,op,"tools/list",&json!({}),4096).unwrap();
            assert!(codec.push(&bytes).is_err());
            assert!(codec.push(&initialized(init)).is_err());
            assert!(codec.finish().is_err());
        }
        let (mut codec, _) =
            McpStdioExchange::new(init, op, "tools/list", &json!({}), 4096).unwrap();
        assert!(codec.push(&vec![b' '; 4097]).is_err());
        let (mut codec, _) =
            McpStdioExchange::new(init, op, "tools/list", &json!({}), 4096).unwrap();
        let mut truncated = initialized(init);
        truncated.pop();
        assert!(codec.push(&truncated).unwrap().is_empty());
        assert!(matches!(codec.finish(), Err(Error::Indeterminate(_))));
        assert!(McpStdioExchange::new(init, init, "tools/list", &json!({}), 4096).is_err());
    }
}
