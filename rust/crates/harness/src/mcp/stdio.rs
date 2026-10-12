//! Bounded newline protocol for an already approved native process exchange.

use super::{
    PROTOCOL_VERSION,
    rpc::{NativeWork, initialization, notification,
        rpc_result},
};
use crate::{Error, OperationId, Result};
use bytes::BytesMut;
use rmcp::transport::async_rw::JsonRpcMessageCodec;
use serde::{Deserialize, Serialize};
use serde_json::{Value, value::RawValue};
#[cfg(test)]
use serde_json::json;
use tokio_util::codec::{Decoder as _, Encoder as _};

/// The only operations this one-shot client can admit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum McpStdioMethod {
    /// One explicit discovery page.
    #[serde(rename = "tools/list")]
    ListTools,
    /// One pinned tool call.
    #[serde(rename = "tools/call")]
    CallTool,
    /// One resource discovery page.
    #[serde(rename = "resources/list")]
    ListResources,
    /// One resource template discovery page.
    #[serde(rename = "resources/templates/list")]
    ListResourceTemplates,
    /// One exact resource read.
    #[serde(rename = "resources/read")]
    ReadResource,
    /// One prompt discovery page.
    #[serde(rename = "prompts/list")]
    ListPrompts,
    /// One exact prompt selection.
    #[serde(rename = "prompts/get")]
    GetPrompt,
}

impl McpStdioMethod {
    /// Exact protocol method, never a callback or ambient operation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ListTools => "tools/list",
            Self::CallTool => "tools/call",
            Self::ListResources => "resources/list",
            Self::ListResourceTemplates => "resources/templates/list",
            Self::ReadResource => "resources/read",
            Self::ListPrompts => "prompts/list",
            Self::GetPrompt => "prompts/get",
        }
    }
}

/// Exact portable exchange descriptor included in native process approval.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct McpStdioRequest {
    /// Initialization identity, distinct from the admitted operation.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub initialization: OperationId,
    /// Exact admitted remote request identity.
    #[cfg_attr(feature = "wasm", tsify(type = "string"))]
    pub operation: OperationId,
    /// Discovery or tool call, explicitly selected by the host.
    pub method: McpStdioMethod,
    /// Exact object parameters included in approval and durable identity.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmToolJsonValue"))]
    pub params: Value,
    /// Positive cumulative byte allowance per input/output direction.
    pub maximum_bytes: u32,
}

impl McpStdioRequest {
    /// Creates transient protocol state and its first write, with no I/O.
    pub fn exchange(&self, work: NativeWork<'_>) -> Result<(McpStdioExchange, Vec<u8>, NativeCustody)> {
        super::services::validate_params(self.method.as_str(), &self.params, work)?;
        McpStdioExchange::new(
            self.initialization, self.operation, self.method.as_str(),
            &self.params, self.maximum_bytes, work,
        )
    }

    /// Verifies the complete descriptor without launching or admitting work.
    pub fn validate(&self) -> Result<()> {
        self.exchange(None).map(drop)
    }

    /// Validates a retained complete transcript without performing its writes.
    pub fn decode_response(&self, stdout: &[u8]) -> Result<Value> {
        let (mut exchange, _, _) = self.exchange(None)?;
        exchange.push(stdout, None)?;
        let (value, _) = exchange.finish()?;
        super::services::validate_result(self.method.as_str(), value, None)
    }
}

/// Transient parser state for one initialized, one-shot stdio request.
pub struct McpStdioExchange {
    initialization: OperationId,
    operation: OperationId,
    request: Option<OwnedWrite>,
    failed: bool,
    line: BytesMut,
    codec: JsonRpcMessageCodec<Box<RawValue>>,
    received: usize,
    maximum_bytes: usize,
    result: Option<OwnedValue>,
    // LAST: all parser/control storage is physically dropped before custody.
    custody: NativeCustody,
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
type NativeCustody = Option<acyclic_fs::NativeAllocation>;
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
type NativeCustody = Option<()>;

struct OwnedWrite {
    bytes: Vec<u8>,
    custody: NativeCustody,
}
struct OwnedValue {
    value: Value,
    custody: NativeCustody,
}

impl McpStdioExchange {
    /// Creates the initialized one-shot engine on the supplied work ledger.
    pub fn new(
        initialization: OperationId, operation: OperationId, method: &str,
        params: &Value, maximum_bytes: u32, work: NativeWork<'_>,
    ) -> Result<(Self, Vec<u8>, NativeCustody)> {
        if initialization == operation || maximum_bytes == 0
            || !matches!(method, "tools/list" | "tools/call" | "resources/list" |
                "resources/templates/list" | "resources/read" | "prompts/list" | "prompts/get")
            || !params.is_object()
        {
            return Err(rejection(work, "invalid MCP stdio exchange contract"));
        }
        let maximum_bytes = maximum_bytes as usize;
        let first = encode_line(&Envelope { id: initialization, jsonrpc: "2.0",
            method: "initialize", params: InitializeParams {
                capabilities: Empty {}, client_info: ClientInfo {
                    name: "acyclic-harness", version: env!("CARGO_PKG_VERSION"),
                }, protocol_version: PROTOCOL_VERSION,
            } }, maximum_bytes, work)?;
        let ready = encode_line(&Initialized { jsonrpc: "2.0",
            method: "notifications/initialized" }, maximum_bytes, work)?;
        let request = encode_line(&Envelope { id: operation, jsonrpc: "2.0",
            method, params }, maximum_bytes, work)?;
        let followup = ready.bytes.len().checked_add(request.bytes.len())
            .ok_or_else(|| rejection(work, "MCP stdio writes exceed admitted bound"))?;
        if first.bytes.len().checked_add(followup).filter(|n| *n <= maximum_bytes).is_none() {
            return Err(rejection(work, "MCP stdio writes exceed admitted bound"));
        }
        let mut combined = ready;
        reserve_write(&mut combined, followup, work)?;
        copy_work(work, request.bytes.len())?;
        combined.bytes.extend_from_slice(&request.bytes);
        drop(request);
        #[allow(unused_mut)] // Native-only capacity admission below.
        let mut custody = native_custody(work)?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(guard) = custody.as_mut() {
            guard.resize_retained(std::mem::size_of::<Self>() as u64).map_err(native_error)?;
            guard.grow(maximum_bytes as u64).map_err(native_error)?;
        }
        // One allocation for all cumulative stdout: library split/advance cannot demand growth because total admitted input never exceeds this capacity.
        let line = if work.is_some() { BytesMut::with_capacity(maximum_bytes) } else { BytesMut::new() };
        Ok((Self {
            initialization, operation, request: Some(combined), failed: false,
            line, codec: JsonRpcMessageCodec::new_with_max_length(maximum_bytes),
            received: 0, maximum_bytes, result: None, custody,
        }, first.bytes, first.custody))
    }

    /// Consumes stdout on the supplied ledger and transfers its writes and custody.
    pub fn push(&mut self, bytes: &[u8], work: NativeWork<'_>) -> Result<(Vec<u8>, NativeCustody)> {
        let result = (|| {
            self.check_work(work)?;
            if self.failed {
                return Err(rejection(work, "MCP stdio exchange already failed"));
            }
            self.consume(bytes, work)
        })();
        if result.is_err() {
            self.failed = true;
            // Drop actual results and pending writes before their LAST guards.
            self.result = None;
            self.request = None;
        }
        result.map(|write| (write.bytes, write.custody))
    }

    fn check_work(&self, work: NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        match (&self.custody, work) {
            (None, None) => {}
            (Some(guard), Some(work)) => {
                checkpoint(Some(work))?;
                if !guard.matches_work(work) {
                    return Err(rejection(Some(work), "MCP stdio original ledger mismatch"));
                }
            }
            _ => return Err(rejection(work, "MCP stdio accounting mode mismatch")),
        }
        checkpoint(work)
    }

    fn consume(&mut self, bytes: &[u8], work: NativeWork<'_>) -> Result<OwnedWrite> {
        let received = self.received.checked_add(bytes.len())
            .filter(|total| *total <= self.maximum_bytes)
            .ok_or_else(|| rejection(work, "MCP stdio stdout exceeds admitted bound"))?;
        let mut writes = OwnedWrite { bytes: Vec::new(), custody: None };
        let mut scanned = self.line.len();
        copy_work(work, bytes.len())?;
        self.line.extend_from_slice(bytes);
        self.received = received;
        loop {
            let mut end = None;
            for (offset, byte) in self.line[scanned..].iter().enumerate() {
                scan_work(work)?;
                if *byte == b'\n' { end = Some(scanned + offset); break; }
            }
            let Some(end) = end else { break };
            let body = &self.line[..end];
            let body = body.strip_suffix(b"\r").unwrap_or(body);
            let body = body.strip_prefix(b"\xef\xbb\xbf").unwrap_or(body);
            let mut frame_custody = frame_admission(body, self.maximum_bytes, work)?;
            let raw_length = validate_raw(body, work, &mut frame_custody)?;
            copy_work(work, raw_length)?;
            let raw = self.codec.decode(&mut self.line)
                .map_err(|error| codec_error(error, work, &mut frame_custody))?
                .ok_or_else(|| rejection(work, "missing MCP stdio message"))?;
            scanned = 0;
            let message: Value = decode_value(raw.get().as_bytes(), work, &mut frame_custody)?;
            drop(raw);
            if message.get("id").is_none() {
                notification(&message, work)?;
                drop(message);
                continue;
            }
            if let Some(request) = self.request.take() {
                let result = rpc_result(message, self.initialization, work)?;
                initialization(&result, work)?;
                drop(result);
                writes = request;
            } else if self.result.is_none() {
                let value = rpc_result(message, self.operation, work)?;
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if let Some(guard) = frame_custody.as_mut() {
                    let retained = value_storage(&value, work)?;
                    guard.substitute(retained).map_err(native_error)?;
                }
                self.result = Some(OwnedValue { value, custody: frame_custody });
            } else {
                return Err(rejection(work, "unexpected MCP stdio response after completion"));
            }
        }
        Ok(writes)
    }

    /// Requires complete newline-terminated responses at process EOF.
    pub fn finish(mut self) -> Result<(Value, NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &self.custody { checkpoint(Some(custody.work()))?; }
        if !self.line.is_empty() {
            return Err(Error::Indeterminate(self.operation));
        }
        let OwnedValue { value, custody } = self.result.take().ok_or(Error::Indeterminate(self.operation))?;
        if self.custody.is_some() && custody.is_none() {
            return Err(rejection(None, "missing MCP result custody"));
        }
        Ok((value, custody))
    }

    /// Returns whether the terminal response was received.
    pub fn is_complete(&self) -> bool { self.result.is_some() }
}

#[derive(Serialize)]
struct Envelope<'a, P> { id: OperationId, jsonrpc: &'static str, method: &'a str, params: P }
#[derive(Serialize)]
struct Empty {}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct InitializeParams { capabilities: Empty, client_info: ClientInfo, protocol_version: &'static str }
#[derive(Serialize)]
struct ClientInfo { name: &'static str, version: &'static str }
#[derive(Serialize)]
struct Initialized { jsonrpc: &'static str, method: &'static str }

#[cfg(test)]
fn line(value: &Value) -> Result<Vec<u8>> {
    Ok(encode_line(value, usize::MAX, None)?.bytes)
}

fn rejection(work: NativeWork<'_>, reason: &'static str) -> Error {
    let _ = work;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() {
        return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
    }
    Error::Invalid(reason.into())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

fn native_custody(work: NativeWork<'_>) -> Result<NativeCustody> {
    let _ = work;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work { return work.retain_owned(0).map(Some).map_err(native_error); }
    Ok(None)
}

fn checkpoint(work: NativeWork<'_>) -> Result<()> {
    let _ = work;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work { work.charge(acyclic_fs::WorkCounters::default()).map_err(native_error)?; }
    Ok(())
}

fn copy_work(work: NativeWork<'_>, bytes: usize) -> Result<()> {
    let _ = (work, bytes);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters { bytes_copied: bytes as u64,
            ..acyclic_fs::WorkCounters::default() }).map_err(native_error)?;
    }
    Ok(())
}

fn scan_work(work: NativeWork<'_>) -> Result<()> {
    let _ = work;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters { source_bytes_read: 1,
            items_examined: 1, ..acyclic_fs::WorkCounters::default() }).map_err(native_error)?;
    }
    Ok(())
}

fn reserve_write(write: &mut OwnedWrite, capacity: usize, work: NativeWork<'_>) -> Result<()> {
    if write.bytes.capacity() >= capacity { return checkpoint(work); }
    let old = write.bytes.capacity();
    let _ = old;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(guard) = write.custody.as_mut() {
        // Reallocation temporarily retains BOTH old and replacement buffers.
        guard.grow(capacity as u64).map_err(native_error)?;
    }
    write.bytes.try_reserve_exact(capacity - write.bytes.len())
        .map_err(|_| rejection(work, "MCP stdio write allocation failed"))?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(guard) = write.custody.as_mut() { guard.release(old as u64).map_err(native_error)?; }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn bounds(maximum: usize) -> crate::JsonBounds {
    crate::JsonBounds { maximum_bytes: maximum as u64,
        maximum_items: maximum as u64, maximum_depth: 256 }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn codec_charge(
    work: &acyclic_fs::NativeMaterializationWork,
    custody: &mut acyclic_fs::NativeAllocation,
    quota: &mut acyclic_fs::NativeAllocation,
    event: crate::JsonCodecCharge,
) -> Result<()> {
    use crate::JsonCodecCharge as Charge;
    match event {
        Charge::Release { bytes } => custody.release(bytes).map_err(native_error),
        Charge::Retain { bytes } => custody.resize_retained(custody.bytes().checked_add(bytes)
            .ok_or_else(|| rejection(Some(work), "MCP codec custody overflows"))?).map_err(native_error),
        Charge::Admission { maximum_owned_bytes, .. } => quota.resize_retained(maximum_owned_bytes).map_err(native_error),
        Charge::CopyAdmission { bytes } => work.admit(acyclic_fs::WorkCounters {
            bytes_copied: bytes, ..acyclic_fs::WorkCounters::default() }).map_err(native_error),
        Charge::AllocationOperation { .. } => work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1, ..acyclic_fs::WorkCounters::default() }).map_err(native_error),
        Charge::Copy { bytes } => work.charge(acyclic_fs::WorkCounters {
            bytes_copied: bytes, ..acyclic_fs::WorkCounters::default() }).map_err(native_error),
        Charge::Read { bytes } => work.charge(acyclic_fs::WorkCounters {
            source_bytes_read: bytes, ..acyclic_fs::WorkCounters::default() }).map_err(native_error),
        Charge::Inspect { items } => work.charge(acyclic_fs::WorkCounters {
            items_examined: items, ..Default::default() }).map_err(native_error),
        Charge::Work { bytes, items } => work.charge(acyclic_fs::WorkCounters {
            bytes_encoded: bytes, items_examined: items, ..acyclic_fs::WorkCounters::default() }).map_err(native_error),
    }
}

fn encode_line<T: Serialize>(value: &T, maximum: usize, work: NativeWork<'_>) -> Result<OwnedWrite> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let mut diagnostic = work.retain_owned(crate::contract::sdk_json_diagnostic_bytes()
            .max(crate::contract::lexical_json_diagnostic_bytes())).map_err(native_error)?;
        let mut custody = work.retain_owned(0).map_err(native_error)?;
        let mut quota = work.retain_owned(0).map_err(native_error)?;
        let encoded = crate::json_bytes_bounded(value, bounds(maximum.saturating_sub(1)),
            None, &mut |event| codec_charge(work, &mut custody, &mut quota, event));
        let bytes = match encoded {
            Ok(bytes) => bytes,
            Err(Error::Invalid(message)) => {
                diagnostic.substitute(message.capacity() as u64).map_err(native_error)?;
                return Err(Error::NativeDiagnostic { code: crate::wire::ErrorCode::Invalid,
                    message, custody: diagnostic });
            }
            Err(error) => return Err(error),
        };
        drop(quota);
        drop(diagnostic);
        let mut write = OwnedWrite { bytes, custody: Some(custody) };
        let capacity = write.bytes.len().checked_add(1)
            .ok_or_else(|| rejection(Some(work), "MCP line length overflows"))?;
        reserve_write(&mut write, capacity, Some(work))?;
        copy_work(Some(work), 1)?;
        write.bytes.push(b'\n');
        return Ok(write);
    }
    let mut bytes = BytesMut::new();
    JsonRpcMessageCodec::new().encode(value, &mut bytes)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    if bytes.len() > maximum { return Err(rejection(work, "MCP stdio writes exceed admitted bound")); }
    Ok(OwnedWrite { bytes: bytes.to_vec(), custody: None })
}

fn frame_admission(body: &[u8], maximum: usize, work: NativeWork<'_>) -> Result<NativeCustody> {
    let _ = (body, maximum);
    #[allow(unused_mut)] // Native-only quota admission below.
    let mut custody = native_custody(work)?;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = work {
        let per_token = (8 * std::mem::size_of::<Value>()
            + std::mem::size_of::<(String, Value)>() + 4) as u64;
        let quota = (body.len() as u64).max(8).checked_mul(per_token)
            .and_then(|n| n.checked_add(crate::contract::sdk_json_diagnostic_bytes()))
            .ok_or_else(|| rejection(Some(work), "MCP frame quota overflows"))?;
        if let Some(guard) = custody.as_mut() { guard.resize_retained(quota).map_err(native_error)?; }
        // Per-byte original cancellation and item admission before any serde.
        let mut diagnostic = work.retain_owned(crate::contract::lexical_json_diagnostic_bytes()).map_err(native_error)?;
        let scanned = crate::contract::admit_json(body, bounds(maximum), &mut |event| match event {
            crate::JsonCodecCharge::Read { bytes } => work.charge(acyclic_fs::WorkCounters {
                source_bytes_read: bytes, ..acyclic_fs::WorkCounters::default() }).map_err(native_error),
            crate::JsonCodecCharge::Work { items, .. } | crate::JsonCodecCharge::Inspect { items } => work.charge(acyclic_fs::WorkCounters {
                items_examined: items, ..acyclic_fs::WorkCounters::default() }).map_err(native_error),
            _ => checkpoint(Some(work)),
        });
        match scanned {
            Err(Error::Invalid(message)) => {
                diagnostic.substitute(message.capacity() as u64).map_err(native_error)?;
                return Err(Error::NativeDiagnostic { code: crate::wire::ErrorCode::Invalid, message, custody: diagnostic });
            }
            Err(error) => return Err(error),
            Ok(()) => {}
        }
    }
    Ok(custody)
}

fn serde_error(source: serde_json::Error, work: NativeWork<'_>, custody: &mut NativeCustody) -> Error {
    let _ = (&work, &custody);
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.is_some() { return Error::NativeDecode { code: crate::wire::ErrorCode::Invalid, source, custody: custody.take() }; }
    Error::Invalid(format!("invalid MCP stdio JSON: {source}"))
}

fn validate_raw(body: &[u8], work: NativeWork<'_>, custody: &mut NativeCustody) -> Result<usize> {
    checkpoint(work)?;
    serde_json::from_slice::<&RawValue>(body)
        .map(|raw| raw.get().len()).map_err(|source| serde_error(source, work, custody))
}

fn codec_error(
    source: rmcp::transport::async_rw::JsonRpcMessageCodecError,
    work: NativeWork<'_>, custody: &mut NativeCustody,
) -> Error {
    match source {
        rmcp::transport::async_rw::JsonRpcMessageCodecError::Serde(source) => serde_error(source, work, custody),
        other => {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if work.is_some() { return rejection(work, "invalid MCP stdio library frame"); }
            Error::Invalid(format!("invalid MCP stdio message: {other}"))
        }
    }
}

fn decode_value(body: &[u8], work: NativeWork<'_>, custody: &mut NativeCustody) -> Result<Value> {
    checkpoint(work)?;
    let mut decoder = serde_json::Deserializer::from_slice(body);
    decoder.disable_recursion_limit();
    let value = Value::deserialize(&mut decoder).map_err(|source| serde_error(source, work, custody))?;
    decoder.end().map_err(|source| serde_error(source, work, custody))?;
    drop(decoder);
    Ok(value)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn value_storage(value: &Value, work: NativeWork<'_>) -> Result<u64> {
    checkpoint(work)?;
    let add = |a: u64, b: u64| a.checked_add(b)
        .ok_or_else(|| rejection(work, "MCP result storage overflows"));
    match value {
        Value::String(text) => Ok(text.capacity() as u64),
        Value::Array(values) => {
            let mut bytes = (values.capacity() * std::mem::size_of::<Value>()) as u64;
            for value in values { scan_work(work)?; bytes = add(bytes, value_storage(value, work)?)?; }
            Ok(bytes)
        }
        Value::Object(values) => {
            let mut bytes = (values.len() * std::mem::size_of::<(String, Value)>()) as u64;
            for (key, value) in values {
                scan_work(work)?;
                bytes = add(add(bytes, key.capacity() as u64)?, value_storage(value, work)?)?;
            }
            Ok(bytes)
        }
        _ => Ok(0),
    }
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
    fn exact_descriptor_and_retained_response_have_one_protocol_boundary() {
        let (initialization, operation) = ids();
        let request = McpStdioRequest {
            initialization,
            operation,
            method: McpStdioMethod::CallTool,
            params: json!({"name":"echo","arguments":{}}),
            maximum_bytes: 4096,
        };
        request.validate().unwrap();
        let expected = json!({"content":[{"type":"text","text":"héllo"}],
            "structuredContent":{"sequence":u64::MAX}});
        let mut transcript = initialized(initialization);
        transcript.extend(
            line(&json!({"jsonrpc":"2.0","id":operation.to_string(),"result":expected})).unwrap(),
        );
        assert_eq!(request.decode_response(&transcript).unwrap(), expected);
        assert!(
            request
                .decode_response(transcript.get(..transcript.len() - 1).unwrap())
                .is_err()
        );
        for params in [
            json!({}),
            json!([]),
            json!({"name":"echo","arguments":[]}),
            json!({"name":"echo","task":{}}),
            json!({"name":"echo","request_state":"retry"}),
            json!({"name":"echo","requestState":"retry"}),
        ] {
            assert!(
                McpStdioRequest {
                    params: params.clone(),
                    ..request
                }
                .validate()
                .is_err(),
                "accepted {params}"
            );
        }
        assert!(
            McpStdioRequest {
                operation: initialization,
                ..request.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            McpStdioRequest {
                maximum_bytes: 0,
                ..request.clone()
            }
            .validate()
            .is_err()
        );
        assert!(
            McpStdioRequest {
                method: McpStdioMethod::ListTools,
                params: json!({"cursor":1}),
                ..request
            }
            .validate()
            .is_err()
        );
        let mut malformed = initialized(initialization);
        malformed.extend(line(&json!({"jsonrpc":"2.0","id":operation.to_string(),"result":{"structuredContent":{}}})).unwrap());
        assert!(request.decode_response(&malformed).is_err());
        let mut encoded = serde_json::to_value(&request).unwrap();
        encoded["method"] = json!("sampling/createMessage");
        assert!(serde_json::from_value::<McpStdioRequest>(encoded).is_err());
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
            let (mut codec, first, _) = McpStdioExchange::new(
                init,
                op,
                "tools/call",
                &json!({"name":"echo","arguments":{}}),
                4096,
                None,
            )
            .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&first).unwrap()["method"],
                "initialize"
            );
            let mut writes = Vec::new();
            for chunk in transcript.chunks(width) {
                writes.extend(codec.push(chunk, None).unwrap().0);
            }
            let messages: Vec<Value> = writes
                .split(|byte| *byte == b'\n')
                .filter(|part| !part.is_empty())
                .map(|part| serde_json::from_slice(part).unwrap())
                .collect();
            assert_eq!(messages.len(), 2);
            assert_eq!(messages[0]["method"], "notifications/initialized");
            assert_eq!(messages[1]["id"], op.to_string());
            assert_eq!(codec.finish().unwrap().0, expected);
        }
    }

    #[test]
    fn malformed_foreign_callback_and_truncated_inputs_cannot_replay() {
        let (init, op) = ids();
        for bytes in [b"not json\n".to_vec(), line(&json!({"jsonrpc":"2.0","id":op.to_string(),"result":{}})).unwrap(),
            line(&json!({"jsonrpc":"2.0","id":init.to_string(),"method":"sampling/createMessage","params":{}})).unwrap()] {
            let (mut codec,_,_) = McpStdioExchange::new(init,op,"tools/list",&json!({}),4096,None).unwrap();
            assert!(codec.push(&bytes, None).is_err());
            assert!(codec.push(&initialized(init), None).is_err());
            assert!(codec.finish().is_err());
        }
        let (mut codec, _, _) =
            McpStdioExchange::new(init, op, "tools/list", &json!({}), 4096, None).unwrap();
        assert!(codec.push(&vec![b' '; 4097], None).is_err());
        let (mut codec, _, _) =
            McpStdioExchange::new(init, op, "tools/list", &json!({}), 4096, None).unwrap();
        let mut truncated = initialized(init);
        truncated.pop();
        assert!(codec.push(&truncated, None).unwrap().0.is_empty());
        assert!(matches!(codec.finish(), Err(Error::Indeterminate(_))));
        assert!(McpStdioExchange::new(init, init, "tools/list", &json!({}), 4096, None).is_err());
    }

    #[test]
    fn admitted_deep_json_and_crlf_bom_survive_stream_cuts() {
        let (init, op) = ids();
        let nested = format!("{}0{}", "[".repeat(192), "]".repeat(192));
        let expected: Value = crate::contract::json_from_slice(nested.as_bytes()).unwrap();
        let mut transcript = initialized(init);
        transcript.extend_from_slice(
            format!("\u{feff}{{\"jsonrpc\":\"2.0\",\"id\":\"{op}\",\"result\":{nested}}}\r\n")
                .as_bytes(),
        );
        for width in [1, 2, 3, 17, transcript.len()] {
            let (mut codec, _, _) =
                McpStdioExchange::new(init, op, "tools/list", &json!({}), 4096, None).unwrap();
            for chunk in transcript.chunks(width) {
                codec.push(chunk, None).unwrap().0;
            }
            assert_eq!(codec.finish().unwrap().0, expected);
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn malformed_private_stdout_never_reaches_library_debug_log() {
        use std::{
            io::Write,
            sync::{Arc, Mutex},
        };

        #[derive(Clone)]
        struct Capture(Arc<Mutex<Vec<u8>>>);
        impl Write for Capture {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .map_err(|_| std::io::Error::other("log capture lock poisoned"))?
                    .extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let logs = Capture(Arc::default());
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let private = b"PRIVATE_STDOUT_FIXTURE invalid json\n";
            let mut unguarded = JsonRpcMessageCodec::<Box<RawValue>>::new();
            assert!(unguarded.decode(&mut BytesMut::from(&private[..])).is_err());
            assert!(
                String::from_utf8_lossy(&logs.0.lock().unwrap()).contains("PRIVATE_STDOUT_FIXTURE")
            );
            logs.0.lock().unwrap().clear();
            let (init, op) = ids();
            let (mut guarded, _, _) =
                McpStdioExchange::new(init, op, "tools/list", &json!({}), 4096, None).unwrap();
            assert!(guarded.push(private, None).is_err());
            assert!(guarded.push(&initialized(init), None).is_err());
            assert!(logs.0.lock().unwrap().is_empty());
        });
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn finite_budget() -> acyclic_fs::WorkBudget {
        let mut encoded = serde_json::to_value(acyclic_fs::WorkBudget::UNBOUNDED).unwrap();
        for value in encoded.as_object_mut().unwrap().values_mut() {
            *value = Value::from(100_000_000u64);
        }
        serde_json::from_value(encoded).unwrap()
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    fn native_work() -> acyclic_fs::NativeMaterializationWork {
        acyclic_fs::NativeMaterializationWork::new(finite_budget(), &acyclic_fs::CancellationToken::new()).unwrap()
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn original_native_all_seven_methods_preserve_protocol_and_result_custody() {
        let (initialization, operation) = ids();
        for (method, params, expected) in [
            (McpStdioMethod::ListTools, json!({}), json!({"tools":[]})),
            (McpStdioMethod::CallTool, json!({"name":"echo","arguments":{"n":u64::MAX}}),
                json!({"content":[{"type":"text","text":"héllo"}],"structuredContent":null})),
            (McpStdioMethod::ListResources, json!({}), json!({"resources":[]})),
            (McpStdioMethod::ListResourceTemplates, json!({}), json!({"resourceTemplates":[]})),
            (McpStdioMethod::ReadResource, json!({"uri":"fixture://binary"}),
                json!({"contents":[{"uri":"fixture://binary","blob":"AAE="}]})),
            (McpStdioMethod::ListPrompts, json!({}), json!({"prompts":[]})),
            (McpStdioMethod::GetPrompt, json!({"name":"fixture","arguments":{}}), json!({"messages":[]})),
        ] {
            let request = McpStdioRequest { initialization, operation, method, params, maximum_bytes: 4096 };
            let work = native_work();
            let baseline = work.live_allocation_bytes();
            let probe = work.retain_owned(0).unwrap();
            let (mut ordinary, ordinary_first, _) = request.exchange(None).unwrap();
            let (mut native, first, first_guard) = request.exchange(Some(&work)).unwrap();
            assert_eq!(first, ordinary_first);
            assert!(first_guard.as_ref().unwrap().shares_ledger(&probe));
            assert!(first_guard.as_ref().unwrap().bytes() >= first.capacity() as u64);
            let mut transcript = initialized(initialization);
            transcript.extend_from_slice(b"\xef\xbb\xbf");
            let mut result = line(&json!({"jsonrpc":"2.0","id":operation.to_string(),"result":expected})).unwrap();
            result.pop();
            result.extend_from_slice(b"\r\n");
            transcript.extend_from_slice(&result);
            for byte in &transcript {
                let chunk = std::slice::from_ref(byte);
                let ordinary_write = ordinary.push(chunk, None).unwrap().0;
                let (write, guard) = native.push(chunk, Some(&work)).unwrap();
                assert_eq!(write, ordinary_write);
                if !write.is_empty() {
                    let guard = guard.as_ref().unwrap();
                    assert!(guard.shares_ledger(&probe));
                    assert!(guard.bytes() >= write.capacity() as u64);
                }
                drop(write);
                drop(guard);
            }
            assert_eq!(ordinary.finish().unwrap().0, expected);
            drop(first);
            drop(first_guard);
            let (value, guard) = native.finish().unwrap();
            assert_eq!(value, expected);
            assert!(guard.as_ref().unwrap().shares_ledger(&probe));
            assert_eq!(guard.as_ref().unwrap().bytes(), value_storage(&value, None).unwrap());
            assert_eq!(work.live_allocation_bytes(), baseline + guard.as_ref().unwrap().bytes());
            drop(value);
            drop(guard);
            assert_eq!(work.live_allocation_bytes(), baseline);
            work.receipt().verify(finite_budget()).unwrap();
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn original_native_partial_line_max_denial_and_failed_state_precede_capture() {
        let (initialization, operation) = ids();
        let work = native_work();
        let request = McpStdioRequest { initialization, operation,
            method: McpStdioMethod::ListTools, params: json!({}), maximum_bytes: 4096 };
        let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
        let (empty, empty_guard) = exchange.push(b"{", Some(&work)).unwrap();
        assert!(empty.is_empty());
        drop(empty);
        drop(empty_guard);
        let copied = work.receipt().bytes_copied;
        assert!(exchange.push(&vec![b' '; 4096], Some(&work)).is_err());
        assert_eq!(work.receipt().bytes_copied, copied);
        assert!(exchange.push(&initialized(initialization), Some(&work)).is_err());
        assert!(exchange.finish().is_err());
        drop(first);
        drop(guard);
        assert_eq!(work.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn original_native_cancellation_and_foreign_ledger_cannot_resume() {
        let (initialization, operation) = ids();
        let request = McpStdioRequest { initialization, operation,
            method: McpStdioMethod::ListTools, params: json!({}), maximum_bytes: 4096 };
        let work = native_work();
        let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
        let copied = work.receipt().bytes_copied;
        work.cancellation().cancel();
        let error = exchange.push(&initialized(initialization), Some(&work)).err().unwrap();
        assert!(matches!(error, Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, .. }));
        assert_eq!(work.receipt().bytes_copied, copied);
        assert!(exchange.line.is_empty());
        drop(first);
        drop(guard);
        drop(exchange);
        assert_eq!(work.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());

        let work = native_work();
        let foreign = native_work();
        let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
        assert!(exchange.push(&initialized(initialization), Some(&foreign)).is_err());
        assert!(exchange.push(&initialized(initialization), Some(&work)).is_err());
        drop(first);
        drop(guard);
        drop(exchange);
        let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
        assert!(exchange.push(&initialized(initialization), None).is_err());
        drop(first);
        drop(guard);
        drop(exchange);
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn original_native_frame_quota_precedes_trusted_parse_and_preserves_owned_cause() {
        let (initialization, operation) = ids();
        let request = McpStdioRequest { initialization, operation,
            method: McpStdioMethod::ListTools, params: json!({}), maximum_bytes: 4096 };
        let sizing = native_work();
        let (exchange, first, guard) = request.exchange(Some(&sizing)).unwrap();
        let preparation_peak = sizing.receipt().peak_allocation_bytes;
        drop(exchange);
        drop(first);
        drop(guard);
        let mut budget = finite_budget();
        budget.peak_allocation_bytes = preparation_peak + 1024;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &acyclic_fs::CancellationToken::new()).unwrap();
        let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
        let error = exchange.push(&initialized(initialization), Some(&work)).err().unwrap();
        assert!(matches!(error, Error::NativeWork { .. }));
        assert!(exchange.result.is_none());
        drop(exchange);
        drop(first);
        drop(guard);
        work.receipt().verify(budget).unwrap();

        let work = native_work();
        let (mut exchange, first, guard) = request.exchange(Some(&work)).unwrap();
        let error = exchange.push(b"{\"jsonrpc\":,\"id\":1}\n", Some(&work)).err().unwrap();
        match error {
            Error::NativeDecode { source, custody: Some(custody), .. } => {
                assert!(source.is_syntax());
                assert!(custody.shares_ledger(guard.as_ref().unwrap()));
                work.cancellation().cancel();
                drop(source);
                drop(custody);
            }
            other => panic!("unexpected actual decode cause: {other}"),
        }
        drop(first);
        drop(guard);
        drop(exchange);
        assert_eq!(work.live_allocation_bytes(), acyclic_fs::NativeMaterializationWork::accounting_state_bytes());
    }

}
