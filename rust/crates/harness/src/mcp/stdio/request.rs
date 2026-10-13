use super::*;

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
    pub fn exchange(
        &self,
        work: NativeWork<'_>,
    ) -> Result<(McpStdioExchange, Vec<u8>, NativeCustody)> {
        super::services::validate_params(self.method.as_str(), &self.params, work)?;
        if self.initialization == self.operation || self.maximum_bytes == 0 {
            return Err(error(
                work,
                ErrorCode::Invalid,
                "invalid MCP stdio exchange contract",
            ));
        }
        let maximum_bytes = self.maximum_bytes as usize;
        let params: &serde_json::value::RawValue = serde_json::from_str(concat!(
            "{\"capabilities\":{},\"clientInfo\":{\"name\":\"acyclic-harness\",\"version\":\"",
            env!("CARGO_PKG_VERSION"),
            "\"},\"protocolVersion\":\"2025-11-25\"}",
        ))
        .map_err(|_| error(work, ErrorCode::Invalid, "invalid MCP client identity"))?;
        let first = encode_line(
            &Envelope {
                id: self.initialization,
                jsonrpc: "2.0",
                method: "initialize",
                params,
            },
            maximum_bytes,
            work,
        )?;
        let initialized: &serde_json::value::RawValue =
            serde_json::from_str(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
                .map_err(|_| error(work, ErrorCode::Invalid, "invalid MCP readiness message"))?;
        let ready = encode_line(initialized, maximum_bytes, work)?;
        let request = encode_line(
            &Envelope {
                id: self.operation,
                jsonrpc: "2.0",
                method: self.method.as_str(),
                params: &self.params,
            },
            maximum_bytes,
            work,
        )?;
        let followup = ready.0.len().checked_add(request.0.len()).ok_or_else(|| {
            error(
                work,
                ErrorCode::Invalid,
                "MCP stdio writes exceed admitted bound",
            )
        })?;
        if first
            .0
            .len()
            .checked_add(followup)
            .filter(|n| *n <= maximum_bytes)
            .is_none()
        {
            return Err(error(
                work,
                ErrorCode::Invalid,
                "MCP stdio writes exceed admitted bound",
            ));
        }
        let mut combined = ready;
        reserve_write(&mut combined, followup, work)?;
        charge(work, 0, 0, request.0.len() as u64)?;
        combined.0.extend_from_slice(&request.0);
        drop(request);
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let custody = if let Some(work) = work {
            // The fixed exchange header plus the u32 transcript bound fit u64.
            Some(work.claim(
                std::mem::size_of::<McpStdioExchange>() as u64 + maximum_bytes as u64,
                1,
                acyclic_fs::WorkCounters::UNCHARGED,
            )?)
        } else {
            None
        };
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let custody = None;
        // Cumulative stdout fits the reserved buffer.
        let line = if work.is_some() {
            BytesMut::with_capacity(maximum_bytes)
        } else {
            BytesMut::new()
        };
        Ok((
            McpStdioExchange {
                initialization: self.initialization,
                operation: self.operation,
                request: Some(combined),
                failed: false,
                line,
                received: 0,
                maximum_bytes,
                result: None,
                custody,
            },
            first.0,
            first.1,
        ))
    }

    /// Validates a retained complete transcript without performing its writes.
    pub fn decode_response(&self, stdout: &[u8]) -> Result<Value> {
        let (mut exchange, _, _) = self.exchange(None)?;
        exchange.push(stdout, None)?;
        let (value, _) = exchange.finish()?;
        super::services::validate_result(self.method.as_str(), value, None)
    }
}
