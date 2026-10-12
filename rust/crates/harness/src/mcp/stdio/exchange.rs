use super::*;

/// Transient parser state for one initialized, one-shot stdio request.
pub struct McpStdioExchange {
    pub(super) initialization: OperationId,
    pub(super) operation: OperationId,
    pub(super) request: Option<(Vec<u8>, NativeCustody)>,
    pub(super) failed: bool,
    pub(super) line: BytesMut,

    pub(super) received: usize,
    pub(super) maximum_bytes: usize,
    pub(super) result: Option<(Value, NativeCustody)>,
    // LAST: all parser/control storage is physically dropped before custody.
    pub(super) custody: NativeCustody,
}

impl McpStdioExchange {
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
        result
    }

    fn check_work(&self, work: NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        match (&self.custody, work) {
            (None, None) => {}
            (Some(guard), Some(work)) => {
                checkpoint(Some(work))?;
                if !guard.work().shares_allocation_context(work) {
                    return Err(rejection(Some(work), "MCP stdio original ledger mismatch"));
                }
            }
            _ => return Err(rejection(work, "MCP stdio accounting mode mismatch")),
        }
        checkpoint(work)
    }

    fn consume(&mut self, bytes: &[u8], work: NativeWork<'_>) -> Result<(Vec<u8>, NativeCustody)> {
        let received = self
            .received
            .checked_add(bytes.len())
            .filter(|total| *total <= self.maximum_bytes)
            .ok_or_else(|| rejection(work, "MCP stdio stdout exceeds admitted bound"))?;
        let mut writes = (Vec::new(), None);
        let mut scanned = self.line.len();
        copy_work(work, bytes.len())?;
        self.line.extend_from_slice(bytes);
        self.received = received;
        loop {
            let mut end = None;
            for (offset, byte) in self.line[scanned..].iter().enumerate() {
                scan_work(work)?;
                if *byte == b'\n' {
                    end = Some(scanned + offset);
                    break;
                }
            }
            let Some(end) = end else { break };
            let body = &self.line[..end];
            let body = body.strip_suffix(b"\r").unwrap_or(body);
            let body = body.strip_prefix(b"\xef\xbb\xbf").unwrap_or(body);
            let mut frame_custody = frame_admission(body, self.maximum_bytes, work)?;
            let message = decode_value(body, work, &mut frame_custody)?;
            self.line.advance(end + 1);
            scanned = 0;
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
                    guard
                        .release(guard.bytes().checked_sub(retained).ok_or_else(|| {
                            acyclic_fs::NativeTreeMaterializationError::from(
                                acyclic_fs::WorkError::Overflow,
                            )
                        })?)
                        .map_err(native_error)?;
                }
                self.result = Some((value, frame_custody));
            } else {
                return Err(rejection(
                    work,
                    "unexpected MCP stdio response after completion",
                ));
            }
        }
        Ok(writes)
    }

    /// Requires complete newline-terminated responses at process EOF.
    pub fn finish(mut self) -> Result<(Value, NativeCustody)> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(custody) = &self.custody {
            checkpoint(Some(custody.work()))?;
        }
        if !self.line.is_empty() {
            return Err(Error::Indeterminate(self.operation));
        }
        let (value, custody) = self
            .result
            .take()
            .ok_or(Error::Indeterminate(self.operation))?;
        if self.custody.is_some() && custody.is_none() {
            return Err(rejection(None, "missing MCP result custody"));
        }
        Ok((value, custody))
    }

    /// Returns whether the terminal response was received.
    pub fn is_complete(&self) -> bool {
        self.result.is_some()
    }
}
