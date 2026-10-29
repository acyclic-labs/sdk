use super::*;

/// Original model call identity retained alongside a tool's durable request.
#[derive(Clone, Copy, Debug)]
pub struct ToolSettlementQuery<'a> {
    /// Original model turn, independent of the tool's effect operation.
    pub turn: OperationId,
    /// Original response step.
    pub step: u32,
    /// Provider call identity within that step.
    pub call_id: &'a str,
    /// Effect identity issued by `ToolInvocation::for_model_call`.
    pub effect: OperationId,
}

/// Terminal observation authenticated by the original journal's committed cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ToolSettlement {
    /// Complete authenticated prefix; a later unindexed tail is not replayed.
    pub cut: u64,
    /// One-based sequence of the terminal observation within that prefix.
    pub sequence: u64,
    /// Which original terminal observation was committed.
    pub outcome: ToolTerminalOutcome,
}

/// A committed terminal tool observation. Absence remains unresolved.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolTerminalOutcome {
    /// The tool's result and model projection were committed.
    Completed,
    /// The tool's original typed failure was committed.
    Failed,
}

impl ToolSettlementQuery<'_> {
    pub(crate) fn validate(self, io: Option<&PreparationIo>) -> Result<()> {
        with_history_io(io, |work| {
            ToolInvocation::validate_identity(self.call_id, None, work)
        })?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let native = io.and_then(PreparationIo::original_native_work);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let native = None;
        if native.is_none()
            && let Some(io) = io
        {
            let bytes_hashed = (b"harness:tool-call:v2".len() as u64)
                .checked_add(20)
                .and_then(|bytes| bytes.checked_add(self.call_id.len() as u64))
                .ok_or_else(|| {
                    stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "tool call hash length overflow",
                    )
                })?;
            #[cfg(feature = "filesystem")]
            io.charge(acyclic_fs::WorkCounters {
                bytes_hashed,
                items_examined: 4,
                ..Default::default()
            })?;
            #[cfg(not(feature = "filesystem"))]
            let _ = bytes_hashed;
        }
        if ToolInvocation::model_call_operation(self.turn, self.step, self.call_id, native)?
            != self.effect
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "tool settlement belongs to another model call",
            ));
        }
        Ok(())
    }
}

pub(crate) fn tool_settlement_key(
    step: u32,
    call_id: &str,
    outcome: ToolTerminalOutcome,
    io: Option<&PreparationIo>,
) -> Result<String> {
    let kind = match outcome {
        ToolTerminalOutcome::Completed => "completed",
        ToolTerminalOutcome::Failed => "failed",
    };
    with_history_io(io, |work| {
        preparation_format(format_args!("tool:{step}:{call_id}:{kind}"), work)
    })
}
