//! Original caller-owned invocation receipts, without a parallel call fold.

use super::{Batch, Row, Table, batch, digest, export_error};
use crate::{Result, agent::AgentPath};
use acyclic_actors::call::{CallPath, Journal, Outcome};
use acyclic_auth::addressing::AccountId;
use acyclic_stream::{Record, StreamPath};

/// Authority supplied by the original authorized call reader.
/// A scope may own calls without containing an agent.
pub struct Source<'a> {
    /// Original scope, independent of agent identity.
    pub scope: &'a StreamPath,
    /// Caller whose own subtree contains `calls/<id>`.
    pub caller: &'a StreamPath,
    /// Original agent owner, only when the caller is an agent.
    pub agent: Option<&'a AgentPath>,
    /// Original stream account, never a provider display name.
    pub account: &'a AccountId,
}

/// Projects the borrowed receipt of the original `call::Journal` into outcomes.
///
/// Supply its original terminal record. Source position/commit/path are checked
/// against that receipt. The caller supplies already-authorized source evidence:
/// this function cannot certify fabricated source records or a forged account.
/// It neither refolds a journal nor interprets opaque result bytes as model
/// content, read coverage or physical work. Cancellation disposition remains
/// the original reader's: cancellation after acceptance is indeterminate.
///
/// Request origin is unknown here;
/// its recorded sequence/commit must not be mislabeled as an original origin.
/// `logical_value_bytes` measures this terminal record; it does not measure
/// callee output or physical storage.
/// Execution operation IDs, agent ownership and timing are never derived from
/// the call path. The projection copies metadata only, with no payload hash,
/// buffer, read, wait, publication or state that grows with history or chunks.
pub fn project(
    source: Source<'_>,
    journal: &Journal,
    call: &CallPath,
    selected: &StreamPath,
    record: &Record,
) -> Result<Batch> {
    let refuse = |reason| {
        export_error(format_args!(
            "call receipt {reason}; recovery: supply the original call::Journal receipt and its authorized terminal record"
        ))
    };
    let receipt = journal
        .receipt_ref(call.log())
        .ok_or_else(|| refuse("has not settled"))?;
    let call_id = receipt
        .log
        .as_str()
        .strip_prefix(source.caller.as_str())
        .and_then(|suffix| suffix.strip_prefix("/calls/"));
    if call_id.is_none_or(|id| id.is_empty() || id.contains('/'))
        || source
            .agent
            .is_some_and(|agent| agent.as_str() != source.caller.as_str())
    {
        return Err(refuse("has a foreign caller"));
    }
    if selected != receipt.log
        || record.sequence != receipt.terminal.sequence
        || record.commit_id != receipt.terminal.commit_id
    {
        return Err(refuse("differs from its original terminal"));
    }
    let mut row = Row::stream(
        source.agent.map(AgentPath::as_str),
        source.account,
        selected,
        record,
        "call_receipt",
    );
    row.scope_path = Some(source.scope.as_str().into());
    row.caller = Some(source.caller.as_str().into());
    row.call_log = Some(receipt.log.as_str().into());
    row.outcome = Some(
        match receipt.outcome {
            Outcome::Ok(_) => "ok",
            Outcome::Error(_) => "error",
            Outcome::Cancelled(_) => "cancelled",
            Outcome::Indeterminate(_) => "indeterminate",
        }
        .into(),
    );
    row.request_sequence = receipt.request.as_ref().map(|request| request.sequence);
    row.request_commit_id = receipt
        .request
        .as_ref()
        .map(|request| digest(*request.commit_id.as_bytes()));
    row.terminal_sequence = Some(receipt.terminal.sequence);
    row.logical_value_bytes = Some(record.value.len() as u64);
    row.coverage = Some("original_call_terminal".into());
    batch(Table::Outcomes, &[row])
}

#[cfg(test)]
mod tests;
