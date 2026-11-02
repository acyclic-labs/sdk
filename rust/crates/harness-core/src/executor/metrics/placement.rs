//! Logical placement from an authorized original settled Machine binding.
//!
//! This is an offline projection, not discovery: provider authority is opaque
//! and certifies neither physical topology nor resource measurements.

use super::{Batch, Row, Table, batch, export_error};
use crate::{Result, agent::AgentPath, machines::MachineBinding};
use acyclic_auth::addressing::AccountId;
use acyclic_stream::{Record, StreamPath};

/// Projects the original settled binding at `agent.machine(target)` into agents.
///
/// Supply the already-authorized record from the fixed settled position
/// (`from=1, limit=1`), not the preceding provisioning plan. Decode and source
/// validation happen once here. The selected path must belong to `agent` and
/// match the original target; a fork's original envelope remains unchanged.
/// The caller retains source-account authority, not a display-name guess.
///
/// `machine` and `provider` contain their complete typed JSON references;
/// `forked_from` is the original logical parent Machine ID, not a physical host.
/// Physical columns remain null. Commit time is the original replicated stream
/// time, not execution start. This pure function has no wait, I/O, publication,
/// lifecycle action or state that grows with history or agent count. Work and
/// retained memory scale only with this one binding and its source paths.
pub fn project(
    agent: &AgentPath,
    account: &AccountId,
    selected: &StreamPath,
    record: &Record,
) -> Result<Batch> {
    let refuse = |reason: &str| {
        export_error(format_args!(
            "Machine placement {reason}; recovery: read the original settled binding at agent.machine(target), from=1, limit=1"
        ))
    };
    let target = selected
        .as_str()
        .strip_prefix(agent.as_str())
        .and_then(|suffix| suffix.strip_prefix("/machines/"))
        .filter(|target| !target.is_empty() && !target.contains('/'))
        .ok_or_else(|| refuse("has a foreign source or target"))?;
    let binding = MachineBinding::decode(target, record)?;
    let mut row = Row::stream(Some(agent.as_str()), account, selected, record, "placement");
    row.binding_version = Some(binding.version);
    row.provider = Some(
        serde_json::to_string(binding.machine.as_resource().provider()).map_err(export_error)?,
    );
    row.machine = Some(serde_json::to_string(&binding.machine).map_err(export_error)?);
    row.target = Some(binding.target);
    row.forked_from = binding.forked_from.map(|id| id.to_string());
    row.coverage = Some("logical_provider_binding".into());
    batch(Table::Agents, &[row])
}

#[cfg(test)]
mod tests;
