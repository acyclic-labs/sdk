//! Immutable settled observations under the original operation's Streams subtree.
//!
//! The physical fork owner stages observations with [`record`]. It includes
//! [`publication`] in its EXISTING terminal commit, alongside the operation's
//! successful outcome. This module never commits, dispatches or watches a Machine.
//! The source prefix is shared by Streams; settlement does not recopy its bytes.
use crate::{MachineObservation, OperationId, ProviderError};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitId, CommitMutation, ReadRequest, StreamPath,
    StreamProvider, StreamProviderExt as _,
};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::{
    num::{NonZeroU32, NonZeroU64},
    sync::Arc,
};

#[derive(Serialize, Deserialize)]
enum Version {
    #[serde(rename = "1")]
    V1,
}

#[derive(Serialize, Deserialize)]
struct Observation<T> {
    version: Version,
    machine: T,
}

/// Encodes one ORIGINAL settled observation, in child-index order. Later
/// inspections of that Machine must never replace this retained result.
pub fn record(machine: &MachineObservation) -> Result<AppendRecord, ProviderError> {
    serde_json::to_vec(&Observation {
        version: Version::V1,
        machine,
    })
    .map(AppendRecord::from)
    .map_err(encoding)
}

fn path(root: &StreamPath, operation: OperationId) -> Result<StreamPath, ProviderError> {
    StreamPath::new(format!("{root}/operations/{operation}/results")).map_err(storage)
}

/// Joins a staged prefix to the caller's EXISTING terminal publication. Add both
/// condition and the mutation to that commit; do not publish them separately.
/// Its operation record must retain `count`; that record's Streams `commit_id`
/// identifies this same settlement, including after a lost acknowledgment.
///
/// `count` is the exact original preparation cut. Streams shares that immutable
/// prefix even if preparation later appends. No source head reread or checksum
/// is needed. Staging itself, its admission and its funded custody belong to the
/// original physical operation owner.
pub fn publication(
    root: &StreamPath,
    operation: OperationId,
    source: StreamPath,
    count: NonZeroU64,
) -> Result<(CommitCondition, CommitMutation), ProviderError> {
    let destination = path(root, operation)?;
    if source == destination {
        return Err(invalid("result preparation cannot replace its publication"));
    }
    Ok((
        CommitCondition::Absent {
            path: destination.clone(),
        },
        CommitMutation::Fork {
            source,
            destination,
            at_tail: count.get(),
            records: Vec::new(),
            exclude: Vec::new(),
        },
    ))
}

/// Reader pinned once to the terminal operation's actual publication. It owns
/// no worker, timer, registry or lifecycle interest. Dropping it does not cancel
/// the operation. Reopen using the SAME retained settlement after reply loss.
pub struct SettledResults {
    streams: Arc<dyn StreamProvider>,
    path: StreamPath,
    operation: OperationId,
    settlement: CommitId,
    count: u64,
}

/// One visible page. Metadata is constant per page; payload and decode work are
/// proportional only to returned observations. `next` is an exact child index.
pub struct ResultPage {
    pub operation: OperationId,
    pub settlement: CommitId,
    pub from: u64,
    pub machines: Vec<MachineObservation>,
    pub next: Option<u64>,
}

impl SettledResults {
    /// The server supplies the SAME authenticated Streams/root and the count
    /// and publication from the original terminal record. These arguments are
    /// not an admission API and must never be accepted as guest authority.
    /// One head verifies the fork publication, not the latest Machine state.
    pub async fn open(
        streams: Arc<dyn StreamProvider>,
        root: &StreamPath,
        operation: OperationId,
        settlement: CommitId,
        count: u64,
    ) -> Result<Self, ProviderError> {
        let path = path(root, operation)?;
        let head = streams.head(path.clone()).await.map_err(storage)?;
        let origin = head
            .origin
            .ok_or_else(|| invalid("result publication has no original fork cut"))?;
        if count == 0
            || origin.commit_id != settlement
            || origin.forked_at != count
            || head.tail < count
        {
            return Err(invalid(
                "result publication differs from terminal operation",
            ));
        }
        Ok(Self {
            streams,
            path,
            operation,
            settlement,
            count,
        })
    }

    /// Reads only the requested original interval. There are no head reads,
    /// resource inspections or completion waits per page. A lost/short source
    /// is a typed error, never an empty success or a readiness retry.
    pub async fn page(&self, from: u64, credit: NonZeroU32) -> Result<ResultPage, ProviderError> {
        let remaining = self
            .count
            .checked_sub(from)
            .ok_or_else(|| invalid("result cursor exceeds its original extent"))?;
        let limit = u32::try_from(remaining.min(u64::from(credit.get())))
            .map_err(|_| invalid("result page credit cannot be represented"))?;
        let end = from + u64::from(limit);
        let machines = if limit == 0 {
            Vec::new()
        } else {
            let records = self
                .streams
                .read(ReadRequest {
                    path: self.path.clone(),
                    from,
                    limit,
                    at_least: Default::default(),
                    key: None,
                })
                .await
                .map_err(storage)?;
            observations(records, from, limit).await?
        };
        Ok(ResultPage {
            operation: self.operation,
            settlement: self.settlement,
            from,
            machines,
            next: (end < self.count).then_some(end),
        })
    }
}

async fn observations(
    mut records: acyclic_stream::RecordStream,
    from: u64,
    limit: u32,
) -> Result<Vec<MachineObservation>, ProviderError> {
    let mut machines = Vec::with_capacity(
        usize::try_from(limit).map_err(|_| invalid("result page credit exceeds address space"))?,
    );
    // Do not await an extra item/EOF after consuming granted credit.
    for index in 0..limit {
        let record = records
            .next()
            .await
            .ok_or_else(|| invalid("original result prefix ended before its admitted extent"))?
            .map_err(storage)?;
        if record.sequence != from + u64::from(index) {
            return Err(invalid("original result sequence was substituted"));
        }
        let value: Observation<MachineObservation> =
            serde_json::from_slice(record.value.as_ref()).map_err(encoding)?;
        machines.push(value.machine);
    }
    Ok(machines)
}

fn invalid(message: &str) -> ProviderError {
    ProviderError::Rejected(format!(
        "{message}; recovery: retain the original operation, publication and result cursor"
    ))
}

fn storage(error: acyclic_stream::StreamError) -> ProviderError {
    ProviderError::Rejected(format!(
        "original result access failed: {error}; recovery: restore the original operation's Streams authority and publication"
    ))
}

fn encoding(error: serde_json::Error) -> ProviderError {
    ProviderError::Rejected(format!(
        "original result encoding failed: {error}; recovery: restore the original version-one observation"
    ))
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;
