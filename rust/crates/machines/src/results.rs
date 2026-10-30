//! Immutable settled observations under the original completion owner.
//!
//! The physical fork owner stages observations with [`record`]. It includes
//! [`publication`] in its EXISTING terminal commit, alongside the operation's
//! successful outcome. This module never commits, dispatches or watches a Machine.
//! The source prefix is shared by Streams; settlement does not recopy its bytes.
//! Native controllers instead retain their original partition revision and
//! observations. [`NativeResults`] borrows that retained result without a second
//! publication, resource inspection or conversion of its revision to a commit ID.
use crate::{MachineObservation, OperationId, ProviderError};
use acyclic_stream::{
    AppendRecord, CommitCondition, CommitId, CommitMutation, ReadRequest, StreamPath,
    StreamProvider, StreamProviderExt as _,
};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::{
    num::{NonZeroU32, NonZeroU64},
    ops::Range,
    sync::Arc,
};

/// The actual terminal publication owner. A native revision is scoped to the
/// original authenticated controller partition; it is not a Streams commit,
/// Machine incarnation or watch watermark. This value grants no authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Settlement {
    Streams(CommitId),
    NativeController(NonZeroU64),
}

/// Immutable identity and extent retained with the original successful outcome.
/// Reopening must use the same authenticated owner, operation and cut after
/// lost acknowledgment. Neither expiry nor this metadata proves physical death.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResultCut {
    pub operation: OperationId,
    pub settlement: Settlement,
    pub count: u64,
}

/// A child index in one exact original result, never in current Machine state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResultCursor {
    pub cut: ResultCut,
    pub from: u64,
}

impl ResultCursor {
    fn interval(
        self,
        original: ResultCut,
        credit: NonZeroU32,
    ) -> Result<(Range<u64>, Option<Self>), ProviderError> {
        if self.cut != original {
            return Err(invalid("result cursor differs from original settlement"));
        }
        let remaining = original
            .count
            .checked_sub(self.from)
            .ok_or_else(|| invalid("result cursor exceeds its original extent"))?;
        let end = self.from + remaining.min(u64::from(credit.get()));
        Ok((
            self.from..end,
            (end < original.count).then_some(Self { from: end, ..self }),
        ))
    }
}

/// One visible page. Metadata is constant per page; payload and decode work are
/// proportional only to returned observations. Native owners can borrow their
/// original protobuf slice; Streams reads own only the decoded visible page.
pub struct ResultPage<M = Vec<MachineObservation>> {
    pub cursor: ResultCursor,
    pub machines: M,
    pub next: Option<ResultCursor>,
}

/// A view over the original native controller's already retained observations.
/// The caller retains that SAME authenticated partition/operation owner through
/// access. This helper performs no admission, terminal verification, decoding,
/// IO or dispatch; those remain with the controller's original outcome reader.
/// Its slice borrow retains the original result through page observation.
pub struct NativeResults<'a, T> {
    cut: ResultCut,
    machines: &'a [T],
}

impl<'a, T> NativeResults<'a, T> {
    /// Call only after verifying original terminal success and reading its
    /// retained outcome/revision, never a later inspection or guest-supplied
    /// array. Accepts the original canonical protobuf observations directly.
    pub fn open(
        operation: OperationId,
        revision: NonZeroU64,
        machines: &'a [T],
    ) -> Result<Self, ProviderError> {
        let count = u64::try_from(machines.len())
            .map_err(|_| invalid("native result extent cannot be represented"))?;
        Ok(Self {
            cut: ResultCut {
                operation,
                settlement: Settlement::NativeController(revision),
                count,
            },
            machines,
        })
    }

    #[must_use]
    pub fn cursor(&self) -> ResultCursor {
        ResultCursor {
            cut: self.cut,
            from: 0,
        }
    }

    /// O(1) validation and slice selection, with zero allocation/copy or wait.
    /// A stale/foreign cut is refused before any payload access. Serialization
    /// of the selected slice remains O(visible bytes), not O(total children).
    pub fn page(
        &self,
        cursor: ResultCursor,
        credit: NonZeroU32,
    ) -> Result<ResultPage<&'a [T]>, ProviderError> {
        let (range, next) = cursor.interval(self.cut, credit)?;
        let start = usize::try_from(range.start)
            .map_err(|_| invalid("native result cursor exceeds address space"))?;
        let end = usize::try_from(range.end)
            .map_err(|_| invalid("native result cursor exceeds address space"))?;
        let machines = self
            .machines
            .get(start..end)
            .ok_or_else(|| invalid("native result extent differs from its retained cut"))?;
        Ok(ResultPage {
            cursor,
            machines,
            next,
        })
    }
}

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
    cut: ResultCut,
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
            cut: ResultCut {
                operation,
                settlement: Settlement::Streams(settlement),
                count,
            },
        })
    }

    #[must_use]
    pub fn cursor(&self) -> ResultCursor {
        ResultCursor {
            cut: self.cut,
            from: 0,
        }
    }

    /// Reads only the requested original interval. There are no head reads,
    /// resource inspections or completion waits per page. A lost/short source
    /// is a typed error, never an empty success or a readiness retry.
    pub async fn page(
        &self,
        cursor: ResultCursor,
        credit: NonZeroU32,
    ) -> Result<ResultPage, ProviderError> {
        // Credit is an allowance, not a demand to exceed the shared read chunk.
        let chunk = u32::try_from(acyclic_stream::MAX_ITEMS)
            .map_err(|_| invalid("Streams read chunk cannot be represented"))?;
        let credit = NonZeroU32::new(credit.get().min(chunk))
            .ok_or_else(|| invalid("Streams read chunk is empty"))?;
        let (range, next) = cursor.interval(self.cut, credit)?;
        let limit = u32::try_from(range.end - range.start)
            .map_err(|_| invalid("result page credit cannot be represented"))?;
        let machines = if limit == 0 {
            Vec::new()
        } else {
            let records = self
                .streams
                .read(ReadRequest {
                    path: self.path.clone(),
                    from: range.start,
                    limit,
                    at_least: Default::default(),
                    key: None,
                })
                .await
                .map_err(storage)?;
            observations(records, range.start, limit).await?
        };
        Ok(ResultPage {
            cursor,
            machines,
            next,
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
