use super::*;
use crate::results::{ResultCursor, ResultCut, ResultPage, Settlement};

impl From<Provenance<ForkSource, ForkFidelity>> for wire::Provenance {
    fn from(value: Provenance<ForkSource, ForkFidelity>) -> Self {
        Self {
            source: Some(match value.source {
                ForkSource::Machine(machine) => {
                    wire::provenance::Source::MachineId(encode_machine(machine))
                }
                ForkSource::Checkpoint(checkpoint) => {
                    wire::provenance::Source::CheckpointId(encode_checkpoint(checkpoint))
                }
            }),
            fidelity: match value.inherited {
                ForkFidelity::MemoryAndDisk => wire::ForkFidelity::MemoryAndDisk,
                ForkFidelity::DiskOnly => wire::ForkFidelity::DiskOnly,
            } as i32,
        }
    }
}

impl From<ResultCut> for wire::ResultCut {
    fn from(value: ResultCut) -> Self {
        Self {
            operation: operation_request(value.operation).operation,
            settlement: Some(match value.settlement {
                Settlement::Streams(commit) => {
                    wire::result_cut::Settlement::StreamsCommit(commit.as_bytes().to_vec())
                }
                Settlement::NativeController(revision) => {
                    wire::result_cut::Settlement::NativeRevision(revision.get())
                }
            }),
            count: value.count,
        }
    }
}

impl TryFrom<wire::ResultCut> for ResultCut {
    type Error = ProviderError;
    fn try_from(value: wire::ResultCut) -> Result<Self, Self::Error> {
        decode_result_cut(Some(&value))
    }
}

pub(in super::super) fn decode_result_cut(
    value: Option<&wire::ResultCut>,
) -> Result<ResultCut, ProviderError> {
    let value = value.ok_or_else(|| invalid("result cut is missing"))?;
    let operation = decode_operation(value.operation.as_ref())?;
    let settlement = match value.settlement.as_ref() {
        Some(wire::result_cut::Settlement::StreamsCommit(bytes)) => {
            Settlement::Streams(acyclic_stream::CommitId::from_bytes(
                bytes
                    .as_slice()
                    .try_into()
                    .map_err(|_| invalid("result commit width is invalid"))?,
            ))
        }
        Some(wire::result_cut::Settlement::NativeRevision(revision)) => {
            Settlement::NativeController(
                std::num::NonZeroU64::new(*revision)
                    .ok_or_else(|| invalid("result revision is empty"))?,
            )
        }
        None => return Err(invalid("result settlement is missing")),
    };
    if value.count == 0 {
        return Err(invalid("fork result extent is empty"));
    }
    Ok(ResultCut {
        operation,
        settlement,
        count: value.count,
    })
}

impl From<ResultCursor> for wire::ResultCursor {
    fn from(value: ResultCursor) -> Self {
        Self {
            cut: Some(value.cut.into()),
            from: value.from,
        }
    }
}

impl TryFrom<wire::ResultCursor> for ResultCursor {
    type Error = ProviderError;
    fn try_from(value: wire::ResultCursor) -> Result<Self, Self::Error> {
        decode_result_cursor(Some(&value))
    }
}

pub(in super::super) fn decode_result_cursor(
    value: Option<&wire::ResultCursor>,
) -> Result<ResultCursor, ProviderError> {
    let value = value.ok_or_else(|| invalid("result cursor is missing"))?;
    let cut = decode_result_cut(value.cut.as_ref())?;
    if value.from > cut.count {
        return Err(invalid("result cursor exceeds original extent"));
    }
    Ok(ResultCursor {
        cut,
        from: value.from,
    })
}

pub(in super::super) fn decode_result_page(
    value: wire::ResultPage,
    cursor: ResultCursor,
    credit: NonZeroU32,
) -> Result<ResultPage, ProviderError> {
    if decode_result_cursor(value.cursor.as_ref())? != cursor {
        return Err(invalid("result page cursor was substituted"));
    }
    let (range, _) = cursor.interval(cursor.cut, credit)?;
    let returned = u64::try_from(value.machines.len())
        .map_err(|_| invalid("result page extent exceeds address space"))?;
    if returned > range.end - range.start || returned == 0 && cursor.from < cursor.cut.count {
        return Err(invalid(
            "result page violates its original extent or credit",
        ));
    }
    let end = cursor.from + returned;
    let next = (end < cursor.cut.count).then_some(ResultCursor {
        from: end,
        ..cursor
    });
    if value
        .next
        .as_ref()
        .map(|cursor| decode_result_cursor(Some(cursor)))
        .transpose()?
        != next
    {
        return Err(invalid(
            "result page extent or continuation was substituted",
        ));
    }
    let machines = value
        .machines
        .into_iter()
        .map(MachineObservation::try_from)
        .collect::<Result<_, _>>()?;
    Ok(ResultPage {
        cursor,
        machines,
        next,
    })
}

fn invalid(message: &str) -> ProviderError {
    ProviderError::Rejected(format!(
        "{message}; recovery: retain the original provider, settlement and cursor"
    ))
}
