//! Native file projections use the original read, decoder and byte owners.
use super::*;
use crate::filesystem::{DecodeReservation, PreparationIo};
use acyclic_fs::NativeMaterializationWork;
use acyclic_stream::{NativeStreamProvider, OwnedBytes, RevisionCut, StreamProviderExt as _};

/// One requested signal at an already captured exclusive source cut.
pub struct SignalFileRead {
    /// Authorized supervision stream of the selected agent.
    pub source: StreamPath,
    /// Exclusive source sequence; later appends cannot enter the projection.
    pub before: u64,
    /// Authority revision required by the original file selection.
    pub at_least: RevisionCut,
    /// Requested file's query identity.
    pub kind: Kind,
}

/// JSON file bytes retaining their original native allocation until retirement.
pub struct NativeSignalFile {
    /// Exact signal record within the request's source stream.
    pub sequence: u64,
    /// Read-only projection, never an authored copy or portable fallback.
    pub bytes: OwnedBytes,
}

fn refused(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Storage,
        reason,
    }
}

struct DecodedEvent<'a> {
    event: Event,
    sequence: u64,
    // The value retires before its original decoded-storage reservation.
    _custody: DecodeReservation<'a>,
}

async fn event_before<'a, P: NativeStreamProvider + ?Sized>(
    provider: &P,
    read: &SignalFileRead,
    key: OwnedBytes,
    io: &'a PreparationIo,
) -> Result<Option<DecodedEvent<'a>>> {
    use crate::store::HistoryIo as _;
    let original = io
        .original_native_work()
        .ok_or_else(|| refused("native signal query has no original owner"))?;
    let result = provider
        .native_latest_before(
            read.source.clone(),
            Some(key),
            read.before,
            read.at_least.clone(),
            original.runtime_meter(),
        )
        .await;
    if matches!(&result, Ok(Err(StreamError::NotFound))) {
        return Ok(None);
    }
    let output = crate::store::history::native_sdk_result(original, result)?;
    let output = crate::store::operations::original_sdk_metadata_output(original, output)?;
    let Some(record) = output.value() else {
        return Ok(None);
    };
    let mut observer = io.history_io();
    observer.receive_native_record(record.value.len() as u64)?;
    let (event, custody) = io.decode(
        std::marker::PhantomData::<Event>,
        &record.value,
        crate::wire::ErrorCode::Storage,
    )?;
    Ok(Some(DecodedEvent {
        event,
        sequence: record.sequence,
        _custody: custody,
    }))
}

/// Demand-select one status file using the caller's original native budget.
/// Run ownership, signal selection, decoding and output encoding all retain
/// that owner. Known absence stays absent; a failed measurement never falls
/// back to an unmetered read, a newer source cut or an authored status cache.
pub async fn native_signal_file<P: NativeStreamProvider + ?Sized>(
    provider: &P,
    agent: &AgentPath,
    read: SignalFileRead,
    original: &NativeMaterializationWork,
) -> Result<Option<NativeSignalFile>> {
    let io = PreparationIo::from_native_work(original)?;
    if read.source.as_str().strip_suffix("/supervision") != Some(agent.as_str()) {
        return Err(refused("signal file source differs"));
    }
    let run = {
        let Some(current) = event_before(
            provider,
            &read,
            OwnedBytes::from_static(CURRENT_RUN_KEY),
            &io,
        )
        .await?
        else {
            return Ok(None);
        };
        current_run_from(&current.event, agent).map_err(refused)?
    };
    let text = signal_key_text(run, read.kind);
    let key = crate::executor::with_history_io(Some(&io), |work| {
        let observer = work
            .as_deref_mut()
            .ok_or_else(|| refused("native signal key has no original owner"))?;
        let text = observer.clone_text(text.as_str())?;
        crate::store::operations::owned_bytes_from_retained_vec(text.into_bytes(), work)
            .map(|(bytes, _)| bytes)
    })?;
    let Some(selected) = event_before(provider, &read, key, &io).await? else {
        return Ok(None);
    };
    let signal = signal_from(&selected.event, agent, run, read.kind).map_err(refused)?;
    let bytes = io.encode(signal, None, false)?;
    io.remaining()?;
    Ok(Some(NativeSignalFile {
        sequence: selected.sequence,
        bytes,
    }))
}
