use super::*;
use futures::{Stream as _, TryStreamExt as _};

pub(crate) fn stream_error_with_work(
    source: StreamError,
    work: &Option<&mut dyn HistoryIo>,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(original_native_decode_mode) {
        let code = if matches!(&source, StreamError::IdempotencyMismatch) {
            crate::wire::ErrorCode::Conflict
        } else {
            Storage
        };
        return Error::NativeStream {
            code,
            source,
            custody: None,
        };
    }
    let _ = work;
    source.into()
}

pub(crate) fn stream<P: StreamProvider>(
    client: &Arc<P>,
    path: &str,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<(
    acyclic_stream::Stream<P>,
    Option<crate::runtime::NativeCustody>,
)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let path_custody = if let Some(observer) = work
        .as_deref_mut()
        .filter(|observer| observer.original_native_work().is_some())
    {
        let bytes = path.len() as u64;
        observer.admit_owned(bytes)?;
        observer.admit_allocation(bytes)?;
        observer.admit_copy(bytes)?;
        observer.retain_owned(bytes)?;
        let custody = observer.take_native_custody(bytes)?;
        observer.copy_bytes(bytes)?;
        Some(custody)
    } else {
        None
    };
    let stream = acyclic_stream::Stream::new(Arc::clone(client), path)
        .map_err(|error| stream_error_with_work(error, work))?;
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let path_custody = None;
    Ok((stream, path_custody))
}

/// Native payload custody is intrinsic to the SDK's `OwnedBytes`.
pub(crate) fn sdk_record_retained_bytes(record: &Record, work: &Option<&mut dyn HistoryIo>) -> u64 {
    if work.is_none() || work.as_deref().is_some_and(original_native_decode_mode) {
        0
    } else {
        record.value.len() as u64
    }
}

/// The original finite producer; the native cursor guard follows its box.
pub(crate) enum ObservedRecords {
    Ordinary(RecordStream),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native(NativeStreamOutput<NativeRecordStream>),
}

/// Retains the record and its SDK metadata allocation.
#[derive(Debug)]
pub(crate) struct ObservedRecord {
    record: Record,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}

impl std::ops::Deref for ObservedRecord {
    type Target = Record;
    fn deref(&self) -> &Record {
        &self.record
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn original_sdk_metadata_output<T>(
    original: &acyclic_fs::NativeMaterializationWork,
    output: NativeStreamOutput<T>,
) -> Result<NativeStreamOutput<T>> {
    let (record, allocation) = output.into_parts();
    let same_meter = Arc::ptr_eq(allocation.meter(), &original.runtime_meter());
    let output = NativeStreamOutput::new(record, allocation);
    if !same_meter {
        return Err(acyclic_fs::NativeTreeMaterializationError::InvalidLayout.into());
    }
    Ok(output)
}

impl ObservedRecords {
    pub(crate) fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Ordinary(records) => records.size_hint(),
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(records) => records.value().size_hint(),
        }
    }

    pub(crate) async fn next(
        &mut self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<ObservedRecord>> {
        match self {
            Self::Ordinary(records) => {
                let record = records
                    .try_next()
                    .await
                    .map_err(|error| stream_error_with_work(error, work))?;
                if let Some(record) = &record
                    && let Some(observer) = work.as_deref_mut()
                {
                    observer.receive_record(record.value.len() as u64)?;
                }
                Ok(record.map(|record| ObservedRecord {
                    record,
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    _allocation: None,
                }))
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(records) => {
                let observer = work.as_deref_mut().ok_or(Error::NativeBoundary {
                    code: Storage,
                    reason: "native cursor has no observer",
                })?;
                let original = observer
                    .original_native_work()
                    .ok_or(Error::NativeBoundary {
                        code: Storage,
                        reason: "native cursor has no original ledger",
                    })?
                    .clone();
                let Some(record) = records
                    .value_mut()
                    .try_next()
                    .await
                    .map_err(|error| Error::from(original.runtime_error(error)))?
                else {
                    return Ok(None);
                };
                let record = crate::store::history::native_sdk_result(&original, Ok(record))?;
                observer.receive_native_record(record.value().value.len() as u64)?;
                let (record, allocation) =
                    original_sdk_metadata_output(&original, record)?.into_parts();
                Ok(Some(ObservedRecord {
                    record,
                    _allocation: Some(allocation),
                }))
            }
        }
    }
}

pub(crate) async fn read_records<P: StreamProvider>(
    client: &Arc<P>,
    path: &StreamPath,
    from: u64,
    count: u32,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<std::result::Result<ObservedRecords, StreamError>> {
    let request = ReadRequest {
        path: path.clone(),
        from,
        limit: count,
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
        let result = client.native_read(request, original.runtime_meter()).await;
        return match crate::distributed::native_call_result(original, result)? {
            Ok(output) => Ok(Ok(ObservedRecords::Native(original_sdk_metadata_output(
                original, output,
            )?))),
            Err(error) => Ok(Err(error)),
        };
    }
    if let Some(observer) = work.as_deref_mut() {
        observer.begin_call()?;
    }
    Ok(client.read(request).await.map(ObservedRecords::Ordinary))
}

/// Preserves the provider's `NotFound` result for the canonical CAS selector.
pub(crate) async fn bounds_with_work<P: StreamProvider>(
    client: &Arc<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<std::result::Result<StreamBounds, StreamError>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work
        .as_deref()
        .and_then(|observer| observer.original_native_work())
    {
        let result = client
            .native_bounds(path.clone(), original.runtime_meter())
            .await;
        return crate::distributed::native_call_result(original, result)
            .map(|result| result.map(|output| *output.value()));
    }
    if let Some(observer) = work.as_deref_mut() {
        observer.begin_call()?;
    }
    Ok(client.bounds(path.clone()).await)
}

pub(crate) async fn tail_with_work<P: StreamProvider>(
    client: &Arc<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    tail_optional_with_work(client, path, work)
        .await?
        .ok_or_else(|| stream_error_with_work(StreamError::NotFound, work))
}

pub(crate) async fn tail_optional_with_work<P: StreamProvider>(
    client: &Arc<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<u64>> {
    let result: Option<std::result::Result<u64, StreamError>> = None;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let mut result = result;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work.as_deref().and_then(|io| io.original_native_work()) {
        let tail = client
            .native_tail(path.clone(), original.runtime_meter())
            .await;
        result = Some(
            crate::distributed::native_call_result(original, tail)?.map(|output| *output.value()),
        );
    }
    let result = if let Some(result) = result {
        result
    } else {
        if let Some(observer) = work.as_deref_mut() {
            observer.begin_call()?;
        }
        client.tail(path.clone()).await
    };
    match result {
        Ok(tail) => Ok(Some(tail)),
        Err(StreamError::NotFound) => Ok(None),
        Err(error) => Err(stream_error_with_work(error, work)),
    }
}

/// Reads one exact record and rejects a producer exceeding the bound.
pub(crate) async fn one_record<P: StreamProvider>(
    client: &Arc<P>,
    path: &StreamPath,
    from: u64,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<Option<ObservedRecord>> {
    let mut quota = CodecQuota::new(work, 0)?;
    let mut records = match read_records(client, path, from, 1, quota.work()).await? {
        Ok(records) => records,
        Err(StreamError::NotFound) => {
            return Ok(None);
        }
        Err(error) => {
            return Err(crate::store::operations::stream_error_with_work(
                error,
                quota.work(),
            ));
        }
    };
    let mut first = None;
    for position in 0..2 {
        if let Some(io) = quota.work().as_deref() {
            io.check_cancellation()?;
        }
        if let Some(record) = records.next(quota.work()).await? {
            let held = sdk_record_retained_bytes(&record, quota.work());
            quota.adopt(held)?;
            if position != 0
                || record.sequence != from
                || record.value.len() > acyclic_stream::MAX_RECORD_BYTES
            {
                return Err(boundary(
                    quota.work(),
                    Storage,
                    "operation location record is invalid",
                ));
            }
            first = Some(record);
        }
    }
    quota.transfer();
    Ok(first)
}

pub(in crate::store) async fn publication_record<P: StreamProvider>(
    client: &Arc<P>,
    stream: (StreamPath, u64),
    from: u64,
    quota: &mut CodecQuota<'_, '_>,
) -> Result<Option<ObservedRecord>> {
    let (stream, held) = stream;
    quota.adopt(held)?;
    let received = one_record(client, &stream, from, quota.work()).await;
    drop(stream);
    quota.relinquish(held)?;
    let record = received?;
    let held = record
        .as_ref()
        .map_or(0, |record| sdk_record_retained_bytes(record, quota.work()));
    quota.adopt(held)?;
    Ok(record)
}
