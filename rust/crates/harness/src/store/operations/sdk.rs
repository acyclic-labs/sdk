use super::*;
use futures::TryStreamExt as _;

pub(crate) fn stream_error_with_work(
    source: acyclic_stream::StreamError,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work.as_deref().is_some_and(original_native_decode_mode) {
        let code = if matches!(&source, acyclic_stream::StreamError::IdempotencyMismatch) {
            crate::wire::ErrorCode::Conflict
        } else {
            crate::wire::ErrorCode::Storage
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
    client: &std::sync::Arc<P>,
    path: &str,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
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
    let stream = acyclic_stream::Stream::new(std::sync::Arc::clone(&client), path)
        .map_err(|error| stream_error_with_work(error, work))?;
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let path_custody = None;
    Ok((stream, path_custody))
}

/// Native payload custody is intrinsic to the SDK's `OwnedBytes`.
pub(crate) fn sdk_record_retained_bytes(
    record: &Record,
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
) -> u64 {
    if work.as_deref().is_some_and(original_native_decode_mode) {
        0
    } else {
        record.value.len() as u64
    }
}

/// The original finite producer; the native cursor guard follows its box.
pub(crate) enum ObservedRecords {
    Ordinary(acyclic_stream::RecordStream),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native(acyclic_stream::NativeStreamOutput<acyclic_stream::NativeRecordStream>),
}

/// Retains the actual record's SDK metadata, never a second byte owner.
pub(crate) enum ObservedRecord {
    Ordinary(Record),
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    Native(acyclic_stream::NativeStreamOutput<Record>),
}

impl std::ops::Deref for ObservedRecord {
    type Target = Record;
    fn deref(&self) -> &Record {
        match self {
            Self::Ordinary(record) => record,
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(output) => output.value(),
        }
    }
}

impl std::fmt::Debug for ObservedRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&**self, formatter)
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn original_sdk_metadata_output<T>(
    original: &acyclic_fs::NativeMaterializationWork,
    output: acyclic_stream::NativeStreamOutput<T>,
) -> Result<acyclic_stream::NativeStreamOutput<T>> {
    let (value, allocation) = output.into_parts();
    let same_meter = std::sync::Arc::ptr_eq(allocation.meter(), &original.runtime_meter());
    let output = acyclic_stream::NativeStreamOutput::new(value, allocation);
    if !same_meter {
        return Err(Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::InvalidLayout,
            custody: None,
        });
    }
    Ok(output)
}

impl ObservedRecords {
    pub(crate) async fn next(
        &mut self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
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
                Ok(record.map(ObservedRecord::Ordinary))
            }
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            Self::Native(records) => {
                let original = work
                    .as_deref()
                    .and_then(|observer| observer.original_native_work())
                    .expect("native cursor has its original observer")
                    .clone();
                let Some(record) =
                    records
                        .value_mut()
                        .try_next()
                        .await
                        .map_err(|error| Error::NativeWork {
                            cause: original.runtime_error(error),
                            custody: None,
                        })?
                else {
                    return Ok(None);
                };
                let record = crate::store::history::native_sdk_result(&original, Ok(record))?;
                work.as_deref_mut()
                    .expect("native cursor observer")
                    .receive_native_record(record.value().value.len() as u64)?;
                Ok(Some(ObservedRecord::Native(original_sdk_metadata_output(
                    &original, record,
                )?)))
            }
        }
    }
}

pub(crate) async fn read_records<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    path: &StreamPath,
    from: u64,
    count: u32,
    work: &mut Option<&mut dyn crate::store::HistoryIo>,
) -> Result<std::result::Result<ObservedRecords, acyclic_stream::StreamError>> {
    let request = acyclic_stream::ReadRequest {
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
    Ok(client
        .read(request)
        .await
        .map(ObservedRecords::Ordinary))
}

/// Preserves the provider's NotFound result for the canonical CAS selector.
pub(crate) async fn bounds_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<std::result::Result<acyclic_stream::StreamBounds, acyclic_stream::StreamError>> {
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
    client: &std::sync::Arc<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<u64> {
    tail_optional_with_work(client, path, work)
        .await?
        .ok_or_else(|| stream_error_with_work(acyclic_stream::StreamError::NotFound, work))
}

pub(crate) async fn tail_optional_with_work<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    path: &StreamPath,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Option<u64>> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(original) = work
        .as_deref()
        .and_then(|observer| observer.original_native_work())
    {
        let result = client
            .native_tail(path.clone(), original.runtime_meter())
            .await;
        let result = crate::distributed::native_call_result(original, result)?;
        return match result {
            Ok(output) => Ok(Some(*output.value())),
            Err(error) if error == acyclic_stream::StreamError::NotFound => {
                drop(error);
                Ok(None)
            }
            Err(error) => Err(crate::store::operations::stream_error_with_work(error, work)),
        };
    }
    if let Some(observer) = work.as_deref_mut() {
        observer.begin_call()?;
    }
    match client.tail(path.clone()).await {
        Ok(tail) => Ok(Some(tail)),
        Err(acyclic_stream::StreamError::NotFound) => Ok(None),
        Err(error) => Err(stream_error_with_work(error, work)),
    }
}

/// The original one-record lookup with receipts at each visible SDK boundary.
pub(crate) async fn one_record<P: StreamProvider>(
    client: &std::sync::Arc<P>,
    path: &StreamPath,
    from: u64,
    work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
) -> Result<Option<ObservedRecord>> {
    let mut records = match read_records(client, path, from, 1, work).await? {
        Ok(records) => records,
        Err(error) if error == acyclic_stream::StreamError::NotFound => {
            drop(error);
            return Ok(None);
        }
        Err(error) => return Err(crate::store::operations::stream_error_with_work(error, work)),
    };
    let mut first = None;
    let mut extra = None;
    let mut received = 0;
    let result = async {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        first = records.next(work).await?;
        if let Some(record) = &first {
            received += sdk_record_retained_bytes(record, work);
        }
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        extra = records.next(work).await?;
        if let Some(record) = &extra {
            received += sdk_record_retained_bytes(record, work);
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "operation lookup exceeded its one-record bound",
            ));
        }
        if let Some(record) = &first
            && (record.sequence != from || record.value.len() > acyclic_stream::MAX_RECORD_BYTES)
        {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "operation location record is invalid",
            ));
        }
        Ok(())
    }
    .await;
    drop(records);
    drop(extra);
    match result {
        Ok(()) => Ok(first),
        Err(error) => {
            drop(first);
            if let Some(observer) = work.as_deref_mut() {
                let _ = observer.release_owned(received);
            }
            Err(error)
        }
    }
}
