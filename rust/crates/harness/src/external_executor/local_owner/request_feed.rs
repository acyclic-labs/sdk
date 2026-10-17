//! Immutable command files are consumed in their provider's acceptance order.
use super::*;
use crate::{
    external_executor::ExternalRequest,
    task_volume::{FileKey, FileValue, TaskFile},
};
use acyclic_stream::{BoxProviderStream, Record, StreamProvider};
use futures::{TryStreamExt, future};

fn decode(record: &Record) -> Result<Option<ExternalRequest>> {
    let Some(file) = FileValue::from_record(record).map_err(storage)? else {
        return Ok(None);
    };
    let FileKey::Task {
        task,
        file: TaskFile::Request(_),
    } = &file.key
    else {
        return Ok(None);
    };
    let request = ExternalRequest::from_file(task, &file).map_err(storage)?;
    if task.as_str() != request.task_id().to_string() {
        return Err(Error::Unauthorized(
            "local request folder differs from its native session".into(),
        ));
    }
    Ok(Some(request))
}

#[cfg(feature = "test-support")]
pub(super) fn fuzz_bytes(bytes: &[u8]) {
    let mut record = Record {
        sequence: 0,
        value: bytes.to_vec().into(),
        commit_id: Default::default(),
        committed_at_micros: 0,
    };
    let _ = decode(&record);
    record.sequence = 1;
    if let Ok(Some(request)) = decode(&record) {
        fuzz_request(&request);
    }
}

#[cfg(feature = "test-support")]
pub(crate) fn fuzz_request(request: &ExternalRequest) {
    let Ok(task) = crate::task_volume::Component::new(request.task_id().to_string()) else {
        return;
    };
    let Ok(file) = request.to_file(task) else {
        return;
    };
    let Ok(value) = file.record_value(None) else {
        return;
    };
    let record = Record {
        sequence: 1,
        value,
        commit_id: Default::default(),
        committed_at_micros: 0,
    };
    assert_eq!(decode(&record).unwrap().as_ref(), Some(request));
}

pub(super) async fn follow<P: StreamProvider>(
    provider: &P,
    path: StreamPath,
) -> Result<BoxProviderStream<'static, Result<ExternalRequest>>> {
    let from = 0;
    #[cfg(acyclic_harness_loom)]
    let from = if model::from_tail() {
        provider.tail(path.clone()).await.map_err(storage)?
    } else {
        from
    };
    let records = provider.follow(path, from).await.map_err(storage)?;
    Ok(Box::pin(
        records
            .map_err(storage)
            .try_filter_map(|record| {
                let decoded = decode(&record);
                #[cfg(acyclic_harness_loom)]
                if model::keep_invalid() {
                    return future::ready(decoded);
                }
                // Rejected immutable files stay in their original task stream;
                // only provider failures may terminate the execution follow.
                future::ready(Ok(decoded.unwrap_or(None)))
            }),
    ))
}

#[cfg(acyclic_harness_loom)]
mod model;
#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
