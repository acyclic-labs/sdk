//! Search requests and terminal results in the task volume.
use super::{SearchProvider, SearchRequest, SearchResponse};
use crate::task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskVolume};
use crate::{Error, NativeWork, Result, durable_host::TaskJournalOwner, scheduler::LeaseFence};
use acyclic_stream::{CommitCondition, OwnedBytes, StreamProvider};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const PUBLICATION_ATTEMPTS: usize = 4;

#[cfg(test)]
thread_local! {
    /// Fault switch: restore the old order, where a replacement owner judged
    /// the claim before consulting the retained response receipt.
    static FENCE_BEFORE_RECEIPT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn attempt(work: NativeWork<'_>) -> Result<()> {
    if let Some(work) = work {
        work.charge(acyclic_fs::WorkCounters {
            items_examined: 1,
            ..Default::default()
        })?;
    }
    Ok(())
}

fn ready(task: &Component, id: &Component) -> FileKey {
    FileKey::Task {
        task: task.clone(),
        file: TaskFile::RequestReady(id.clone()),
    }
}

/// Contents of `requests/search-<id>/result.json`. Credentials are never published.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum SearchOutcome {
    /// The provider returned the normalized, ranked sources.
    Succeeded {
        /// Ranked, normalized sources returned by the provider.
        response: SearchResponse,
    },
    /// The request or the complete provider response was definitively rejected.
    Rejected,
    /// A known failure occurred before a successful result could be published.
    Failed,
    /// The previous execution owner may have sent the request. Never redispatch.
    Indeterminate,
}

fn file(task: &Component, id: &Component, result: bool) -> FileKey {
    FileKey::Task {
        task: task.clone(),
        file: if result {
            TaskFile::RequestResult(id.clone())
        } else {
            TaskFile::RequestStarted(id.clone())
        },
    }
}

fn storage(error: crate::task_volume::Error) -> Error {
    if let crate::task_volume::Error::FileRead(error) = error {
        return error;
    }
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Storage,
        reason: "cannot access search request files",
    }
}

fn encode(value: &impl Serialize, work: NativeWork<'_>) -> Result<OwnedBytes> {
    let io = work
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?;
    // Keep the fixed file bound separate from the original work allowance.
    // The callback admits every codec operation before allocation or copying.
    let bytes = crate::executor::json::json_bytes_bounded(
        value,
        crate::executor::json::JsonBounds {
            maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
            maximum_items: io
                .as_ref()
                .map(|io| io.remaining())
                .transpose()?
                .map_or(u64::MAX, |remaining| remaining.items_examined),
            maximum_depth: 256,
        },
        work.map(|_| crate::wire::ErrorCode::Invalid),
        &mut |charge| {
            io.as_ref()
                .map_or(Ok(()), |io| io.codec_charge(charge, true))
        },
    )
    .map_err(|error| match error {
        Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "JSON exceeds admitted output",
        }
        | Error::Invalid(_) => super::rejection(work, "search file exceeds byte limit"),
        error => error,
    })?;
    crate::executor::with_history_io(io.as_ref(), |history| {
        crate::store::operations::owned_bytes_from_retained_vec(bytes, history)
            .map(|(bytes, _)| bytes)
    })
}

fn request(bytes: &[u8], work: NativeWork<'_>) -> Result<(SearchRequest, crate::NativeCustody)> {
    if bytes.len() > acyclic_fs::NativeMaterializationWork::MAXIMUM_BUILTIN_BYTES {
        return Err(super::rejection(
            work,
            "search request file exceeds byte limit",
        ));
    }
    let (decoded, custody) = if let Some(work) = work {
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let decoded: SearchRequest = io.decode_json(bytes).map_err(|error| match error {
            Error::NativeJsonAdmission { .. } | Error::NativeJson { .. } => {
                super::rejection(Some(work), "invalid search request file")
            }
            error => error,
        })?;
        let retained = (std::mem::size_of::<SearchRequest>() + decoded.query.capacity()) as u64;
        (decoded, Some(io.into_native_custody(retained)?))
    } else {
        (
            serde_json::from_slice(bytes)
                .map_err(|_| super::rejection(work, "invalid search request file"))?,
            None,
        )
    };
    let decoded: SearchRequest = decoded;
    decoded.validate_with_work(work)?;
    Ok((decoded, custody))
}

/// Exercises the production request admission boundary without a record-size cap.
#[cfg(feature = "test-support")]
#[doc(hidden)]
pub fn admit_request_for_test(
    bytes: &[u8],
    work: NativeWork<'_>,
) -> Result<(SearchRequest, crate::NativeCustody)> {
    request(bytes, work)
}

async fn condition<P: StreamProvider>(
    owner: &TaskJournalOwner<P>,
    work: NativeWork<'_>,
    settlement: bool,
) -> Result<Vec<CommitCondition>> {
    let mut history = work
        .map(crate::executor::PreparationIo::from_native_work)
        .transpose()?
        .map(crate::store::HistoryWork::new);
    let mut history = history
        .as_mut()
        .map(|io| io as &mut dyn crate::store::HistoryIo);
    let kind = if settlement {
        crate::distributed::JournalWrite::Settlement
    } else {
        crate::distributed::JournalWrite::Fresh
    };
    Ok(vec![owner.condition(kind, &mut history).await?])
}

/// Follows `requests/search-<id>.json` using the volume's atomic snapshot/cursor.
/// The host binds this worker to its authorized task volume and execution owner.
/// The resolver reads host configuration/credentials privately for each dispatch.
/// `maximum_files` bounds submitted requests; lifecycle outputs use exact file queries.
pub async fn follow<P: StreamProvider>(
    volume: &TaskVolume<P>,
    owner: &TaskJournalOwner<P>,
    mut resolve: impl FnMut() -> Result<Option<Arc<dyn SearchProvider>>>,
    work: NativeWork<'_>,
    maximum_files: usize,
) -> Result<()> {
    if !volume.shares_provider(&owner.stream()) {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "search volume uses another task provider",
        });
    }
    let task = Component::new(hex::encode(owner.task_id().into_bytes())).map_err(storage)?;
    let query = Query::Requests(task.clone());
    let mut observation = volume
        .observe(query, None, maximum_files)
        .await
        .map_err(storage)?;
    let authorize = |settlement| condition(owner, work, settlement);
    if let Some(snapshot) = observation.snapshot.take() {
        for value in snapshot.files {
            if let TaskFile::Request(id) = match &value.key {
                FileKey::Task { file, .. } => file,
                _ => continue,
            } && id.as_str().starts_with("search-")
            {
                settle(
                    volume,
                    &task,
                    id,
                    owner.task_binding().1,
                    &mut resolve,
                    &authorize,
                    work,
                )
                .await?;
            }
        }
    }
    loop {
        let next = observation.changes.next();
        let change = if let Some(work) = work {
            tokio::select! { biased;
                _ = work.cancellation().cancelled() => return Err(acyclic_fs::NativeTreeMaterializationError::Cancelled.into()),
                change = next => change,
            }
        } else {
            next.await
        };
        let Some(change) = change else {
            return Err(Error::Storage("search request follow ended".into()));
        };
        if let Some(FileValue {
            key:
                FileKey::Task {
                    file: TaskFile::Request(id),
                    ..
                },
            ..
        }) = change.map_err(storage)?.file
            && id.as_str().starts_with("search-")
        {
            settle(
                volume,
                &task,
                &id,
                owner.task_binding().1,
                &mut resolve,
                &authorize,
                work,
            )
            .await?;
        }
    }
}

async fn settle<P, F, A, Fut>(
    volume: &TaskVolume<P>,
    task: &Component,
    id: &Component,
    fence: &LeaseFence,
    resolve: &mut F,
    authorize: &A,
    work: NativeWork<'_>,
) -> Result<()>
where
    P: StreamProvider,
    F: FnMut() -> Result<Option<Arc<dyn SearchProvider>>>,
    A: Fn(bool) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<CommitCondition>>>,
{
    let started = file(task, id, false);
    let result = file(task, id, true);
    let (outcome, retain) = loop {
        let completed = volume
            .snapshot(&Query::File(result.clone()), 1)
            .await
            .map_err(storage)?;
        if !completed.files.is_empty() {
            return Ok(());
        }
        let claimed = volume
            .snapshot(&Query::File(started.clone()), 1)
            .await
            .map_err(storage)?;
        if claimed.generation != completed.generation {
            continue;
        }
        if let Some(claim) = claimed.files.first() {
            if let Some(work) = work {
                work.charge(acyclic_fs::WorkCounters {
                    source_bytes_read: claim.bytes.len() as u64,
                    items_examined: claim.bytes.len() as u64,
                    ..Default::default()
                })?;
            }
            // The retained response is the authoritative receipt of the one
            // dispatch. Any owner materializes it; only its absence leaves the
            // claimant's request uncertain.
            #[cfg(test)]
            let fence_first = FENCE_BEFORE_RECEIPT.with(std::cell::Cell::get);
            #[cfg(not(test))]
            let fence_first = false;
            let claimant = claim.bytes == encode(fence, work)?;
            if !fence_first || claimant {
                let retained = volume
                    .snapshot(&Query::File(ready(task, id)), 1)
                    .await
                    .map_err(storage)?;
                let retained = if retained.files.is_empty() {
                    let completion = volume
                        .file_volume(&ready(task, id))
                        .await
                        .map_err(storage)?;
                    completion
                        .snapshot(&Query::File(ready(task, id)), 1)
                        .await
                        .map_err(storage)?
                } else {
                    retained
                };
                if let Some(response) = retained.files.first() {
                    if let Some(work) = work {
                        work.charge(acyclic_fs::WorkCounters {
                            source_bytes_read: response.bytes.len() as u64,
                            items_examined: 1,
                            ..Default::default()
                        })?;
                    }
                    return finish(volume, result, response.bytes.clone(), authorize, work).await;
                }
                if claimant {
                    return Err(Error::SearchPublicationDeferred);
                }
            }
            break (SearchOutcome::Indeterminate, false);
        }
        let key = FileKey::Task {
            task: task.clone(),
            file: TaskFile::Request(id.clone()),
        };
        let snapshot = volume
            .snapshot(&Query::File(key), 1)
            .await
            .map_err(storage)?;
        if snapshot.generation != claimed.generation {
            continue;
        }
        let Some(input) = snapshot.files.first() else {
            return Ok(());
        };
        // A failed open must leave the request unclaimed and safe to retry.
        let completion = volume
            .file_volume(&ready(task, id))
            .await
            .map_err(storage)?;
        let claim = FileValue::new(started.clone(), encode(fence, work)?);
        match volume
            .append_if(
                snapshot.generation,
                &claim,
                authorize(false).await?,
                None,
                work,
            )
            .await
        {
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(storage(error)),
            Ok(_) => {}
        }
        let dispatch = async {
            // Tuple fields retire the decoded query before its allocation owner.
            let input = request(&input.bytes, work)?;
            let request = &input.0;
            let provider =
                resolve()?.ok_or_else(|| super::rejection(work, "search is not configured"))?;
            authorize(false).await?;
            let response = if let Some(work) = work {
                if !provider.supports_native_search(work)? {
                    return Err(super::error(
                        Some(work),
                        "search provider has no native producer",
                    ));
                }
                let response = provider.search_with_native_work(request, work)?.await?;
                if !response.1.work().shares_allocation_context(work)
                    || response.1.bytes() < super::native::response_bytes(&response.0)?
                {
                    return Err(super::error(
                        Some(work),
                        "search response lacks original-ledger custody",
                    ));
                }
                if response.0.results.len() > request.max_results as usize {
                    return Err(super::rejection(
                        Some(work),
                        "search backend exceeded result limit",
                    ));
                }
                // Encode before releasing the provider's original allocation.
                let (response, custody) = response;
                let outcome = SearchOutcome::Succeeded { response };
                let encoded = encode(&outcome, Some(work))?;
                drop(outcome);
                drop(custody);
                return Ok(encoded);
            } else {
                provider.search(request).await?
            };
            if response.results.len() > request.max_results as usize {
                return Err(super::rejection(
                    work,
                    "search backend exceeded result limit",
                ));
            }
            encode(&SearchOutcome::Succeeded { response }, work)
        }
        .await;
        match dispatch {
            Ok(encoded) => match retain_response(&completion, ready(task, id), encoded, work).await
            {
                Ok(encoded) => return finish(volume, result, encoded, authorize, work).await,
                Err(Error::ToolRejected { .. } | Error::Invalid(_)) => {
                    break (SearchOutcome::Rejected, true);
                }
                Err(error) => return Err(error),
            },
            Err(Error::ToolRejected { .. } | Error::Invalid(_)) => {
                break (SearchOutcome::Rejected, true);
            }
            Err(
                Error::Storage(_)
                | Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    ..
                },
            ) => break (SearchOutcome::Indeterminate, true),
            Err(error) if error.is_native_failure() => return Err(error),
            Err(_) => break (SearchOutcome::Failed, true),
        }
    };
    let encoded = encode(&outcome, work)?;
    let encoded = if retain {
        let completion = volume
            .file_volume(&ready(task, id))
            .await
            .map_err(storage)?;
        retain_response(&completion, ready(task, id), encoded, work).await?
    } else {
        encoded
    };
    finish(volume, result, encoded, authorize, work).await
}

async fn retain_response<P: StreamProvider>(
    volume: &TaskVolume<P>,
    key: FileKey,
    bytes: OwnedBytes,
    work: NativeWork<'_>,
) -> Result<OwnedBytes> {
    for _ in 0..PUBLICATION_ATTEMPTS {
        attempt(work)?;
        let snapshot = volume
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        if let Some(saved) = snapshot.files.first() {
            return Ok(saved.bytes.clone());
        }
        match volume
            .append_if(
                snapshot.generation,
                &FileValue::new(key.clone(), bytes.clone()),
                vec![],
                None,
                work,
            )
            .await
        {
            Ok(_) => return Ok(bytes),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(crate::task_volume::Error::LimitExceeded) => {
                return Err(super::rejection(
                    work,
                    "search file exceeds packed byte limit",
                ));
            }
            Err(error) => return Err(storage(error)),
        }
    }
    Err(Error::NativeBoundary {
        code: crate::wire::ErrorCode::Storage,
        reason: "cannot persist completed search response",
    })
}

async fn finish<P, A, Fut>(
    volume: &TaskVolume<P>,
    key: FileKey,
    bytes: OwnedBytes,
    authorize: &A,
    work: NativeWork<'_>,
) -> Result<()>
where
    P: StreamProvider,
    A: Fn(bool) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<CommitCondition>>>,
{
    match publish(volume, key.clone(), bytes, authorize, work).await {
        Err(Error::ToolRejected { .. } | Error::Invalid(_)) => {
            publish(
                volume,
                key,
                encode(&SearchOutcome::Rejected, work)?,
                authorize,
                work,
            )
            .await
        }
        other => other,
    }
}

async fn publish<P, A, Fut>(
    volume: &TaskVolume<P>,
    key: FileKey,
    bytes: OwnedBytes,
    authorize: &A,
    work: NativeWork<'_>,
) -> Result<()>
where
    P: StreamProvider,
    A: Fn(bool) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<CommitCondition>>>,
{
    for _ in 0..PUBLICATION_ATTEMPTS {
        attempt(work)?;
        let snapshot = volume
            .snapshot(&Query::File(key.clone()), 1)
            .await
            .map_err(storage)?;
        if !snapshot.files.is_empty() {
            return Ok(());
        }
        match volume
            .append_if(
                snapshot.generation,
                &FileValue::new(key.clone(), bytes.clone()),
                authorize(true).await?,
                None,
                work,
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(
                crate::task_volume::Error::Conflict(_) | crate::task_volume::Error::FenceConflict,
            ) => continue,
            Err(crate::task_volume::Error::LimitExceeded) => {
                return Err(super::rejection(
                    work,
                    "search file exceeds packed byte limit",
                ));
            }
            Err(error) => return Err(storage(error)),
        }
    }
    Err(Error::SearchPublicationDeferred)
}

/// Drives the actual file worker once over scripted task-volume fixtures.
#[cfg(any(test, feature = "test-support"))]
pub async fn drive_for_test<P: StreamProvider>(
    volume: &TaskVolume<P>,
    task: &Component,
    fence: &LeaseFence,
    mut resolve: impl FnMut() -> Result<Option<Arc<dyn SearchProvider>>>,
    conditions: Vec<CommitCondition>,
    work: NativeWork<'_>,
    maximum_files: usize,
) -> Result<()> {
    let snapshot = volume
        .snapshot(&Query::Requests(task.clone()), maximum_files)
        .await
        .map_err(storage)?;
    let authorize = |_| std::future::ready(Ok(conditions.clone()));
    for input in snapshot.files {
        if let FileKey::Task {
            file: TaskFile::Request(id),
            ..
        } = input.key
            && id.as_str().starts_with("search-")
        {
            settle(volume, task, &id, fence, &mut resolve, &authorize, work).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod crash_cut_tests;
#[cfg(test)]
#[cfg(acyclic_loom)]
mod model;
#[cfg(test)]
mod native_retry_tests;
#[cfg(test)]
mod retry_tests;
#[cfg(test)]
mod staging_tests;
#[cfg(test)]
mod tests;
