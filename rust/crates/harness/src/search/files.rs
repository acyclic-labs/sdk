//! Search requests and terminal results in the task volume.
use super::{SearchProvider, SearchRequest, SearchResponse};
use crate::task_volume::{Component, FileKey, FileValue, Query, TaskFile, TaskVolume};
use crate::{Error, NativeWork, Result, durable_host::TaskJournalOwner, scheduler::LeaseFence};
use acyclic_stream::{CommitCondition, OwnedBytes, StreamProvider};
use futures::StreamExt as _;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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

fn storage(_: crate::task_volume::Error) -> Error {
    Error::NativeBoundary {
        code: crate::wire::ErrorCode::Storage,
        reason: "cannot access search request files",
    }
}

fn encode(value: &impl Serialize, work: NativeWork<'_>) -> Result<OwnedBytes> {
    if let Some(work) = work {
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        return io.encode(value, acyclic_stream::MAX_RECORD_BYTES as u64, false);
    }
    serde_json::to_vec(value)
        .map(Into::into)
        .map_err(|_| super::rejection(work, "invalid search file"))
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
            Error::NativeJson { .. } => super::rejection(Some(work), "invalid search request file"),
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
                    maximum_files,
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
                maximum_files,
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
    maximum_files: usize,
) -> Result<()>
where
    P: StreamProvider,
    F: FnMut() -> Result<Option<Arc<dyn SearchProvider>>>,
    A: Fn(bool) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<CommitCondition>>>,
{
    let started = file(task, id, false);
    let result = file(task, id, true);
    let outcome = loop {
        let snapshot = volume
            .snapshot(&Query::Requests(task.clone()), maximum_files)
            .await
            .map_err(storage)?;
        if snapshot.files.iter().any(|value| value.key == result) {
            return Ok(());
        }
        if let Some(claim) = snapshot.files.iter().find(|value| value.key == started) {
            if let Some(work) = work {
                work.charge(acyclic_fs::WorkCounters {
                    source_bytes_read: claim.bytes.len() as u64,
                    items_examined: claim.bytes.len() as u64,
                    ..Default::default()
                })?;
            }
            if claim.bytes == encode(fence, work)? {
                return Ok(());
            }
            break SearchOutcome::Indeterminate;
        }
        let key = FileKey::Task {
            task: task.clone(),
            file: TaskFile::Request(id.clone()),
        };
        let Some(input) = snapshot.files.iter().find(|value| value.key == key) else {
            return Ok(());
        };
        let claim = FileValue::new(started.clone(), encode(fence, work)?);
        match volume
            .append_if(snapshot.generation, &claim, authorize(false).await?, None)
            .await
        {
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(storage(error)),
            Ok(_) => {}
        }
        let dispatch = async {
            let (request, _input_custody) = request(&input.bytes, work)?;
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
                let (response, custody) = provider.search_with_native_work(&request, work)?.await?;
                if !custody.work().shares_allocation_context(work)
                    || custody.bytes() < super::native::response_bytes(&response)?
                {
                    return Err(super::error(
                        Some(work),
                        "search response lacks original-ledger custody",
                    ));
                }
                if response.results.len() > request.max_results as usize {
                    return Err(super::rejection(
                        Some(work),
                        "search backend exceeded result limit",
                    ));
                }
                // Encode before releasing the provider's original allocation.
                let outcome = SearchOutcome::Succeeded { response };
                let encoded = encode(&outcome, Some(work))?;
                drop(custody);
                return Ok(encoded);
            } else {
                provider.search(&request).await?
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
            Ok(encoded) => {
                publish(volume, task, result, encoded, authorize, maximum_files).await?;
                return Ok(());
            }
            Err(Error::ToolRejected(_) | Error::Invalid(_)) => break SearchOutcome::Rejected,
            Err(
                Error::Storage(_)
                | Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    ..
                },
            ) => break SearchOutcome::Indeterminate,
            Err(error) if error.is_native_failure() => return Err(error),
            Err(_) => break SearchOutcome::Failed,
        }
    };
    publish(
        volume,
        task,
        result,
        encode(&outcome, work)?,
        authorize,
        maximum_files,
    )
    .await
}

async fn publish<P, A, Fut>(
    volume: &TaskVolume<P>,
    task: &Component,
    key: FileKey,
    bytes: OwnedBytes,
    authorize: &A,
    maximum_files: usize,
) -> Result<()>
where
    P: StreamProvider,
    A: Fn(bool) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<CommitCondition>>>,
{
    loop {
        let snapshot = volume
            .snapshot(&Query::Requests(task.clone()), maximum_files)
            .await
            .map_err(storage)?;
        if snapshot.files.iter().any(|value| value.key == key) {
            return Ok(());
        }
        match volume
            .append_if(
                snapshot.generation,
                &FileValue::new(key.clone(), bytes.clone()),
                authorize(true).await?,
                None,
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(storage(error)),
        }
    }
}

/// Drives the actual file worker once over scripted task-volume fixtures.
#[cfg(feature = "test-support")]
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
            settle(
                volume,
                task,
                &id,
                fence,
                &mut resolve,
                &authorize,
                work,
                maximum_files,
            )
            .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
