//! G40: drive the actual discovery loop twice, preserving only backend storage.
use super::{Backend, Result, fixtures, observe::Observed, vectors};
use acyclic_actors::local::{Error, State};
use acyclic_harness_core::agent::{AgentHost, AgentPath};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{CommitMutation, StreamError, StreamPath, StreamProvider};
use futures::TryFutureExt as _;
use futures::future::Either;
use std::sync::Arc;

async fn output(
    host: &AgentHost<Observed>,
    agent: &AgentPath,
    index: usize,
    expected: &[u8],
) -> Result {
    use futures::TryStreamExt as _;
    let provider = host.provider();
    let path = host.runtime().store().path(&host.actor(agent)?)?;
    let mut changes = provider
        .follow(acyclic_stream::FollowRequest::new(path, 0))
        .map_ok(acyclic_stream::records_only)
        .await?;
    loop {
        let values = vectors::outputs(&**provider, agent).await?;
        if let Some(value) = values.get(index) {
            if value != expected {
                return Err("wrong discovery output".into());
            }
            return Ok(());
        }
        changes
            .try_next()
            .await?
            .ok_or("actor state follow ended")?;
    }
}

pub(super) async fn until(
    host: &AgentHost<Observed>,
    work: impl std::future::Future<Output = Result>,
) -> Result {
    let (report, failure) = futures::channel::oneshot::channel::<super::Failure>();
    let report = std::sync::Mutex::new(Some(report));
    let running = host.run(|actor, error| {
        if let Some(report) = report
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            let _ = report.send(format!("discovery delivery for {actor:?} failed: {error}").into());
        }
    });
    let work = core::pin::pin!(work);
    let failure = core::pin::pin!(failure);
    let running = core::pin::pin!(running);
    match futures::future::select(running, futures::future::select(work, failure)).await {
        Either::Left((Err(error), _)) => Err(error.into()),
        Either::Left((Ok(never), _)) => match never {},
        Either::Right((Either::Left((result, _)), _)) => result,
        Either::Right((Either::Right((error, _)), _)) => Err(error?),
    }
}

async fn checkpoint(provider: &Observed, schedule: &StreamPath, cut: u64) -> Result {
    // Await the shard's own dispatcher publication, rather than reconstructing
    // its private address or manufacturing its durable state.
    let path = provider
        .committed(|request| {
            request.mutations().find_map(|mutation| match mutation {
                CommitMutation::Append { path, records } => records
                    .iter()
                    .any(|record| {
                        State::decode(record.value.as_ref()).is_ok_and(|state| {
                            state
                                .subscriptions()
                                .get("activation")
                                .is_some_and(|activation| &activation.path == schedule)
                        })
                    })
                    .then(|| path.clone()),
                _ => None,
            })
        })
        .await;
    saved_cursor(provider, path, cut).await
}

async fn saved_cursor(provider: &Observed, path: StreamPath, cut: u64) -> Result {
    use futures::TryStreamExt as _;
    // A captured publication may still be in flight to the backend.
    let mut request = acyclic_stream::FollowRequest::new(path, 0);
    request.when_absent = acyclic_stream::WhenAbsent::Wait;
    let mut changes = acyclic_stream::records_only(provider.follow(request).await?);
    while let Some(record) = changes.try_next().await? {
        if State::decode(record.value.as_ref())?
            .cursor("activation")
            .is_some_and(|cursor| cursor >= cut)
        {
            return Ok(());
        }
    }
    Err("dispatcher state follow ended before its cursor was saved".into())
}

pub(crate) async fn verify(backend: &dyn Backend) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::capturing(storage.open().await?));
    let host = vectors::host(backend, provider.clone())?;
    let agent = host
        .create(&vectors::intent("discovery-root")?, fixtures::adopted())
        .await?;
    // No explicit track after restart: only the dispatcher can rediscover it.
    host.send(&agent, &fixtures::sender(), &fixtures::request("before")?)
        .await?;
    // The agent's wakes land in its stable scheduling shard, which its
    // dispatcher checkpoints.
    let store = host.runtime().store();
    let schedule = store.scheduling_path(store.scheduling_shard(&host.actor(&agent)?))?;
    let mut cut = 0;
    until(&host, async {
        output(&host, &agent, 0, b"before").await?;
        cut = provider.tail(schedule.clone()).await?;
        checkpoint(&provider, &schedule, cut).await
    })
    .await?;
    drop(host);
    drop(provider);
    let reopened = Arc::new(Observed::capturing(storage.open().await?));
    let host = vectors::host(backend, reopened.clone())?;
    host.send(&agent, &fixtures::sender(), &fixtures::request("after")?)
        .await?;
    until(&host, output(&host, &agent, 1, b"after")).await?;
    let follows: Vec<_> = reopened
        .take_follows()
        .into_iter()
        .filter(|(path, _)| path == &schedule)
        .collect();
    // Checkpoints coalesce, so the restart cursor may trail the cut, but it
    // never replays from the shard's opening record.
    if !matches!(follows.as_slice(), [(_, from)] if (1..=cut).contains(from)) {
        return Err(format!(
            "restart replayed activation history instead of its checkpoint: {follows:?}"
        )
        .into());
    }
    cursor_crashes(backend).await
}

async fn cursor_crashes(backend: &dyn Backend) -> Result {
    use super::observe::Fault;
    for fault in [Fault::Before, Fault::After] {
        let storage = backend.fresh().await?;
        let provider = Arc::new(Observed::capturing(storage.open().await?));
        let host = vectors::host(backend, provider.clone())?;
        let agent = host
            .create(&vectors::intent("cursor-crash")?, fixtures::adopted())
            .await?;
        host.send(&agent, &fixtures::sender(), &fixtures::request("once")?)
            .await?;
        // Actor deliveries and dispatcher writes interleave. Select the
        // original cursor publication by its state, never its global ordinal.
        provider.arm_matching(cursor_advance, fault);
        let error = until(&host, std::future::pending())
            .await
            .expect_err("cursor crash did not interrupt discovery");
        let Some(Error::Unknown {
            publication,
            source: StreamError::Unavailable,
        }) = error.downcast_ref::<Error>()
        else {
            return Err(format!("cursor crash lost its original publication: {error}").into());
        };
        let (path, cut) = cursor_publication(publication.request().mutations())
            .ok_or("cursor crash did not retain its original dispatcher publication")?;
        drop(host);
        drop(provider);
        let reopened = Arc::new(Observed::capturing(storage.open().await?));
        let host = vectors::host(backend, reopened.clone())?;
        until(&host, async {
            output(&host, &agent, 0, b"once").await?;
            saved_cursor(&reopened, path.clone(), cut).await
        })
        .await?;
        // A restarted owner recomputes its shard checkpoint, which coalesces
        // at its own idle points. Each candidate is conditioned on the same
        // dispatcher head and keyed by its cursor, so at most one lands.
        if vectors::outputs(&*reopened, &agent).await? != [b"once".to_vec()] {
            return Err("cursor crash duplicated delivery".into());
        }
    }
    Ok(())
}

fn cursor_advance(request: &acyclic_stream::CommitRequest) -> bool {
    cursor_publication(request.mutations()).is_some_and(|(_, cursor)| cursor > 0)
}

fn cursor_publication<'a>(
    mut mutations: impl Iterator<Item = &'a CommitMutation>,
) -> Option<(&'a StreamPath, u64)> {
    mutations.find_map(|mutation| match mutation {
        CommitMutation::Append { path, records } => records.iter().find_map(|record| {
            State::decode(record.value.as_ref())
                .ok()?
                .cursor("activation")
                .map(|cursor| (path, cursor))
        }),
        _ => None,
    })
}
