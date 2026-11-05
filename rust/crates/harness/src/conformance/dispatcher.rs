//! G40: drive the actual discovery loop twice, preserving only backend storage.
use super::{Backend, Result, fixtures, observe::Observed, vectors};
use acyclic_actors::local::{Error, State};
use acyclic_harness_core::agent::{ACTORS, AgentHost, AgentPath};
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
        let values = vectors::values(&**provider, agent.conversation()?).await?;
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

async fn checkpoint(provider: &Observed, cut: u64) -> Result {
    // Locate the original dispatcher publication, rather than reconstructing
    // its private address or manufacturing its durable state.
    let path = provider
        .take_commits()
        .iter()
        .find_map(cursor_publication)
        .map(|(path, _)| path.clone())
        .ok_or("dispatcher state was not published")?;
    saved_cursor(provider, path, cut).await
}

async fn saved_cursor(provider: &Observed, path: StreamPath, cut: u64) -> Result {
    use futures::TryStreamExt as _;
    let mut changes = acyclic_stream::records_only(
        provider
            .follow(acyclic_stream::FollowRequest::new(path, 0))
            .await?,
    );
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
    let root = StreamPath::new(ACTORS)?;
    let mut cut = 0;
    until(&host, async {
        output(&host, &agent, 0, b"before").await?;
        cut = provider.tail(root.clone()).await?;
        checkpoint(&provider, cut).await
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
        .filter(|(path, _)| path == &root)
        .collect();
    // Checkpoints coalesce, so the restart cursor may trail the cut, but it
    // never replays from the log's opening record.
    if !matches!(follows.as_slice(), [(path, from)] if *path == root && (1..=cut).contains(from)) {
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
        let failed = publication.request().clone();
        let (path, cut) = cursor_publication(&failed)
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
        if matches!(fault, Fault::Before)
            && reopened
                .take_commits()
                .iter()
                .find(|request| cursor_advance(request))
                != Some(&failed)
        {
            return Err("uncommitted cursor did not retry its exact request/key".into());
        }
        if vectors::values(&*reopened, agent.conversation()?).await? != [b"once".to_vec()] {
            return Err("cursor crash duplicated delivery".into());
        }
    }
    Ok(())
}

fn cursor_advance(request: &acyclic_stream::CommitRequest) -> bool {
    cursor_publication(request).is_some_and(|(_, cursor)| cursor > 0)
}

fn cursor_publication(request: &acyclic_stream::CommitRequest) -> Option<(&StreamPath, u64)> {
    request
        .mutations
        .iter()
        .find_map(|mutation| match mutation {
            CommitMutation::Append { path, records } => records.iter().find_map(|record| {
                State::decode(record.value.as_ref())
                    .ok()?
                    .cursor("activation")
                    .map(|cursor| (path, cursor))
            }),
            _ => None,
        })
}
