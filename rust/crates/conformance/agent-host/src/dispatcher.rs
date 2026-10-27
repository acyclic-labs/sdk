//! G40: drive the actual discovery loop twice, preserving only backend storage.
use crate::{Backend, Result, fixtures, observe::Observed, vectors};
use acyclic_harness_core::agent::{ACTORS, AgentHost, AgentPath};
use acyclic_stream::{
    StreamPath, StreamProvider,
    runtime::{Executor as _, PlatformExecutor},
};
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
    let mut changes = provider.follow(path, 0, Default::default()).await?;
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

async fn until(
    host: &AgentHost<Observed>,
    work: impl std::future::Future<Output = Result>,
) -> Result {
    let running = async {
        let running = host.run(|_, _| {});
        let timeout = PlatformExecutor.sleep(5_000);
        futures::pin_mut!(running, timeout);
        match futures::future::select(running, timeout).await {
            Either::Left((result, _)) => {
                Err(format!("dispatcher stopped before observation: {result:?}").into())
            }
            Either::Right(_) => Err("dispatcher observation timed out".into()),
        }
    };
    futures::pin_mut!(work, running);
    match futures::future::select(running, work).await {
        Either::Left((result, _)) => result,
        Either::Right((result, _)) => result,
    }
}

pub(crate) async fn verify(backend: &dyn Backend) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let host = vectors::host(backend, provider.clone())?;
    let agent = host
        .create(&vectors::intent("discovery-root")?, fixtures::adopted())
        .await?;
    // No explicit track after restart: only the dispatcher can rediscover it.
    host.send(&agent, &fixtures::sender(), &fixtures::request("before")?)
        .await?;
    until(&host, output(&host, &agent, 0, b"before")).await?;
    let root = StreamPath::new(ACTORS)?;
    let cut = provider.tail(root.clone()).await?;
    let opened = provider
        .appends()
        .into_iter()
        .find(|request| request.path == root)
        .ok_or("activation open was not attempted")?;
    if opened.idempotency_key.is_none() {
        return Err("activation open was not keyed".into());
    }
    drop(host);
    let reopened = Arc::new(Observed::new(storage.open().await?));
    let host = vectors::host(backend, reopened.clone())?;
    host.send(&agent, &fixtures::sender(), &fixtures::request("after")?)
        .await?;
    until(&host, output(&host, &agent, 1, b"after")).await?;
    let follows: Vec<_> = reopened
        .follows()
        .into_iter()
        .filter(|(path, _)| path == &root)
        .collect();
    if follows != [(root, cut)] {
        return Err(format!(
            "restart replayed activation history instead of its checkpoint: {follows:?}"
        )
        .into());
    }
    if reopened.appends().first() != Some(&opened) {
        return Err("activation reopen changed its request or key".into());
    }
    cursor_crashes(backend).await
}

async fn cursor_crashes(backend: &dyn Backend) -> Result {
    use crate::observe::Fault;
    for fault in [Fault::Before, Fault::After] {
        let storage = backend.fresh().await?;
        let provider = Arc::new(Observed::new(storage.open().await?));
        let host = vectors::host(backend, provider.clone())?;
        let agent = host
            .create(&vectors::intent("cursor-crash")?, fixtures::adopted())
            .await?;
        host.send(&agent, &fixtures::sender(), &fixtures::request("once")?)
            .await?;
        // First commit creates dispatcher state; second advances its cursor.
        provider.arm(1, fault);
        if until(&host, std::future::pending()).await.is_ok() {
            return Err("cursor crash did not interrupt discovery".into());
        }
        let failed = provider
            .commits()
            .last()
            .cloned()
            .ok_or("cursor commit was not attempted")?;
        drop(host);
        let reopened = Arc::new(Observed::new(storage.open().await?));
        let host = vectors::host(backend, reopened.clone())?;
        until(&host, output(&host, &agent, 0, b"once")).await?;
        if matches!(fault, Fault::Before) && reopened.commits().first() != Some(&failed) {
            return Err("uncommitted cursor did not retry its exact request/key".into());
        }
        if vectors::values(&*reopened, agent.conversation()?).await? != [b"once".to_vec()] {
            return Err("cursor crash duplicated delivery".into());
        }
    }
    Ok(())
}
