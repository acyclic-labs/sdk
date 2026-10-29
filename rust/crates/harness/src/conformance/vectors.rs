use crate::conformance::{
    Backend, Result,
    fixtures::{Driver, adopted, request, sender},
    observe::{Fault, Observed},
};
use acyclic_actors::{
    addressing::ActorReference, domain::ActorLimits, guest::Handler, local::SystemClock,
};
use acyclic_harness_core::agent::*;
use acyclic_stream::{ReadRequest, StreamError, StreamPath, StreamProvider};
use futures::TryStreamExt;
use std::sync::Arc;

pub(crate) fn intent(value: &str) -> Result<IntentId> {
    Ok(IntentId::try_from(value)?)
}

pub(crate) fn limits() -> Result<ActorLimits> {
    Ok(ActorLimits::new(1_000, 1 << 20, 1 << 16)?)
}

pub(crate) fn handler(provider: &Arc<Observed>) -> Result<Arc<dyn Handler>> {
    Ok(Arc::new(AgentHandler::new(
        actor_store(provider)?,
        Driver,
        limits()?,
    )?))
}

pub(crate) fn host(backend: &dyn Backend, provider: Arc<Observed>) -> Result<AgentHost<Observed>> {
    let handler = backend.handler(handler(&provider)?)?;
    hosting(provider, handler, None)
}

pub(crate) fn hosting(
    provider: Arc<Observed>,
    handler: Arc<dyn Handler>,
    composer: Option<Arc<dyn acyclic_actors::guest::Composer>>,
) -> Result<AgentHost<Observed>> {
    Ok(AgentHost::hosting(
        provider,
        ActorReference::new(b"conformance", b"agents", b"host")?,
        handler,
        Arc::new(SystemClock),
        composer,
    )?)
}

pub(crate) async fn drain(host: &AgentHost<Observed>) -> Result {
    let failures = host.runtime().drain().await;
    if !failures.is_empty() {
        return Err(format!("AgentHost drain: {failures:?}").into());
    }
    Ok(())
}

pub(crate) async fn values(
    provider: &dyn StreamProvider,
    path: StreamPath,
) -> Result<Vec<Vec<u8>>> {
    match provider
        .read(ReadRequest {
            key: None,
            path,
            from: 0,
            limit: 128,
            at_least: Default::default(),
        })
        .await
    {
        Ok(records) => Ok(records
            .map_ok(|record| record.value.to_vec())
            .try_collect()
            .await?),
        Err(StreamError::NotFound) => Ok(Vec::new()),
        Err(error) => Err(error.into()),
    }
}

pub(crate) async fn controls(backend: &dyn Backend) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let host = host(backend, provider.clone())?;
    let root = host.create(&intent("create")?, adopted()).await?;
    if host.create(&intent("create")?, adopted()).await? != root {
        return Err("creation replay changed identity".into());
    }
    drain(&host).await?;
    let record = request("once")?;
    let delivered = host.send(&root, &sender(), &record).await?;
    host.drain(&root).await?;
    if host.send(&root, &sender(), &record).await? != delivered {
        return Err("send replay changed receipt".into());
    }
    host.drain(&root).await?;
    if values(&*provider, root.conversation()?).await? != [b"once".to_vec()] {
        return Err("turn did not commit exactly once".into());
    }
    let Delivered::Committed { inbox } = host.send(&root, &sender(), &request("again")?).await?
    else {
        return Err("second input was refused".into());
    };
    host.drain(&root).await?;
    let statuses = values(&*provider, root.status()?).await?;
    let status = StatusRecord::decode(statuses.last().ok_or("settled turn omitted its status")?)?;
    if status.phase != Phase::Idle || status.cursor <= inbox {
        return Err("settled turn status does not cover its consumed input".into());
    }
    let mut changed = adopted();
    changed.brief = Some(prompt()?);
    if !matches!(
        host.create(&intent("create")?, changed).await,
        Err(Error::Stream(StreamError::IdempotencyMismatch))
    ) {
        return Err("changed creation prompt was not a conflict".into());
    }
    host.archive(&root, &sender(), &intent("archive")?).await?;
    if host.send(&root, &sender(), &request("late")?).await? != Delivered::Closed {
        return Err("archived recipient accepted input".into());
    }
    Ok(())
}

pub(crate) fn prompt() -> Result<acyclic_harness_core::conversation::FileRef> {
    use acyclic_harness_core::{conversation::*, resources::ProviderRef};
    let provider = ProviderRef::new(
        "conformance".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let volume = VolumeRef::new(
        provider,
        "private".into(),
        VolumeClass::AgentPrivate,
        VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes([1; 16])),
        &mut None,
    )?;
    Ok(FileRef::new(
        volume,
        "prompt".into(),
        "v1".into(),
        FileDescriptor::from_bytes(b"changed", "text/plain", &mut None)?,
        "prompt".into(),
        &mut None,
    )?)
}

pub(crate) async fn spawn_cut(backend: &dyn Backend) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let host = host(backend, provider.clone())?;
    let root = host.create(&intent("create")?, adopted()).await?;
    drain(&host).await?;
    host.send(&root, &sender(), &request("before")?).await?;
    drain(&host).await?;
    host.send(&root, &sender(), &request("spawn")?).await?;
    // Input beyond the saved cursor must not appear in the child's prefix.
    host.send(&root, &sender(), &request("after")?).await?;
    drain(&host).await?;
    let child = root.child(&AgentId::derive(&root.id(), "child"))?;
    let state = host.state(&child).await?;
    if state.parent.as_ref() != Some(&root) || state.conversation_tail != 1 {
        return Err("child adoption differs from the saved cut".into());
    }
    if values(&*provider, child.conversation()?).await? != [b"before".to_vec()] {
        return Err("spawn inherited a later conversation cut".into());
    }
    let parent_inbox = values(&*provider, root.inbox()?).await?;
    let child_inbox = values(&*provider, child.inbox()?).await?;
    let (Some(prefix), Some(inherited)) = (parent_inbox.get(..3), child_inbox.get(..3)) else {
        return Err("spawn omitted its saved inbox prefix".into());
    };
    if prefix != inherited || child_inbox.len() != 4 {
        return Err("spawn inherited input beyond its saved cursor".into());
    }
    let snapshot = actor_store(&provider)?.load(&host.actor(&child)?).await?;
    if snapshot
        .state()
        .and_then(|state| state.checkpoint())
        .is_some()
    {
        return Err("child ran eagerly before being hosted".into());
    }
    host.send(&root, &sender(), &request("respawn")?).await?;
    if host.runtime().drain().await.is_empty() {
        return Err("respawn with a changed prompt was not a conflict".into());
    }
    if host.state(&child).await? != state
        || values(&*provider, child.inbox()?).await? != child_inbox
    {
        return Err("conflicting respawn changed the saved child".into());
    }
    Ok(())
}

async fn program(backend: &dyn Backend, provider: Arc<Observed>) -> Result<AgentState> {
    let host = host(backend, provider)?;
    let root = host.create(&intent("create")?, adopted()).await?;
    host.send(&root, &sender(), &request("once")?).await?;
    drain(&host).await?;
    Ok(host.state(&root).await?)
}

pub(crate) async fn restart(backend: &dyn Backend) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let expected = program(backend, provider.clone()).await?;
    let steps = provider.calls();
    drop(provider);
    for at in 0..steps {
        for fault in [Fault::Before, Fault::After] {
            let storage = backend.fresh().await?;
            let provider = Arc::new(Observed::new(storage.open().await?));
            provider.crash_at(at, fault);
            if program(backend, provider.clone()).await.is_ok() {
                return Err(format!("crash boundary {at}/{fault:?} was not exercised").into());
            }
            drop(provider);
            let reopened = Arc::new(Observed::new(storage.open().await?));
            let actual = program(backend, reopened.clone()).await?;
            if actual != expected {
                return Err(format!("reopen diverged at {at}/{fault:?}").into());
            }
            if values(&*reopened, actual.path.conversation()?).await? != [b"once".to_vec()] {
                return Err(format!("reopen duplicated an effect at {at}/{fault:?}").into());
            }
        }
    }
    Ok(())
}
