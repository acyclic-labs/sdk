//! The same in-flight interrupt vector for every public AgentHost backend.
use super::{
    Backend, Result, fixtures,
    observe::{Fault, Observed},
    vectors,
};
use acyclic_actors::guest::CancellationToken;
use acyclic_harness_core::agent::*;
use futures::channel::oneshot;
use std::sync::{Arc, Mutex};

struct Waiting(Mutex<Option<oneshot::Sender<()>>>);

impl TurnDriver for Waiting {
    fn step<'a>(
        &'a self,
        _agent: &'a AgentState,
        _cursor: u64,
        input: Option<Queued>,
        cancellation: CancellationToken,
    ) -> TurnFuture<'a> {
        let started = self
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return Err(Error::Record(
                    "delivery inherited another turn's cancellation",
                ));
            }
            let output = match started {
                Some(started) => {
                    started
                        .send(())
                        .map_err(|_| Error::Record("interrupt observer dropped"))?;
                    cancellation.cancelled().await;
                    b"cancelled".to_vec()
                }
                None => match input.map(|queued| queued.input) {
                    Some(
                        TurnInput::Message { id, .. } | TurnInput::ApprovalRequested { id, .. },
                    ) => id.as_str().as_bytes().to_vec(),
                    _ => return Err(Error::Record("unexpected interrupt fixture input")),
                },
            };
            Ok(TurnStep {
                checkpoint: None,
                reserved_children: 0,
                drops: None,
                outs: vec![Out::Conversation(output)],
                wait: Wait::Input,
            })
        })
    }
}

pub(crate) async fn verify(backend: &dyn Backend) -> Result {
    let error = interrupted(backend, Some(Fault::Before))
        .await
        .expect_err("pre-turn provider loss did not terminate the interrupt vector");
    if !matches!(
        error.downcast_ref::<acyclic_actors::local::Error>(),
        Some(acyclic_actors::local::Error::Stream(
            acyclic_stream::StreamError::Unavailable
        ))
    ) {
        return Err(format!("pre-turn provider loss changed its original cause: {error}").into());
    }
    interrupted(backend, None).await
}

async fn interrupted(backend: &dyn Backend, fault: Option<Fault>) -> Result {
    use acyclic_stream::runtime::{Executor as _, PlatformExecutor};
    let work = drive(backend, fault);
    let timeout = PlatformExecutor.sleep(5_000);
    futures::pin_mut!(work, timeout);
    match futures::future::select(work, timeout).await {
        futures::future::Either::Left((result, _)) => result,
        futures::future::Either::Right(_) => Err("runtime interrupt timed out".into()),
    }
}

async fn drive(backend: &dyn Backend, fault: Option<Fault>) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let (started, running) = oneshot::channel();
    let handler = backend.handler(Arc::new(AgentHandler::new(
        actor_store(&provider)?,
        Waiting(Mutex::new(Some(started))),
        vectors::limits()?,
    )?))?;
    let host = vectors::hosting(provider.clone(), handler, None)?;
    let agent = host
        .create(&vectors::intent("interrupt-create")?, fixtures::adopted())
        .await?;
    vectors::drain(&host).await?;
    let send = |id: &str| -> Result<InboxRecord> { fixtures::request(id) };
    host.send(&agent, &fixtures::sender(), &send("running")?)
        .await?;
    if let Some(fault) = fault {
        provider.crash_at(provider.calls(), fault);
    }
    let controls = async {
        running.await?;
        host.send(&agent, &fixtures::sender(), &send("queued")?)
            .await?;
        host.send(
            &agent,
            &fixtures::sender(),
            &InboxRecord::Cancel(Control {
                by: fixtures::sender(),
                id: vectors::intent("stop")?,
            }),
        )
        .await?;
        host.send(&agent, &fixtures::sender(), &send("after")?)
            .await?;
        Result::Ok(())
    };
    let (drained, controls) = futures::join!(vectors::drain(&host), controls);
    drained?;
    controls?;
    let outputs = vectors::values(&*provider, agent.conversation()?).await?;
    if outputs != [b"cancelled".to_vec(), b"queued".to_vec(), b"after".to_vec()] {
        return Err(format!("interrupt lost or repeated committed inputs: {outputs:?}").into());
    }
    Ok(())
}
