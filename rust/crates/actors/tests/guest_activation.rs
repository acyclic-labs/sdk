//! Deterministic delivery/checkpoint schedules through the production host adapter.
use acyclic_actors::guest::*;
use bytes::Bytes;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Semaphore;

struct Signals {
    restores: AtomicUsize,
    entered: AtomicUsize,
    finish: Semaphore,
}
fn codec() -> CodecIdentity {
    CodecIdentity {
        name: "counter".into(),
        version: "1".into(),
    }
}
fn definition() -> ActorDefinition<u8, Arc<Signals>> {
    ActorDefinition {
        abi_version: GUEST_ABI_VERSION,
        codec: CheckpointCodec {
            identity: codec(),
            readable: vec![codec()],
            encode: |value| Ok(Bytes::copy_from_slice(&[*value])),
            decode: |_| Ok(0),
        },
        restore: |_, context| {
            Box::pin(async move {
                context.env.restores.fetch_add(1, Ordering::SeqCst);
                Ok(0)
            })
        },
        event: |state, _, context| {
            Box::pin(async move {
                context.env.entered.fetch_add(1, Ordering::SeqCst);
                context
                    .env
                    .finish
                    .acquire()
                    .await
                    .map_err(|e| GuestError::Handler(e.to_string()))?
                    .forget();
                state.with(|value| *value += 1)?;
                Ok(None)
            })
        },
        checkpoint: |state, _| {
            Box::pin(async move {
                let mut value = 0;
                state.with(|current| value = *current)?;
                Ok(Checkpoint {
                    codec: codec(),
                    bytes: Bytes::copy_from_slice(&[value]),
                })
            })
        },
        fetch: |_, _, _| Box::pin(async { Err(GuestError::Handler("unused fetch".into())) }),
    }
}
fn event() -> GuestEvent {
    GuestEvent::Invoke {
        payload: Bytes::new(),
    }
}

#[tokio::test]
async fn initialization_once_interleaved_handlers_and_quiesced_checkpoint() -> Result<(), GuestError>
{
    let signals = Arc::new(Signals {
        restores: AtomicUsize::new(0),
        entered: AtomicUsize::new(0),
        finish: Semaphore::new(0),
    });
    let activation = GuestActivation::new(
        definition(),
        None,
        GuestContext::builder(signals.clone(), Box::new(|| 0)).build(),
    )?;
    assert_eq!(signals.restores.load(Ordering::SeqCst), 0);
    let context = GuestContext::builder(signals.clone(), Box::new(|| 0))
        .delivery("delivery".into())
        .build();
    let mut first = Box::pin(activation.event(event(), &context));
    assert!(futures::poll!(first.as_mut()).is_pending());
    let mut second = Box::pin(activation.event(event(), &context));
    assert!(futures::poll!(second.as_mut()).is_pending());
    // Negative control for serializing entire handlers: both must enter before either finishes.
    assert_eq!(signals.entered.load(Ordering::SeqCst), 2);
    assert_eq!(signals.restores.load(Ordering::SeqCst), 1);
    let mut checkpoint = Box::pin(activation.checkpoint());
    assert!(futures::poll!(checkpoint.as_mut()).is_pending());
    let mut third = Box::pin(activation.event(event(), &context));
    assert!(futures::poll!(third.as_mut()).is_pending());
    assert_eq!(signals.entered.load(Ordering::SeqCst), 2);
    signals.finish.add_permits(2);
    first.await?;
    second.await?;
    assert_eq!(checkpoint.await?.bytes.as_ref(), &[2]);
    assert!(futures::poll!(third.as_mut()).is_pending());
    assert_eq!(signals.entered.load(Ordering::SeqCst), 3);
    signals.finish.add_permits(1);
    third.await?;
    Ok(())
}

#[test]
fn activation_rejects_same_local_id_in_another_admitted_namespace()
-> Result<(), Box<dyn std::error::Error>> {
    let signals = Arc::new(Signals {
        restores: AtomicUsize::new(0),
        entered: AtomicUsize::new(0),
        finish: Semaphore::new(0),
    });
    let caller = AdmittedCaller::new(
        "account".into(),
        "org".into(),
        "principal".into(),
        "credential".into(),
        "same".into(),
        1,
    )?
    .with_namespace("ClassOne".into())?;
    let mut runtime = ActorRuntime::default();
    runtime.account = Some("account".into());
    runtime.id = Some("same".into());
    runtime.namespace = Some("ClassTwo".into());
    assert_eq!(runtime.id.as_deref(), Some(caller.actor_id())); // Old local comparison aliases.
    let context = GuestContext::builder(signals.clone(), Box::new(|| 0))
        .actor(runtime.clone())
        .admitted_identity(caller.clone())
        .build();
    assert!(GuestActivation::new(definition(), None, context).is_err());
    assert_eq!(signals.restores.load(Ordering::SeqCst), 0);
    runtime.namespace = Some("ClassOne".into());
    let context = GuestContext::builder(signals, Box::new(|| 0))
        .actor(runtime)
        .admitted_identity(caller)
        .build();
    assert!(GuestActivation::new(definition(), None, context).is_ok());
    Ok(())
}
