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
        b"account".to_vec(),
        "org".into(),
        "principal".into(),
        "credential".into(),
        "same".into(),
        1,
    )?
    .with_namespace("ClassOne".into())?;
    let mut runtime = ActorRuntime::default();
    runtime.account = Some(b"account".to_vec());
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

#[test]
fn opaque_account_runtime_and_caller_use_the_shared_actor_vectors()
-> Result<(), Box<dyn std::error::Error>> {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../auth/tests/fixtures/resource-identity-v1.json"
    ))?;
    let decode = |value: &serde_json::Value| -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let text = value.as_str().ok_or("hex field")?;
        (0..text.len())
            .step_by(2)
            .map(|i| Ok(u8::from_str_radix(&text[i..i + 2], 16)?))
            .collect()
    };
    for vector in vectors["actors"].as_array().ok_or("actor vectors")? {
        let account = decode(&vector["account_hex"])?;
        let namespace = String::from_utf8(decode(&vector["namespace_hex"])?)?;
        let id = String::from_utf8(decode(&vector["id_hex"])?)?;
        let mut runtime = ActorRuntime::default();
        runtime.account = Some(account.clone());
        runtime.namespace = Some(namespace.clone());
        runtime.id = Some(id.clone());
        let runtime: ActorRuntime = serde_json::from_slice(&serde_json::to_vec(&runtime)?)?;
        let caller = AdmittedCaller::new(
            account.clone(),
            "org".into(),
            "principal".into(),
            "credential".into(),
            id,
            1,
        )?
        .with_namespace(namespace)?;
        let caller: AdmittedCaller = serde_json::from_slice(&serde_json::to_vec(&caller)?)?;
        let reference = runtime.reference()?.ok_or("actor reference")?;
        assert_eq!(reference.encode(), decode(&vector["encoded_hex"])?);
        assert_eq!(caller.reference()?, reference);
        assert_eq!(caller.account_id(), account);
        let signals = Arc::new(Signals {
            restores: AtomicUsize::new(0),
            entered: AtomicUsize::new(0),
            finish: Semaphore::new(0),
        });
        let context = GuestContext::builder(signals.clone(), Box::new(|| 0))
            .actor(runtime.clone())
            .admitted_identity(caller.clone())
            .build();
        assert!(GuestActivation::new(definition(), None, context).is_ok());
        assert_eq!(signals.restores.load(Ordering::SeqCst), 0);
        // Negative control: treating account bytes as UTF-8 text loses opaque identities.
        if std::str::from_utf8(&account).is_err() {
            assert_ne!(String::from_utf8_lossy(&account).as_bytes(), account);
            let mut raw = serde_json::to_value(&caller)?;
            raw["accountId"] = serde_json::json!(String::from_utf8_lossy(&account));
            assert!(serde_json::from_value::<AdmittedCaller>(raw).is_err());
            let mut raw = serde_json::to_value(&runtime)?;
            raw["account"] = serde_json::json!(String::from_utf8_lossy(&account));
            assert!(serde_json::from_value::<ActorRuntime>(raw).is_err());
        }
    }
    Ok(())
}
