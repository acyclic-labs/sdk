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

#[cfg(not(target_arch = "wasm32"))]
#[tokio::test]
async fn credited_fetch_enters_before_eof_and_retires_original_transport_owners()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_stream::body::BodyChunk;
    use futures::{StreamExt as _, channel::mpsc};
    use http_body::{Body as _, Frame};
    use http_body_util::BodyExt as _;

    // These are the fixture's original, finite byte and receive-frame issuers.
    // The product adapter is given their existing admission, never a new root.
    let capacity = Arc::new(Semaphore::new(4 * 1024 * 1024));
    let credit = Arc::new(Semaphore::new(1));
    let baseline = capacity.available_permits();
    let mut transport_control = None;
    for (length, denied) in [(1024, false), (1024 * 1024, false), (1024, true)] {
        let (sender, frames) = mpsc::unbounded();
        let inspected = Arc::new(AtomicUsize::new(0));
        let observed = inspected.clone();
        let frames = frames.inspect(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
        });
        let payload = Bytes::from(vec![7; length]);
        let pointer = payload.as_ptr();
        sender.unbounded_send(Ok(Frame::data(BodyChunk::new(
            payload,
            (
                credit.clone().try_acquire_owned()?,
                capacity.clone().try_acquire_many_owned(length as u32)?,
            ),
        ))))?;
        let original = capacity.clone();
        let mut body = None;
        let adapter = acyclic_test_support::measure(|| {
            body = Some(GuestBody::from_credited(
                http_body_util::StreamBody::new(frames),
                move |bytes| {
                    original
                        .clone()
                        .try_acquire_many_owned(bytes.try_into().expect("fixed control fits u32"))
                        .map_err(|_| GuestError::Handler("original byte credit refused".into()))
                },
            ));
        });
        assert_eq!(adapter.count_total, 1);
        assert_eq!(
            baseline - capacity.available_permits(),
            length + adapter.bytes_total as usize
        );
        assert_eq!(inspected.load(Ordering::SeqCst), 0);
        let body = body.ok_or("missing body construction")??;

        let signals = Arc::new(Signals {
            restores: AtomicUsize::new(0),
            entered: AtomicUsize::new(0),
            finish: Semaphore::new(0),
        });
        let mut definition = definition();
        definition.fetch = |_, mut request, context| {
            Box::pin(async move {
                context.env.entered.fetch_add(1, Ordering::SeqCst);
                let frame = request
                    .body_mut()
                    .frame()
                    .await
                    .ok_or_else(|| GuestError::Handler("missing original data".into()))??;
                let data = frame
                    .into_data()
                    .map_err(|_| GuestError::Handler("expected original data".into()))?;
                // Return immediately while the request producer still withholds EOF.
                Ok(http::Response::new(GuestBody::Buffered(data)))
            })
        };
        let activation = GuestActivation::new(
            definition,
            None,
            GuestContext::builder(signals.clone(), Box::new(|| 0)).build(),
        )?;
        let mut runtime = ActorRuntime::default();
        if denied {
            runtime.account = Some(b"other-account".to_vec());
            runtime.namespace = Some("other-class".into());
            runtime.id = Some("other-actor".into());
        }
        let context = GuestContext::builder(signals.clone(), Box::new(|| 0))
            .actor(runtime)
            .delivery("original-fetch".into())
            .build();
        let request = http::Request::new(body);
        let mut delivery = Box::pin(activation.fetch(request, &context));
        let response = match futures::poll!(delivery.as_mut()) {
            std::task::Poll::Ready(response) => response,
            std::task::Poll::Pending => return Err("fetch waited for withheld EOF".into()),
        };
        drop(delivery);
        if denied {
            assert!(response.is_err());
            assert_eq!(signals.entered.load(Ordering::SeqCst), 0);
            assert_eq!(signals.restores.load(Ordering::SeqCst), 0);
            assert_eq!(inspected.load(Ordering::SeqCst), 0);
            assert!(sender.is_closed());
            assert_eq!(credit.available_permits(), 1);
            assert_eq!(capacity.available_permits(), baseline);
            continue;
        }
        let mut response = response?;
        assert_eq!(signals.entered.load(Ordering::SeqCst), 1);
        assert_eq!(inspected.load(Ordering::SeqCst), 1);
        assert!(
            sender.is_closed(),
            "return drops the same unread request source"
        );
        assert_eq!(credit.available_permits(), 0);
        let data = response.body_mut().frame().await.ok_or("missing echo")??;
        let data = data.into_data().map_err(|_| "expected echo data")?;
        assert_eq!(data.as_ptr(), pointer);
        let held = baseline - capacity.available_permits();
        let control = held - length;
        if let Some(previous) = transport_control {
            assert_eq!(control, previous);
        }
        transport_control = Some(control);
        let mut part = None;
        let sharing = acyclic_test_support::measure(|| {
            part = Some(data.clone().slice(1..length - 1));
        });
        assert_eq!(sharing.count_total, 0);
        let copied = acyclic_test_support::measure(|| {
            std::hint::black_box(data.to_vec());
        });
        assert!(copied.bytes_total >= length as u64);
        drop(response);
        drop(data);
        assert_eq!(credit.available_permits(), 0);
        assert_eq!(baseline - capacity.available_permits(), held);
        let part = part.ok_or("missing retained slice")?;
        assert_eq!(part.as_ptr(), pointer.wrapping_add(1));
        assert!(part.iter().all(|byte| *byte == 7));
        drop(part);
        assert_eq!(credit.available_permits(), 1);
        assert_eq!(capacity.available_permits(), baseline);
    }

    // Withheld EOF exposes the old eager collector as a deterministic failure.
    let (sender, mut source) = mpsc::unbounded::<Result<Frame<BodyChunk<()>>, GuestError>>();
    sender.unbounded_send(Ok(Frame::data(BodyChunk::new(
        Bytes::from_static(b"x"),
        (),
    ))))?;
    let mut collect = Box::pin(async move { while source.next().await.is_some() {} });
    assert!(futures::poll!(collect.as_mut()).is_pending());
    drop(collect);
    assert!(sender.is_closed());

    // The original terminal and duplicate trailers pass through unchanged. A
    // later queued error cannot replace the original cause or resume the body.
    let (sender, frames) = mpsc::unbounded();
    let original = capacity.clone();
    let mut body =
        GuestBody::from_credited(http_body_util::StreamBody::new(frames), move |bytes| {
            original
                .clone()
                .try_acquire_many_owned(bytes.try_into().expect("fixed control fits u32"))
                .map_err(|_| GuestError::Handler("original byte credit refused".into()))
        })?;
    let payload = Bytes::from(vec![9; 1024]);
    let pointer = payload.as_ptr();
    sender.unbounded_send(Ok(Frame::data(BodyChunk::new(
        payload,
        credit.clone().try_acquire_owned()?,
    ))))?;
    let mut frame = None;
    let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
    let transfer = acyclic_test_support::measure(|| {
        frame = Some(std::pin::Pin::new(&mut body).poll_frame(&mut cx));
    });
    let frame = match frame.ok_or("missing frame poll")? {
        std::task::Poll::Ready(Some(Ok(frame))) => frame,
        _ => return Err("original body data was not ready".into()),
    };
    let data = frame.into_data().map_err(|_| "expected credited data")?;
    assert_eq!(data.as_ptr(), pointer);
    assert_eq!(transfer.count_total, 1);
    assert_eq!(
        transfer.bytes_total as usize,
        acyclic_auth::operation::bytes_owner_allocation_bytes::<
            BodyChunk<(
                tokio::sync::OwnedSemaphorePermit,
                tokio::sync::OwnedSemaphorePermit
            )>,
        >()
    );
    let mut trailers = http::HeaderMap::new();
    trailers.append("x-terminal", "a".parse()?);
    trailers.append("x-terminal", "b".parse()?);
    sender.unbounded_send(Ok(Frame::trailers(trailers.clone())))?;
    let frame = body.frame().await.ok_or("missing original trailers")??;
    assert_eq!(frame.trailers_ref(), Some(&trailers));
    sender.unbounded_send(Err(GuestError::MissingIdentity))?;
    sender.unbounded_send(Err(GuestError::IncompatibleCodec))?;
    assert_eq!(
        body.frame().await.ok_or("missing original error")?.err(),
        Some(GuestError::MissingIdentity)
    );
    assert!(sender.is_closed());
    assert!(body.frame().await.is_none());
    assert_eq!(credit.available_permits(), 0);
    drop(body);
    assert_eq!(credit.available_permits(), 0);
    drop(data);
    assert_eq!(credit.available_permits(), 1);
    assert_eq!(capacity.available_permits(), baseline);

    for refuse_adapter in [false, true] {
        let original = Arc::new(Semaphore::new(4096));
        let (sender, frames) = mpsc::unbounded();
        sender.unbounded_send(Ok(Frame::data(BodyChunk::new(
            Bytes::from_static(b"unread"),
            credit.clone().try_acquire_owned()?,
        ))))?;
        let issuer = original.clone();
        if refuse_adapter {
            original.close();
        }
        let mut body = None;
        let construction = acyclic_test_support::measure(|| {
            body = Some(GuestBody::from_credited(
                http_body_util::StreamBody::new(frames),
                move |bytes| {
                    issuer
                        .clone()
                        .try_acquire_many_owned(bytes.try_into().expect("fixed control fits u32"))
                        .map_err(|_| GuestError::MissingIdentity)
                },
            ));
        });
        let body = body.ok_or("missing admission result")?;
        if refuse_adapter {
            assert_eq!(construction.count_total, 0);
            assert!(matches!(body, Err(GuestError::MissingIdentity)));
        } else {
            let mut body = body?;
            original.close();
            let mut frame = None;
            let refusal = acyclic_test_support::measure(|| {
                frame = Some(std::pin::Pin::new(&mut body).poll_frame(&mut cx));
            });
            assert_eq!(refusal.count_total, 0);
            assert!(matches!(
                frame,
                Some(std::task::Poll::Ready(Some(Err(
                    GuestError::MissingIdentity
                ))))
            ));
            assert!(body.frame().await.is_none());
        }
        assert!(sender.is_closed());
        assert_eq!(credit.available_permits(), 1);
        assert_eq!(original.available_permits(), 4096);
    }
    Ok(())
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
fn activation_refuses_other_checkpoint_versions_before_restore() {
    let signals = Arc::new(Signals {
        restores: AtomicUsize::new(0),
        entered: AtomicUsize::new(0),
        finish: Semaphore::new(0),
    });
    for version in ["0", "2", "unknown"] {
        let mut codec = codec();
        codec.version = version.into();
        let checkpoint = Checkpoint {
            codec,
            bytes: Bytes::new(),
        };
        let context = GuestContext::builder(signals.clone(), Box::new(|| 0)).build();
        assert!(matches!(
            GuestActivation::new(definition(), Some(checkpoint), context),
            Err(GuestError::IncompatibleCodec)
        ));
    }
    assert_eq!(signals.restores.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn activation_refuses_emitting_another_checkpoint_format() -> Result<(), GuestError> {
    let signals = Arc::new(Signals {
        restores: AtomicUsize::new(0),
        entered: AtomicUsize::new(0),
        finish: Semaphore::new(0),
    });
    let mut definition = definition();
    definition.checkpoint = |_, _| {
        Box::pin(async {
            let mut codec = codec();
            codec.version = "2".into();
            Ok(Checkpoint {
                codec,
                bytes: Bytes::new(),
            })
        })
    };
    let context = GuestContext::builder(signals.clone(), Box::new(|| 0)).build();
    let activation = GuestActivation::new(definition, None, context)?;
    assert_eq!(
        activation.checkpoint().await,
        Err(GuestError::IncompatibleCodec)
    );
    assert_eq!(signals.restores.load(Ordering::SeqCst), 1);
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
        let (pairs, remainder) = text.as_bytes().as_chunks::<2>();
        if !remainder.is_empty() {
            return Err("odd hex field".into());
        }
        pairs
            .iter()
            .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
            .collect()
    };
    for vector in vectors
        .get("actors")
        .and_then(serde_json::Value::as_array)
        .ok_or("actor vectors")?
    {
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
            *raw.get_mut("accountId").ok_or("caller account")? =
                serde_json::json!(String::from_utf8_lossy(&account));
            assert!(serde_json::from_value::<AdmittedCaller>(raw).is_err());
            let mut raw = serde_json::to_value(&runtime)?;
            *raw.get_mut("account").ok_or("runtime account")? =
                serde_json::json!(String::from_utf8_lossy(&account));
            assert!(serde_json::from_value::<ActorRuntime>(raw).is_err());
        }
    }
    Ok(())
}
