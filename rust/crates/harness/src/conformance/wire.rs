use crate::conformance::{Backend, Result, fixtures, observe::Observed, vectors};
use acyclic_actors::guest::{
    self, Checkpoint, Delivery, GuestFuture, Handler, Manifest, Transition, wire,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub(crate) fn remote(handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>> {
    let manifest = wire::decode_manifest(&wire::encode_manifest(handler.manifest()))?;
    let interrupt_handler = handler.clone();
    Ok(Arc::new(wire::Remote::new(
        manifest,
        move |_request, bytes: Vec<u8>, cancellation| -> GuestFuture<'static, Vec<u8>> {
            let handler = handler.clone();
            Box::pin(async move {
                let (delivery, checkpoint) = wire::decode_delivery(&bytes)?;
                let transition = handler
                    .on_record(delivery, checkpoint.as_ref(), cancellation)
                    .await?;
                wire::encode_transition(&transition)
            })
        },
        move |_request, query| -> acyclic_actors::guest::GuestFuture<'static, bool> {
            let handler = interrupt_handler.clone();
            Box::pin(async move { wire::accepts_interrupt(handler.as_ref(), &query).await })
        },
    )))
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// A guest whose every turn stalls. Each turn counts its start and its drop,
/// and records whether an earlier turn was still alive when it started.
struct Stalled {
    manifest: Manifest,
    started: Arc<AtomicUsize>,
    aborted: Arc<AtomicUsize>,
    overlapped: Arc<AtomicUsize>,
}
impl Handler for Stalled {
    fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    fn on_record<'a>(
        &'a self,
        _: Delivery,
        _: Option<&'a Checkpoint>,
        _cancellation: acyclic_actors::guest::CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        // The runtime admits one turn per actor at a time, and a timed-out
        // input is attempted again. So every earlier turn must already have
        // been dropped by its timeout when the next one starts.
        let earlier = self.started.fetch_add(1, Ordering::SeqCst);
        if self.aborted.load(Ordering::SeqCst) != earlier {
            self.overlapped.fetch_add(1, Ordering::SeqCst);
        }
        let aborted = Dropped(self.aborted.clone());
        Box::pin(async move {
            let _aborted = aborted;
            futures::future::pending().await
        })
    }
}

pub(crate) async fn verify(backend: &dyn Backend) -> Result {
    let storage = backend.fresh().await?;
    let provider = Arc::new(Observed::new(storage.open().await?));
    let handler = vectors::handler(&provider)?;
    strict(handler.manifest())?;
    let mut manifest = wire::decode_manifest(&wire::encode_manifest(handler.manifest()))?;
    manifest.limits = acyclic_actors::domain::ActorLimits::new(1, 1 << 20, 1 << 16)?;
    let started = Arc::new(AtomicUsize::new(0));
    let aborted = Arc::new(AtomicUsize::new(0));
    let overlapped = Arc::new(AtomicUsize::new(0));
    let handler = backend.handler(Arc::new(Stalled {
        manifest,
        started: started.clone(),
        aborted: aborted.clone(),
        overlapped: overlapped.clone(),
    }))?;
    let host = vectors::hosting(provider.clone(), handler, None)?;
    let root = host
        .create(&vectors::intent("timeout")?, fixtures::adopted())
        .await?;
    super::dispatcher::until(&host, std::future::pending())
        .await
        .expect_err("guest timeout did not surface a failure");
    // Whether the retry has started when the failure surfaces is scheduling,
    // so count turns rather than expecting exactly one. None may outlive the
    // host, and none may still be running when the next one starts.
    let started = started.load(Ordering::SeqCst);
    if started == 0 {
        return Err("stalled guest turn never started".into());
    }
    if overlapped.load(Ordering::SeqCst) != 0 {
        return Err("a timed-out guest turn was still running when its retry started".into());
    }
    if aborted.load(Ordering::SeqCst) != started {
        return Err("timed-out guest turn was not aborted".into());
    }
    let snapshot = acyclic_harness_core::agent::actor_store(&provider)?
        .load(&host.actor(&root)?)
        .await?;
    if snapshot
        .state()
        .and_then(|s| s.cursor(acyclic_harness_core::agent::INBOX))
        != Some(0)
    {
        return Err("timeout consumed its input".into());
    }
    Ok(())
}

fn strict(manifest: &Manifest) -> Result {
    let encoded = wire::encode_manifest(manifest);
    if wire::encode_manifest(&wire::decode_manifest(&encoded)?) != encoded {
        return Err("manifest codec changed bytes".into());
    }
    let mut unknown = encoded;
    unknown.extend_from_slice(&[0x78, 1]);
    if wire::decode_manifest(&unknown).is_ok() {
        return Err("manifest codec accepted an unknown field".into());
    }
    let delivery = Delivery {
        request: acyclic_actors::guest::RequestId::new(1),
        actor: acyclic_actors::addressing::ActorReference::new(b"conformance", b"agents", b"host")?,
        epoch: 1,
        now_unix_ms: 9,
        trigger: guest::Trigger::Alarm {
            generation: 1,
            now_unix_ms: 9,
        },
        record: None,
        subscriptions: guest::SubscriptionCuts::new(
            acyclic_stream::StreamPath::new("conformance/original-state")?,
            acyclic_stream::CommitId::from_bytes([17; 32]),
            [(
                "inbox".into(),
                guest::Subscription {
                    path: acyclic_stream::StreamPath::new("conformance/inbox")?,
                    paused: false,
                    cursor: 17,
                },
            )]
            .into(),
        ),
    };
    let encoded = wire::encode_delivery(&delivery, None);
    if wire::decode_delivery(&encoded)? != (delivery, None) {
        return Err("delivery codec changed its value".into());
    }
    let mut unknown = encoded;
    unknown.extend_from_slice(&[0x78, 1]);
    if wire::decode_delivery(&unknown).is_ok() {
        return Err("delivery codec accepted an unknown field".into());
    }
    let transition = Transition {
        pause: false,
        response: None,
        checkpoint: Checkpoint {
            codec: manifest.codec.clone(),
            bytes: bytes::Bytes::from_static(b"state"),
        },
        alarm: guest::AlarmChange::Clear,
        effects: Vec::new(),
        participant: None,
        subscriptions: Vec::new(),
    };
    let encoded = wire::encode_transition(&transition)?;
    if wire::decode_transition(&encoded)? != transition {
        return Err("transition codec changed its value".into());
    }
    for invalid in [
        {
            let mut bytes = encoded.clone();
            bytes.extend_from_slice(&[0x78, 1]);
            bytes
        },
        {
            let mut bytes = encoded.clone();
            bytes.extend_from_slice(&[0x30, 1]);
            bytes
        },
        {
            let mut bytes = encoded;
            bytes.pop();
            bytes
        },
    ] {
        if wire::decode_transition(&invalid).is_ok() {
            return Err("transition codec accepted non-canonical input".into());
        }
    }
    Ok(())
}
