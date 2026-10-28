use crate::{Backend, Result, fixtures, observe::Observed, vectors};
use acyclic_actors::guest::{
    self, Checkpoint, Delivery, GuestFuture, Handler, Manifest, Transition, wire,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

pub(crate) fn remote(handler: Arc<dyn Handler>) -> Result<Arc<dyn Handler>> {
    let manifest = wire::decode_manifest(&wire::encode_manifest(handler.manifest()))?;
    Ok(Arc::new(wire::Remote::new(
        manifest,
        move |bytes: Vec<u8>, cancellation| -> GuestFuture<'static, Vec<u8>> {
            let handler = handler.clone();
            Box::pin(async move {
                let (delivery, checkpoint) = wire::decode_delivery(&bytes)?;
                let transition = handler
                    .on_record(delivery, checkpoint.as_ref(), cancellation)
                    .await?;
                wire::encode_transition(&transition)
            })
        },
    )))
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Stalled {
    manifest: Manifest,
    aborted: Arc<AtomicUsize>,
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
    let aborted = Arc::new(AtomicUsize::new(0));
    let handler = backend.handler(Arc::new(Stalled {
        manifest,
        aborted: aborted.clone(),
    }))?;
    let host = vectors::hosting(provider.clone(), handler, None)?;
    let root = host
        .create(&vectors::intent("timeout")?, fixtures::adopted())
        .await?;
    if host.runtime().drain().await.is_empty() {
        return Err("guest timeout did not surface a failure".into());
    }
    if aborted.load(Ordering::SeqCst) != 1 {
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
        actor: acyclic_actors::addressing::ActorReference::new(b"conformance", b"agents", b"host")?,
        epoch: 1,
        now_unix_ms: 9,
        trigger: guest::Trigger::Alarm {
            generation: 1,
            now_unix_ms: 9,
        },
        record: None,
        subscriptions: guest::SubscriptionCuts::new(
            acyclic_stream::CommitId::from_bytes([17; 32]),
            [(
                "inbox".into(),
                guest::Subscription {
                    path: acyclic_stream::StreamPath::new("conformance/inbox")?,
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
        checkpoint: Checkpoint {
            codec: manifest.codec.clone(),
            bytes: bytes::Bytes::from_static(b"state"),
        },
        alarm: guest::AlarmChange::Clear,
        effects: Vec::new(),
        participant: None,
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
