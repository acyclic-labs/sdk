use super::*;
use crate::guest::{CancellationToken, GuestError, wire};

struct RefusedRecord {
    package: Package,
    error: GuestError,
    failures: AtomicUsize,
    recover: bool,
}
impl Handler for RefusedRecord {
    fn manifest(&self) -> &Manifest {
        self.package.manifest()
    }
    fn on_record<'a>(
        &'a self,
        _: Delivery,
        _: Option<&'a Checkpoint>,
        _: CancellationToken,
    ) -> GuestFuture<'a, Transition> {
        Box::pin(async { Err(self.error.clone()) })
    }
    fn on_failure<'a>(
        &'a self,
        delivery: Delivery,
        checkpoint: Option<&'a Checkpoint>,
        _: String,
    ) -> GuestFuture<'a, Option<Transition>> {
        self.failures.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if !self.recover {
                return Ok(None);
            }
            self.package
                .on_record(delivery, checkpoint, CancellationToken::new())
                .await
                .map(Some)
        })
    }
}

#[tokio::test]
async fn handler_failure_transition_consumes_only_decided_work() -> TestResult {
    for (pending, wire, recover) in [
        (false, false, true),
        (true, false, true),
        (false, true, true),
        (true, true, true),
        (false, false, false),
        (false, true, false),
    ] {
        let provider = Arc::new(MemoryStream::default());
        let store = Store::new(Arc::clone(&provider), root()?);
        let address = spawn_with(&store, "handler-failure", &[b"emit"]).await?;
        let error = if pending {
            GuestError::Pending("unknown external acknowledgement".into())
        } else {
            GuestError::Handler("definitive handler failure".into())
        };
        let handler = Arc::new(RefusedRecord {
            package: Package::new()?,
            error: error.clone(),
            failures: AtomicUsize::new(0),
            recover,
        });
        let dispatched: Arc<dyn Handler> = if wire {
            let manifest = wire::decode_manifest(&wire::encode_manifest(handler.manifest()))?;
            let handler = handler.clone();
            Arc::new(wire::Remote::new(
                manifest,
                move |bytes: Vec<u8>, cancellation| {
                    let handler = handler.clone();
                    Box::pin(
                        async move { wire::invoke(handler.as_ref(), &bytes, cancellation).await },
                    ) as GuestFuture<'static, Vec<u8>>
                },
            ))
        } else {
            handler.clone()
        };
        let runtime = Runtime::new(store, dispatched, Arc::new(SystemClock));
        runtime.track(address.clone())?;
        let result = runtime.drain_actor(&address).await;
        let snapshot = runtime.store().load(&address).await?;
        let state = snapshot.state().ok_or("missing actor state")?;
        if pending || !recover {
            assert!(matches!(result, Err(Error::Handler(ref source)) if source == &error));
            assert_eq!(state.cursor("inbox"), Some(0));
            assert_eq!(
                handler.failures.load(Ordering::SeqCst),
                usize::from(!pending)
            );
            assert!(records(&*provider, "out/handler-failure").await?.is_empty());
        } else {
            result?;
            assert_eq!(state.cursor("inbox"), Some(1));
            assert_eq!(handler.failures.load(Ordering::SeqCst), 1);
            assert_eq!(
                records(&*provider, "out/handler-failure").await?,
                vec![Bytes::from_static(b"r1")]
            );
        }
    }
    Ok(())
}
