use super::*;
use crate::session::runtime::Executor;

struct Clock {
    release: Arc<gate::Gate>,
    broken: bool,
}
impl Executor for Clock {
    fn spawn(&self, _: crate::BoxProviderFuture<'static, ()>) {
        panic!("this bounded driver owns all polling");
    }
    fn sleep(&self, milliseconds: u32) -> crate::BoxProviderFuture<'static, ()> {
        let release = Arc::clone(&self.release);
        let broken = self.broken;
        Box::pin(async move {
            if milliseconds == 10 {
                if !broken {
                    release.changed(false).await.unwrap();
                }
            } else {
                future::pending::<()>().await;
            }
        })
    }
}
struct Peer(Arc<AtomicU64>);
impl transport::Transport for Peer {
    fn session(
        &self,
        _: transport::Requests,
    ) -> crate::BoxProviderFuture<'_, Result<Response, StreamError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            let response: Response = stream::iter([Ok(frame(
                0,
                0,
                Body::Welcome(wire::SessionWelcome {
                    version: 1,
                    node_id: "good".into(),
                    ..Default::default()
                }),
            ))])
            .boxed();
            Ok(response)
        })
    }
}

/// The real reconnect must yield before retrying an unopened subscription,
/// including when clock release races its owner's withdrawal and late Welcome.
pub fn verify_initial_retry_yield(broken: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let channel = tonic::transport::Endpoint::from_static("http://model.invalid").connect_lazy();
    loom::model(move || {
        let (mut pool, _healthy) = fixture::fixture(&channel);
        pool.links.remove("bad");
        pool.routes.remove(&1);
        pool.active.store(1, Ordering::Release);
        let (ready, _opened) = oneshot::channel();
        pool.routes.get_mut(&3).unwrap().ready = Some(ready);
        let progress = Arc::clone(&pool.routes[&3].progress);
        let release = Arc::new(gate::Gate::new());
        pool.executor = std::sync::Arc::new(Clock {
            release: Arc::clone(&release),
            broken,
        });
        let calls = Arc::new(AtomicU64::new(0));
        pool.links.get_mut("good").unwrap().channel = std::sync::Arc::new(Peer(Arc::clone(&calls)));
        pool.reconnect("good").unwrap();
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        assert!(
            poll_dials(&mut pool.dials, &mut cx).is_pending(),
            "initial retry must yield before another physical RPC"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let clock = loom::thread::spawn(move || release.set(true).unwrap());
        let withdrawing = loom::thread::spawn(move || progress.cancel());
        clock.join().unwrap();
        withdrawing.join().unwrap();
        // A completion can already be queued when cancellation is dispatched.
        let std::task::Poll::Ready(Some((node, epoch, result))) =
            poll_dials(&mut pool.dials, &mut cx)
        else {
            panic!("released retry did not complete");
        };
        pool.dial_ready(&node, epoch, result).unwrap();
        pool.close_idle();
        assert!(
            pool.links.is_empty(),
            "withdrawn retry installed an ownerless session"
        );
        assert!(pool.dials.is_empty());
    });
}
