//! Deterministic executor and local platform I/O for the real Streams pool.
use acyclic_stream::{
    BoxProviderFuture, StreamError, StreamProvider,
    session::{
        self, Client,
        runtime::Executor,
        transport::{Endpoint, Requests, Responses, Transport},
    },
    wire,
};
use futures::{FutureExt as _, StreamExt as _, channel::mpsc, stream::FuturesUnordered};
use std::{future::Future, pin::Pin, sync::Arc};

pub type RequestMap = Arc<dyn Fn(Requests) -> Requests + Send + Sync>;
pub type ResponseMap = Arc<dyn Fn(Responses) -> Responses + Send + Sync>;

struct Local<P> {
    provider: Arc<P>,
    requests: RequestMap,
    responses: ResponseMap,
    sent: mpsc::UnboundedSender<wire::SessionFrame>,
}
impl<P: StreamProvider + 'static> Transport for Local<P> {
    fn session(&self, source: Requests) -> BoxProviderFuture<'_, Result<Responses, StreamError>> {
        Box::pin(async move {
            let sent = self.sent.clone();
            let source = (self.requests)(source).map(move |frame| {
                if !sent.is_closed() {
                    let _ = sent.unbounded_send(frame.clone());
                }
                Ok(frame)
            });
            let output = session::serve(
                self.provider.clone(),
                "local".into(),
                Default::default(),
                source.boxed(),
            )?;
            Ok((self.responses)(output))
        })
    }
}

struct Scheduled(mpsc::UnboundedSender<BoxProviderFuture<'static, ()>>);
impl Executor for Scheduled {
    fn spawn(&self, future: BoxProviderFuture<'static, ()>) {
        let _ = self.0.unbounded_send(future);
    }
    fn sleep(&self, _: u32) -> BoxProviderFuture<'static, ()> {
        // These scenarios never advance virtual time. Readiness and completion
        // come only from actual provider/session publications, not a timeout.
        Box::pin(futures::future::pending())
    }
}

pub struct Runtime {
    tasks: mpsc::UnboundedReceiver<BoxProviderFuture<'static, ()>>,
    running: FuturesUnordered<BoxProviderFuture<'static, ()>>,
}
impl Runtime {
    pub async fn drive(&mut self) {
        loop {
            let task = {
                let completed = async {
                    if self.running.is_empty() {
                        futures::future::pending().await
                    } else {
                        self.running.next().await
                    }
                }
                .fuse();
                futures::pin_mut!(completed);
                futures::select! {
                    task = self.tasks.next().fuse() => Some(task),
                    _ = completed => None,
                }
            };
            if let Some(task) = task {
                let Some(task) = task else {
                    return;
                };
                self.running.push(task);
            }
        }
    }

    pub async fn until_idle<F: Future + ?Sized>(
        &mut self,
        future: Pin<&mut F>,
    ) -> Option<F::Output> {
        let driver = self.drive();
        futures::pin_mut!(driver);
        let mut combined = Box::pin(futures::future::select(future, driver));
        match super::quiescent::until_idle(combined.as_mut()).await {
            Some(futures::future::Either::Left((output, _))) => Some(output),
            Some(futures::future::Either::Right(_)) => panic!("session executor ended"),
            None => None,
        }
    }
}

pub fn open<P: StreamProvider + 'static>(
    provider: Arc<P>,
    requests: RequestMap,
    responses: ResponseMap,
) -> (Client, Runtime, mpsc::UnboundedReceiver<wire::SessionFrame>) {
    let (sent, observed) = mpsc::unbounded();
    let endpoint: Endpoint = Arc::new(Local {
        provider,
        requests,
        responses,
        sent,
    });
    let (scheduled, tasks) = mpsc::unbounded();
    let connection = endpoint.clone();
    let client = Client::with_transport(
        endpoint,
        Arc::new(move |_| {
            let endpoint = connection.clone();
            Box::pin(async move { Ok(endpoint) })
        }),
        Arc::new(Scheduled(scheduled)),
    );
    (
        client,
        Runtime {
            tasks,
            running: FuturesUnordered::new(),
        },
        observed,
    )
}
