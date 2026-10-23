use super::*;
use acyclic_stream::{ChildrenRequest, ReadRequest};
use futures::{TryStreamExt, future::Either};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// Observation of a runtime-owned subscription. Dropping this observation
/// releases no handler claim and never cancels actor work. Use durable actor
/// cancellation to stop work and drain effects.
pub struct ActorSubscription {
    completion: futures::channel::oneshot::Receiver<Result<Snapshot>>,
}

impl Future for ActorSubscription {
    type Output = Result<Snapshot>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.completion).poll(cx) {
            Poll::Ready(Ok(result)) => Poll::Ready(result),
            Poll::Ready(Err(_)) => Poll::Ready(Err(Error::Storage(
                "actor runtime stopped before subscription settlement".into(),
            ))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<P: StreamProvider> InProcessRuntime<P> {
    /// Starts independently scheduled subscription work. The executor owns the
    /// complete handler/effect lifetime; the returned value observes settlement.
    pub fn subscribe(
        self: &Arc<Self>,
        executor: &dyn acyclic_stream::session::runtime::Executor,
        placement: Placement,
    ) -> ActorSubscription {
        let runtime = self.clone();
        let (settled, completion) = futures::channel::oneshot::channel();
        executor.spawn(Box::pin(async move {
            let result = runtime.run(placement).await;
            let _ = settled.send(result);
        }));
        ActorSubscription { completion }
    }

    /// Runs the package subscription until this actor settles or loses placement.
    /// The runtime owner polls this future independently of attached clients.
    /// Idle actors wait on provider follows; no timer or request polling is used.
    pub(crate) fn run(&self, placement: Placement) -> BoxProviderFuture<'_, Result<Snapshot>> {
        Box::pin(async move {
            let (_, journal) = self
                .checked(AdmissionAction::Recover, &placement, None)
                .await?;
            let _owner = journal.claim_supervisor()?;
            loop {
                let snapshot = self.recover(placement.clone()).await?;
                if snapshot.cancelled || !matches!(snapshot.status, MachineStatus::Suspended) {
                    return Ok(snapshot);
                }
                let current = journal.load().await?;
                journal.require_owner(&current)?;
                if current.last.snapshot != snapshot
                    || current.last.cancelling
                    || !snapshot.pending.is_empty()
                    || journal.next_input(snapshot.consumed).await?.is_some()
                {
                    continue;
                }
                journal.changed(&current).await?;
            }
        })
    }
}

impl<P: StreamProvider> Journal<P> {
    async fn inbox_change(&self, consumed: u64) -> Result<()> {
        // The first admission creates the inbox. Children replay plus follow
        // closes the creation race without manufacturing an empty input record.
        if consumed == 0 {
            let mut children = self
                .stream
                .children(ChildrenRequest {
                    parent: self.inbox.parent(),
                    limit: 2,
                })
                .await
                .map_err(|error| journal::stream_error(&error))?;
            loop {
                let child = children
                    .try_next()
                    .await
                    .map_err(|error| journal::stream_error(&error))?
                    .ok_or_else(|| Error::Storage("actor discovery follow ended".into()))?;
                if child.path == self.inbox {
                    break;
                }
            }
        }
        let mut follow = if consumed == 0 {
            self.stream
                .follow(
                    self.inbox.clone(),
                    0,
                    acyclic_stream::FollowFilter::default(),
                )
                .await
        } else {
            let mut records = self
                .stream
                .read(ReadRequest {
                    path: self.inbox.clone(),
                    from: consumed - 1,
                    limit: 1,
                })
                .await
                .map_err(|error| journal::stream_error(&error))?;
            let anchor = records
                .try_next()
                .await
                .map_err(|error| journal::stream_error(&error))?
                .ok_or_else(|| Error::Storage("actor inbox anchor is missing".into()))?;
            self.stream
                .follow_checked(
                    self.inbox.clone(),
                    consumed,
                    anchor.commit_id,
                    acyclic_stream::FollowFilter::default(),
                )
                .await
        }
        .map_err(|error| journal::stream_error(&error))?;
        follow
            .try_next()
            .await
            .map_err(|error| journal::stream_error(&error))?
            .ok_or_else(|| Error::Storage("actor inbox follow ended".into()))?;
        Ok(())
    }

    pub(in super::super) async fn changed(&self, current: &journal::Loaded) -> Result<()> {
        let mut workflow = self
            .stream
            .follow_checked(
                self.path.clone(),
                current.tail,
                current.head,
                acyclic_stream::FollowFilter::default(),
            )
            .await
            .map_err(|error| journal::stream_error(&error))?;
        let checkpoint = Box::pin(async {
            workflow
                .try_next()
                .await
                .map_err(|error| journal::stream_error(&error))?
                .ok_or_else(|| Error::Storage("actor workflow follow ended".into()))?;
            Ok(())
        });
        let inbox = Box::pin(self.inbox_change(current.last.snapshot.consumed));
        match futures::future::select(checkpoint, inbox).await {
            Either::Left((result, _)) | Either::Right((result, _)) => result,
        }
    }
}
