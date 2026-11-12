use super::*;
use futures::{Stream, StreamExt as _};
use std::{pin::Pin, sync::Arc};

#[derive(Clone, Debug)]
pub(crate) enum Interest {
    Stream(StreamPath),
    Resource(Arc<str>),
}

pub(super) enum Delivery {
    Opened,
    Frame(Option<acyclic_stream::session::Frame>),
}

pub(super) type Event = (Interest, Result<Delivery, StreamError>);

pub(super) enum SubscriptionState {
    Opening(BoxProviderFuture<'static, Result<Follow, StreamError>>),
    Open(Follow),
    Ended,
}

pub(super) struct Subscription {
    pub(super) id: u64,
    pub(super) interest: Interest,
    pub(super) state: SubscriptionState,
    pub(super) report_open: bool,
}

impl Stream for Subscription {
    type Item = Event;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            let result = match &mut this.state {
                SubscriptionState::Opening(opening) => match opening.as_mut().poll(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(follow)) => {
                        this.state = SubscriptionState::Open(follow);
                        if this.report_open {
                            return Poll::Ready(Some((
                                this.interest.clone(),
                                Ok(Delivery::Opened),
                            )));
                        }
                        continue;
                    }
                    Poll::Ready(Err(error)) => Err(error),
                },
                SubscriptionState::Open(follow) => match follow.poll_next_unpin(context) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(frame))) => Ok(Delivery::Frame(Some(frame))),
                    Poll::Ready(Some(Err(error))) => Err(error),
                    Poll::Ready(None) => Ok(Delivery::Frame(None)),
                },
                SubscriptionState::Ended => return Poll::Ready(None),
            };
            if !matches!(&result, Ok(Delivery::Frame(Some(frame)))
                if !matches!(frame.wire().body, Some(wire::session_frame::Body::Closed(_))))
            {
                this.state = SubscriptionState::Ended;
            }
            return Poll::Ready(Some((this.interest.clone(), result)));
        }
    }
}
