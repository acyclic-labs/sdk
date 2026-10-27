//! The actor's subscription follow observes interrupts while its handler runs.
//! Lookahead never commits or buffers inputs: the next delivery re-reads its
//! original committed cursor, including records passed while finding a control.
use super::*;
use crate::guest::CancellationToken;

impl<P: StreamProvider + ?Sized> Runtime<P> {
    pub(super) async fn interrupt_follows(
        &self,
        inner: &mut Inner,
        state: &State,
        trigger: &Trigger,
    ) -> Result<(), Error> {
        for (id, subscription) in state.subscriptions() {
            if !self
                .handler
                .manifest()
                .interrupts
                .iter()
                .any(|filter| &filter.subscription == id)
            {
                continue;
            }
            let from = match trigger {
                Trigger::Record {
                    subscription: delivered,
                    sequence,
                } if delivered == id => sequence
                    .checked_add(1)
                    .ok_or(Error::Invalid("interrupt cursor overflow"))?,
                _ => subscription.cursor,
            };
            self.open(inner, id, &subscription.path, from).await?;
        }
        Ok(())
    }

    pub(super) async fn interrupt(
        &self,
        inner: &mut Inner,
        cancellation: CancellationToken,
    ) -> Error {
        let mut cancelled = false;
        poll_fn(|context| {
            if cancelled {
                return Poll::Pending;
            }
            // Bound lookahead per poll so a busy inbox cannot starve the
            // handler or its original wall-clock timeout.
            let mut remaining = 256;
            for (id, follow) in &mut inner.follows {
                loop {
                    if remaining == 0 {
                        context.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                    match follow.poll_next_unpin(context) {
                        Poll::Ready(Some(Ok(record))) => {
                            remaining -= 1;
                            if self.handler.manifest().interrupts.iter().any(|filter| {
                                &filter.subscription == id
                                    && !filter.value_prefix.is_empty()
                                    && record.value.starts_with(&filter.value_prefix)
                            }) {
                                cancellation.cancel();
                                cancelled = true;
                                // No unpolled provider future may obstruct the
                                // handler's cleanup after cancellation.
                                inner.follows.clear();
                                return Poll::Pending;
                            }
                        }
                        Poll::Ready(Some(Err(error))) => return Poll::Ready(error.into()),
                        Poll::Ready(None) => return Poll::Ready(StreamError::Unavailable.into()),
                        Poll::Pending => break,
                    }
                }
            }
            Poll::Pending
        })
        .await
    }
}
