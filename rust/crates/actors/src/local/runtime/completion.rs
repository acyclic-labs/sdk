//! Original completion requests retire on acknowledgement or caller loss.
use super::*;

pub(super) type Completion = tokio::sync::oneshot::Sender<Result<(), Arc<Error>>>;

struct Pending(Completion);

impl std::future::Future for Pending {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        self.0.poll_closed(context)
    }
}

/// Only live observers retain nodes; cancellation uses the original oneshot.
/// The ready queue is allocated only while a completion batch exists.
#[derive(Default)]
pub(super) struct Completions(Option<FuturesUnordered<Pending>>);

impl Completions {
    pub(super) fn insert(&mut self, completion: Completion) {
        if !completion.is_closed() {
            self.0
                .get_or_insert_with(FuturesUnordered::new)
                .push(Pending(completion));
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.as_ref().is_none_or(FuturesUnordered::is_empty)
    }

    pub(super) fn poll_cancelled(&mut self, context: &mut Context<'_>) -> Poll<()> {
        let Some(pending) = &mut self.0 else {
            return Poll::Pending;
        };
        if matches!(pending.poll_next_unpin(context), Poll::Ready(Some(()))) {
            if pending.is_empty() {
                self.0 = None;
            }
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    pub(super) fn finish(&mut self, failure: Option<&Arc<Error>>) {
        if let Some(pending) = self.0.take() {
            for Pending(completion) in pending {
                let result = failure.map_or(Ok(()), |error| Err(Arc::clone(error)));
                let _cancelled = completion.send(result);
            }
        }
    }
}
