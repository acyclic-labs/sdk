/// Completes a streaming RPC on exhaustion, first error, or unfinished drop.
struct ObservedBody<S> {
    body: obs::Scoped<S>,
    completion: Completion,
}

impl<S, T> Stream for ObservedBody<S>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    type Item = Result<T, Status>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.completion.finished {
            return Poll::Ready(None);
        }
        let next = Pin::new(&mut this.body).poll_next(cx);
        match &next {
            Poll::Ready(Some(Err(error))) => this.completion.status(error),
            Poll::Ready(None) => this.completion.ok(),
            _ => {}
        }
        next
    }
}
