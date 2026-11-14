use acyclic_harness_core::Result;

/// Keep protocol assertions independent of request observation spans. Dropping a
/// filtered native item also drops its original custody; errors and waits remain.
pub(crate) fn without_request_traces<S, T, F>(
    events: S,
    event: F,
) -> impl futures::Stream<Item = Result<T>> + Unpin
where
    S: futures::Stream<Item = Result<T>> + Unpin,
    T: Unpin,
    F: Fn(&T) -> &acyclic_harness_core::model::ModelEvent + Unpin,
{
    futures::TryStreamExt::try_filter(events, move |item| {
        std::future::ready(!matches!(
            event(item),
            acyclic_harness_core::model::ModelEvent::Trace { .. }
        ))
    })
}
