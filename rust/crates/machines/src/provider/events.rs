//! One ordered event contract for local, simulated and remote producers.
use super::*;

pub(crate) fn checked<S, N, F>(
    machine: MachineId,
    after: Option<u64>,
    state: S,
    mut next: N,
) -> EventStream
where
    S: acyclic_stream::ProviderTask + 'static,
    N: FnMut(S) -> F + acyclic_stream::ProviderTask + 'static,
    F: std::future::Future<Output = Result<Option<(MachineEvent, S)>, ProviderError>>
        + acyclic_stream::ProviderTask
        + 'static,
{
    Box::pin(stream::try_unfold(
        (state, after.unwrap_or(0), false),
        move |(state, previous, done)| {
            let next = (!done).then(|| next(state));
            async move {
                let Some(next) = next else {
                    return Ok(None);
                };
                let (event, state) = next.await?.ok_or(ProviderError::Unavailable)?;
                if event.machine != machine || previous.checked_add(1) != Some(event.sequence) {
                    return Err(ProviderError::Rejected(
                        "Machine event identity or sequence differs from its original cursor"
                            .into(),
                    ));
                }
                let sequence = event.sequence;
                let done = event.fact == EventFact::State(MachineState::Destroyed);
                Ok(Some((event, (state, sequence, done))))
            }
        },
    ))
}
