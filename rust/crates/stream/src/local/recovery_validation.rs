/// The blocking worker waits without releasing ownership; replay never authorizes
fn validate_recovery(recovered: &mpsc::Sender<Recovered>) -> Result<bool, LocalStreamError> {
    let (complete, validation) = tokio::sync::oneshot::channel();
    blocking_send(recovered, Recovered::Validate(complete))
        .map_err(|_| LocalStreamError::Executor)?;
    // Unit tests use the same controlled native workers as test-support users.
    // Yield that worker on the original validation event so replay can answer.
    #[cfg(any(test, feature = "test-support"))]
    let result = acyclic_native_runtime::testing::blocking_on(validation);
    #[cfg(not(any(test, feature = "test-support")))]
    let result = validation.blocking_recv();
    result.map_err(|_| LocalStreamError::Executor)
}

fn blocking_send<T>(sender: &mpsc::Sender<T>, value: T) -> Result<(), mpsc::error::SendError<T>> {
    #[cfg(any(test, feature = "test-support"))]
    return acyclic_native_runtime::testing::blocking_on(sender.send(value));
    #[cfg(not(any(test, feature = "test-support")))]
    sender.blocking_send(value)
}
