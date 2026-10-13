/// The blocking worker waits without releasing ownership; replay never authorizes
fn validate_recovery(recovered: &mpsc::Sender<Recovered>) -> Result<bool, LocalStreamError> {
    let (complete, validation) = tokio::sync::oneshot::channel();
    recovered
        .blocking_send(Recovered::Validate(complete))
        .map_err(|_| LocalStreamError::Executor)?;
    validation
        .blocking_recv()
        .map_err(|_| LocalStreamError::Executor)
}
