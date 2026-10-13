async fn run_owned_initialization<T, F>(
    span: &obs::OwnedSpan,
    initialize: F,
) -> Result<T, LocalStreamError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, LocalStreamError>> + Send + 'static,
{
    let operation = span.clone();
    tokio::spawn(async move { obs::finish(&operation, operation.scope(initialize).await) })
        .await
        .map_err(|_| {
            obs::failed(span, obs::ErrorKind::kind(&LocalStreamError::Executor));
            LocalStreamError::Executor
        })?
}
