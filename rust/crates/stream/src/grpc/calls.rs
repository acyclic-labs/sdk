use super::*;

impl Client {
    pub(super) async fn unary<T, U, F>(
        &self,
        attempt_timeout: std::time::Duration,
        body: T,
        mut call: F,
    ) -> Result<(U, usize, Completion), StreamError>
    where
        T: Clone,
        F: FnMut(
            wire::stream_service_client::StreamServiceClient<Channel>,
            Request<T>,
        ) -> Pin<Box<dyn Future<Output = Result<Response<U>, Status>> + Send>>,
    {
        let deadline = tokio::time::Instant::now() + OPERATION_DEADLINE;
        let mut last = None;
        let mut attempt = 0_u64;
        loop {
            let start = self.preferred.load(Ordering::Relaxed) % self.channels.len();
            for offset in 0..self.channels.len() {
                let index = (start + offset) % self.channels.len();
                let attempt_deadline = deadline.min(tokio::time::Instant::now() + attempt_timeout);
                attempt += 1;
                // One span describes one physical RPC attempt; the ordinal is not a logical retry total.
                let span = tracing::info_span!(
                    "acyclic.stream.grpc.call",
                    rpc = std::any::type_name::<T>().rsplit("::").next(),
                    attempt,
                    rpc.code = Empty,
                    outcome = Empty,
                    error.kind = Empty
                );
                let mut completion = Completion::new(span);
                let result = completion
                    .span
                    .scope(tokio::time::timeout_at(
                        attempt_deadline,
                        call(Self::service(&self.channels, index), self.request(body.clone())),
                    ))
                    .await;
                let error = match result {
                    Ok(Ok(response)) => {
                        self.preferred.store(index, Ordering::Relaxed);
                        return Ok((response.into_inner(), index, completion));
                    }
                    Ok(Err(error)) => error,
                    Err(_) => Status::deadline_exceeded("endpoint attempt expired"),
                };
                completion.status(&error);
                if !retryable(&error) {
                    return Err(status(&error));
                }
                completion.span.in_scope(|| {
                    tracing::warn!(
                        attempt,
                        rpc.code = error.code() as u64,
                        "stream rpc retried"
                    );
                });
                last = Some(error);
            }
            tokio::time::sleep_until((tokio::time::Instant::now() + RETRY_DELAY).min(deadline))
                .await;
            if tokio::time::Instant::now() >= deadline {
                return Err(last.as_ref().map_or(StreamError::Unavailable, status));
            }
        }
    }
}
