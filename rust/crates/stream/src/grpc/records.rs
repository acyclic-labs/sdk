use super::*;

impl Client {
    pub(super) async fn records(
        &self,
        path: StreamPath,
        from: u64,
        limit: Option<u32>,
    ) -> Result<RecordStream, StreamError> {
        let span = if limit.is_some() {
            tracing::info_span!(
                "acyclic.stream.grpc.read",
                rev = from,
                items = limit,
                outcome = Empty,
                error.kind = Empty
            )
        } else {
            tracing::info_span!(
                "acyclic.stream.grpc.follow",
                rev = from,
                outcome = Empty,
                error.kind = Empty
            )
        };
        let completion = Completion::new(span);
        let context = completion.span.clone();
        Ok(context
            .scope(stream::unfold(
                RecordCursor {
                    client: self.clone(),
                    path,
                    next: from,
                    remaining: limit,
                    active: None,
                    buffered: Vec::new().into_iter(),
                    completion,
                    acknowledgements: None,
                },
                |mut cursor| async move {
                    loop {
                        if cursor.remaining == Some(0) {
                            cursor.completion.ok();
                            return None;
                        }
                        if let Some(record) = cursor.buffered.next() {
                            if record.sequence == cursor.next {
                                cursor.next = cursor.next.saturating_add(1);
                                if let Some(remaining) = &mut cursor.remaining {
                                    *remaining = remaining.saturating_sub(1);
                                    if *remaining == 0 {
                                        cursor.completion.ok();
                                    }
                                }
                                return Some((Ok(record), cursor));
                            }
                            if record.sequence < cursor.next {
                                continue;
                            }
                            if let Some(active) = cursor.active.as_mut() {
                                active.2.invalid_response();
                            }
                            cursor.completion.invalid_response();
                            return cursor.failure(StreamError::Unavailable);
                        }
                        if cursor.active.is_none() {
                            match cursor.completion.span.scope(cursor.open()).await {
                                Ok(active) => cursor.active = Some(active),
                                Err(error) => return cursor.failure(error),
                            }
                        }
                        let Some(active) = cursor.active.as_mut() else {
                            return cursor.failure(StreamError::Unavailable);
                        };
                        let active_endpoint = active.1;
                        let response = active.0.next().await;
                        match &response {
                            Some(Err(error)) => active.2.status(error),
                            None => active.2.ok(),
                            _ => {}
                        }
                        match response {
                            Some(Ok(mut response)) => {
                                let token = response.delivery_token.take();
                                match read_response(response, cursor.next, cursor.remaining) {
                                    Ok(records) => {
                                        if let Some(token) = token {
                                            if token.len() != 32 {
                                                active.2.invalid_response();
                                                cursor.completion.invalid_response();
                                                return cursor.failure(StreamError::Unavailable);
                                            }
                                            if cursor.client.acknowledge_deliveries
                                                && records.len() != 0
                                            {
                                                let worker = cursor
                                                    .acknowledgements
                                                    .get_or_insert_with(|| {
                                                        DeliveryAcknowledgements::new(
                                                            cursor.client.clone(),
                                                        )
                                                    });
                                                let _ =
                                                    worker.queue.try_send((active_endpoint, token));
                                            }
                                        }
                                        cursor.buffered = records;
                                    }
                                    Err(error) => {
                                        active.2.invalid_response();
                                        cursor.completion.invalid_response();
                                        return cursor.failure(error);
                                    }
                                }
                            }
                            Some(Err(error)) if !retryable(&error) => {
                                return cursor.failure(status(&error));
                            }
                            None if cursor.remaining.is_some() => {
                                cursor.completion.ok();
                                return None;
                            }
                            Some(Err(_)) | None => {
                                cursor.advance_follow(active_endpoint);
                                tokio::time::sleep(RETRY_DELAY).await;
                                cursor.active = None;
                            }
                        }
                    }
                },
            ))
            .boxed())
    }
}

struct RecordCursor {
    client: Client,
    path: StreamPath,
    next: u64,
    remaining: Option<u32>,
    active: Option<(
        obs::Scoped<tonic::Streaming<wire::ReadResponse>>,
        usize,
        Completion,
    )>,
    /// Decoded records from the latest frame that the caller has not yet taken.
    buffered: std::vec::IntoIter<Record>,
    completion: Completion,
    acknowledgements: Option<DeliveryAcknowledgements>,
}

impl RecordCursor {
    /// Delivered errors terminate the logical cursor.
    fn failure(mut self, error: StreamError) -> Option<(Result<Record, StreamError>, Self)> {
        self.completion.finish(Some(error.code()), None);
        self.active = None;
        self.buffered = Vec::new().into_iter();
        self.acknowledgements = None;
        self.remaining = Some(0);
        Some((Err(error), self))
    }

    fn advance_follow(&self, observed: usize) {
        if self.remaining.is_some() {
            return;
        }
        let next = (observed + 1) % self.client.channels.len();
        let _ = self.client.preferred.compare_exchange(
            observed,
            next,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
    }

    async fn open(
        &self,
    ) -> Result<
        (
            obs::Scoped<tonic::Streaming<wire::ReadResponse>>,
            usize,
            Completion,
        ),
        StreamError,
    > {
        let response = if let Some(limit) = self.remaining {
            self.client
                .unary(
                    OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
                    wire::ReadRequest {
                        path: self.path.to_string(),
                        from: self.next,
                        limit,
                    },
                    |mut service, request| Box::pin(async move { service.read(request).await }),
                )
                .await?
        } else {
            self.client
                .unary(
                    FOLLOW_ENDPOINT_ATTEMPT_TIMEOUT,
                    wire::FollowRequest {
                        path: self.path.to_string(),
                        from: self.next,
                    },
                    |mut service, request| Box::pin(async move { service.follow(request).await }),
                )
                .await?
        };
        let (body, endpoint, completion) = response;
        Ok((completion.span.scope(body), endpoint, completion))
    }
}

fn read_response(
    value: wire::ReadResponse,
    next: u64,
    remaining: Option<u32>,
) -> Result<std::vec::IntoIter<Record>, StreamError> {
    let records: Vec<Record> = read_response_records(value)?
        .into_iter()
        .map(record)
        .collect::<Result<_, _>>()?;
    let mut expected = records.first().map_or(next, |record| record.sequence);
    if expected > next {
        return Err(StreamError::Unavailable);
    }
    let mut new_records = 0_u64;
    for record in &records {
        if record.sequence != expected {
            return Err(StreamError::Unavailable);
        }
        expected = expected.checked_add(1).ok_or(StreamError::Unavailable)?;
        if record.sequence >= next {
            new_records += 1;
        }
    }
    if remaining.is_some_and(|remaining| new_records > u64::from(remaining)) {
        return Err(StreamError::Unavailable);
    }
    Ok(records.into_iter())
}
