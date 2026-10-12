struct RecordCursor {
    client: Client,
    path: StreamPath,
    next: u64,
    remaining: Option<u32>,
    active: Option<ActiveRecords>,
    /// Decoded records from the latest frame that the caller has not yet taken.
    buffered: VecDeque<Record>,
    completion: Completion,
}

impl RecordCursor {
    /// Delivered errors terminate the logical cursor. Retryable transport failures
    fn failure(mut self, error: StreamError) -> Option<(Result<Record, StreamError>, Self)> {
        self.completion.finish(Some(error.code()), None);
        self.active = None;
        self.buffered.clear();
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

    async fn open(&self) -> Result<ActiveRecords, StreamError> {
        if let Some(limit) = self.remaining {
            self.client
                .unary(
                    wire::ReadRequest {
                        path: self.path.to_string(),
                        from: self.next,
                        limit,
                    },
                    |mut service, request| Box::pin(async move { service.read(request).await }),
                )
                .await
                .map(|response| {
                    let RpcResponse { body, completion } = response;
                    let records = completion.span.scope(body);
                    ActiveRecords {
                        records,
                        completion,
                        endpoint: 0,
                    }
                })
        } else {
            self.client
                .follow_unary(
                    wire::FollowRequest {
                        path: self.path.to_string(),
                        from: self.next,
                    },
                    |mut service, request| Box::pin(async move { service.follow(request).await }),
                )
                .await
                .map(|(records, endpoint, completion)| {
                    let records = completion.span.scope(records);
                    ActiveRecords {
                        records,
                        endpoint,
                        completion,
                    }
                })
        }
    }
}
