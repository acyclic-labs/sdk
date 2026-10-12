use super::*;

#[async_trait]
impl StreamProvider for Client {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        let response = self
            .unary(
                OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
                wire::InspectIdempotencyRequest {
                    idempotency_key: Bytes::copy_from_slice(idempotency_key.as_bytes()),
                },
                |mut service, request| {
                    Box::pin(async move { service.inspect_idempotency(request).await })
                },
            )
            .await?;
        decode_response(response, |response| {
            bind_observation(&idempotency_key, response.observation)
        })
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        let response = self
            .unary(
                OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
                wire::TailRequest {
                    path: path.to_string(),
                },
                |mut service, request| Box::pin(async move { service.tail(request).await }),
            )
            .await?;
        decode_response(response, |response| Ok(response.tail))
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        Ok(StreamBounds {
            tail: self.tail(path).await?,
        })
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let idempotency_key = request.idempotency_key.map_or_else(
            || Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            |key| Bytes::copy_from_slice(key.as_bytes()),
        );
        let response = self
            .unary(
                OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
                wire::AppendRequest {
                    path: request.path.to_string(),
                    records: request.records.into_iter().map(Bytes::from_owner).collect(),
                    if_tail: request.if_tail,
                    idempotency_key: Some(idempotency_key),
                },
                |mut service, request| Box::pin(async move { service.append(request).await }),
            )
            .await?;
        decode_response(response, append_outcome_from_wire)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let idempotency_key = request.idempotency_key.map_or_else(
            || Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()),
            |key| Bytes::copy_from_slice(key.as_bytes()),
        );
        let receipt = self
            .unary(
                OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
                wire::ForkRequest {
                    source: request.source.to_string(),
                    destination: request.destination.to_string(),
                    at_tail: request.at_tail,
                    idempotency_key: Some(idempotency_key),
                },
                |mut service, request| Box::pin(async move { service.fork(request).await }),
            )
            .await?;
        decode_response(receipt, |receipt| {
            Ok(ForkReceipt {
                source: path(receipt.source)?,
                destination: path(receipt.destination)?,
                forked_at: receipt.forked_at,
                tail: receipt.tail,
                commit_id: commit_id(&receipt.commit_id)?,
            })
        })
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.records(request.path, request.from, Some(request.limit))
            .await
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.records(path, from, None).await
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        let body = wire::ChildrenRequest {
            parent: request.parent.map(|path| path.to_string()),
            limit: request.limit,
        };
        let mut last = None;
        for _ in 0..self.channels.len() {
            let response = self
                .unary(
                    OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
                    body.clone(),
                    |mut service, request| Box::pin(async move { service.children(request).await }),
                )
                .await?;
            let (response, _, mut completion) = response;
            let mut response = completion.span.scope(response);
            let result = async {
                let mut children = Vec::new();
                while let Some(item) = response.next().await {
                    let item = item.map_err(|error| {
                        completion.status(&error);
                        status(&error)
                    })?;
                    let child = item
                        .child
                        .ok_or(StreamError::Unavailable)
                        .and_then(|child| {
                            Ok(Child {
                                path: path(child.path)?,
                            })
                        })
                        .inspect_err(|_| completion.invalid_response())?;
                    children.push(child);
                }
                completion.ok();
                Ok(children)
            }
            .await;
            match result {
                Ok(children) => return Ok(stream::iter(children.into_iter().map(Ok)).boxed()),
                Err(error) if error != StreamError::Unavailable => return Err(error),
                Err(error) => last = Some(error),
            }
        }
        Err(last.unwrap_or(StreamError::Unavailable))
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        let response = self
            .unary(
                OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
                wire::ChildrenPageRequest {
                    parent: request.parent.map(|path| path.to_string()),
                    after: request.after.map(|path| path.to_string()),
                    hierarchy_version: request
                        .hierarchy_version
                        .map(|version| Bytes::copy_from_slice(version.as_bytes())),
                    limit: request.limit,
                },
                |mut service, request| {
                    Box::pin(async move { service.children_page(request).await })
                },
            )
            .await?;
        decode_response(response, |response| {
            Ok(ChildrenPage {
                hierarchy_version: commit_id(&response.hierarchy_version)?,
                children: response
                    .children
                    .into_iter()
                    .map(|child| {
                        Ok(Child {
                            path: path(child.path)?,
                        })
                    })
                    .collect::<Result<_, StreamError>>()?,
                next_after: response.next_after.map(path).transpose()?,
            })
        })
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let response = self
            .unary(
                OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
                wire::CommitRequest {
                    conditions: request.conditions.into_iter().map(condition_wire).collect(),
                    mutations: request.mutations.into_iter().map(mutation_wire).collect(),
                    idempotency_key: Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
                    deadline_unix_millis: None,
                },
                |mut service, request| Box::pin(async move { service.commit(request).await }),
            )
            .await?;
        decode_response(response, commit_outcome_from_wire)
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        let response = self
            .unary(
                OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
                wire::CommitRequest {
                    conditions: request.conditions.into_iter().map(condition_wire).collect(),
                    mutations: request.mutations.into_iter().map(mutation_wire).collect(),
                    idempotency_key: Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
                    deadline_unix_millis: Some(deadline_unix_millis),
                },
                |mut service, request| Box::pin(async move { service.commit(request).await }),
            )
            .await?;
        decode_response(response, commit_outcome_from_wire)
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        let envelope = self
            .unary(
                OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
                wire::ReadCommitRequest {
                    commit_id: Bytes::copy_from_slice(commit_id.as_bytes()),
                },
                |mut service, request| Box::pin(async move { service.read_commit(request).await }),
            )
            .await?;
        decode_response(envelope, envelope_from_wire)
    }
}
