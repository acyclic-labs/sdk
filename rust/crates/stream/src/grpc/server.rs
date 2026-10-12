use super::*;

#[async_trait]
impl<P: StreamProvider> wire::stream_service_server::StreamService for Service<P> {
    type ReadStream = futures::stream::BoxStream<'static, Result<wire::ReadResponse, Status>>;
    type FollowStream = futures::stream::BoxStream<'static, Result<wire::ReadResponse, Status>>;
    type ChildrenStream =
        futures::stream::BoxStream<'static, Result<wire::ChildrenResponse, Status>>;

    async fn acknowledge_delivery(
        &self,
        request: Request<wire::AcknowledgeDeliveryRequest>,
    ) -> Result<Response<wire::AcknowledgeDeliveryResponse>, Status> {
        check_command_size(request.get_ref())?;
        if request.get_ref().delivery_token.len() != 32 {
            return Err(error_status(&StreamError::InvalidArgument));
        }
        // Provider-independent adapters have no hosted receipt authority.
        Err(error_status(&StreamError::Unsupported))
    }

    async fn inspect_idempotency(
        &self,
        request: Request<wire::InspectIdempotencyRequest>,
    ) -> Result<Response<wire::InspectIdempotencyResponse>, Status> {
        let span = tracing::info_span!(
            "acyclic.stream.grpc.serve.inspect_idempotency",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        serve(
            span.clone(),
            async {
                check_command_size(request.get_ref())?;
                let key = IdempotencyKey::new(request.into_inner().idempotency_key)
                    .map_err(|error| error_status(&error))?;
                let observation = self
                    .provider
                    .inspect_idempotency(key)
                    .await
                    .map_err(|error| error_status(&error))?
                    .map(observation_wire);
                Ok(Response::new(wire::InspectIdempotencyResponse {
                    observation,
                }))
            },
            false,
        )
        .await
    }

    async fn append(
        &self,
        request: Request<wire::AppendRequest>,
    ) -> Result<Response<wire::AppendResponse>, Status> {
        let span = tracing::info_span!(
            "acyclic.stream.grpc.serve.append",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        serve(
            span.clone(),
            async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let outcome = self
                    .provider
                    .append(append_from_wire(request).map_err(|error| error_status(&error))?)
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(append_outcome_to_wire(outcome)))
            },
            false,
        )
        .await
    }

    async fn tail(
        &self,
        request: Request<wire::TailRequest>,
    ) -> Result<Response<wire::TailResponse>, Status> {
        let span = tracing::info_span!(
            "acyclic.stream.grpc.serve.tail",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        serve(
            span.clone(),
            async {
                check_command_size(request.get_ref())?;
                let path = path(request.into_inner().path).map_err(|error| error_status(&error))?;
                let bounds = self
                    .provider
                    .bounds(path)
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(wire::TailResponse { tail: bounds.tail }))
            },
            false,
        )
        .await
    }

    async fn fork(
        &self,
        request: Request<wire::ForkRequest>,
    ) -> Result<Response<wire::ForkReceipt>, Status> {
        let span = tracing::info_span!(
            "acyclic.stream.grpc.serve.fork",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        serve(
            span.clone(),
            async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let receipt = self
                    .provider
                    .fork(ForkRequest {
                        source: path(request.source).map_err(|error| error_status(&error))?,
                        destination: path(request.destination)
                            .map_err(|error| error_status(&error))?,
                        at_tail: request.at_tail,
                        idempotency_key: optional_key(request.idempotency_key)
                            .map_err(|error| error_status(&error))?,
                    })
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(fork_receipt_to_wire(&receipt)))
            },
            false,
        )
        .await
    }

    async fn read(
        &self,
        request: Request<wire::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let span = tracing::info_span!(
            "acyclic.stream.grpc.serve.read",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        serve(
            span.clone(),
            async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let records = self
                    .provider
                    .read(ReadRequest {
                        path: path(request.path).map_err(|error| error_status(&error))?,
                        from: request.from,
                        limit: request.limit,
                    })
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(
                    observe_body(
                        &span,
                        records.map(|record| {
                            record
                                .map(record_wire)
                                .map(|record| read_response_wire(vec![record]))
                                .map_err(|error| error_status(&error))
                        }),
                    )
                    .boxed(),
                ))
            },
            true,
        )
        .await
    }

    async fn follow(
        &self,
        request: Request<wire::FollowRequest>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        let span = tracing::info_span!(
            "acyclic.stream.grpc.serve.follow",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        serve(
            span.clone(),
            async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let records = self
                    .provider
                    .follow(
                        path(request.path).map_err(|error| error_status(&error))?,
                        request.from,
                    )
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(
                    observe_body(
                        &span,
                        records.map(|record| {
                            record
                                .map(record_wire)
                                .map(|record| read_response_wire(vec![record]))
                                .map_err(|error| error_status(&error))
                        }),
                    )
                    .boxed(),
                ))
            },
            true,
        )
        .await
    }

    async fn children(
        &self,
        request: Request<wire::ChildrenRequest>,
    ) -> Result<Response<Self::ChildrenStream>, Status> {
        let span = tracing::info_span!(
            "acyclic.stream.grpc.serve.children",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        serve(
            span.clone(),
            async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let children = self
                    .provider
                    .children(ChildrenRequest {
                        parent: request
                            .parent
                            .map(path)
                            .transpose()
                            .map_err(|error| error_status(&error))?,
                        limit: request.limit,
                    })
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(
                    observe_body(
                        &span,
                        children.map(|child| {
                            child
                                .map(|child| wire::ChildrenResponse {
                                    child: Some(wire::Child {
                                        path: child.path.to_string(),
                                    }),
                                })
                                .map_err(|error| error_status(&error))
                        }),
                    )
                    .boxed(),
                ))
            },
            true,
        )
        .await
    }

    async fn children_page(
        &self,
        request: Request<wire::ChildrenPageRequest>,
    ) -> Result<Response<wire::ChildrenPageResponse>, Status> {
        let span = tracing::info_span!(
            "acyclic.stream.grpc.serve.children_page",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        serve(
            span.clone(),
            async {
                let request = request.into_inner();
                let page = self
                    .provider
                    .children_page(ChildrenPageRequest {
                        parent: request
                            .parent
                            .map(path)
                            .transpose()
                            .map_err(|error| error_status(&error))?,
                        after: request
                            .after
                            .map(path)
                            .transpose()
                            .map_err(|error| error_status(&error))?,
                        hierarchy_version: request
                            .hierarchy_version
                            .as_deref()
                            .map(commit_id)
                            .transpose()
                            .map_err(|error| error_status(&error))?,
                        limit: request.limit,
                    })
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(wire::ChildrenPageResponse {
                    hierarchy_version: Bytes::copy_from_slice(page.hierarchy_version.as_bytes()),
                    children: page
                        .children
                        .into_iter()
                        .map(|child| wire::Child {
                            path: child.path.to_string(),
                        })
                        .collect(),
                    next_after: page.next_after.map(|path| path.to_string()),
                }))
            },
            false,
        )
        .await
    }

    async fn commit(
        &self,
        request: Request<wire::CommitRequest>,
    ) -> Result<Response<wire::CommitResponse>, Status> {
        let span = tracing::info_span!(
            "acyclic.stream.grpc.serve.commit",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        serve(
            span.clone(),
            async {
                check_command_size(request.get_ref())?;
                let request = request.into_inner();
                let deadline_unix_millis = request.deadline_unix_millis;
                let request = commit_from_wire(request).map_err(|error| error_status(&error))?;
                let outcome = if let Some(deadline) = deadline_unix_millis {
                    self.provider.commit_before(request, deadline).await
                } else {
                    self.provider.commit(request).await
                }
                .map_err(|error| error_status(&error))?;
                Ok(Response::new(commit_outcome_to_wire(outcome)))
            },
            false,
        )
        .await
    }

    async fn read_commit(
        &self,
        request: Request<wire::ReadCommitRequest>,
    ) -> Result<Response<wire::CommittedEnvelope>, Status> {
        let span = tracing::info_span!(
            "acyclic.stream.grpc.serve.read_commit",
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        );
        serve(
            span.clone(),
            async {
                check_command_size(request.get_ref())?;
                let commit_id = commit_id(&request.into_inner().commit_id)
                    .map_err(|error| error_status(&error))?;
                let envelope = self
                    .provider
                    .read_commit(commit_id)
                    .await
                    .map_err(|error| error_status(&error))?;
                Ok(Response::new(envelope_to_wire(envelope)))
            },
            false,
        )
        .await
    }
}

async fn serve<T>(
    span: tracing::Span,
    response: impl Future<Output = Result<T, Status>>,
    streaming: bool,
) -> Result<T, Status> {
    let mut completion = Completion::new(span);
    let result = completion.span.scope(response).await;
    match &result {
        Err(error) => completion.status(error),
        Ok(_) if streaming => completion.finished = true,
        Ok(_) => completion.ok(),
    }
    result
}
