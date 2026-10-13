use super::*;

type Body<T> = futures::stream::BoxStream<'static, Result<T, Status>>;

/// Opens the documented `acyclic.stream.grpc.serve.<op>` span.
macro_rules! serve_span {
    ($op:literal) => {
        tracing::info_span!(
            concat!("acyclic.stream.grpc.serve.", $op),
            rpc.code = Empty,
            outcome = Empty,
            error.kind = Empty
        )
    };
}

fn wire_error(error: StreamError) -> Status {
    error_status(&error)
}

fn check_command_size<T: Message>(request: &Request<T>) -> Result<(), Status> {
    if request.get_ref().encoded_len() > crate::MAX_COMMAND_BYTES {
        Err(error_status(&StreamError::LimitExceeded))
    } else {
        Ok(())
    }
}

#[async_trait]
impl<P: StreamProvider> wire::stream_service_server::StreamService for Service<P> {
    type ReadStream = Body<wire::ReadResponse>;
    type FollowStream = Body<wire::ReadResponse>;
    type ChildrenStream = Body<wire::ChildrenResponse>;

    async fn inspect_idempotency(
        &self,
        request: Request<wire::InspectIdempotencyRequest>,
    ) -> Result<Response<wire::InspectIdempotencyResponse>, Status> {
        unary(serve_span!("inspect_idempotency"), async {
            check_command_size(&request)?;
            let key =
                IdempotencyKey::new(request.into_inner().idempotency_key).map_err(wire_error)?;
            let observation = self
                .provider
                .inspect_idempotency(key)
                .await
                .map_err(wire_error)?;
            Ok(wire::InspectIdempotencyResponse {
                observation: observation.map(observation_wire),
            })
        })
        .await
    }

    async fn append(
        &self,
        request: Request<wire::AppendRequest>,
    ) -> Result<Response<wire::AppendResponse>, Status> {
        unary(serve_span!("append"), async {
            check_command_size(&request)?;
            let request = append_from_wire(request.into_inner()).map_err(wire_error)?;
            let outcome = self.provider.append(request).await.map_err(wire_error)?;
            Ok(append_outcome_to_wire(outcome))
        })
        .await
    }

    async fn tail(
        &self,
        request: Request<wire::TailRequest>,
    ) -> Result<Response<wire::TailResponse>, Status> {
        unary(serve_span!("tail"), async {
            check_command_size(&request)?;
            let path = path(request.into_inner().path).map_err(wire_error)?;
            let bounds = self.provider.bounds(path).await.map_err(wire_error)?;
            Ok(wire::TailResponse { tail: bounds.tail })
        })
        .await
    }

    async fn fork(
        &self,
        request: Request<wire::ForkRequest>,
    ) -> Result<Response<wire::ForkReceipt>, Status> {
        unary(serve_span!("fork"), async {
            check_command_size(&request)?;
            let request = request.into_inner();
            let request = ForkRequest {
                source: path(request.source).map_err(wire_error)?,
                destination: path(request.destination).map_err(wire_error)?,
                at_tail: request.at_tail,
                idempotency_key: optional_key(request.idempotency_key).map_err(wire_error)?,
            };
            let receipt = self.provider.fork(request).await.map_err(wire_error)?;
            Ok(fork_receipt_to_wire(&receipt))
        })
        .await
    }

    async fn read(
        &self,
        request: Request<wire::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let span = serve_span!("read");
        streaming(span.clone(), async {
            check_command_size(&request)?;
            let request = request.into_inner();
            let request = ReadRequest {
                path: path(request.path).map_err(wire_error)?,
                from: request.from,
                limit: request.limit,
            };
            let records = self.provider.read(request).await.map_err(wire_error)?;
            Ok(observe_body(&span, records.map(record_frame)).boxed())
        })
        .await
    }

    async fn follow(
        &self,
        request: Request<wire::FollowRequest>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        let span = serve_span!("follow");
        streaming(span.clone(), async {
            check_command_size(&request)?;
            let request = request.into_inner();
            let path = path(request.path).map_err(wire_error)?;
            let records = self
                .provider
                .follow(path, request.from)
                .await
                .map_err(wire_error)?;
            Ok(observe_body(&span, records.map(record_frame)).boxed())
        })
        .await
    }

    async fn children(
        &self,
        request: Request<wire::ChildrenRequest>,
    ) -> Result<Response<Self::ChildrenStream>, Status> {
        let span = serve_span!("children");
        streaming(span.clone(), async {
            check_command_size(&request)?;
            let request = request.into_inner();
            let request = ChildrenRequest {
                parent: request.parent.map(path).transpose().map_err(wire_error)?,
                limit: request.limit,
            };
            let children = self.provider.children(request).await.map_err(wire_error)?;
            let children = children.map(|child| {
                child
                    .map(|child| wire::ChildrenResponse {
                        child: Some(wire::Child {
                            path: child.path.to_string(),
                        }),
                    })
                    .map_err(wire_error)
            });
            Ok(observe_body(&span, children).boxed())
        })
        .await
    }

    async fn children_page(
        &self,
        request: Request<wire::ChildrenPageRequest>,
    ) -> Result<Response<wire::ChildrenPageResponse>, Status> {
        unary(serve_span!("children_page"), async {
            let request = request.into_inner();
            let request = ChildrenPageRequest {
                parent: request.parent.map(path).transpose().map_err(wire_error)?,
                after: request.after.map(path).transpose().map_err(wire_error)?,
                hierarchy_version: request
                    .hierarchy_version
                    .as_deref()
                    .map(commit_id)
                    .transpose()
                    .map_err(wire_error)?,
                limit: request.limit,
            };
            let page = self
                .provider
                .children_page(request)
                .await
                .map_err(wire_error)?;
            Ok(wire::ChildrenPageResponse {
                hierarchy_version: Bytes::copy_from_slice(page.hierarchy_version.as_bytes()),
                children: page
                    .children
                    .into_iter()
                    .map(|child| wire::Child {
                        path: child.path.to_string(),
                    })
                    .collect(),
                next_after: page.next_after.map(|path| path.to_string()),
            })
        })
        .await
    }

    async fn commit(
        &self,
        request: Request<wire::CommitRequest>,
    ) -> Result<Response<wire::CommitResponse>, Status> {
        unary(serve_span!("commit"), async {
            check_command_size(&request)?;
            let request = request.into_inner();
            let deadline = request.deadline_unix_millis;
            let request = commit_from_wire(request).map_err(wire_error)?;
            let outcome = match deadline {
                Some(deadline) => self.provider.commit_before(request, deadline).await,
                None => self.provider.commit(request).await,
            }
            .map_err(wire_error)?;
            Ok(commit_outcome_to_wire(outcome))
        })
        .await
    }

    async fn read_commit(
        &self,
        request: Request<wire::ReadCommitRequest>,
    ) -> Result<Response<wire::CommittedEnvelope>, Status> {
        unary(serve_span!("read_commit"), async {
            check_command_size(&request)?;
            let commit_id = commit_id(&request.into_inner().commit_id).map_err(wire_error)?;
            let envelope = self
                .provider
                .read_commit(commit_id)
                .await
                .map_err(wire_error)?;
            Ok(envelope_to_wire(envelope))
        })
        .await
    }
}

fn record_frame(record: Result<Record, StreamError>) -> Result<wire::ReadResponse, Status> {
    record
        .map(|record| read_response_wire(vec![record_wire(record)]))
        .map_err(wire_error)
}

async fn unary<T>(
    span: tracing::Span,
    response: impl Future<Output = Result<T, Status>>,
) -> Result<Response<T>, Status> {
    let mut completion = Completion::new(span);
    let result = completion.span.scope(response).await;
    match &result {
        Ok(_) => completion.ok(),
        Err(error) => completion.status(error),
    }
    result.map(Response::new)
}

/// A streamed response completes on its body's EOF, observed by [`observe_body`].
async fn streaming<T>(
    span: tracing::Span,
    response: impl Future<Output = Result<T, Status>>,
) -> Result<Response<T>, Status> {
    let mut completion = Completion::new(span);
    let result = completion.span.scope(response).await;
    match &result {
        Ok(_) => completion.finished = true,
        Err(error) => completion.status(error),
    }
    result.map(Response::new)
}

fn observe_body<S, T>(
    span: &tracing::Span,
    body: S,
) -> impl Stream<Item = Result<T, Status>> + Unpin + use<S, T>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    let completion = Completion::new(span.clone());
    // Drop the producer under its origin before retiring completion.
    let mut state = (completion.span.scope(body), completion);
    stream::poll_fn(move |cx| {
        let (body, completion) = &mut state;
        if completion.finished {
            return Poll::Ready(None);
        }
        let next = Pin::new(body).poll_next(cx);
        match &next {
            Poll::Ready(Some(Err(error))) => completion.status(error),
            Poll::Ready(None) => completion.ok(),
            _ => {}
        }
        next
    })
}
