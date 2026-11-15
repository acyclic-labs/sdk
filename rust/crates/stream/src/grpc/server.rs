use super::*;
use crate::wire_codec;

type Body<T> = futures::stream::BoxStream<'static, Result<T, Status>>;

fn check_command_size<T: Message>(request: &Request<T>) -> Result<(), Status> {
    if request.get_ref().encoded_len() > crate::MAX_COMMAND_BYTES {
        Err(Status::from(&StreamError::LimitExceeded))
    } else {
        Ok(())
    }
}

#[async_trait]
impl<P: StreamProvider> wire::stream_service_server::StreamService for Service<P> {
    type ReadStream = Body<wire::ReadResponse>;

    async fn inspect_idempotency(
        &self,
        request: Request<wire::InspectIdempotencyRequest>,
    ) -> Result<Response<wire::InspectIdempotencyResponse>, Status> {
        unary("stream.grpc.serve.inspect_idempotency", async {
            check_command_size(&request)?;
            let key = IdempotencyKey::new(request.into_inner().idempotency_key)
                .map_err(|error| Status::from(&error))?;
            let observation = self
                .provider
                .inspect_idempotency(key)
                .await
                .map_err(|error| Status::from(&error))?;
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
        unary("stream.grpc.serve.append", async {
            check_command_size(&request)?;
            let request =
                append_from_wire(request.into_inner()).map_err(|error| Status::from(&error))?;
            let outcome = self
                .provider
                .append(request)
                .await
                .map_err(|error| Status::from(&error))?;
            Ok(append_outcome_to_wire(outcome))
        })
        .await
    }

    async fn heads(
        &self,
        request: Request<wire::HeadsRequest>,
    ) -> Result<Response<wire::HeadsResponse>, Status> {
        unary("stream.grpc.serve.heads", async {
            check_command_size(&request)?;
            let request = wire_codec::heads_from_wire(request.into_inner())
                .map_err(|error| Status::from(&error))?;
            let view = self
                .provider
                .heads(request)
                .await
                .map_err(|error| Status::from(&error))?;
            wire_codec::heads_to_wire(view).map_err(|error| Status::from(&error))
        })
        .await
    }

    async fn head(
        &self,
        request: Request<wire::HeadRequest>,
    ) -> Result<Response<wire::PathHead>, Status> {
        unary("stream.grpc.serve.head", async {
            check_command_size(&request)?;
            let path = path(request.into_inner().path).map_err(|error| Status::from(&error))?;
            let head = self
                .provider
                .head(path)
                .await
                .map_err(|error| Status::from(&error))?;
            Ok(wire_codec::head_to_wire(head))
        })
        .await
    }

    async fn tail(
        &self,
        request: Request<wire::TailRequest>,
    ) -> Result<Response<wire::TailResponse>, Status> {
        unary("stream.grpc.serve.tail", async {
            check_command_size(&request)?;
            wire_codec::tail_response(&*self.provider, request.into_inner())
                .await
                .map_err(|error| Status::from(&error))
        })
        .await
    }

    async fn fork(
        &self,
        request: Request<wire::ForkRequest>,
    ) -> Result<Response<wire::ForkReceipt>, Status> {
        unary("stream.grpc.serve.fork", async {
            check_command_size(&request)?;
            let request = request.into_inner();
            let request = ForkRequest {
                exclude: request
                    .exclude
                    .into_iter()
                    .map(path)
                    .collect::<Result<_, _>>()
                    .map_err(|error| Status::from(&error))?,
                source: path(request.source).map_err(|error| Status::from(&error))?,
                destination: path(request.destination).map_err(|error| Status::from(&error))?,
                at_tail: request.at_tail,
                idempotency_key: IdempotencyKey::new(request.idempotency_key)
                    .map_err(|error| Status::from(&error))?,
            };
            let receipt = self
                .provider
                .fork(request)
                .await
                .map_err(|error| Status::from(&error))?;
            Ok(fork_receipt_to_wire(&receipt))
        })
        .await
    }

    async fn read(
        &self,
        request: Request<wire::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        streaming("stream.grpc.serve.read", async {
            check_command_size(&request)?;
            let request = request.into_inner();
            let request =
                wire_codec::read_from_wire(request).map_err(|error| Status::from(&error))?;
            let path = request.path.clone();
            let records = self
                .provider
                .read(request)
                .await
                .map_err(|error| Status::from(&error))?;
            let mut applied = Some(wire_codec::revision_to_wire(&records.snapshot));
            let mut records = records.peekable();
            if std::pin::Pin::new(&mut records).peek().await.is_none() {
                // An empty read still reports the revision it was served at.
                let mut frame = read_response_wire(Vec::new());
                frame.applied = applied;
                return Ok(stream::iter([Ok(frame)]).boxed());
            }
            let records = records.map(move |record| {
                record_frame(record, &path).map(|mut frame| {
                    frame.applied = applied.take();
                    frame
                })
            });
            Ok(records.boxed())
        })
        .await
    }

    async fn children_page(
        &self,
        request: Request<wire::ChildrenPageRequest>,
    ) -> Result<Response<wire::ChildrenPageResponse>, Status> {
        unary("stream.grpc.serve.children_page", async {
            let request = wire_codec::children_page_from_wire(request.into_inner())
                .map_err(|error| Status::from(&error))?;
            let page = self
                .provider
                .children_page(request)
                .await
                .map_err(|error| Status::from(&error))?;
            Ok(wire_codec::children_page_to_wire(page))
        })
        .await
    }

    async fn commit(
        &self,
        request: Request<wire::CommitRequest>,
    ) -> Result<Response<wire::CommitResponse>, Status> {
        unary("stream.grpc.serve.commit", async {
            check_command_size(&request)?;
            let request = request.into_inner();
            let accepted = request.include_envelope;
            let request = commit_from_wire(request).map_err(|error| Status::from(&error))?;
            let request = if accepted {
                crate::WriteRequest::CommitAccepted(request)
            } else {
                crate::WriteRequest::Commit(request)
            };
            let outcome = self
                .provider
                .write(request)
                .await
                .map_err(|error| Status::from(&error))?;
            wire_codec::commit_write_outcome_to_wire(outcome).map_err(|error| Status::from(&error))
        })
        .await
    }

    async fn read_commit(
        &self,
        request: Request<wire::ReadCommitRequest>,
    ) -> Result<Response<wire::CommittedEnvelope>, Status> {
        unary("stream.grpc.serve.read_commit", async {
            check_command_size(&request)?;
            let commit_id =
                commit_id(&request.into_inner().commit_id).map_err(|error| Status::from(&error))?;
            let envelope = self
                .provider
                .read_commit(commit_id)
                .await
                .map_err(|error| Status::from(&error))?;
            Ok(envelope_to_wire(envelope))
        })
        .await
    }
}

fn record_frame(
    record: Result<Record, StreamError>,
    path: &crate::StreamPath,
) -> Result<wire::ReadResponse, Status> {
    record
        .map(|record| read_response_wire(vec![record_wire(record, Some(path))]))
        .map_err(|error| Status::from(&error))
}

async fn unary<T>(
    _name: &'static str,
    response: impl Future<Output = Result<T, Status>>,
) -> Result<Response<T>, Status> {
    #[cfg(feature = "serde")]
    let result = crate::trace::observe(
        crate::trace::Request {
            name: _name,
            mechanism: crate::trace::Mechanism::ProviderCall,
            parent: None,
            guarantee: Some("original_provider_result_encoded_for_rpc"),
        },
        response,
        |_| None,
    )
    .await;
    #[cfg(not(feature = "serde"))]
    let result = response.await;
    result.map(Response::new)
}

/// Admission transfers the same span to the original finite response body.
async fn streaming<T: Send + 'static>(
    _name: &'static str,
    response: impl Future<Output = Result<Body<T>, Status>>,
) -> Result<Response<Body<T>>, Status> {
    #[cfg(feature = "serde")]
    {
        let (result, recording) = crate::trace::accompany(
            crate::trace::Request {
                name: _name,
                mechanism: crate::trace::Mechanism::ProviderCall,
                parent: None,
                guarantee: Some("original_finite_read_cut_delivered_through_body_eof"),
            },
            response,
        )
        .await;
        result.map(|body| Response::new(crate::trace::RecordedStream::new(body, recording).boxed()))
    }
    #[cfg(not(feature = "serde"))]
    response.await.map(Response::new)
}
