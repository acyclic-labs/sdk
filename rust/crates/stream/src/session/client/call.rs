//! Finite one-shot replies on the existing authenticated, credited session pool.
use super::*;

/// Original reply progress and, on source failure, its original typed cause.
#[derive(Debug, thiserror::Error)]
enum ReplyFailure {
    #[error(
        "invalid invocation length {bytes}, command limit {command_limit}, reply bound {reply_bound:?}"
    )]
    Invocation {
        bytes: usize,
        command_limit: usize,
        reply_bound: Option<usize>,
    },
    #[error("reply ended after {received} bytes; expected {expected:?}")]
    Ended {
        expected: Option<usize>,
        received: usize,
    },
    #[error("reply advertises {advertised} bytes beyond original bound {maximum}")]
    Bound { maximum: usize, advertised: usize },
    #[error("reply total changed from {expected} to {advertised} after {received} bytes")]
    Changed {
        expected: usize,
        advertised: usize,
        received: usize,
    },
    #[error("reply payload kind is invalid after {received} bytes")]
    Payload { received: usize },
    #[error("reply fragment of {fragment} bytes after {received} exceeds total {advertised}")]
    Length {
        advertised: usize,
        received: usize,
        fragment: usize,
    },
    #[error("reply failed after {received} bytes; expected {expected:?}: {source}")]
    Source {
        expected: Option<usize>,
        received: usize,
        #[source]
        source: StreamError,
    },
}

impl ReplyFailure {
    fn ended(expected: Option<usize>, received: usize) -> StreamError {
        StreamError::Unavailable.with_source(Self::Ended { expected, received })
    }
    fn source(source: StreamError, expected: Option<usize>, received: usize) -> StreamError {
        source.clone().with_source(Self::Source {
            expected,
            received,
            source,
        })
    }
}

impl Client {
    /// Invokes one canonical encoded Actors request on the shared session.
    ///
    /// The caller retains the invocation and its intent throughout this borrow.
    /// A lost connection returns an error; a call is never automatically replayed
    /// or resumed into a newly computed response. Only a complete reply is returned.
    /// `None` imposes no total byte limit; a finite limit is enforced before
    /// collecting any payload. The host must authorize `actors/invoke`.
    pub async fn actor_call(
        &self,
        invocation: &Bytes,
        credential: u32,
        maximum_reply_bytes: Option<usize>,
    ) -> Result<Bytes, StreamError> {
        validate_call(invocation, maximum_reply_bytes)?;
        self.reply(
            Kind::ActorCall(wire::SessionCall {
                invocation: invocation.clone(),
                input: false,
            }),
            credential,
            maximum_reply_bytes,
        )
        .await
    }

    /// Opens the same one-shot invocation and returns original reply fragments.
    ///
    /// Each item pairs the immutable total encoded reply length with its next
    /// fragment. Holding a fragment retains its original session receive credit;
    /// dropping the stream cancels its original interest. Source loss or an
    /// incomplete reply is a typed error, never a successful end. The request is
    /// still one complete invocation; this does not stream its input body.
    pub async fn actor_call_chunks(
        &self,
        invocation: &Bytes,
        credential: u32,
        maximum_reply_bytes: Option<usize>,
    ) -> Result<
        impl futures::Stream<
            Item = Result<(usize, crate::body::BodyChunk<impl Sized + use<>>), StreamError>,
        >
        + 'static
        + use<>,
        StreamError,
    > {
        validate_call(invocation, maximum_reply_bytes)?;
        self.reply_chunks(
            Kind::ActorCall(wire::SessionCall {
                invocation: invocation.clone(),
                input: false,
            }),
            credential,
            maximum_reply_bytes,
        )
        .await
    }

    /// Admit the canonical product header on the original call, returning its
    /// credited input sender and the SAME reply projection as a finite call.
    /// The handler may consume input and reply before EOF. No body collector,
    /// automatic replay or second authenticated connection is created.
    pub async fn actor_call_input(
        &self,
        invocation: &Bytes,
        credential: u32,
        maximum_reply_bytes: Option<usize>,
    ) -> Result<
        (
            CallInputSender,
            impl futures::Stream<
                Item = Result<(usize, crate::body::BodyChunk<impl Sized + use<>>), StreamError>,
            >
            + 'static
            + use<>,
        ),
        StreamError,
    > {
        validate_call(invocation, maximum_reply_bytes)?;
        let cursor = self
            .open_cursor(
                Kind::ActorCall(wire::SessionCall {
                    invocation: invocation.clone(),
                    input: true,
                }),
                cursor(0),
                false,
                false,
                None,
                credential,
            )
            .await?;
        let sender = input::State::sender(cursor.input.as_ref().ok_or(StreamError::Unavailable)?)?;
        Ok((sender, Self::reply_cursor(cursor, maximum_reply_bytes)))
    }

    /// Reads one page of direct children on the shared session, with the
    /// semantics of [`crate::StreamProviderExt::children_page`]. The host must
    /// authorize `streams/list` on the parent; each child is returned only
    /// when the credential may list it, and its tail record only when it may
    /// follow that stream. A page is a pure read: a lost connection reissues
    /// it until its first fragment arrives and fails afterwards.
    pub async fn children_page(
        &self,
        request: crate::ChildrenPageRequest,
        credential: u32,
    ) -> Result<crate::ChildrenPage, StreamError> {
        let reply = self
            .reply(
                Kind::ChildrenPage(wire_codec::children_page_request_to_wire(request.clone())),
                credential,
                None,
            )
            .await?;
        let page = wire_codec::children_page_from_response(
            wire::ChildrenPageResponse::decode(reply)
                .map_err(|source| StreamError::InvalidResponse.with_source(source))?,
        )?;
        page.validate_for(&request)?;
        Ok(page)
    }

    /// Observes named heads and children at one cut, on the shared credited session.
    /// A revision-only request checks credential context without a resource
    /// grant; named heads and children retain their resource authorization.
    pub async fn heads(
        &self,
        mut request: crate::HeadsRequest,
        credential: u32,
    ) -> Result<crate::Heads, StreamError> {
        request.validate()?;
        request.at_least = self.revisions.floor(&request.at_least)?;
        let reply = self
            .reply(
                Kind::Heads(wire_codec::heads_request_to_wire(request.clone())),
                credential,
                None,
            )
            .await?;
        let view = wire_codec::heads_from_response(
            wire::HeadsResponse::decode(reply)
                .map_err(|source| StreamError::InvalidResponse.with_source(source))?,
        )?;
        view.validate_for(&request)?;
        self.revisions.observe(&view.applied);
        Ok(view)
    }

    async fn reply(
        &self,
        kind: Kind,
        credential: u32,
        maximum_reply_bytes: Option<usize>,
    ) -> Result<Bytes, StreamError> {
        let work = async {
            let source = self
                .reply_chunks(kind, credential, maximum_reply_bytes)
                .await?;
            let mut source = core::pin::pin!(source);
            let mut reply = Vec::new();
            while let Some(item) = source.next().await {
                let (length, chunk) = item?;
                // Preserve original per-fragment receive credit and allocation.
                reply
                    .try_reserve(chunk.as_ref().len())
                    .map_err(|source| StreamError::Capacity.with_source(source))?;
                #[cfg(feature = "serde")]
                crate::trace::observe_sync(
                    crate::trace::Request {
                        name: "stream.reply_copy",
                        mechanism: crate::trace::Mechanism::Copy,
                        parent: None,
                        guarantee: Some("contiguous_complete_reply_ownership"),
                    },
                    || reply.extend_from_slice(chunk.as_ref()),
                    |_| {
                        Some((
                            u64::try_from(chunk.as_ref().len()).ok(),
                            crate::trace::Outcome::Succeeded,
                            None,
                        ))
                    },
                );
                #[cfg(not(feature = "serde"))]
                reply.extend_from_slice(chunk.as_ref());
                if reply.len() == length {
                    return Ok(reply.into());
                }
            }
            Err(ReplyFailure::ended(None, reply.len()))
        };
        #[cfg(feature = "serde")]
        {
            crate::trace::observe(
                crate::trace::Request {
                    name: "stream.finite_reply",
                    mechanism: crate::trace::Mechanism::RoundTrip,
                    parent: None,
                    guarantee: Some("complete_bounded_reply_on_original_session"),
                },
                work,
                |reply: &Bytes| u64::try_from(reply.len()).ok(),
            )
            .await
        }
        #[cfg(not(feature = "serde"))]
        work.await
    }

    async fn reply_chunks(
        &self,
        kind: Kind,
        credential: u32,
        maximum_reply_bytes: Option<usize>,
    ) -> Result<
        impl futures::Stream<Item = Result<(usize, crate::body::BodyChunk<Credit>), StreamError>>
        + 'static
        + use<>,
        StreamError,
    > {
        // Use the original cursor directly: the generic subscription projection
        // returns delivery credit before yielding its frame.
        let cursor = self
            .open_cursor(kind, cursor(0), false, false, None, credential)
            .await?;
        Ok(Self::reply_cursor(cursor, maximum_reply_bytes))
    }

    fn reply_cursor(
        cursor: Cursor,
        maximum_reply_bytes: Option<usize>,
    ) -> impl futures::Stream<Item = Result<(usize, crate::body::BodyChunk<Credit>), StreamError>>
    + 'static
    + use<> {
        let operation = cursor.observation.operation.clone();
        operation.scope(stream::try_unfold(
            (cursor, None, 0_usize, false),
            move |(mut cursor, total, received, complete)| async move {
                if complete {
                    return Ok(None);
                }
                let (data, credit) = cursor
                    .receive_data()
                    .await
                    .map_err(|source| ReplyFailure::source(source, total, received))?
                    .ok_or_else(|| ReplyFailure::ended(total, received))?;
                let length = wire_codec::session::actor_call_total(&data)?;
                if let Some(maximum) = maximum_reply_bytes
                    && length > maximum
                {
                    return Err(
                        StreamError::InvalidResponse.with_source(ReplyFailure::Bound {
                            maximum,
                            advertised: length,
                        }),
                    );
                }
                if let Some(expected) = total
                    && expected != length
                {
                    return Err(
                        StreamError::InvalidResponse.with_source(ReplyFailure::Changed {
                            expected,
                            advertised: length,
                            received,
                        }),
                    );
                }
                let bytes = match data.item {
                    Some(
                        wire::session_data::Item::ActorCall(bytes)
                        | wire::session_data::Item::ChildrenPage(bytes)
                        | wire::session_data::Item::Heads(bytes),
                    ) => bytes,
                    _ => {
                        return Err(StreamError::InvalidResponse
                            .with_source(ReplyFailure::Payload { received }));
                    }
                };
                let received = received
                    .checked_add(bytes.len())
                    .filter(|received| *received <= length)
                    .ok_or_else(|| {
                        StreamError::InvalidResponse.with_source(ReplyFailure::Length {
                            advertised: length,
                            received,
                            fragment: bytes.len(),
                        })
                    })?;
                let chunk = crate::body::BodyChunk::new(bytes, credit);
                if received == length && cursor.input.is_some() {
                    // A complete reply retires only its half. The same input
                    // owner retains the admitted call until EOF or cancellation.
                    cursor.progress.end();
                    cursor.observation.finish(None);
                }
                Ok(Some((
                    (length, chunk),
                    (cursor, Some(length), received, received == length),
                )))
            },
        ))
    }
}

fn validate_call(
    invocation: &Bytes,
    maximum_reply_bytes: Option<usize>,
) -> Result<(), StreamError> {
    if invocation.is_empty()
        || invocation.len() > crate::MAX_COMMAND_BYTES
        || maximum_reply_bytes == Some(0)
    {
        return Err(
            StreamError::InvalidArgument.with_source(ReplyFailure::Invocation {
                bytes: invocation.len(),
                command_limit: crate::MAX_COMMAND_BYTES,
                reply_bound: maximum_reply_bytes,
            }),
        );
    }
    Ok(())
}
