//! The one wire dispatch over any [`Blobs`]: gRPC and binary adapters serve
//! each route through these functions, each paired with the revision its
//! result carries.
use crate::convert::{self, reply};
use crate::error::invalid;
use crate::{Blobs, Digest, Error, ObjectsMethod, RootId, wire};
use acyclic_auth::action::{Action, ObjectsAction};
use acyclic_stream::Revision;
use bytes::Bytes;
use prost::Message;

macro_rules! http_routes {
    (
        unary { $($method:ident => ($route:literal, $input:ident, $output:ident)),+ $(,)? }
        streaming { $($stream:ident => ($stream_route:literal, $stream_in:ident, $stream_out:ident)),+ $(,)? }
    ) => {
        /// Canonical unary HTTP route and message names relative to `/v1/objects/`.
        pub const HTTP_ROUTES: &[(&str, &str, &str)] = &[
            $(ObjectsMethod::$method.http_metadata()),+
        ];

        /// Streaming HTTP routes with length-delimited Protobuf frames.
        pub const HTTP_STREAM_ROUTES: &[(&str, &str, &str)] = &[
            $(ObjectsMethod::$stream.http_metadata()),+
        ];

        impl ObjectsMethod {
            /// Resolve an exact binary HTTP route to its generated method identity.
            #[must_use]
            pub fn from_http_route(route: &str) -> Option<Self> {
                match route {
                    $($route => Some(Self::$method),)+
                    $($stream_route => Some(Self::$stream),)+
                    _ => None,
                }
            }

            /// Canonical binary HTTP route relative to `/v1/objects/`.
            #[must_use]
            pub const fn http_route(self) -> &'static str {
                self.http_metadata().0
            }

            /// Canonical Protobuf request and response (or stream item) names.
            #[must_use]
            pub const fn http_message_types(self) -> (&'static str, &'static str) {
                let (_, input, output) = self.http_metadata();
                (input, output)
            }

            #[cfg(not(target_arch = "wasm32"))]
            pub(crate) const fn is_streaming_http(self) -> bool {
                matches!(self, $(Self::$stream)|+)
            }

            const fn http_metadata(self) -> (&'static str, &'static str, &'static str) {
                match self {
                    $(Self::$method => ($route, stringify!($input), stringify!($output)),)+
                    $(Self::$stream => ($stream_route, stringify!($stream_in), stringify!($stream_out)),)+
                }
            }
        }
    };
}

crate::route_inventory::objects_http_routes!(http_routes);

impl ObjectsMethod {
    /// The exact authorization action declared by this method's contract.
    /// An absent, foreign or non-singleton declaration refuses admission.
    #[must_use]
    pub const fn action(self) -> Option<ObjectsAction> {
        match self.authorization().single_action() {
            Some(Action::Objects(action)) => Some(action),
            _ => None,
        }
    }
}

/// A route's response and the revision committing or serving it.
pub type Served<T> = (T, Option<Revision>);

/// Registers the authenticated account's exact physical identity before its
/// first cursor is returned. Both streaming transports use this one dispatch.
pub async fn availability(
    blobs: &dyn Blobs,
    request: wire::AvailabilityRequest,
) -> Result<crate::Availability, Error> {
    blobs
        .availability(convert::digest(request.digest.as_ref())?)
        .await
}

/// Canonical opening cursor followed by coalesced changes. EOF/failure remains
/// an error; it never fabricates a successful verification or catalog revision.
pub fn availability_frames(
    observation: crate::Availability,
) -> impl futures::Stream<Item = Result<wire::AvailabilityCursor, Error>>
+ acyclic_stream::ProviderTask
+ 'static {
    use futures::{StreamExt as _, stream};
    let initial = observation.cursor();
    stream::once(std::future::ready(Ok(initial.into()))).chain(stream::try_unfold(
        observation,
        |mut observation| async move {
            let next = observation.changed().await?;
            Ok(Some((next.into(), observation)))
        },
    ))
}

/// Serves a streaming binary route using canonical Protobuf message framing.
/// HTTP gateways authenticate the account before calling this, and revalidate
/// their original grant before disclosing each returned frame.
pub async fn invoke_stream(
    blobs: &dyn Blobs,
    route: &str,
    input: Bytes,
) -> Result<acyclic_stream::BoxProviderStream<'static, Result<Vec<u8>, Error>>, Error> {
    use futures::StreamExt as _;
    if ObjectsMethod::from_http_route(route) != Some(ObjectsMethod::Availability) {
        return Err(wire::ErrorCode::InvalidArgument.into());
    }
    let observation = availability(blobs, decode(input)?).await?;
    Ok(Box::pin(
        availability_frames(observation).map(|frame| Ok(frame?.encode_length_delimited_to_vec())),
    ))
}

/// Verifies the supplied digest, then publishes the body.
///
/// # Errors
/// The provider's semantic failure.
pub async fn put_blob(
    blobs: &dyn Blobs,
    request: wire::PutBlobRequest,
) -> Result<Served<wire::PutResponse>, Error> {
    let (body, references) = member(request)?;
    let put = blobs.put_durable(body, references).await?;
    Ok((convert::wire_put(&put), put_revision(&put)))
}

/// Verifies each member's digest, publishes the members through the one
/// batch driver, then waits for durability once for the whole batch.
///
/// # Errors
/// `Unavailable` when the provider answers another number of members. Each
/// member's own failure is its result instead.
pub async fn put_blobs(
    blobs: &dyn Blobs,
    request: wire::PutBlobsRequest,
) -> Result<Served<wire::PutBlobsResponse>, Error> {
    let mut outcomes: Vec<Option<Result<crate::Put, Error>>> =
        Vec::with_capacity(request.puts.len());
    let mut admitted = Vec::with_capacity(request.puts.len());
    for put in request.puts {
        match member(put) {
            Ok(member) => {
                admitted.push(member);
                outcomes.push(None);
            }
            Err(error) => outcomes.push(Some(Err(error))),
        }
    }
    let mut published = blobs.put_batch_durable(admitted).await.into_iter();
    for outcome in &mut outcomes {
        if outcome.is_none() {
            *outcome = Some(published.next().ok_or_else(|| {
                Error::caused(
                    wire::ErrorCode::Unavailable,
                    acyclic_auth::refusal::Cause::new("objects.route", "batch_result_missing"),
                )
            })?);
        }
    }
    if published.next().is_some() {
        return Err(Error::caused(
            wire::ErrorCode::Unavailable,
            acyclic_auth::refusal::Cause::new("objects.route", "batch_result_surplus"),
        ));
    }
    let outcomes: Vec<_> = outcomes.into_iter().flatten().collect();
    let revision = covering(outcomes.iter().flatten());
    let results = outcomes
        .into_iter()
        .map(|outcome| wire::PutResult {
            outcome: Some(match outcome {
                Ok(put) => wire::put_result::Outcome::Put(convert::wire_put(&put)),
                Err(error) => wire::put_result::Outcome::Error(error.into()),
            }),
        })
        .collect();
    Ok((wire::PutBlobsResponse { results }, revision))
}

/// One batch member, hashed once here at the trust boundary.
fn member(put: wire::PutBlobRequest) -> Result<(crate::Sealed, Vec<Digest>), Error> {
    let expected = convert::digest(put.digest.as_ref())?;
    let body = crate::Sealed::new(put.body.into());
    if Digest::sealed(&body) != expected {
        return Err(wire::ErrorCode::DigestMismatch.into());
    }
    Ok((body, convert::digests(&put.references)?))
}

/// The one revision covering every member's: the latest of one authority.
/// Members committed under several authorities have no single revision.
fn covering<'a>(puts: impl Iterator<Item = &'a crate::Put>) -> Option<Revision> {
    let mut covering: Option<Revision> = None;
    for revision in puts.filter_map(put_revision) {
        match &mut covering {
            None => covering = Some(revision),
            Some(held) if held.same_authority(&revision) => {
                if revision.index > held.index {
                    *held = revision;
                }
            }
            Some(_) => return None,
        }
    }
    covering
}

fn put_revision(put: &crate::Put) -> Option<Revision> {
    put.verified()
        .and_then(|verified| verified.revision.clone())
}

/// Publishes a manifest of existing parts.
///
/// # Errors
/// The provider's semantic failure.
pub async fn put_manifest(
    blobs: &dyn Blobs,
    request: wire::PutManifestRequest,
) -> Result<Served<wire::PutResponse>, Error> {
    let manifest = convert::from_manifest(request.manifest.as_ref())?;
    let put = blobs.put_manifest_durable(manifest).await?;
    Ok((convert::wire_put(&put), put_revision(&put)))
}

/// Reads a logical range.
///
/// # Errors
/// The provider's semantic failure.
pub async fn get_blob(
    blobs: &dyn Blobs,
    request: wire::GetBlobRequest,
) -> Result<Served<wire::GetBlobResponse>, Error> {
    let (digest, range) = convert::from_get(&request)?;
    let object = blobs.get(digest, range, request.maximum_bytes).await?;
    let revision = object.revision().cloned();
    Ok((
        wire::GetBlobResponse {
            info: Some(convert::wire_head(object.head())),
            start: object.start(),
            body: bytes::Bytes::from_owner(object.into_body()),
        },
        revision,
    ))
}

/// Reads an envelope.
///
/// # Errors
/// The provider's semantic failure.
pub async fn head_blob(
    blobs: &dyn Blobs,
    request: wire::HeadBlobRequest,
) -> Result<Served<wire::BlobInfo>, Error> {
    let digest = convert::digest(request.digest.as_ref())?;
    let head = blobs.head(digest).await?;
    Ok((convert::wire_head(&head), head.revision))
}

/// Deletes an unreferenced blob.
///
/// # Errors
/// The provider's semantic failure.
pub async fn delete_blob(
    blobs: &dyn Blobs,
    request: wire::DeleteBlobRequest,
) -> Result<Served<wire::DeleteBlobResponse>, Error> {
    let digest = convert::digest(request.digest.as_ref())?;
    let mut deletion = blobs.delete(digest).await?;
    let revision = deletion.revision.take();
    Ok((convert::wire_delete(deletion), revision))
}

/// Lists digests after a cursor.
///
/// # Errors
/// The provider's semantic failure.
pub async fn list_blobs(
    blobs: &dyn Blobs,
    request: wire::ListBlobsRequest,
) -> Result<Served<wire::ListBlobsResponse>, Error> {
    let after = convert::optional_digest(request.after.as_ref())?;
    let page = blobs.list(after, request.limit).await?;
    Ok((convert::wire_page(&page), page.revision))
}

fn retained_reply(retained: crate::Retained) -> Served<wire::RetainResponse> {
    (
        wire::RetainResponse {
            references: retained.references,
        },
        retained.revision,
    )
}

fn released_reply(released: crate::Released) -> Served<wire::ReleaseResponse> {
    (
        wire::ReleaseResponse {
            existed: released.existed,
        },
        released.revision,
    )
}

/// Appends a reference batch to a root.
///
/// # Errors
/// The provider's semantic failure.
pub async fn retain(
    blobs: &dyn Blobs,
    request: wire::RetainRequest,
) -> Result<Served<wire::RetainResponse>, Error> {
    let references = convert::digests(&request.references)?;
    let retained = blobs
        .retain(&RootId::new(request.root)?, references)
        .await?;
    Ok(retained_reply(retained))
}

/// Releases a root.
///
/// # Errors
/// The provider's semantic failure.
pub async fn release(
    blobs: &dyn Blobs,
    request: wire::ReleaseRequest,
) -> Result<Served<wire::ReleaseResponse>, Error> {
    let released = blobs.release(&RootId::new(request.root)?).await?;
    Ok(released_reply(released))
}

/// Retains one batch at a fenced intent's original generation.
pub async fn retain_fenced(
    blobs: &dyn Blobs,
    request: wire::RetainFencedRequest,
) -> Result<Served<wire::RetainResponse>, Error> {
    let retained = blobs
        .retain_fenced(
            &RootId::new(request.root)?,
            request.fence,
            convert::digests(&request.references)?,
        )
        .await?;
    Ok(retained_reply(retained))
}

/// Durably closes a fenced intent through its supplied generation.
pub async fn release_terminal(
    blobs: &dyn Blobs,
    request: wire::ReleaseTerminalRequest,
) -> Result<Served<wire::ReleaseResponse>, Error> {
    let released = blobs
        .release_terminal(&RootId::new(request.root)?, request.fence)
        .await?;
    Ok(released_reply(released))
}

/// Runs one mark-and-sweep pass.
///
/// # Errors
/// The provider's semantic failure.
pub async fn collect(
    blobs: &dyn Blobs,
    _request: wire::CollectRequest,
) -> Result<Served<wire::CollectResponse>, Error> {
    let collection = blobs.collect().await?;
    Ok((convert::wire_collection(&collection), collection.revision))
}

/// Reports unique stored bytes.
///
/// # Errors
/// The provider's semantic failure.
pub async fn usage(
    blobs: &dyn Blobs,
    _request: wire::UsageRequest,
) -> Result<Served<wire::UsageResponse>, Error> {
    let usage = blobs.usage().await?;
    Ok((
        wire::UsageResponse {
            blobs: usage.blobs,
            unique_bytes: usage.unique_bytes,
        },
        None,
    ))
}

fn decode<T: Message + Default>(bytes: Bytes) -> Result<T, Error> {
    T::decode(bytes).map_err(|_| invalid())
}

/// Serves one [`crate::HTTP_ROUTES`] route from its binary input message,
/// returning the binary output message.
/// The owned input backs decoded body fields without copying their contents.
///
/// # Errors
/// The route's semantic failure, or `InvalidArgument` for an unknown route or
/// a malformed input.
pub async fn invoke(blobs: &dyn Blobs, route: &str, input: Bytes) -> Result<Vec<u8>, Error> {
    Ok(invoke_at(blobs, route, input).await?.0)
}

/// Serves the canonical binary operation without discarding its original revision.
/// The transport must keep the body and this revision in the same admitted reply.
pub async fn invoke_at(
    blobs: &dyn Blobs,
    route: &str,
    input: Bytes,
) -> Result<Served<Vec<u8>>, Error> {
    fn encode<T: Message>((body, revision): Served<T>) -> Served<Vec<u8>> {
        (body.encode_to_vec(), revision)
    }
    Ok(
        match ObjectsMethod::from_http_route(route).ok_or(wire::ErrorCode::InvalidArgument)? {
            ObjectsMethod::PutBlob => encode(put_blob(blobs, decode(input)?).await?),
            ObjectsMethod::PutBlobs => encode(put_blobs(blobs, decode(input)?).await?),
            ObjectsMethod::PutManifest => encode(put_manifest(blobs, decode(input)?).await?),
            ObjectsMethod::GetBlob => encode(get_blob(blobs, decode(input)?).await?),
            ObjectsMethod::HeadBlob => encode(head_blob(blobs, decode(input)?).await?),
            ObjectsMethod::DeleteBlob => encode(delete_blob(blobs, decode(input)?).await?),
            ObjectsMethod::ListBlobs => encode(list_blobs(blobs, decode(input)?).await?),
            ObjectsMethod::Retain => encode(retain(blobs, decode(input)?).await?),
            ObjectsMethod::Release => encode(release(blobs, decode(input)?).await?),
            ObjectsMethod::RetainFenced => encode(retain_fenced(blobs, decode(input)?).await?),
            ObjectsMethod::ReleaseTerminal => {
                encode(release_terminal(blobs, decode(input)?).await?)
            }
            ObjectsMethod::Collect => encode(collect(blobs, decode(input)?).await?),
            ObjectsMethod::Usage => encode(usage(blobs, decode(input)?).await?),
            ObjectsMethod::Availability => return Err(wire::ErrorCode::InvalidArgument.into()),
        },
    )
}

/// Checks one [`crate::HTTP_ROUTES`] reply against the request it answers,
/// exactly as the Rust clients check theirs, for clients in other languages
/// that carry the binary messages themselves.
/// Owned buffers back decoded bodies for the duration of validation.
///
/// A put request's digest is trusted as the caller's own: the client hashed
/// the body when it built the request, and the service refuses a mismatch.
///
/// # Errors
/// `InvalidArgument` for an unknown route or malformed request, and
/// `Unavailable` for a reply that is malformed or does not answer it.
pub fn verify(route: &str, input: Bytes, output: Bytes) -> Result<(), Error> {
    match ObjectsMethod::from_http_route(route).ok_or(wire::ErrorCode::InvalidArgument)? {
        ObjectsMethod::PutBlob => {
            let request: wire::PutBlobRequest = decode(input)?;
            let digest = convert::digest(request.digest.as_ref())?;
            convert::verified_put(&reply(output)?, digest, request.body.len() as u64, None)
                .map(drop)
        }
        ObjectsMethod::PutBlobs => {
            let request: wire::PutBlobsRequest = decode(input)?;
            let expected = request
                .puts
                .iter()
                .map(|put| Ok((convert::digest(put.digest.as_ref())?, put.body.len() as u64)))
                .collect::<Result<Vec<_>, Error>>()?;
            convert::verified_puts(reply(output)?, &expected, None).map(drop)
        }
        ObjectsMethod::PutManifest => {
            let request: wire::PutManifestRequest = decode(input)?;
            let manifest = convert::from_manifest(request.manifest.as_ref())?;
            let (digest, length) = (Digest::of(&manifest.encode()?), manifest.length()?);
            convert::verified_put(&reply(output)?, digest, length, None).map(drop)
        }
        ObjectsMethod::GetBlob => {
            let request: wire::GetBlobRequest = decode(input)?;
            let (digest, range) = convert::from_get(&request)?;
            convert::object(reply(output)?, digest, range, request.maximum_bytes, None).map(drop)
        }
        ObjectsMethod::HeadBlob => {
            let request: wire::HeadBlobRequest = decode(input)?;
            let digest = convert::digest(request.digest.as_ref())?;
            convert::headed(&reply(output)?, digest, None).map(drop)
        }
        ObjectsMethod::DeleteBlob => convert::delete(&reply(output)?, None).map(drop),
        ObjectsMethod::ListBlobs => {
            let request: wire::ListBlobsRequest = decode(input)?;
            let after = convert::optional_digest(request.after.as_ref())?;
            convert::listed(&reply(output)?, after, request.limit, None).map(drop)
        }
        ObjectsMethod::Retain | ObjectsMethod::RetainFenced => {
            reply::<wire::RetainResponse>(output).map(drop)
        }
        ObjectsMethod::Release | ObjectsMethod::ReleaseTerminal => {
            reply::<wire::ReleaseResponse>(output).map(drop)
        }
        ObjectsMethod::Collect => reply::<wire::CollectResponse>(output).map(drop),
        ObjectsMethod::Usage => reply::<wire::UsageResponse>(output).map(drop),
        ObjectsMethod::Availability => Err(wire::ErrorCode::InvalidArgument.into()),
    }
}

#[cfg(test)]
mod tests;
