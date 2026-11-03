//! Revision admission over the caller's original gRPC service and response body.
use super::*;
use futures::future::{Either, Ready, ready};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tonic::codegen::{Service, http};

/// Revision-conditioned generated Objects calls over any admitted Tower service.
///
/// Clones share observations; [`Self::with_revision_scope`] selects one account
/// without discarding the other accounts' floors. Explicit request minima are
/// merged with that floor, including for PUT and DELETE. These are minimum
/// applied revisions, not compare-and-swap conditions on blob contents.
///
/// A successful final gRPC status must satisfy the requested revision floor.
/// Failed calls retain their original status/details/trailers and may advance
/// observations from valid server revision metadata. Unary clients receive no
/// successful value below their floor. Streaming prefixes are not atomic unary
/// results: this transport never buffers a body or delays its data frames.
#[derive(Clone)]
pub struct RevisionChannel<S> {
    inner: S,
    revisions: RevisionArc<RevisionTracker>,
}

impl<S> RevisionChannel<S> {
    /// Retains a caller-authenticated transport, with no connection or I/O.
    #[must_use]
    pub fn new(inner: S) -> Self {
        Self::with_revisions(inner, RevisionArc::default())
    }

    /// Retains the same revision frontier across replacement transports.
    #[must_use]
    pub fn with_revisions(inner: S, revisions: RevisionArc<RevisionTracker>) -> Self {
        Self { inner, revisions }
    }

    /// Selects the implicit account floor; every clone keeps shared observations.
    #[must_use]
    pub fn with_revision_scope(mut self, scope: acyclic_stream::RevisionScope) -> Self {
        self.revisions = RevisionTracker::scoped(&self.revisions, scope);
        self
    }

    /// The same revision owner used by every cloned transport.
    #[must_use]
    pub fn revisions(&self) -> &RevisionTracker {
        &self.revisions
    }
}

/// One common admission rule for typed clients and generic gRPC services.
pub(super) fn admit(
    tracker: &RevisionTracker,
    floor: &RevisionCut,
    revision: Result<Option<Revision>, Error>,
    success: bool,
) -> Result<Option<Revision>, Error> {
    if !success {
        let revision = revision.ok().flatten();
        if let Some(revision) = &revision {
            tracker.observe(revision);
        }
        return Ok(revision);
    }
    let revision = revision?;
    tracker
        .served(floor, revision.as_ref())
        .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?;
    Ok(revision)
}

pub(super) fn requested(metadata: &MetadataMap) -> Result<RevisionCut, tonic::Status> {
    cut_from_wire(
        metadata
            .get_all_bin(MINIMUM_REVISION)
            .iter()
            .map(|value| {
                value
                    .to_bytes()
                    .ok()
                    .and_then(|bytes| acyclic_stream::wire::Revision::decode(bytes).ok())
                    .ok_or_else(|| {
                        tonic::Status::invalid_argument("invalid Objects minimum revision")
                    })
            })
            .collect::<Result<Vec<_>, _>>()?,
    )
    .map_err(|_| tonic::Status::invalid_argument("invalid Objects minimum revision"))
}

/// Exercises the original revision metadata decoders and terminal admission.
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub fn check_revision_metadata(bytes: &[u8]) -> bool {
    // The server parses grpc-timeout only after the HTTP ASCII boundary.
    if let Ok(value) = http::HeaderValue::from_bytes(bytes)
        && let Ok(value) = value.to_str()
    {
        let _ = super::grpc_timeout(value);
    }
    let mut word = [0; 4];
    for (field, byte) in word.iter_mut().zip(bytes) {
        *field = *byte;
    }
    // Tonic's original request writer selects all six units at its exact
    // eight-digit boundary. Smaller generated amounts also retain their value.
    for unit in [
        std::time::Duration::from_secs(3600),
        std::time::Duration::from_secs(60),
        std::time::Duration::from_secs(1),
        std::time::Duration::from_millis(1),
        std::time::Duration::from_micros(1),
        std::time::Duration::from_nanos(1),
    ] {
        for amount in [99_999_999, u32::from_le_bytes(word) % 100_000_000] {
            let duration = unit * amount;
            let mut request = Request::new(());
            request.set_timeout(duration);
            let written = request
                .metadata()
                .get("grpc-timeout")
                .expect("original timeout writer emits timeout metadata")
                .to_str()
                .expect("original timeout writer produces ASCII");
            if super::grpc_timeout(written) != Some(duration) {
                return false;
            }
            let mut invalid = written.to_owned();
            invalid.pop();
            invalid.push('!');
            let mut nondigit = written.to_owned();
            nondigit.replace_range(..1, "x");
            let oversized = format!("000000000{written}");
            for invalid in [
                "",
                &written[written.len() - 1..],
                &invalid,
                &nondigit,
                &oversized,
            ] {
                if super::grpc_timeout(invalid).is_some() {
                    return false;
                }
            }
        }
    }
    let authority = acyclic_stream::AppliedRevisions::new(
        acyclic_stream::RevisionScope::new(bytes.to_vec()),
        7,
        9,
    );
    let revision = authority.current();
    let floor = RevisionCut::of(revision.clone());
    let mut written = MetadataMap::new();
    stamp(&mut written, Some(&revision));
    // The expected writer value is independent of the decoder under test.
    if issued(&written).ok() != Some(Some(revision.clone())) {
        return false;
    }
    let mut request = Request::new(());
    minimum(&mut request, Some(&floor));
    if requested(request.metadata()).ok() != Some(floor.clone()) {
        return false;
    }
    if requested(&MetadataMap::new()).ok() != Some(RevisionCut::default()) {
        return false;
    }
    // Mutate the original writer's HTTP value before binary metadata decoding.
    // from_bytes alone always writes valid base64 and cannot reach this branch.
    let mut invalid = request.metadata().clone().into_headers();
    let mut encoded = invalid[MINIMUM_REVISION].as_bytes().to_vec();
    encoded.push(b'!');
    invalid.insert(
        MINIMUM_REVISION,
        http::HeaderValue::from_bytes(&encoded).expect("writer header plus visible byte"),
    );
    if requested(&MetadataMap::from_headers(invalid))
        .err()
        .map(|error| error.code())
        != Some(tonic::Code::InvalidArgument)
    {
        return false;
    }
    // Two real writer tokens for different incarnations cannot form one cut.
    let foreign = acyclic_stream::AppliedRevisions::new(revision.scope.clone(), 8, 9).current();
    let mut conflicting = request.metadata().clone();
    conflicting.append_bin(
        MINIMUM_REVISION,
        MetadataValue::from_bytes(&revision_to_wire(&foreign).encode_to_vec()),
    );
    if requested(&conflicting).err().map(|error| error.code()) != Some(tonic::Code::InvalidArgument)
    {
        return false;
    }
    let mut arbitrary = MetadataMap::new();
    arbitrary.insert_bin(REVISION, MetadataValue::from_bytes(bytes));
    let mut explicit = MetadataMap::new();
    explicit.insert_bin(MINIMUM_REVISION, MetadataValue::from_bytes(bytes));
    // A parsed explicit floor must survive the original request writer.
    if let Ok(parsed) = requested(&explicit) {
        let mut request = Request::new(());
        minimum(&mut request, Some(&parsed));
        if requested(request.metadata()).ok() != Some(parsed) {
            return false;
        }
    }
    for header in [&written, &arbitrary] {
        for trailer in [&MetadataMap::new(), &arbitrary] {
            let revisions = RevisionArc::default();
            let successful = Admission {
                revisions,
                floor: floor.clone(),
                header: issued(header),
            }
            .finish(tonic::Status::ok(""), trailer);
            let permitted = issued(header).ok().flatten().is_some_and(|header| {
                header == revision
                    && issued(trailer)
                        .ok()
                        .is_some_and(|trailer| trailer.is_none_or(|trailer| trailer == revision))
            });
            if successful.is_ok() != permitted {
                return false;
            }
            // Refusal diagnostics, even malformed ones, never replace a failure.
            let admission = Admission {
                revisions: RevisionArc::default(),
                floor: floor.clone(),
                header: issued(header),
            };
            if admission
                .finish(super::status(wire::ErrorCode::NotFound.into()), trailer)
                .is_err()
            {
                return false;
            }
        }
    }
    true
}

struct Admission {
    revisions: RevisionArc<RevisionTracker>,
    floor: RevisionCut,
    header: Result<Option<Revision>, Error>,
}

impl Admission {
    fn finish(self, status: tonic::Status, trailer: &MetadataMap) -> Result<(), tonic::Status> {
        let revision = match (self.header, issued(trailer)) {
            (Ok(Some(header)), Ok(Some(trailer))) if header != trailer => {
                Err(wire::ErrorCode::Unavailable.into())
            }
            (Ok(header), Ok(trailer)) => Ok(trailer.or(header)),
            (Err(error), _) | (_, Err(error)) => Err(error),
        };
        admit(
            &self.revisions,
            &self.floor,
            revision,
            status.code() == tonic::Code::Ok,
        )
        .map(|_| ())
        .map_err(super::status)
    }
}

pin_project_lite::pin_project! {
    /// Response body retaining the original revision admission until final status.
    pub struct RevisionBody<B> {
        #[pin]
        inner: B,
        admission: Option<Admission>,
    }
}

impl<B: http_body::Body> http_body::Body for RevisionBody<B>
where
    B::Error: Into<tonic::codegen::StdError>,
{
    type Data = B::Data;
    type Error = tonic::Status;

    fn is_end_stream(&self) -> bool {
        self.admission.is_none() && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        match std::task::ready!(this.inner.poll_frame(cx)) {
            Some(Ok(frame)) => {
                if let Some(trailers) = frame.trailers_ref()
                    && let Some(status) = tonic::Status::from_header_map(trailers)
                    && let Some(admission) = this.admission.take()
                    && let Err(error) =
                        admission.finish(status, &MetadataMap::from_headers(trailers.clone()))
                {
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Some(Err(error)) => {
                this.admission.take();
                Poll::Ready(Some(Err(tonic::Status::from_error(error.into()))))
            }
            None => {
                // Missing grpc-status supplies no successful revision evidence.
                this.admission.take();
                Poll::Ready(None)
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// The original service future, without an allocated wrapper future.
    pub struct RevisionFuture<F> {
        #[pin]
        inner: F,
        admission: Option<Admission>,
    }
}

impl<F, B, E> Future for RevisionFuture<F>
where
    F: Future<Output = Result<http::Response<B>, E>>,
    E: Into<tonic::codegen::StdError>,
{
    type Output = Result<http::Response<RevisionBody<B>>, tonic::Status>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut response = std::task::ready!(this.inner.poll(cx))
            .map_err(|error| tonic::Status::from_error(error.into()))?;
        let mut admission = this
            .admission
            .take()
            .expect("response polled after completion");
        let metadata = MetadataMap::from_headers(std::mem::take(response.headers_mut()));
        admission.header = issued(&metadata);
        let terminal = tonic::Status::from_header_map(metadata.as_ref());
        *response.headers_mut() = metadata.into_headers();
        let admission = if let Some(status) = terminal {
            admission.finish(status, &MetadataMap::new())?;
            None
        } else {
            Some(admission)
        };
        Poll::Ready(Ok(response.map(|inner| RevisionBody { inner, admission })))
    }
}

impl<S, B, R> Service<http::Request<B>> for RevisionChannel<S>
where
    S: Service<http::Request<B>, Response = http::Response<R>>,
    S::Error: Into<tonic::codegen::StdError>,
{
    type Response = http::Response<RevisionBody<R>>;
    type Error = tonic::Status;
    type Future = Either<Ready<Result<Self::Response, Self::Error>>, RevisionFuture<S::Future>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_ready(cx)
            .map_err(|error| tonic::Status::from_error(error.into()))
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        let mut metadata = MetadataMap::from_headers(std::mem::take(request.headers_mut()));
        let floor = match requested(&metadata).and_then(|explicit| {
            self.revisions
                .floor(&explicit)
                .map_err(|_| super::status(wire::ErrorCode::RevisionForeign.into()))
        }) {
            Ok(floor) => floor,
            Err(error) => return Either::Left(ready(Err(error))),
        };
        metadata.remove_bin(MINIMUM_REVISION);
        let mut original = Request::new(());
        *original.metadata_mut() = metadata;
        minimum(&mut original, Some(&floor));
        *request.headers_mut() = std::mem::take(original.metadata_mut()).into_headers();
        Either::Right(RevisionFuture {
            inner: self.inner.call(request),
            admission: Some(Admission {
                revisions: RevisionArc::clone(&self.revisions),
                floor,
                header: Ok(None),
            }),
        })
    }
}
