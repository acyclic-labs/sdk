//! Exercise the public binary client and its actual request-bound admission.
use acyclic_objects::{
    Availability, Blobs, ByteRange, Digest, Error, Manifest, MemoryMedium, Part, RootId, Store,
    binary::{BinaryObjects, Reply, Transport},
    wire,
};
use acyclic_stream::OwnedBytes;
use bytes::Bytes;
use futures::{FutureExt as _, StreamExt as _};
use prost::Message;
use std::{
    fmt::Debug,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

const BODY: &[u8] = b"canonical object decoder";
const BATCH_BODY: &[u8] = b"other batch member";

fn body() -> OwnedBytes {
    OwnedBytes::from_static(BODY)
}

fn digest() -> Digest {
    Digest::of(BODY)
}

fn manifest() -> Manifest {
    Manifest {
        parts: vec![Part {
            digest: digest(),
            length: BODY.len() as u64,
        }],
        edges: Vec::new(),
    }
}

fn root() -> RootId {
    RootId::new(b"codec-reference".to_vec()).expect("nonempty public root identity")
}

fn ready<T>(future: impl Future<Output = T>) -> T {
    future
        .now_or_never()
        .expect("the response producer settles on this path without a wait")
}

struct Response {
    route: &'static str,
    body: Bytes,
    lost: bool,
    calls: Arc<AtomicUsize>,
    verified: Arc<Mutex<Option<Result<(), Error>>>>,
}

impl Transport for Response {
    async fn invoke(&self, route: &str, input: Bytes) -> Result<Reply, Error> {
        assert_eq!(route, self.route);
        assert_eq!(self.calls.fetch_add(1, Ordering::Relaxed), 0);
        if self.lost {
            return Err(wire::ErrorCode::Unavailable.into());
        }
        *self.verified.lock().expect("response admission custody") = Some(
            acyclic_objects::route::verify(route, input, self.body.clone()),
        );
        Ok((self.body.clone(), None))
    }

    async fn availability(&self, _digest: Digest) -> Result<Availability, Error> {
        Err(wire::ErrorCode::Unsupported.into())
    }
}

fn compare<W: Message + Default, T: Debug + PartialEq>(
    bytes: &[u8],
    route: &'static str,
    call: impl Fn(&BinaryObjects<Response>) -> Result<T, Error>,
) {
    let run = |body: Bytes, lost| {
        let calls = Arc::new(AtomicUsize::new(0));
        let verified = Arc::new(Mutex::new(None));
        let client = BinaryObjects::new(Response {
            route,
            body,
            lost,
            calls: calls.clone(),
            verified: verified.clone(),
        });
        let result = call(&client);
        if !lost {
            assert_eq!(
                result.as_ref().map(|_| ()).map_err(Clone::clone),
                verified
                    .lock()
                    .expect("response admission custody")
                    .take()
                    .expect("the transport verified the original request and response"),
            );
        }
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        ready(client.flush(None)).expect("the original producer acknowledges puts durably");
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        result
    };
    let original = run(Bytes::copy_from_slice(bytes), false);
    match W::decode(bytes) {
        Ok(value) => assert_eq!(run(value.encode_to_vec().into(), false), original),
        Err(_) => assert!(original.is_err(), "malformed wire cannot become a value"),
    }
    assert_eq!(
        run(Bytes::new(), true),
        Err(wire::ErrorCode::Unavailable.into()),
        "producer loss remains the original typed failure",
    );
}

fn publication(put: &acyclic_objects::Put, expected: (Digest, u64)) {
    if let Some(value) = put.verified() {
        assert_eq!(
            value.digest, expected.0,
            "put answers the original requested digest"
        );
        assert_eq!(
            value.length, expected.1,
            "put answers the original requested length"
        );
    }
}

fn batch(bytes: &[u8]) {
    let run = |packet: Bytes, lost| {
        let calls = Arc::new(AtomicUsize::new(0));
        let verified = Arc::new(Mutex::new(None));
        let client = BinaryObjects::new(Response {
            route: "blobs/put-batch",
            body: packet.clone(),
            lost,
            calls: calls.clone(),
            verified: verified.clone(),
        });
        let results = ready(client.put_batch(vec![
            (body(), Vec::new()),
            (OwnedBytes::from_static(BATCH_BODY), Vec::new()),
        ]));
        assert_eq!(results.len(), 2, "a batch answers every original member");
        let unavailable = Error::from(wire::ErrorCode::Unavailable);
        if lost
            || verified
                .lock()
                .expect("batch response custody")
                .as_ref()
                .is_some_and(Result::is_err)
        {
            assert!(results.iter().all(|result| result == &Err(unavailable.clone())));
        } else {
            let reply = wire::PutBlobsResponse::decode(packet).expect("admitted batch wire");
            for ((result, item), expected) in results.iter().zip(reply.results).zip([
                (digest(), BODY.len() as u64),
                (Digest::of(BATCH_BODY), BATCH_BODY.len() as u64),
            ]) {
                match (item.outcome, result) {
                    (Some(wire::put_result::Outcome::Error(code)), Err(error)) => {
                        assert_eq!(error.code as i32, code, "member refusal remains typed");
                    }
                    (Some(wire::put_result::Outcome::Put(reply)), Ok(put)) => {
                        use acyclic_objects::Put;
                        use wire::put_response::Outcome;
                        assert!(
                            matches!(
                                (reply.outcome, put),
                                (Some(Outcome::Created(_)), Put::Created { .. })
                                    | (Some(Outcome::AlreadyExists(_)), Put::AlreadyExists { .. })
                                    | (Some(Outcome::Unknown(true)), Put::Unknown)
                            ),
                            "member outcome remains original"
                        );
                        publication(put, expected);
                    }
                    _ => panic!("batch admission changed a member outcome"),
                }
            }
        }
        ready(client.flush(None)).expect("original batch producer settles");
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        results
    };
    let original = run(Bytes::copy_from_slice(bytes), false);
    match wire::PutBlobsResponse::decode(bytes) {
        Ok(value) => assert_eq!(run(value.encode_to_vec().into(), false), original),
        Err(_) => assert!(original.iter().all(Result::is_err)),
    }
    run(Bytes::new(), true);
}

pub(super) fn check(name: &str, bytes: &[u8]) {
    if !name.starts_with("acyclic.objects.v1.") {
        return;
    }
    // Syntax equivalence and a scripted response cannot detect a server that
    // refuses every request. Admit the original service writers once per
    // process before comparing arbitrary packets through the same client.
    static WRITERS: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    WRITERS.get_or_init(|| {
        seeds().expect("original Objects service writers must be admitted");
    });
    request(name, bytes);
    if name == "acyclic.objects.v1.AvailabilityCursor"
        && let Ok(cursor) = wire::AvailabilityCursor::decode(bytes)
    {
        let valid =
            cursor.incarnation.len() == 16 && cursor.incarnation.iter().any(|byte| *byte != 0);
        assert_eq!(
            acyclic_objects::AvailabilityCursor::try_from(cursor).is_ok(),
            valid,
            "cursor admission must match its wire identity"
        );
    }
    macro_rules! response {
        ($wire:ident, $route:literal, |$client:ident| $call:expr) => {
            compare::<wire::$wire, _>(bytes, $route, |$client| ready($call))
        };
    }
    match name {
        "acyclic.objects.v1.PutBlobsResponse" => batch(bytes),
        "acyclic.objects.v1.PutResponse" => {
            compare::<wire::PutResponse, _>(bytes, "blobs/put", |client| {
                ready(client.put(body(), Vec::new()))
                    .inspect(|put| publication(put, (digest(), BODY.len() as u64)))
            });
            compare::<wire::PutResponse, _>(bytes, "blobs/put-manifest", |client| {
                ready(client.put_manifest(manifest())).inspect(|put| {
                    publication(put, (Digest::of(&manifest().encode()), BODY.len() as u64));
                })
            });
        }
        "acyclic.objects.v1.GetBlobResponse" => {
            for (range, maximum) in [
                (ByteRange::FULL, u64::MAX),
                (ByteRange::FULL, 0),
                (
                    ByteRange {
                        start: 1,
                        end: Some(BODY.len() as u64),
                    },
                    u64::MAX,
                ),
            ] {
                compare::<wire::GetBlobResponse, _>(bytes, "blobs/get", |client| {
                    ready(client.get(digest(), range, maximum)).map(|object| {
                        assert_eq!(
                            object.head().digest,
                            digest(),
                            "read answers the original requested digest"
                        );
                        assert!(
                            object.body().len() as u64 <= maximum,
                            "read respects original byte admission"
                        );
                        let end = range.end.unwrap_or(object.head().length);
                        assert_eq!(
                            object.start(),
                            range.start,
                            "read starts at the original requested offset"
                        );
                        assert_eq!(
                            object.body().len() as u64,
                            end - range.start,
                            "read contains the complete requested selection"
                        );
                        if object.head().kind == acyclic_objects::Kind::Blob
                            && range.start == 0
                            && end == object.head().length
                        {
                            assert_eq!(
                                Digest::of(object.body()),
                                digest(),
                                "complete blob read authenticates its original bytes"
                            );
                        }
                        (
                            object.head().clone(),
                            object.start(),
                            object.body().to_vec(),
                        )
                    })
                });
            }
        }
        "acyclic.objects.v1.BlobInfo" => {
            compare::<wire::BlobInfo, _>(bytes, "blobs/head", |client| {
                ready(client.head(digest())).map(|head| {
                    assert_eq!(
                        head.digest,
                        digest(),
                        "head answers the original requested digest"
                    );
                    head
                })
            });
        }
        "acyclic.objects.v1.DeleteBlobResponse" => {
            response!(DeleteBlobResponse, "blobs/delete", |client| client
                .delete(digest()));
        }
        "acyclic.objects.v1.ListBlobsResponse" => {
            for (after, limit) in [
                (None, u32::MAX),
                (None, 0),
                (None, 1),
                (Some(digest()), u32::MAX),
                (Some(digest()), 1),
            ] {
                compare::<wire::ListBlobsResponse, _>(bytes, "blobs/list", |client| {
                    ready(client.list(after, limit)).inspect(|page| {
                        assert!(
                            page.digests.windows(2).all(|pair| pair[0] < pair[1]),
                            "listed digests are strictly increasing"
                        );
                        assert!(
                            page.next
                                .is_none_or(|next| page.digests.last() == Some(&next)),
                            "listed continuation is the last returned digest"
                        );
                        assert!(
                            page.digests.len() <= limit as usize,
                            "listing respects the original requested limit"
                        );
                        assert!(
                            after.is_none_or(|after| {
                                page.digests.first().is_none_or(|first| *first > after)
                            }),
                            "listing starts after the original requested cursor"
                        );
                    })
                });
            }
        }
        "acyclic.objects.v1.RetainResponse" => {
            response!(RetainResponse, "blobs/retain", |client| client
                .retain(&root(), vec![digest()]));
        }
        "acyclic.objects.v1.ReleaseResponse" => {
            response!(ReleaseResponse, "blobs/release", |client| client
                .release(&root()));
        }
        "acyclic.objects.v1.CollectResponse" => {
            response!(CollectResponse, "blobs/collect", |client| client.collect());
        }
        "acyclic.objects.v1.UsageResponse" => {
            response!(UsageResponse, "blobs/usage", |client| client.usage());
        }
        _ => {}
    }
}

fn request(name: &str, bytes: &[u8]) {
    let route = match name {
        "acyclic.objects.v1.PutBlobRequest" => "blobs/put",
        "acyclic.objects.v1.PutBlobsRequest" => "blobs/put-batch",
        "acyclic.objects.v1.PutManifestRequest" => "blobs/put-manifest",
        "acyclic.objects.v1.GetBlobRequest" => "blobs/get",
        "acyclic.objects.v1.HeadBlobRequest" => "blobs/head",
        "acyclic.objects.v1.DeleteBlobRequest" => "blobs/delete",
        "acyclic.objects.v1.ListBlobsRequest" => "blobs/list",
        "acyclic.objects.v1.RetainRequest" => "blobs/retain",
        "acyclic.objects.v1.ReleaseRequest" => "blobs/release",
        "acyclic.objects.v1.CollectRequest" => "blobs/collect",
        "acyclic.objects.v1.UsageRequest" => "blobs/usage",
        _ => return,
    };
    // A fresh real account has no other lock holder or external producer. Every
    // request must settle in this poll; no executor loop can hide a lost event.
    let store = Store::new(MemoryMedium::default());
    let account = store
        .account(b"codec-admission")
        .expect("public account identity");
    let input = Bytes::copy_from_slice(bytes);
    if let Ok((body, _)) = ready(acyclic_objects::route::invoke_at(
        &account,
        route,
        input.clone(),
    )) {
        assert!(acyclic_objects::route::verify(route, input, body.into()).is_ok());
    }
}

type Seed = (&'static str, &'static str, Vec<u8>);

/// The transport delegates to the real account's one production route dispatch.
struct Capture {
    account: acyclic_objects::MemoryObjects,
    outputs: Arc<Mutex<Vec<Seed>>>,
}

impl Transport for Capture {
    async fn invoke(&self, route: &str, input: Bytes) -> Result<Reply, Error> {
        let original = input.clone();
        let (body, revision) =
            acyclic_objects::route::invoke_at(&self.account, route, input).await?;
        macro_rules! messages {
            ($request:literal, $response:literal, $variant:expr) => {
                (
                    concat!("acyclic.objects.v1.", $request),
                    concat!("acyclic.objects.v1.", $response),
                    $variant,
                )
            };
        }
        let (request, response, variant) = match route {
            "blobs/put" => match wire::PutResponse::decode(body.as_slice())
                .expect("production put writer output")
                .outcome
            {
                Some(wire::put_response::Outcome::Created(_)) => {
                    messages!("PutBlobRequest", "PutResponse", "created")
                }
                Some(wire::put_response::Outcome::AlreadyExists(_)) => {
                    messages!("PutBlobRequest", "PutResponse", "existing")
                }
                _ => panic!("the memory producer settles a durable put"),
            },
            "blobs/put-manifest" => messages!("PutManifestRequest", "PutResponse", "manifest"),
            "blobs/put-batch" => messages!("PutBlobsRequest", "PutBlobsResponse", "batch"),
            "blobs/get" => messages!("GetBlobRequest", "GetBlobResponse", "selected"),
            "blobs/head" => messages!("HeadBlobRequest", "BlobInfo", "selected"),
            "blobs/list" => messages!("ListBlobsRequest", "ListBlobsResponse", "ordered"),
            "blobs/retain" => messages!("RetainRequest", "RetainResponse", "retained"),
            "blobs/release" => messages!("ReleaseRequest", "ReleaseResponse", "released"),
            "blobs/delete" => messages!("DeleteBlobRequest", "DeleteBlobResponse", "referenced"),
            "blobs/collect" => messages!("CollectRequest", "CollectResponse", "collected"),
            "blobs/usage" => messages!("UsageRequest", "UsageResponse", "account"),
            _ => panic!("capture uses the original public routes"),
        };
        assert!(
            acyclic_objects::route::verify(route, original.clone(), body.clone().into()).is_ok()
        );
        self.outputs
            .lock()
            .expect("capture output custody")
            .extend([
                (request, variant, original.to_vec()),
                (response, variant, body.clone()),
            ]);
        Ok((body.into(), revision))
    }

    async fn availability(&self, digest: Digest) -> Result<Availability, Error> {
        self.account.availability(digest).await
    }
}

pub(super) fn seeds() -> Result<Vec<Seed>, Box<dyn std::error::Error>> {
    let store = Store::new(MemoryMedium::default());
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let client = BinaryObjects::new(Capture {
        account: store.account(b"codec-capture")?,
        outputs: outputs.clone(),
    });
    ready(async {
        client.put(body(), Vec::new()).await?;
        client.put(body(), Vec::new()).await?;
        client.put_manifest(manifest()).await?;
        client.get(digest(), ByteRange::FULL, u64::MAX).await?;
        client.head(digest()).await?;
        client.list(None, u32::MAX).await?;
        client.retain(&root(), vec![digest()]).await?;
        client.release(&root()).await?;
        client.delete(digest()).await?;
        client.collect().await?;
        client.usage().await?;
        let mut availability = Box::pin(acyclic_objects::route::availability_frames(
            client.availability(digest()).await?,
        ));
        let opening = availability
            .next()
            .await
            .expect("original availability writer emits its opening cursor")?;
        acyclic_objects::AvailabilityCursor::try_from(opening.clone())?;
        // This is a mutation of the original source's packet, never a second
        // producer or evidence of a different account's physical availability.
        let mut zero = opening;
        zero.incarnation.fill(0);
        assert!(
            acyclic_objects::AvailabilityCursor::try_from(zero).is_err(),
            "original cursor with a zero incarnation must be refused"
        );
        // Keep prior service-writer captures unchanged when extending the corpus.
        let other = BATCH_BODY;
        let batch = client
            .put_batch(vec![
                (body(), Vec::new()),
                (OwnedBytes::from_static(other), Vec::new()),
            ])
            .await;
        assert_eq!(batch.len(), 2, "original batch answers every member");
        for (put, expected) in batch.into_iter().zip([
            (digest(), BODY.len() as u64),
            (Digest::of(other), other.len() as u64),
        ]) {
            let put = put?;
            let verified = put.verified().expect("original batch writer");
            assert_eq!((verified.digest, verified.length), expected);
        }
        Ok::<_, Error>(())
    })?;
    let seeds = std::mem::take(&mut *outputs.lock().expect("capture output custody"));
    Ok(seeds)
}

#[cfg(test)]
mod tests {
    #[test]
    fn original_service_writers_satisfy_the_semantic_oracle() {
        let captured = acyclic_proto_build::golden::frame(
            "corpus/protobuf/acyclic.objects.v1.PutBlobRequest-created",
        )
        .expect("retained original service request");
        super::check("acyclic.objects.v1.PutBlobRequest", &captured[8..]);
        use prost::Message;
        for variant in ["created", "existing", "manifest"] {
            let captured = acyclic_proto_build::golden::frame(&format!(
                "corpus/protobuf/acyclic.objects.v1.PutResponse-{variant}",
            ))
            .expect("retained original service publication writer");
            super::check("acyclic.objects.v1.PutResponse", &captured[8..]);
            for digest in [true, false] {
                let mut wrong = super::wire::PutResponse::decode(&captured[8..])
                    .expect("original service publication wire");
                use super::wire::put_response::Outcome;
                let value = match wrong.outcome.as_mut() {
                    Some(Outcome::Created(value) | Outcome::AlreadyExists(value)) => value,
                    _ => panic!("original publication writer has a durable acknowledgement"),
                };
                if digest {
                    value.digest.as_mut().expect("original put digest").blake3[0] ^= 1;
                } else {
                    value.length += 1;
                }
                super::check("acyclic.objects.v1.PutResponse", &wrong.encode_to_vec());
            }
        }
        let captured = acyclic_proto_build::golden::frame(
            "corpus/protobuf/acyclic.objects.v1.PutBlobsResponse",
        )
        .expect("retained original protobuf batch writer");
        super::check("acyclic.objects.v1.PutBlobsResponse", &captured[8..]);
        let captured = acyclic_proto_build::golden::frame(
            "corpus/protobuf/acyclic.objects.v1.BlobInfo-selected",
        )
        .expect("retained original service head writer");
        super::check("acyclic.objects.v1.BlobInfo", &captured[8..]);
        let mut wrong =
            super::wire::BlobInfo::decode(&captured[8..]).expect("original service head wire");
        wrong.digest.as_mut().expect("original head digest").blake3[0] ^= 1;
        super::check("acyclic.objects.v1.BlobInfo", &wrong.encode_to_vec());
        let captured = acyclic_proto_build::golden::frame(
            "corpus/protobuf/acyclic.objects.v1.GetBlobResponse-selected",
        )
        .expect("retained original service read writer");
        super::check("acyclic.objects.v1.GetBlobResponse", &captured[8..]);
        let original = super::wire::GetBlobResponse::decode(&captured[8..])
            .expect("original service read wire");
        let mut wrong = original.clone();
        wrong
            .info
            .as_mut()
            .expect("original read head")
            .digest
            .as_mut()
            .expect("original read digest")
            .blake3[0] ^= 1;
        super::check("acyclic.objects.v1.GetBlobResponse", &wrong.encode_to_vec());
        let mut wrong = original.clone();
        wrong.start += 1;
        super::check("acyclic.objects.v1.GetBlobResponse", &wrong.encode_to_vec());
        let mut wrong = original.clone();
        let mut bytes = wrong.body.to_vec();
        bytes[0] ^= 1;
        wrong.body = bytes.into();
        super::check("acyclic.objects.v1.GetBlobResponse", &wrong.encode_to_vec());
        // Derive a selected range from the original writer's bytes. This is an
        // oracle operand, not a claimed capture of another provider operation.
        let mut selected = original;
        selected.start = 1;
        selected.body = selected.body.slice(1..);
        super::check(
            "acyclic.objects.v1.GetBlobResponse",
            &selected.encode_to_vec(),
        );
        selected.body.truncate(selected.body.len() - 1);
        super::check(
            "acyclic.objects.v1.GetBlobResponse",
            &selected.encode_to_vec(),
        );
        let captured = acyclic_proto_build::golden::frame(
            "corpus/protobuf/acyclic.objects.v1.ListBlobsResponse-ordered",
        )
        .expect("retained original service listing writer");
        super::check("acyclic.objects.v1.ListBlobsResponse", &captured[8..]);
        let original = super::wire::ListBlobsResponse::decode(&captured[8..])
            .expect("original service listing wire");
        let mut wrong = original.clone();
        wrong.digests.swap(0, 1);
        super::check(
            "acyclic.objects.v1.ListBlobsResponse",
            &wrong.encode_to_vec(),
        );
        let mut wrong = original.clone();
        wrong.digests[1] = wrong.digests[0].clone();
        super::check(
            "acyclic.objects.v1.ListBlobsResponse",
            &wrong.encode_to_vec(),
        );
        // Continuations are derived operands, not additional provider captures.
        let mut page = original;
        page.next = page.digests.first().cloned();
        super::check(
            "acyclic.objects.v1.ListBlobsResponse",
            &page.encode_to_vec(),
        );
        page.next = page.digests.last().cloned();
        super::check(
            "acyclic.objects.v1.ListBlobsResponse",
            &page.encode_to_vec(),
        );
    }
}
