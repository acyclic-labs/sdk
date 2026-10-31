//! Exercise the public binary client and its actual request-bound admission.
use acyclic_objects::{
    Availability, Blobs, ByteRange, Digest, Error, Manifest, MemoryMedium, Part, RootId, Store,
    binary::{BinaryObjects, Reply, Transport},
    wire,
};
use acyclic_stream::OwnedBytes;
use bytes::Bytes;
use futures::FutureExt as _;
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
                result.as_ref().map(|_| ()).map_err(|error| *error),
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

pub(super) fn check(name: &str, bytes: &[u8]) {
    request(name, bytes);
    macro_rules! response {
        ($wire:ident, $route:literal, |$client:ident| $call:expr) => {
            compare::<wire::$wire, _>(bytes, $route, |$client| ready($call))
        };
    }
    match name {
        "acyclic.objects.v1.PutResponse" => {
            response!(PutResponse, "blobs/put", |client| client
                .put(body(), Vec::new()));
            response!(PutResponse, "blobs/put-manifest", |client| client
                .put_manifest(manifest()));
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
            response!(BlobInfo, "blobs/head", |client| client.head(digest()));
        }
        "acyclic.objects.v1.DeleteBlobResponse" => {
            response!(DeleteBlobResponse, "blobs/delete", |client| client
                .delete(digest()));
        }
        "acyclic.objects.v1.ListBlobsResponse" => {
            response!(ListBlobsResponse, "blobs/list", |client| client
                .list(None, u32::MAX));
            response!(ListBlobsResponse, "blobs/list", |client| client
                .list(Some(digest()), 1));
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
        Ok::<_, Error>(())
    })?;
    let seeds = std::mem::take(&mut *outputs.lock().expect("capture output custody"));
    Ok(seeds)
}
