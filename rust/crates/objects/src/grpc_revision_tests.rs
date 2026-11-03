//! Actual generated calls, provider responses and HTTP/2 bodies through the
//! public transport. Faults transform the original server output, not mock wire.
use super::*;
use crate::wire::blobs_service_server::BlobsService as _;

fn put(body: &'static [u8]) -> wire::PutBlobRequest {
    wire::PutBlobRequest {
        digest: Some(Digest::of(body).into()),
        body: bytes::Bytes::from_static(body),
        references: Vec::new(),
    }
}

fn serving(provider: Arc<MemoryObjects>) -> AccountResolver {
    Arc::new(move |_| Ok(provider.clone()))
}

#[tokio::test]
async fn admission_preserves_authority_and_original_failure() -> TestResult {
    let provider = Arc::new(MemoryObjects::memory());
    let server = BlobsServer::new(serving(provider.clone()));
    let response = server
        .put_blob(Request::new(put(b"revision admission")))
        .await?;
    let metadata = response.metadata().clone();
    let captured = metadata
        .get_bin(REVISION)
        .ok_or("writer metadata")?
        .to_bytes()?;
    println!("OBJECT_REVISION_CAPTURE put {}", hex::encode(&captured));
    assert!(check_revision_metadata(&captured));
    let retained = acyclic_proto_build::golden::frame("corpus/object_revisions/put-revision")?;
    assert!(check_revision_metadata(&retained));
    let committed = issued(&metadata)?.ok_or("writer omitted revision")?;
    let objects = GrpcObjects::over(
        channel_over(serving(provider)),
        CredentialProvider::new(|| async { Ok("fixture".into()) }),
    );
    let response = || {
        let mut response = tonic::Response::new(());
        *response.metadata_mut() = metadata.clone();
        Ok(response)
    };
    assert!(
        objects
            .observed(response(), Some(&RevisionCut::of(committed.clone())))
            .is_ok()
    );
    for floor in [
        AppliedRevisions::in_process().at(committed.index),
        Revision {
            incarnation: committed.incarnation.wrapping_add(1),
            ..committed.clone()
        },
        Revision {
            index: committed.index + 1,
            ..committed.clone()
        },
    ] {
        assert_eq!(
            objects
                .observed(response(), Some(&RevisionCut::of(floor)))
                .map(|_| ())
                .map_err(|error| error.code),
            Err(wire::ErrorCode::Unavailable)
        );
    }
    let minimum = RevisionCut::of(committed);
    assert_eq!(
        objects
            .observed(Ok(tonic::Response::new(())), Some(&minimum))
            .map_err(|error| error.code),
        Err(wire::ErrorCode::Unavailable)
    );
    assert!(objects.observed(Ok(tonic::Response::new(())), None).is_ok());
    assert!(
        objects
            .observed(Ok(tonic::Response::new(())), Some(&RevisionCut::default()))
            .is_ok()
    );
    let mut failure = server
        .head_blob(Request::new(wire::HeadBlobRequest {
            digest: Some(Digest::of(b"absent").into()),
        }))
        .await
        .expect_err("absent blob");
    failure
        .metadata_mut()
        .insert_bin(REVISION, MetadataValue::from_bytes(&[0xff]));
    assert_eq!(
        objects
            .observed::<()>(Err(failure), Some(&minimum))
            .map_err(|error| error.code),
        Err(wire::ErrorCode::NotFound)
    );
    Ok(())
}

#[tokio::test]
async fn generated_get_put_delete_share_floors_and_scoped_views() -> TestResult {
    let provider = Arc::new(MemoryObjects::memory());
    let original = provider.clone();
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = requests.clone();
    let resolve: AccountResolver = Arc::new(move |metadata| {
        captured.lock().unwrap().push(metadata.clone());
        Ok(provider.clone())
    });
    let revisions = RevisionArc::default();
    let channel = RevisionChannel::with_revisions(
        channel_over(resolve.clone()),
        RevisionArc::clone(&revisions),
    );
    let owner = channel.clone();
    let mut client = wire::blobs_service_client::BlobsServiceClient::new(channel);
    let response = client.put_blob(put(b"conditional objects")).await?;
    let committed = issued(response.metadata())?.ok_or("put revision")?;
    let digest = Digest::of(b"conditional objects");
    assert!(
        owner
            .revisions()
            .floor(&RevisionCut::default())?
            .covers(&committed)
    );
    // A replacement connection retains this original session's frontier.
    client = wire::blobs_service_client::BlobsServiceClient::new(RevisionChannel::with_revisions(
        channel_over(resolve),
        revisions,
    ));
    let response = client
        .get_blob(wire::GetBlobRequest {
            digest: Some(digest.into()),
            range: None,
            maximum_bytes: u64::MAX,
        })
        .await?;
    assert_eq!(response.get_ref().body.as_ref(), b"conditional objects");
    let served = issued(response.metadata())?.ok_or("get revision")?;
    // Advance through the same real provider, outside the client's observations.
    let barrier = BlobsServer::new(serving(original))
        .put_blob(Request::new(put(b"explicit floor")))
        .await?;
    let barrier = issued(barrier.metadata())?.ok_or("explicit revision")?;
    let mut request = Request::new(put(b"conditional objects"));
    minimum(&mut request, Some(&RevisionCut::of(barrier.clone())));
    let response = client.put_blob(request).await?;
    let replaced = issued(response.metadata())?.ok_or("replacement revision")?;
    let response = client
        .delete_blob(wire::DeleteBlobRequest {
            digest: Some(digest.into()),
        })
        .await?;
    let deleted = issued(response.metadata())?.ok_or("delete revision")?;
    let captured = requests.lock().unwrap();
    assert!(revision::requested(&captured[0])?.is_empty());
    assert!(revision::requested(&captured[1])?.covers(&committed));
    assert!(revision::requested(&captured[2])?.covers(&served));
    assert!(revision::requested(&captured[2])?.covers(&barrier));
    assert!(revision::requested(&captured[3])?.covers(&replaced));
    drop(captured);
    owner
        .revisions()
        .observe(&AppliedRevisions::in_process().at(1));
    assert!(
        client
            .head_blob(wire::HeadBlobRequest {
                digest: Some(digest.into())
            })
            .await
            .is_err()
    );
    let scoped = owner.with_revision_scope(deleted.scope.clone());
    assert!(
        scoped
            .revisions()
            .floor(&RevisionCut::default())?
            .covers(&deleted)
    );
    let mut client = wire::blobs_service_client::BlobsServiceClient::new(scoped);
    let failure = client
        .head_blob(wire::HeadBlobRequest {
            digest: Some(digest.into()),
        })
        .await
        .unwrap_err();
    assert_eq!(error_from_status(&failure).code, wire::ErrorCode::NotFound);
    Ok(())
}

#[tokio::test]
async fn a_late_refusal_retains_its_original_status_instead_of_becoming_stale_success() -> TestResult
{
    use http_body_util::BodyExt as _;
    use tonic::codegen::Service as _;
    let provider = Arc::new(MemoryObjects::memory());
    let server = BlobsServer::new(serving(provider.clone()));
    let response = server
        .put_blob(Request::new(put(b"original frames")))
        .await?;
    let committed = issued(response.metadata())?.ok_or("writer revision")?;
    let failure = server
        .head_blob(Request::new(wire::HeadBlobRequest {
            digest: Some(Digest::of(b"absent").into()),
        }))
        .await
        .expect_err("original provider refusal");
    let expected = failure.details().to_vec();
    let mut trailers = failure.metadata().clone().into_headers();
    failure.add_header(&mut trailers)?;
    // Each fault transforms the same original HTTP/2 response at its terminal.
    for terminal in [Ok(Some(trailers)), Ok(None), Err(failure.clone())] {
        let expected_code = if matches!(terminal, Ok(None)) {
            tonic::Code::Unknown
        } else {
            failure.code()
        };
        let channel = channel_over(serving(provider.clone()));
        let proxy = tower::service_fn(
            move |mut request: tonic::codegen::http::Request<tonic::body::Body>| {
                let mut channel = channel.clone();
                let terminal = terminal.clone();
                // Ignore only the minimum, so a stale prefix cannot replace the
                // original terminal refusal, missing status or transport cause.
                request.headers_mut().remove(MINIMUM_REVISION);
                async move {
                    futures::future::poll_fn(|cx| channel.poll_ready(cx)).await?;
                    let response = channel.call(request).await?;
                    Ok::<_, tonic::transport::Error>(response.map(|body| {
                        tonic::body::Body::new(http_body_util::StreamBody::new(
                            futures::stream::unfold(
                                (body, Some(terminal)),
                                |(mut body, mut terminal)| async move {
                                    let frame = match body.frame().await? {
                                        Ok(frame) if frame.is_trailers() => {
                                            match terminal.take()? {
                                                Ok(trailers) => {
                                                    Ok(http_body::Frame::trailers(trailers?))
                                                }
                                                Err(error) => Err(error),
                                            }
                                        }
                                        original => original,
                                    };
                                    Some((frame, (body, terminal)))
                                },
                            ),
                        ))
                    }))
                }
            },
        );
        let channel = RevisionChannel::new(proxy);
        channel.revisions().observe(&Revision {
            index: committed.index + 1,
            ..committed.clone()
        });
        let mut client = wire::blobs_service_client::BlobsServiceClient::new(channel);
        let observed = client
            .get_blob(wire::GetBlobRequest {
                digest: Some(Digest::of(b"original frames").into()),
                range: None,
                maximum_bytes: u64::MAX,
            })
            .await
            .expect_err("original terminal cannot admit a successful prefix");
        assert_eq!(observed.code(), expected_code);
        if expected_code == failure.code() {
            assert_eq!(observed.details(), expected);
        }
    }
    Ok(())
}
