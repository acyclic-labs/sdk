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
    refused_writes_preserve_storage().await?;
    closed_write_preserves_storage().await?;
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

async fn stored(
    medium: &crate::MemoryMedium,
) -> Result<Vec<(String, Vec<u8>)>, crate::MediumError> {
    use crate::Medium as _;
    let mut stored = Vec::new();
    for (key, _) in medium.list("", None, usize::MAX).await? {
        let value = medium.read(&key, None).await?.expect("original stored key");
        stored.push((key, value.as_ref().to_vec()));
    }
    Ok(stored)
}

async fn refused_writes_preserve_storage() -> TestResult {
    for foreign in [false, true] {
        let medium = crate::MemoryMedium::default();
        let provider = Arc::new(crate::Store::new(medium.clone()).account(b"write-minimum")?);
        let body = b"original retained bytes";
        let digest = Digest::of(body);
        provider.put(body.as_slice().into(), Vec::new()).await?;
        let root = RootId::new(b"original-root".to_vec())?;
        provider.retain(&root, vec![digest]).await?;
        let deletable = provider
            .put(b"unretained".as_slice().into(), Vec::new())
            .await?;
        provider.flush(None).await?;
        let current = provider
            .revisions()
            .await?
            .expect("original authority")
            .current();
        let wanted = if foreign {
            AppliedRevisions::in_process().at(1)
        } else {
            Revision {
                incarnation: current.incarnation.wrapping_add(1),
                ..current
            }
        };
        let floor = RevisionCut::of(wanted);
        let before = stored(&medium).await?;
        let mut client = wire::blobs_service_client::BlobsServiceClient::new(RevisionChannel::new(
            channel_over(serving(provider.clone())),
        ));
        macro_rules! refused {
            ($method:ident, $message:expr) => {{
                let mut request = Request::new($message);
                minimum(&mut request, Some(&floor));
                let result = client.$method(request).await;
                assert_eq!(
                    stored(&medium).await?,
                    before,
                    "refused write changed physical storage"
                );
                let failure =
                    result.expect_err("foreign write minimum must refuse before dispatch");
                assert_eq!(
                    error_from_status(&failure).code,
                    wire::ErrorCode::RevisionForeign
                );
            }};
        }
        refused!(
            delete_blob,
            wire::DeleteBlobRequest {
                digest: Some(deletable.verified().ok_or("original put")?.digest.into()),
            }
        );
        refused!(put_blob, put(b"refused singleton"));
        refused!(
            put_blobs,
            wire::PutBlobsRequest {
                puts: vec![put(b"refused batch")]
            }
        );
        refused!(
            put_manifest,
            wire::PutManifestRequest {
                manifest: Some(wire::Manifest {
                    parts: vec![wire::Part {
                        digest: Some(digest.into()),
                        length: body.len() as u64
                    }],
                    edges: Vec::new(),
                }),
            }
        );
        refused!(
            retain,
            wire::RetainRequest {
                root: b"refused-root".as_slice().into(),
                references: vec![digest.into()],
            }
        );
        refused!(
            release,
            wire::ReleaseRequest {
                root: b"original-root".as_slice().into()
            }
        );
        refused!(collect, wire::CollectRequest {});
    }
    Ok(())
}

async fn closed_write_preserves_storage() -> TestResult {
    // The original authority ends a parked mutation when its producer closes;
    // no deadline, readiness retry or replacement revision can release it.
    let medium = crate::MemoryMedium::default();
    let provider = Arc::new(crate::Store::new(medium.clone()).account(b"closed-write-minimum")?);
    let put = provider.put(b"held".as_slice().into(), Vec::new()).await?;
    provider.flush(None).await?;
    let authority = provider.revisions().await?.expect("original authority");
    let floor = RevisionCut::of(authority.at(authority.current().index + 1));
    let before = stored(&medium).await?;
    let (entered, received) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let account = provider.clone();
    let resolve: AccountResolver = Arc::new(move |_| {
        if let Some(entered) = entered.lock().unwrap().take() {
            let _ = entered.send(());
        }
        Ok(account.clone())
    });
    let mut client = wire::blobs_service_client::BlobsServiceClient::new(RevisionChannel::new(
        channel_over(resolve),
    ));
    let mut request = Request::new(wire::DeleteBlobRequest {
        digest: Some(put.verified().ok_or("original put")?.digest.into()),
    });
    minimum(&mut request, Some(&floor));
    let (result, closed) = tokio::join!(client.delete_blob(request), async {
        received.await?;
        authority.close();
        Ok::<_, tokio::sync::oneshot::error::RecvError>(())
    });
    closed?;
    assert_eq!(
        stored(&medium).await?,
        before,
        "closed producer must not dispatch a write"
    );
    assert_eq!(
        error_from_status(&result.expect_err("closed original authority")).code,
        wire::ErrorCode::Unavailable
    );
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
