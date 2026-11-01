use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};
use futures::{FutureExt as _, StreamExt as _, future::Either};

/// Exercise the original OAuth decoder with the native body producer held at
/// its terminal boundary. No filesystem publication or endpoint is involved.
#[tokio::test]
async fn refresh_body_retires_its_buffer_before_the_credential_operation() -> Result<()> {
    let tokens = auth(&format!("fixture-{}", "α\\\"\n".repeat(128)))["tokens"].clone();
    let encoded = serde_json::to_vec(&tokens).expect("original token document");
    for chunk_bytes in [1, 7, encoded.len()] {
        for terminal in 0..4 {
            refresh_body_control(&encoded, &tokens, chunk_bytes, terminal).await?;
        }
    }
    Ok(())
}

async fn refresh_body_control(
    encoded: &[u8],
    expected: &Value,
    chunk_bytes: usize,
    terminal: u8,
) -> Result<()> {
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(100_000, 8_000_000),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| acyclic_harness_core::Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let mut parent = work.claim(0, 0, WorkCounters::UNCHARGED)?;
    let mut io = CredentialIo::new(Some(&mut parent))?;
    let operation_bytes = work.live_allocation_bytes();
    let chunks = encoded
        .chunks(chunk_bytes)
        .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
        .collect::<Vec<_>>();
    let (release, released) = futures::channel::oneshot::channel();
    let source = futures::stream::iter(chunks)
        .chain(futures::stream::once(async move {
            match released.await {
                Ok(false) => Ok(Bytes::new()),
                Ok(true) => Err(acyclic_harness_core::Error::NativeBoundary {
                    code: acyclic_harness_core::wire::ErrorCode::Storage,
                    reason: "original refresh body error",
                }),
                Err(_) => Err(crate::transport_failure(
                    true,
                    acyclic_harness_core::ModelTransportFailure::Interrupted,
                    "original refresh body producer lost",
                )),
            }
        }))
        .boxed();
    let body = HttpResponse::with_native_body(200, source, &work)?.body;
    let mut pending = Box::pin(refresh_document(body, &mut io));
    assert!(pending.as_mut().now_or_never().is_none());
    assert!(work.live_allocation_bytes() > operation_bytes);
    assert_eq!(work.receipt().source_bytes_read, encoded.len() as u64);
    match terminal {
        0 => {
            release
                .send(false)
                .expect("original body terminal retained");
            assert_eq!(&pending.await?, expected);
        }
        1 => {
            release.send(true).expect("original body terminal retained");
            assert!(matches!(
                pending.await,
                Err(acyclic_harness_core::Error::NativeBoundary { reason, .. })
                    if reason == "original refresh body error"
            ));
        }
        2 => {
            drop(pending);
            assert!(release.is_canceled(), "body cancellation lost its producer");
        }
        _ => {
            drop(release);
            assert!(matches!(
                pending.await,
                Err(acyclic_harness_core::Error::ModelUnavailable { failure, .. })
                    if failure == acyclic_harness_core::ModelTransportFailure::Interrupted
            ));
        }
    }
    if terminal != 0 {
        // The former private collector put each full growth claim into io,
        // retaining those claims after its buffer and source had retired.
        assert_eq!(work.live_allocation_bytes(), operation_bytes);
    }
    drop(io);
    assert_eq!(work.live_allocation_bytes(), baseline);
    drop(parent);
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

/// Uses the actual private-file publisher and original refresh owner. The event
/// is produced inside publication, so cancellation follows physical admission.
#[tokio::test]
async fn accepted_publication_retains_ownership_after_observer_cancellation() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services()
        .map_err(acyclic_fs::NativeTreeMaterializationError::Io)?;
    let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
    let control = async move {
        let result = schedule.scope(publication_control()).await;
        schedule.drained().await;
        result
    };
    let driver = async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    };
    let (result, ()) = futures::join!(control, driver);
    let (work, baseline) = result?;
    assert_eq!(work.live_allocation_bytes(), baseline);
    Ok(())
}

async fn publication_control() -> Result<(NativeMaterializationWork, u64)> {
    let now = crate::codex::fixed_now().unix_timestamp();
    let (root, login, transport) = fixture(&jwt(now + 3600), 200)?;
    let path = root.path().join("auth.json");
    let work = NativeMaterializationWork::new(
        acyclic_fs::performance::work_budget(65_536, 64 * 1024 * 1024),
        &CancellationToken::new(),
    )
    .map_err(|(cause, work)| acyclic_harness_core::Error::NativeConstruction { cause, work })?;
    let baseline = work.live_allocation_bytes();
    let mut parent = work.claim(0, 0, WorkCounters::UNCHARGED)?;
    let mut io = CredentialIo::new(Some(&mut parent))?;
    let mut permit = Some(login.refresh.acquire().await.ok().unwrap());
    let (_, original) = load(&path, &mut io)?;
    let replacement = jwt(now + 7200);
    let updated = RefreshUpdate {
        tokens: json!({"access_token":replacement}),
        last_refresh: original["last_refresh"].clone(),
    };
    let (entered, entering) = futures::channel::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let publication = io
        .blocking(&mut permit, move |io| {
            persist_with(
                &path,
                &original,
                updated,
                || {
                    entered
                        .send(())
                        .map_err(|_| invalid("publication observer lost"))?;
                    released
                        .recv()
                        .map_err(|_| invalid("publication producer lost"))
                },
                io,
            )
        })
        .boxed();
    let publication = match futures::future::select(entering, publication).await {
        Either::Left((entered, publication)) => {
            if entered.is_err() {
                publication.await?;
                panic!("publication completed without its original event");
            }
            publication
        }
        Either::Right(_) => panic!("publication completed before its release event"),
    };
    drop(publication);
    assert!(permit.is_none(), "physical operation must own the permit");
    drop((io, parent));
    assert!(
        work.live_allocation_bytes() > baseline,
        "accepted publication must retain its original payload custody"
    );

    let next = login.refresh.acquire();
    futures::pin_mut!(next);
    assert!(
        futures::poll!(&mut next).is_pending(),
        "accepted publication released refresh ownership"
    );
    // A blocked publisher cannot stall another account or the async executor.
    let (_other_root, other, other_transport) = fixture(&jwt(now + 3600), 200)?;
    other.bearer(&other_transport, None, None).await?;
    assert!(other_transport.sent.lock().unwrap().is_empty());
    release
        .send(())
        .expect("original physical publisher retained");
    let retained = next
        .await
        .ok()
        .expect("original completion wakes its waiter");
    login.refresh.complete(retained, None);
    assert_eq!(login.bearer(&transport, None, None).await?.0, replacement);
    assert!(transport.sent.lock().unwrap().is_empty());
    assert_eq!(work.receipt().durability_operations, 1);

    // Producer loss returns the original typed error and releases serialization;
    // it cannot strand the waiter or publish the staged update.
    let mut io = CredentialIo::new(None)?;
    let mut permit = Some(login.refresh.acquire().await.ok().unwrap());
    let path = root.path().join("auth.json");
    let (_, original) = load(&path, &mut io)?;
    let updated = RefreshUpdate {
        tokens: json!({"access_token":jwt(now + 10_800)}),
        last_refresh: original["last_refresh"].clone(),
    };
    let (producer, event) = std::sync::mpsc::channel::<()>();
    drop(producer);
    let error = io
        .blocking(&mut permit, move |io| {
            persist_with(
                &path,
                &original,
                updated,
                || {
                    event
                        .recv()
                        .map_err(|_| invalid("publication producer lost"))
                },
                io,
            )
        })
        .await
        .err()
        .expect("lost publication producer must refuse");
    assert!(
        matches!(error, acyclic_harness_core::Error::Invalid(ref reason) if reason == "publication producer lost")
    );
    login.refresh.complete(permit.take().unwrap(), None);
    assert_eq!(login.bearer(&transport, None, None).await?.0, replacement);
    Ok((work, baseline))
}
