use super::*;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};
use futures::{FutureExt as _, future::Either};

/// Uses the actual private-file publisher and original refresh owner. The event
/// is produced inside publication, so cancellation follows physical admission.
#[tokio::test]
async fn accepted_publication_retains_ownership_after_observer_cancellation() -> Result<()> {
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
    let original = read(&path, &mut io)?;
    let replacement = jwt(now + 7200);
    let mut updated = document(&original, &mut io)?;
    updated["tokens"]["access_token"] = json!(replacement);
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
            entered.expect("publication must reach the original event");
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
    let original = read(&path, &mut io)?;
    let updated = document(&original, &mut io)?;
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
        .expect_err("lost publication producer must refuse");
    assert!(
        matches!(error, acyclic_harness_core::Error::Invalid(ref reason) if reason == "publication producer lost")
    );
    login.refresh.complete(permit.take().unwrap(), None);
    assert_eq!(login.bearer(&transport, None, None).await?.0, replacement);
    Ok((work, baseline))
}
