//! Outcome controls at the real log's original I/O boundaries.
use super::*;
use acyclic_native_runtime::testing::{DurableFault, DurableWrite, Schedule, drive};

fn bytes(value: &[u8]) -> OwnedBytes {
    OwnedBytes::from(value.to_vec())
}

#[tokio::test]
async fn empty_ranges_preserve_native_presence_without_dispatch()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::Blobs as _;

    async fn counted<T>(operation: impl Future<Output = T>) -> (T, usize) {
        let (schedule, mut events) = Schedule::new();
        let mut jobs = 0;
        let result = drive(
            &schedule,
            &mut events,
            |boundary| {
                jobs += usize::from(boundary.name == "native:submitted");
            },
            operation,
        )
        .await;
        (result, jobs)
    }

    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
    assert!(medium.create("key", bytes(b"original")).await?);
    assert!(medium.create("empty", bytes(b"")).await?);
    assert!(medium.create("a-longer-key", bytes(b"short")).await?);
    assert_eq!(medium.close().await?, LocalClose::Released);
    let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;

    for (key, offset, present) in [
        ("key", 0, true),
        ("key", 8, true),
        ("empty", 0, true),
        ("absent", 0, false),
    ] {
        let (result, jobs) = counted(medium.read(key, Some(offset..offset))).await;
        assert_eq!(result?.as_deref(), present.then_some(&b""[..]));
        assert_eq!(jobs, 0, "empty range dispatched a physical job for {key}");
    }
    let (result, jobs) = counted(medium.read("key", Some(9..9))).await;
    assert!(
        matches!(result, Err(MediumError::Failed(error)) if error.kind() == std::io::ErrorKind::InvalidInput)
    );
    assert_eq!(
        jobs, 0,
        "out-of-range index refusal dispatched physical work"
    );

    // Counter sensitivity: actual requested bytes still go through the original
    // native owner and return the selected bytes from the reopened log.
    let (result, jobs) = counted(medium.read("key", Some(1..5))).await;
    assert_eq!(result?.as_deref(), Some(&b"rigi"[..]));
    assert_eq!(jobs, 1);

    // A value no longer than its key is held by the replayed index: every
    // range of it, and its observation, is answered without a job.
    for range in [None, Some(1..4)] {
        let (result, jobs) = counted(medium.read("a-longer-key", range.clone())).await;
        let expected: &[u8] = match range {
            Some(range) => b"short"
                .get(usize::try_from(range.start)?..usize::try_from(range.end)?)
                .ok_or("held range outside value")?,
            None => b"short",
        };
        assert_eq!(result?.as_deref(), Some(expected));
        assert_eq!(jobs, 0, "held value dispatched a physical read");
    }
    let (observed, jobs) = counted(medium.observe("a-longer-key".to_owned())).await;
    assert_eq!(
        observed?.ok_or("held value absent")?.bytes.as_ref(),
        b"short"
    );
    assert_eq!(jobs, 0, "held observation dispatched a physical read");

    // A writer holds the log over disk work. Presence and listings take one
    // path regardless: they read the index in the caller's first poll, so a
    // seeded schedule never branches on another thread's lock timing.
    {
        let _log = medium.shared.log();
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let std::task::Poll::Ready(present) = Box::pin(medium.read("key", Some(8..8)))
            .as_mut()
            .poll(&mut context)
        else {
            panic!("presence check depended on the writer's log lock");
        };
        assert_eq!(present?.as_deref(), Some(&b""[..]));
        let std::task::Poll::Ready(listed) = Box::pin(medium.list("", None, 8))
            .as_mut()
            .poll(&mut context)
        else {
            panic!("listing depended on the writer's log lock");
        };
        let expected = [("a-longer-key", 5), ("empty", 0), ("key", 8)];
        assert_eq!(
            listed?,
            expected.map(|(key, length)| (key.to_owned(), length))
        );
    }

    assert!(medium.remove("key").await?);
    let (result, jobs) = counted(medium.read("key", Some(8..8))).await;
    assert!(result?.is_none());
    assert_eq!(jobs, 0, "removed payload was treated as present");
    assert_eq!(medium.close().await?, LocalClose::Released);

    // The public account path still reads the original envelope, but uses
    // the same reopened index for its full-length completeness probe.
    let root = tempfile::tempdir()?;
    let account = LocalObjects::open(root.path(), LocalObjectsLimits::default(), None).await?;
    let digest = account
        .put(bytes(b"original"), Vec::new())
        .await?
        .verified()
        .ok_or("original put outcome unknown")?
        .digest;
    assert_eq!(account.close().await?, LocalClose::Released);
    let account = LocalObjects::open(root.path(), LocalObjectsLimits::default(), None).await?;
    let (head, jobs) = counted(account.head(digest)).await;
    assert_eq!(head?.length, 8);
    assert_eq!(jobs, 0, "HEAD read its held envelope from the log");
    let (head, jobs) = counted(account.head(crate::Digest::of(b"absent"))).await;
    assert_eq!(head.unwrap_err().code, crate::wire::ErrorCode::NotFound);
    assert_eq!(jobs, 0, "a missing envelope dispatched a physical read");
    assert_eq!(account.close().await?, LocalClose::Released);
    Ok(())
}

/// GC-285: each new tree version re-admits every child, reading the child's
/// envelope and probing its payload. Those reads are answered by the index,
/// so publishing versions over a shared child reads nothing from the log.
#[tokio::test]
async fn new_versions_admit_their_children_without_reading_the_log()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::Blobs as _;

    acyclic_native_runtime::initialize_native_io_services()?;
    let root = tempfile::tempdir()?;
    let account = LocalObjects::open(root.path(), LocalObjectsLimits::default(), None).await?;
    let child = account
        .put(bytes(b"shared child"), Vec::new())
        .await?
        .verified()
        .ok_or("child put outcome unknown")?
        .digest;
    let before = account.borrowed_medium().reads();
    for version in 0..8_u8 {
        account
            .put(bytes(&[version]), vec![child])
            .await?
            .verified()
            .ok_or("parent put outcome unknown")?;
    }
    assert_eq!(
        account.borrowed_medium().reads() - before,
        0,
        "admitting a held child read the log"
    );
    assert_eq!(account.close().await?, LocalClose::Released);
    Ok(())
}

#[tokio::test]
async fn owned_close_reports_physical_completion_and_failure()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::Blobs as _;

    acyclic_native_runtime::initialize_native_io_services()?;
    // A committed root needs no hidden durability on final Drop. Its original
    // recovery marker remains, and reopening verifies the retained real blob.
    let root = tempfile::tempdir()?;
    let anchor = acyclic_native_runtime::OwnershipAnchor::new(());
    let released = anchor.release_barrier();
    let account =
        LocalObjects::open(root.path(), LocalObjectsLimits::default(), Some(anchor)).await?;
    let digest = account
        .put(bytes(b"committed"), Vec::new())
        .await?
        .verified()
        .ok_or("committed put outcome unknown")?
        .digest;
    account
        .retain(&crate::RootId::new(b"root".to_vec())?, vec![digest])
        .await?;
    drop(account);
    assert!(
        released.is_released(),
        "Drop retained the original root lock"
    );
    assert!(
        root.path().join(OPEN).exists(),
        "implicit Drop performed unobserved durability"
    );
    let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
    assert!(medium.recovered(), "Drop bypassed original recovery");
    // Even explicit close cannot certify a root before its repair settles.
    assert_eq!(medium.close().await?, LocalClose::Released);
    assert!(root.path().join(OPEN).exists());
    let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
    assert!(medium.recovered());
    let reopened = Store::new(medium).account(b"local")?;
    assert_eq!(
        reopened
            .get(digest, crate::ByteRange::FULL, 9)
            .await?
            .body()
            .as_ref(),
        b"committed"
    );
    assert_eq!(reopened.close().await?, LocalClose::Released);
    assert!(!root.path().join(OPEN).exists());

    let root = tempfile::tempdir()?;
    let account = LocalObjects::open(root.path(), LocalObjectsLimits::default(), None).await?;
    let digest = account
        .put(bytes(b"retained"), Vec::new())
        .await?
        .verified()
        .ok_or("put outcome unknown")?
        .digest;
    assert_eq!(account.clone().close().await?, LocalClose::StillReferenced);
    assert!(matches!(
        LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await,
        Err(LocalOpenError::AlreadyOwned)
    ));
    assert_eq!(account.close().await?, LocalClose::Released);
    let reopened = LocalObjects::open(root.path(), LocalObjectsLimits::default(), None).await?;
    assert_eq!(
        reopened
            .get(digest, crate::ByteRange::FULL, 8)
            .await?
            .body()
            .as_ref(),
        b"retained"
    );
    assert_eq!(reopened.close().await?, LocalClose::Released);

    // Every original durability barrier has both a before-effect and a lost-
    // acknowledgement control. Drop used to swallow these physical failures.
    let mut barriers = 0;
    for cut in std::iter::once(None).chain((0..2).flat_map(|ordinal| {
        [DurableFault::Failed, DurableFault::After].map(|fault| Some((ordinal, fault)))
    })) {
        let root = tempfile::tempdir()?;
        let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
        assert!(medium.create("key", bytes(b"dirty")).await?);
        let (schedule, mut events) = Schedule::new();
        if let Some((ordinal, fault)) = cut {
            schedule.cut_at(ordinal, fault);
        }
        let result = drive(&schedule, &mut events, |_| {}, medium.close()).await;
        let trace = schedule.durable_writes();
        match cut {
            None => {
                assert_eq!(result?, LocalClose::Released);
                barriers = trace.len();
                assert_eq!(barriers, 2, "one data and one directory barrier: {trace:?}");
            }
            Some((ordinal, fault)) => {
                let Err(LocalCloseError::Io(error)) = result else {
                    return Err(format!("close hid {cut:?}: {result:?}").into());
                };
                assert_eq!(
                    error
                        .get_ref()
                        .and_then(|cause| cause
                            .downcast_ref::<acyclic_native_runtime::testing::InjectedStorageFault>(
                        ))
                        .map(|cause| (cause.ordinal, cause.fault)),
                    Some((ordinal, fault))
                );
                assert_eq!(trace.len(), ordinal + 1, "Drop retried the failed close");
                assert!(ordinal < barriers);
            }
        }
        // Both success and error are delivered after the actual lock retires.
        let reopened = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
        assert_eq!(
            reopened.read("key", None).await?.as_deref(),
            Some(&b"dirty"[..])
        );
        if cut.is_some_and(|(ordinal, _)| ordinal == 0) {
            assert!(
                reopened.recovered(),
                "failed data flush lost recovery marker"
            );
        }
        assert_eq!(reopened.close().await?, LocalClose::Released);
    }

    let root = tempfile::tempdir()?;
    let anchor = acyclic_native_runtime::OwnershipAnchor::new(());
    let released = anchor.release_barrier();
    let medium =
        LocalMedium::open(root.path(), LocalObjectsLimits::default(), Some(anchor)).await?;
    assert!(medium.create("key", bytes(b"accepted")).await?);
    let (schedule, mut events) = Schedule::new();
    let mut close = Box::pin(schedule.scope(medium.close()));
    let submitted = tokio::select! {
        biased;
        result = &mut close => return Err(format!("close ended before submission: {result:?}").into()),
        boundary = events.recv() => boundary.ok_or("close submission producer lost")?,
    };
    assert_eq!(submitted.name, "task:submitted");
    submitted.release();
    let submitted = tokio::select! {
        biased;
        result = &mut close => return Err(format!("close ended before native submission: {result:?}").into()),
        boundary = events.recv() => boundary.ok_or("native close submission producer lost")?,
    };
    assert_eq!(submitted.name, "native:submitted");
    drop(close);
    assert!(
        !released.is_released(),
        "cancelled caller released accepted close"
    );
    assert!(matches!(
        LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await,
        Err(LocalOpenError::AlreadyOwned)
    ));
    submitted.release();
    let completed = tokio::select! {
        biased;
        boundary = events.recv() => boundary.ok_or("close completion producer lost")?,
        () = schedule.drained() => return Err("close producer ended without completion".into()),
    };
    assert_eq!(completed.name, "native:completed");
    completed.release();
    let completed = tokio::select! {
        biased;
        boundary = events.recv() => boundary.ok_or("close task terminal producer lost")?,
        () = schedule.drained() => return Err("close task ended without its terminal".into()),
    };
    assert_eq!(completed.name, "task:completed");
    completed.release();
    schedule.drained().await;
    assert!(
        released.is_released(),
        "physical close retained its original anchor"
    );
    let reopened = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
    assert!(!reopened.recovered());
    assert_eq!(
        reopened.read("key", None).await?.as_deref(),
        Some(&b"accepted"[..])
    );
    assert_eq!(reopened.close().await?, LocalClose::Released);
    Ok(())
}

async fn apply_mutation<'a>(
    medium: &'a LocalMedium,
    operation: &str,
    observed: Option<Observation<'a>>,
) -> Result<bool, MediumError> {
    match operation {
        "create" => medium.create("key", bytes(b"new")).await,
        "replace" => {
            let Some(observed) = observed else {
                return Err(MediumError::Failed(std::io::ErrorKind::InvalidInput.into()));
            };
            medium
                .replace(observed, bytes(b"new"))
                .await
                .map(|value| value.is_some())
        }
        "remove" => medium.remove("key").await,
        "remove-if" => {
            let Some(observed) = observed else {
                return Err(MediumError::Failed(std::io::ErrorKind::InvalidInput.into()));
            };
            medium.remove_if(observed).await
        }
        _ => Err(MediumError::Failed(std::io::ErrorKind::InvalidInput.into())),
    }
}

fn expected_outcome(operation: &str, fault: DurableFault) -> Option<&'static [u8]> {
    match (operation, fault) {
        ("create" | "replace", DurableFault::After) => Some(b"new"),
        ("remove" | "remove-if", DurableFault::After) | ("create", _) => None,
        _ => Some(b"old"),
    }
}

async fn mutation_outcome(
    operation: &str,
    fault: DurableFault,
) -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
    if operation != "create" {
        assert!(medium.create("key", bytes(b"old")).await?);
    }
    let observed = if operation == "create" {
        None
    } else {
        Some(observation(&medium, "key").await)
    };
    let (schedule, mut events) = Schedule::new();
    schedule.cut_at(0, fault);
    let result = drive(
        &schedule,
        &mut events,
        |_| {},
        apply_mutation(&medium, operation, observed),
    )
    .await;
    assert!(
        matches!(result, Err(MediumError::Ambiguous(_))),
        "{operation} {fault:?}: {result:?}"
    );
    let actual = schedule
        .storage_fault()
        .ok_or("cut schedule recorded no fault")?;
    assert_eq!(actual.fault, fault);
    // The stopped root can prove this later proposal did not run.
    assert!(matches!(
        medium.create("later", bytes(b"later")).await,
        Err(MediumError::Failed(_))
    ));
    assert!(medium.read("later", None).await?.is_none());
    assert!(matches!(
        medium.flush(&[]).await,
        Err(MediumError::Ambiguous(_))
    ));
    drop(medium);
    let reopened = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
    let value = reopened.read("key", None).await?;
    let expected = expected_outcome(operation, fault);
    assert_eq!(value.as_deref(), expected, "{operation} {fault:?}");
    let before = std::fs::metadata(root.path().join(LOG))?.len();
    assert!(matches!(
        reopened.list("invalid-prefix", None, 1).await,
        Err(MediumError::Failed(_))
    ));
    assert_eq!(std::fs::metadata(root.path().join(LOG))?.len(), before);
    Ok(())
}

async fn assert_cut_stopped(
    medium: &LocalMedium,
    schedule: &Schedule,
    cut: Option<(usize, DurableFault)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some((ordinal, fault)) = cut else {
        return Ok(());
    };
    let actual = schedule
        .storage_fault()
        .ok_or("cut schedule recorded no fault")?;
    assert_eq!((actual.ordinal, actual.fault), (ordinal, fault));
    assert!(matches!(
        medium.flush(&[]).await,
        Err(MediumError::Ambiguous(_))
    ));
    assert!(matches!(
        medium.create("later", bytes(b"later")).await,
        Err(MediumError::Failed(_))
    ));
    Ok(())
}

async fn assert_reopened(
    root: &std::path::Path,
    expected: Option<&[u8]>,
) -> Result<(), Box<dyn std::error::Error>> {
    let reopened = LocalMedium::open(root, LocalObjectsLimits::default(), None).await?;
    assert_eq!(reopened.read("key", None).await?.as_deref(), expected);
    assert_eq!(
        reopened
            .read("kept", None)
            .await?
            .ok_or("kept absent")?
            .as_ref(),
        b"kept"
    );
    Ok(())
}

async fn compacting_mutation(
    remove: bool,
    cut: Option<(usize, DurableFault)>,
) -> Result<Vec<DurableWrite>, Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let medium = LocalMedium::open(root.path(), LocalObjectsLimits::default(), None).await?;
    let chunk = usize::try_from(COMPACTION_CHUNK)?;
    assert!(medium.create("key", bytes(&vec![7; chunk])).await?);
    assert!(medium.create("kept", bytes(b"kept")).await?);
    medium.flush(&[]).await?;
    let observed = observation(&medium, "key").await;
    let (schedule, mut events) = Schedule::new();
    if let Some((ordinal, fault)) = cut {
        schedule.cut_at(ordinal, fault);
    }
    let result = drive(&schedule, &mut events, |_| {}, async {
        if remove {
            medium.remove_if(observed).await
        } else {
            medium
                .replace(observed, bytes(b"new"))
                .await
                .map(|value| value.is_some())
        }
    })
    .await;
    assert!(
        matches!(result, Ok(true)),
        "maintenance {cut:?}: {result:?}"
    );
    let expected = if remove {
        None
    } else {
        Some(b"new".as_slice())
    };
    assert_eq!(medium.read("key", None).await?.as_deref(), expected);
    assert_eq!(
        medium
            .read("kept", None)
            .await?
            .ok_or("kept absent")?
            .as_ref(),
        b"kept"
    );
    assert_cut_stopped(&medium, &schedule, cut).await?;
    let trace = schedule.durable_writes();
    drop(medium);
    assert_reopened(root.path(), expected).await?;
    Ok(trace)
}

async fn maintenance_failure_cannot_refuse_an_already_visible_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    futures::future::try_join_all([false, true].map(|remove| async move {
        // Capture this OS's actual successful flush/rename trace, then cut its
        // original calls. No invented syscall inventory or replacement medium.
        let trace = compacting_mutation(remove, None).await?;
        let cuts: Vec<_> = trace
            .iter()
            .filter(|call| call.operation.contains("sync") || call.operation == "namespace:rename")
            .collect();
        assert!(
            cuts.iter().any(|call| call.operation.contains("sync")),
            "{trace:?}"
        );
        assert!(
            cuts.iter().any(|call| call.operation == "namespace:rename"),
            "{trace:?}"
        );
        futures::future::try_join_all(cuts.into_iter().flat_map(|call| {
            [DurableFault::Before, DurableFault::After]
                .map(move |fault| compacting_mutation(remove, Some((call.ordinal, fault))))
        }))
        .await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    }))
    .await?;
    Ok(())
}

#[tokio::test]
async fn physical_outcomes_preserve_visible_mutations_and_uncertainty()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let mutations: Vec<_> = ["create", "replace", "remove", "remove-if"]
        .into_iter()
        .flat_map(|operation| {
            [
                DurableFault::After,
                DurableFault::Torn(1),
                DurableFault::Failed,
            ]
            .map(move |fault| mutation_outcome(operation, fault))
        })
        .collect();
    let planned = mutations.len();
    let (schedule, mut events) = Schedule::new();
    let mut in_flight = 0;
    drive(
        &schedule,
        &mut events,
        |_| {
            in_flight = in_flight.max(schedule.pending_native_work());
        },
        async {
            futures::future::try_join_all(mutations).await?;
            maintenance_failure_cannot_refuse_an_already_visible_mutation().await
        },
    )
    .await?;
    assert!(
        in_flight >= planned,
        "independent roots serialized: {in_flight} < {planned}"
    );
    Ok(())
}
