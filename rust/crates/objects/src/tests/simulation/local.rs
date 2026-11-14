//! Original local log faults, followed by physical root reopening.
use super::*;
use acyclic_native_runtime::testing::DurableFault;

async fn episode(seed: u64, fault: DurableFault) -> Result<(), Failure> {
    let directory = tempfile::tempdir()?;
    let stable = b"flushed before the failed put";
    let stable_digest = Digest::of(stable);
    let mut candidate = vec![0; 32];
    let (schedule, mut boundaries) = Schedule::seeded(seed);
    drive(&schedule, &mut boundaries, async {
        assert!(fill_seeded(&mut candidate));
        let store =
            Store::new(crate::LocalMedium::open(directory.path(), Default::default(), None).await?);
        let blobs = store.account(b"simulation")?;
        let put = blobs.put(stable.as_slice().into(), Vec::new()).await?;
        assert_eq!(
            put.verified().ok_or("unverified baseline")?.digest,
            stable_digest
        );
        blobs.flush(Some(&[stable_digest])).await?;
        // Arm the original next log operation, not a replacement Medium.
        schedule.cut_next(fault);
        let attempted = blobs.put(candidate.clone().into(), Vec::new()).await;
        // A put alone promises availability, not crash durability. Only a
        // successful flush would promise persistence across the next open.
        let confirmed = if attempted.is_ok() {
            blobs.flush(Some(&[Digest::of(&candidate)])).await.is_ok()
        } else {
            false
        };
        assert!(!confirmed, "a failed physical root acknowledged durability");
        Ok::<_, Failure>(())
    })
    .await?;
    let cause = schedule
        .storage_fault()
        .ok_or("selected fault was not reached")?;
    assert_eq!(cause.fault, fault);
    drop(schedule);

    let (recovery, mut boundaries) = Schedule::seeded(seed);
    drive(&recovery, &mut boundaries, async {
        let store =
            Store::new(crate::LocalMedium::open(directory.path(), Default::default(), None).await?);
        let blobs = store.account(b"simulation")?;
        assert_eq!(
            blobs
                .get(stable_digest, ByteRange::FULL, 128)
                .await?
                .body()
                .as_ref(),
            stable
        );
        // The interrupted envelope/payload can be absent or survive. Re-put
        // must repair its closure, and its later flush must make it durable.
        let put = blobs.put(candidate.clone().into(), Vec::new()).await?;
        let digest = put
            .verified()
            .ok_or("recovery put was not verified")?
            .digest;
        assert_eq!(digest, Digest::of(&candidate));
        blobs.flush(Some(&[stable_digest, digest])).await?;
        assert_eq!(
            blobs
                .get(digest, ByteRange::new(3, 23), 128)
                .await?
                .body()
                .as_ref(),
            &candidate[3..23]
        );
        Ok::<_, Failure>(())
    })
    .await?;
    drop(recovery);

    let (reopened, mut boundaries) = Schedule::seeded(seed);
    drive(&reopened, &mut boundaries, async {
        let store =
            Store::new(crate::LocalMedium::open(directory.path(), Default::default(), None).await?);
        let blobs = store.account(b"simulation")?;
        for bytes in [stable.as_slice(), candidate.as_slice()] {
            assert_eq!(
                blobs
                    .get(Digest::of(bytes), ByteRange::FULL, 128)
                    .await?
                    .body()
                    .as_ref(),
                bytes
            );
        }
        Ok::<_, Failure>(())
    })
    .await
}

#[test]
fn seeded_local_faults_preserve_flushed_objects_and_allow_repair() -> Result<(), Failure> {
    acyclic_native_runtime::initialize_native_io_services()?;
    for seed in [0, 7] {
        for fault in [
            DurableFault::Before,
            DurableFault::After,
            DurableFault::Torn(1),
        ] {
            eprintln!("seed={seed} fault={fault:?}");
            seeded_runtime(seed)?.block_on(async {
                tokio::time::pause();
                episode(seed, fault).await
            })?;
        }
    }
    Ok(())
}
