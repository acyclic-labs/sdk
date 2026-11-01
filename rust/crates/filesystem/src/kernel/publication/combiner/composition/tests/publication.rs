use super::*;

#[derive(Clone)]
struct CachedOwner {
    fs: crate::LocalFs,
    cache: Arc<crate::kernel::PublicationCache>,
}

impl PublicationOwner for CachedOwner {
    type Authority = <crate::LocalFs as PublicationOwner>::Authority;
    type Objects = <crate::LocalFs as PublicationOwner>::Objects;

    fn authority(&self) -> &Self::Authority {
        self.fs.authority()
    }
    fn objects(&self) -> &Self::Objects {
        self.fs.objects()
    }
    fn publication_cache(&self) -> Option<&crate::kernel::PublicationCache> {
        Some(&self.cache)
    }
}

async fn retained_publication(delivery: Delivery) -> TestResult {
    crate::initialize_test_native_io();
    let directory = tempfile::tempdir()?;
    let fs = crate::LocalFs::local(crate::LocalOptions::new(directory.path())).await?;
    let cancel = CancellationToken::new();
    let original = crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancel)
        .map_err(|(error, _)| error)?;
    let mut accounting = KernelAccounting::native(&original, &cancel);
    let (core, _) = core(&fs, &mut accounting, &cancel).await?;
    let request = core.request;
    let owner = CachedOwner {
        fs: fs.clone(),
        cache: Arc::default(),
    };
    let mut intent = PublicationIntent::guarded(None, PublicationPermit::Unrestricted);
    intent.preconditions = Some(core.preconditions.clone());
    drop(core);
    let preparation = crate::kernel::publication::preparation::Preparation {
        owner: &owner,
        request,
        intent,
        limits: ClosureLimits::default(),
        cache: Some(&owner.cache.inner),
        cancellation: &cancel,
        resolved: None,
    };
    let prepared = preparation
        .prepare::<crate::FsError, _>(&mut accounting, &mut |_, _, _| Ok(()))
        .await?;
    let crate::kernel::publication::preparation::Prepared::Ready(prepared) = prepared else {
        return Err("publication did not prepare an original append".into());
    };
    let crate::kernel::publication::preparation::PreparedGeneration {
        mut candidate,
        authentication,
        append,
        projection_fence,
        publication_hold,
    } = prepared;
    let generation = authentication.proof.generation_id;
    let mut ticket = candidate
        .ticket
        .take()
        .ok_or("missing publication ticket")?;
    let group = ticket.group.clone();
    let publication = SubmittedPublication {
        authentication: crate::kernel::publication::authenticate::retain_authentication(
            authentication,
            &mut accounting,
        )?,
        published: None,
        _projection_fence: projection_fence,
        _publication_hold: publication_hold,
    };
    let gate = Arc::new(Gate {
        armed: AtomicBool::new(false),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        fail: matches!(delivery, Delivery::Failure),
    });
    let seal = crate::kernel::publication::seals::shared(&original, {
        let gate = Arc::clone(&gate);
        move |_: &crate::NativeMaterializationWork| {
            let gate = Arc::clone(&gate);
            async move {
                gate.entered.notify_one();
                gate.release.notified().await;
                if gate.fail {
                    Err(crate::FsError::InvalidDiff)
                } else {
                    Ok(())
                }
            }
        }
    })?;
    let (schedule, mut boundaries) = acyclic_native_runtime::testing::Schedule::new();
    composition_events(&mut boundaries, async {
        let mut reply = Box::pin(schedule.scope(ticket.submit(
            &owner,
            append,
            Some(seal.seal),
            publication,
            &mut accounting,
            &cancel,
        )));
        tokio::select! {
            () = gate.entered.notified() => {},
            _ = &mut reply => panic!("publication completed before its original seal"),
        }
        match delivery {
            Delivery::Cancel => {
                cancel.cancel();
                assert!(matches!(reply.await, Err(crate::FsError::Cancelled(_))));
            }
            Delivery::Drop => drop(reply),
            Delivery::Complete | Delivery::Failure => {
                gate.release.notify_one();
                let result = reply.await;
                if matches!(delivery, Delivery::Failure) {
                    assert!(result.is_err());
                } else {
                    let (outcome, publication) = result?;
                    assert!(matches!(outcome, AppendOutcome::Committed(_)));
                    assert!(publication.published.is_some());
                }
            }
            Delivery::Collected => unreachable!(),
        }
        drop(ticket);
        if matches!(delivery, Delivery::Drop | Delivery::Cancel) {
            assert!(
                matches!(
                    crate::access::projection::enter(
                        fs.authority().operation_claims(),
                        request.volume_id
                    ),
                    Err(crate::FsError::MutationNotAllowed)
                ),
                "reply withdrawal retired the original projection fence"
            );
            let collection = fs.objects().collection().ok_or("missing collection")?;
            let collecting = collection.begin();
            tokio::pin!(collecting);
            assert!(
                futures::poll!(&mut collecting).is_pending(),
                "reply withdrawal retired the original flush hold"
            );
            gate.release.notify_one();
        }
        schedule.drained().await;
        let observer = CancellationToken::new();
        let mut observe = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &observer);
        let head = fs
            .authority()
            .head(
                request.authority_id,
                &mut crate::AuthorityWork::new(WorkBudget::UNBOUNDED, &observer),
            )
            .await?;
        let committed = !matches!(delivery, Delivery::Failure);
        assert_eq!(
            head.sequence.get(),
            request.expected.sequence.get() + u64::from(committed)
        );
        assert_eq!(
            owner
                .cache
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .published(
                    request.volume_id,
                    generation,
                    ClosureLimits::default(),
                    None,
                    &mut observe
                )?
                .is_some(),
            committed,
            "only the actual committed member may publish its original pending proof"
        );
        drop(group);
        assert!(
            crate::access::projection::enter(fs.authority().operation_claims(), request.volume_id)
                .is_ok()
        );
        drop(
            fs.objects()
                .collection()
                .ok_or("missing collection")?
                .begin()
                .await,
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn original_publication_custody_survives_reply_withdrawal_until_member_confirmation()
-> TestResult {
    for delivery in [
        Delivery::Complete,
        Delivery::Drop,
        Delivery::Cancel,
        Delivery::Failure,
    ] {
        retained_publication(delivery).await?;
    }
    Ok(())
}
