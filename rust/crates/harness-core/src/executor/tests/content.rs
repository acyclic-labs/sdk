use super::*;

mod continuation;
mod selection;

#[tokio::test]
async fn later_model_references_fail_preflight_before_earlier_content_reads() -> Result<()> {
    let journal = Journal::default();
    let first = journal
        .stage(OperationId::new(), "first", &[1, 2], "image/png", None)?
        .await?;
    let second = journal
        .stage(OperationId::new(), "second", &[3; 8], "image/png", None)?
        .await?;
    let contents = [first.clone(), second.clone()].map(|file| {
        ModelContent::Part(ModelContentPart::File {
            file,
            policy: crate::model::FileProjectionPolicy::BoundedFull,
        })
    });
    let first_only = RuntimeScope::new(
        Capabilities::new([first.read_capability(&mut None)?]),
        Limits::default(),
    )?;
    assert!(matches!(
        verify_model_contents_scoped(
            &journal,
            contents.iter(),
            Limits::default(),
            Some(&first_only),
            None,
            None,
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    let grants = Capabilities::new([
        first.read_capability(&mut None)?,
        second.read_capability(&mut None)?,
    ]);
    let narrow = RuntimeScope::new(
        grants.clone(),
        Limits {
            file_bytes: Some(4),
            ..Limits::default()
        },
    )?;
    assert!(matches!(
        verify_model_contents_scoped(
            &journal,
            contents.iter(),
            Limits::default(),
            Some(&narrow),
            None,
            None,
        )
        .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    let allowed = RuntimeScope::new(grants, Limits::default())?;
    verify_model_contents_scoped(
        &journal,
        contents.iter(),
        Limits::default(),
        Some(&allowed),
        None,
        None,
    )
    .await?;
    assert_eq!(journal.4.load(Ordering::SeqCst), 2);
    Ok(())
}

/// A prepared request's admission bounded each message once. Verifying its
/// contents measures nothing for limits that admission implies, and still
/// bounds them under a narrower task scope.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn admitted_request_contents_are_not_bounded_twice() {
    use acyclic_native_runtime::{cost::Cost, testing::Schedule};
    let contents: Vec<ModelContent> = (0..4)
        .map(|index| {
            ModelContent::Part(ModelContentPart::Notices {
                notices: vec![crate::conversation::wire::Notice {
                    source: "tools".into(),
                    kind: "tools.changed".into(),
                    body: serde_json::json!({"history": format!("{index}").repeat(256)}),
                    wake: false,
                    coalesce_key: None,
                }],
            })
        })
        .collect();
    let bounded = Limits {
        file_bytes: Some(1 << 20),
        path_bytes: std::num::NonZeroUsize::new(4096),
        attachments: std::num::NonZeroUsize::new(1024),
        render_bytes: Some(1 << 22),
        ..Limits::default()
    };
    let narrow = Limits {
        render_bytes: Some(1 << 20),
        ..bounded
    };
    let measured = |admitted: Option<Limits>, scope: Limits| {
        let journal = Journal::default();
        let scope =
            RuntimeScope::new(Capabilities::new(std::iter::empty::<String>()), scope).unwrap();
        let (schedule, mut boundaries) = Schedule::seeded(1);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async {
            let driver = async {
                while let Some(boundary) = boundaries.recv().await {
                    boundary.release();
                }
            };
            let verified = async {
                match admitted {
                    Some(admitted) => {
                        crate::executor::content::verify_admitted_model_contents(
                            &journal,
                            contents.iter(),
                            bounded,
                            admitted,
                            Some(&scope),
                            None,
                            None,
                        )
                        .await
                    }
                    None => {
                        verify_model_contents_scoped(
                            &journal,
                            contents.iter(),
                            bounded,
                            Some(&scope),
                            None,
                            None,
                        )
                        .await
                    }
                }
            };
            tokio::select! {
                verified = schedule.scope(verified) => verified.unwrap(),
                () = driver => unreachable!("the schedule outlives its driver"),
            }
        });
        schedule.ledger().unattributed.count(Cost::Measure)
    };
    let walked = measured(None, bounded);
    assert_ne!(walked, 0, "unproven contents are bounded");
    assert_eq!(measured(Some(bounded), bounded), 0);
    let scoped = measured(Some(bounded), narrow);
    assert!(scoped != 0 && scoped < walked, "{scoped} of {walked}");
}
