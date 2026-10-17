use super::*;
use acyclic_harness::{OperationId, conversation::ContentPublisher};

fn capabilities(scope: &Scope) -> Capabilities {
    Capabilities::new(
        scope
            .capabilities()
            .iter()
            .filter(|label| label.starts_with(SCOPE_PREFIX)),
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    #[test]
    fn task_writer_intersects_both_capabilities(
        bound_path in any::<bool>(), caller_path in any::<bool>(),
        bound_expired in any::<bool>(), caller_expired in any::<bool>(),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build()?.block_on(async {
            let fixture = Fixture::new().await?;
            let actions = [Action::Create, Action::Write, Action::List, Action::ReadHistory];
            let bound = fixture.narrowed_for(&actions, bound_path, bound_expired)?;
            let caller = fixture.narrowed_for(&actions, caller_path, caller_expired)?;
            let writer = acyclic_harness::filesystem::FilesystemContentPublisher::new(
                fixture.host.clone(), &fixture.volume, &fixture.issuer.verifier(&mut None)?,
                &bound, 4096, None,
            )?;
            let task = context::content_context(fixture.reader(&fixture.scope)?, Some(writer), capabilities(&caller)).await?;
            let result = task.stage_file_once(OperationId::new(), "secrets/new", b"new", "text/plain", "new", None).await;
            let allowed = bound_path && caller_path && !bound_expired && !caller_expired;
            assert_eq!(result.is_ok(), allowed, "{result:?}");
            if !allowed {
                assert_eq!(result.unwrap_err().code(), acyclic_harness::wire::ErrorCode::Unauthorized);
            }
            Ok::<_, Box<dyn Error>>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}

#[tokio::test]
async fn direct_writer_rejects_empty_and_unrelated_callers() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let grant = ContentGrant::verify(
        &fixture.issuer.verifier(&mut None)?,
        &fixture.scope,
        &fixture.volume,
        VolumeOperation::Write,
        4096,
        &mut None,
    )?;
    let writer = acyclic_harness::filesystem::FilesystemContentPublisher::new(
        fixture.host.clone(),
        &fixture.volume,
        &fixture.issuer.verifier(&mut None)?,
        &fixture.scope,
        4096,
        None,
    )?;
    for grants in [
        Capabilities::default(),
        Capabilities::new([fixture
            .volume
            .capability(VolumeOperation::Read, &mut None)?]),
    ] {
        assert!(
            fixture
                .host
                .put_content(
                    &fixture.volume,
                    &grant,
                    "secrets/new",
                    b"new",
                    "text/plain",
                    "new",
                    4096,
                    &IdempotencyKey::new("direct")?,
                    None,
                    Some(&grants),
                    None
                )
                .await
                .is_err()
        );
        assert!(
            writer
                .stage_once(
                    OperationId::new(),
                    "secrets/new",
                    b"new",
                    "text/plain",
                    "new",
                    Some(&grants),
                    None
                )?
                .await
                .is_err()
        );
    }
    let caller = fixture.narrowed_for(&[Action::Create, Action::Write], true, false)?;
    let caller = capabilities(&caller);
    let _ = writer
        .stage_at(
            OperationId::new(),
            &fixture.file,
            b"blind replacement",
            Some(&caller),
            None,
        )?
        .await?;
    Ok(())
}

#[tokio::test]
async fn cached_reply_checks_the_callers_list_ceiling() -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let writer = acyclic_harness::filesystem::FilesystemContentPublisher::new(
        fixture.host.clone(),
        &fixture.volume,
        &fixture.issuer.verifier(&mut None)?,
        &fixture.scope,
        4096,
        None,
    )?;
    let operation = OperationId::new();
    let (created, _) = writer
        .stage_once(
            operation,
            "secrets/cached",
            b"cached",
            "text/plain",
            "cached",
            None,
            None,
        )?
        .await?;
    for (list, expired, allowed) in [
        (false, false, false),
        (true, true, false),
        (true, false, true),
    ] {
        let mut actions = vec![Action::Read, Action::ReadHistory];
        if list {
            actions.push(Action::List);
        }
        let caller = fixture.narrowed_for(&actions, true, expired)?;
        let task = context::content_context(
            fixture.reader(&fixture.scope)?,
            Some(writer.clone()),
            capabilities(&caller),
        )
        .await?;
        let result = task.observe_staged_file_once(operation, None).await;
        assert_eq!(result.is_ok(), allowed, "{result:?}");
        if allowed {
            assert_eq!(result?.0, Some(created.clone()));
        }
    }
    Ok(())
}

#[tokio::test]
async fn structured_service_grant_is_governed_by_policy_and_its_resource()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    for granted in [false, true] {
        let capability = Capability {
            layers: vec![vec![Grant {
                actions: [Action::Create].into_iter().collect(),
                resource: ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: Generations::Head,
                    path: if granted { "**" } else { "public/**" }.into(),
                },
            }]],
            expires_at: None,
        };
        let scope = fixture.issuer.issue(
            "service",
            &Capabilities::new([capability.to_scope_label()?]),
            None,
            None,
            &mut None,
        )?;
        let grant = ContentGrant::verify(
            &fixture.issuer.verifier(&mut None)?,
            &scope,
            &fixture.volume,
            VolumeOperation::Write,
            4096,
            &mut None,
        )?;
        let result = fixture
            .host
            .put_content(
                &fixture.volume,
                &grant,
                "secrets/service",
                b"new",
                "text/plain",
                "new",
                4096,
                &IdempotencyKey::new("service")?,
                None,
                None,
                None,
            )
            .await;
        assert_eq!(result.is_ok(), granted, "{result:?}");
    }
    Ok(())
}
