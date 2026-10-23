use super::*;

fn ceiling(allowed: bool) -> Result<Capabilities, Box<dyn Error>> {
    Ok(Capabilities::new([Capability {
        layers: vec![vec![Grant {
            actions: [Action::Read, Action::ReadHistory].into_iter().collect(),
            resource: ResourcePattern {
                lineage_root: None,
                volume: None,
                generations: Generations::All,
                path: if allowed { "**" } else { "public/**" }.into(),
            },
        }]],
        expires_at: None,
    }
    .to_scope_label()?]))
}

async fn check(
    scope_allowed: bool,
    caller_allowed: bool,
    denied: bool,
    start: u8,
    count: u8,
) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let scope = fixture.issuer.issue(
        "range",
        &ceiling(scope_allowed)?,
        Some(AgentId::from_bytes([7; 16])),
        None,
        &mut None,
    )?;
    let reader = fixture.reader(&scope)?;
    let caller = ceiling(caller_allowed)?;
    fixture.policy(denied, 2).await?;
    let start = usize::from(start).min(7);
    let end = (start + usize::from(count)).min(7);
    let range = acyclic_fs::ByteRange {
        offset: start as u64,
        length: (end - start) as u64,
    };
    let result = reader
        .read_range(&fixture.file, range, Some(&caller), None)?
        .await;
    if scope_allowed && caller_allowed && !denied {
        assert_eq!(
            result?.as_ref(),
            b"private"
                .get(start..end)
                .ok_or("invalid fixture interval")?
        );
    } else {
        assert!(
            matches!(result, Err(error) if error.code() == acyclic_harness::wire::ErrorCode::Unauthorized)
        );
    }
    Ok(())
}

#[tokio::test]
async fn ranged_reads_obey_policy_and_both_capability_ceilings() -> Result<(), Box<dyn Error>> {
    for (scope, caller, deny) in [
        (true, true, false),
        (false, true, false),
        (true, false, false),
        (true, true, true),
    ] {
        check(scope, caller, deny, 1, 3).await?;
    }
    let fixture = Fixture::new().await?;
    let reader = fixture.reader(&fixture.scope)?;
    let range = acyclic_fs::ByteRange {
        offset: 1,
        length: 3,
    };
    assert_eq!(
        reader
            .read_range(&fixture.file, range, None, None)?
            .await?
            .as_ref(),
        b"riv"
    );
    fixture.policy(true, 2).await?;
    let denied = reader.read_range(&fixture.file, range, None, None)?.await;
    assert!(
        matches!(denied, Err(error) if error.code() == acyclic_harness::wire::ErrorCode::Unauthorized)
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // An arbitrary authenticated interval is readable exactly when current
    // policy, the signed grant and the caller ceiling all permit its file.
    #[test]
    fn ranges_equal_the_shared_access_intersection(
        scope in any::<bool>(), caller in any::<bool>(), deny in any::<bool>(),
        start in 0u8..9, count in 0u8..9,
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check(scope, caller, deny, start, count))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
