use super::*;

async fn listing_case(rows: &[(bool, bool)], page_size: u32) -> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let verifier = fixture.issuer.verifier(&mut None)?;
    let writer = ContentGrant::verify(
        &verifier,
        &fixture.scope,
        &fixture.volume,
        VolumeOperation::Write,
        4096,
        &mut None,
    )?;
    let mut policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)?;
    policy.version = 2;
    let mut grants = Vec::new();
    let mut expected = Vec::new();
    let make_grant = |path: String| Grant {
        actions: [Action::List, Action::ReadHistory].into_iter().collect(),
        resource: ResourcePattern {
            lineage_root: None,
            volume: None,
            generations: Generations::All,
            path,
        },
    };
    grants.push(make_grant("secrets".into()));
    for (index, &(hidden, granted)) in rows.iter().enumerate() {
        let name = format!("{index:02}");
        let path = format!("secrets/{name}");
        fixture
            .host
            .put_content(
                &fixture.volume,
                &writer,
                &path,
                b"row",
                "text/plain",
                &name,
                4096,
                &IdempotencyKey::new(format!("row-{index}"))?,
                None,
                None,
                None,
            )
            .await?;
        if hidden {
            let mut rule = policy.layers[0][0].clone();
            rule.id = format!("hidden-{index}");
            rule.effect = Effect::Deny;
            rule.actions = [Action::List].into_iter().collect();
            rule.resource.path = path.clone();
            policy.layers[0].push(rule);
        }
        if granted {
            grants.push(make_grant(path));
        }
        if !hidden && granted {
            expected.push(name);
        }
    }
    // A list-only projection must not depend on content-read permission.
    let mut read = policy.layers[0][0].clone();
    read.id = "no-content".into();
    read.effect = Effect::Deny;
    read.actions = [Action::Read].into_iter().collect();
    read.resource.path = "secrets/**".into();
    policy.layers[0].push(read);
    fixture.publish_policy(policy.clone()).await?;
    let scope = fixture.issuer.issue(
        "listing",
        &Capabilities::new([
            fixture
                .volume
                .capability(VolumeOperation::Read, &mut None)?,
            Capability {
                layers: vec![grants],
                expires_at: None,
            }
            .to_scope_label()?,
        ]),
        fixture.scope.agent(),
        None,
        &mut None,
    )?;
    let grant = ContentGrant::verify_directory_read(
        &verifier,
        &scope,
        &fixture.volume,
        "secrets",
        &mut None,
    )?;
    let mut generation = None;
    let mut cursor = None;
    let mut observed = Vec::new();
    for attempt in 0..=rows.len() + 1 {
        let (selected, page) = fixture
            .host
            .list_private_directory(
                &fixture.volume,
                &grant,
                "secrets",
                generation.as_ref(),
                cursor.as_ref(),
                page_size,
                None,
            )
            .await?;
        if let Some(generation) = &generation {
            assert_eq!(&selected, generation);
        }
        generation = Some(selected);
        assert!(!page.has_more || !page.entries.is_empty());
        assert!(page.entries.len() <= page_size as usize);
        assert!(page.next_residency.is_none());
        for entry in page.entries {
            let name = entry
                .name
                .unicode_text(WorkBudget::UNBOUNDED, &CancellationToken::new())?
                .value;
            assert!(cursor.as_ref().is_none_or(|cursor| &entry.name > cursor));
            observed.push(name);
            cursor = Some(entry.name);
        }
        if !page.has_more {
            break;
        }
        assert!(
            attempt < rows.len() + 1,
            "visible pagination did not terminate"
        );
    }
    assert_eq!(observed, expected);
    // New pages consult current policy even when their content generation is pinned.
    let mut revoke = policy.layers[0][0].clone();
    revoke.id = "revoke-directory".into();
    revoke.effect = Effect::Deny;
    revoke.actions = [Action::List].into_iter().collect();
    revoke.resource.path = "secrets".into();
    policy.version = 3;
    policy.layers[0].push(revoke);
    fixture.publish_policy(policy).await?;
    assert!(matches!(
        fixture
            .host
            .list_private_directory(
                &fixture.volume,
                &grant,
                "secrets",
                generation.as_ref(),
                cursor.as_ref(),
                page_size,
                None
            )
            .await,
        Err(acyclic_harness::Error::Unauthorized(_))
    ));
    Ok(())
}

#[tokio::test]
async fn hidden_pages_do_not_hide_the_later_visible_sibling() -> Result<(), Box<dyn Error>> {
    let mut rows = vec![(true, true); 12];
    rows.push((false, true));
    listing_case(&rows, 1).await?;
    listing_case(&vec![(true, true); 12], 1).await?;
    Ok(())
}

#[tokio::test]
async fn read_without_list_cannot_enumerate_or_disclose_directory_metadata()
-> Result<(), Box<dyn Error>> {
    let fixture = Fixture::new().await?;
    let scope = fixture.narrowed(true, false)?;
    let grant = ContentGrant::verify_directory_read(
        &fixture.issuer.verifier(&mut None)?,
        &scope,
        &fixture.volume,
        "secrets",
        &mut None,
    )?;
    assert!(matches!(
        fixture
            .host
            .list_private_directory(&fixture.volume, &grant, "secrets", None, None, 1, None)
            .await,
        Err(acyclic_harness::Error::Unauthorized(_))
    ));
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn visible_pages_equal_policy_intersected_with_capability(
        rows in prop::collection::vec((any::<bool>(), any::<bool>()), 0..16),
        page_size in 1u32..5,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(listing_case(&rows, page_size))
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
