use super::*;
use crate::core::{AggregateKind, Authority, AuthorityIssuer};
use acyclic_fs::access::{Capability, Generations, Grant, ResourcePattern};
use proptest::prelude::*;

async fn fixture(
    mask: [bool; 8],
) -> Result<(
    Arc<FilesystemHost<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>>,
    VolumeRef,
    ContentGrant,
)> {
    let provider = ProviderRef::new(
        "native-list".into(),
        "filesystem".into(),
        "1".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let volume = VolumeRef::new(
        provider,
        "project".into(),
        VolumeClass::Project,
        VolumeOwner::Project("owner".into()),
        &mut None,
    )?;
    host.create_volume(&volume).await?;
    let issuer = AuthorityIssuer::new(
        "native-list",
        [7; 32],
        Authority {
            kind: AggregateKind::Task,
            id: "list".into(),
        },
    );
    let agent = Some(AgentId::from_bytes([9; 16]));
    let legacy = volume.capability(VolumeOperation::Read, &mut None)?;
    let scope = issuer.issue(
        "writer",
        &crate::Capabilities::new([
            legacy.clone(),
            volume.capability(VolumeOperation::Write, &mut None)?,
        ]),
        agent,
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let writer = ContentGrant::verify(
        &verifier,
        &scope,
        &volume,
        VolumeOperation::Write,
        4096,
        &mut None,
    )?;
    let grant = |path: String| Grant {
        actions: [Action::List, Action::ReadHistory].into_iter().collect(),
        resource: ResourcePattern {
            lineage_root: None,
            volume: None,
            generations: Generations::All,
            path,
        },
    };
    let mut grants = vec![grant("rows".into())];
    for (index, keep) in mask.into_iter().enumerate() {
        let name = format!("{index:02}");
        let path = format!("rows/{name}");
        host.put_content(
            &volume,
            &writer,
            &path,
            b"row",
            "text/plain",
            &name,
            4096,
            &IdempotencyKey::new(format!("row-{index}"))?,
            None,
            crate::conversation::ContentAccess::default(),
        )
        .await?;
        if keep {
            grants.push(grant(path));
        }
    }
    let scope = issuer.issue(
        "reader",
        &crate::Capabilities::new([
            legacy,
            Capability {
                layers: vec![grants],
                expires_at: None,
            }
            .to_scope_label()
            .map_err(|error| Error::Invalid(error.to_string()))?,
        ]),
        agent,
        None,
        &mut None,
    )?;
    let read = ContentGrant::verify_directory_read(&verifier, &scope, &volume, "rows", &mut None)?;
    Ok((host, volume, read))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn native_directory_pages_release_custody_and_honor_each_guard(
        mask in any::<[bool; 8]>(), caller_mask in any::<[bool; 8]>(), page_size in 1u32..5,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        runtime.block_on(async {
            let (host, volume, grant) = fixture(mask).await?;
            let caller = crate::Capabilities::new([Capability {
                layers: vec![std::iter::once("rows".into())
                    .chain(caller_mask.into_iter().enumerate().filter(|(_, keep)| *keep)
                        .map(|(index, _)| format!("rows/{index:02}")))
                    .map(|path| Grant {
                        actions: [Action::List, Action::ReadHistory].into_iter().collect(),
                        resource: ResourcePattern { lineage_root: None, volume: None,
                            generations: Generations::All, path },
                    }).collect()],
                expires_at: None,
            }.to_scope_label().map_err(|error| Error::Invalid(error.to_string()))?]);
            let cancellation = acyclic_fs::CancellationToken::new();
            let work = acyclic_fs::NativeMaterializationWork::new(
                acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation,
            ).map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            // SDK receipts reconcile deferred proof-control retirement.
            let _ = work.receipt();
            let baseline = work.live_allocation_bytes();
            let mut refuse_at = 0;
            let mut guard_count = 0;
            loop {
                let mut calls = 0;
                let result = host.list_private_directory_guarded_with_native_work(DirectoryListing {
                    volume: &volume, grant: &grant, ceiling: Some(&caller), path: "rows", generation: None,
                    after: None, maximum_entries: page_size,
                }, &work, || {
                    calls += 1;
                    std::future::ready(if calls == refuse_at { Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized, reason: "original owner revoked",
                    }) } else { Ok(()) })
                }).await;
                if refuse_at == 0 { guard_count = calls; }
                assert_eq!(result.is_ok(), refuse_at == 0, "guard {refuse_at}: {result:?}");
                if let Ok(((generation, page), custody)) = result {
                    assert_eq!(custody.bytes(), owned_bytes(&page)? + generation.logical_owned_bytes()?);
                    assert!(!page.entries.is_empty() || !page.has_more);
                    assert!(page.entries.len() <= page_size as usize);
                    for entry in &page.entries {
                        let name = std::str::from_utf8(entry.name.as_bytes()).unwrap();
                        let index: usize = name.parse().unwrap();
                        assert!(mask[index] && caller_mask[index]);
                    }
                    drop(generation);
                    drop(page);
                    drop(custody);
                }
                let _ = work.receipt();
                assert_eq!(work.live_allocation_bytes(), baseline);
                refuse_at += 1;
                if refuse_at > guard_count { break; }
            }
            Ok::<_, Error>(())
        }).map_err(|error| TestCaseError::fail(error.to_string()))?;
    }
}
