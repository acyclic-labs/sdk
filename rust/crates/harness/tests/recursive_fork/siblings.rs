use super::*;

#[tokio::test]
async fn thirty_two_sibling_forks_reject_stale_and_conflicting_merges() -> Result<()> {
    let provider = ProviderRef::new(
        "qualification".into(),
        "filesystem".into(),
        "2".into(),
        &mut None,
    )?;
    let host = FilesystemHost::new(Fs::memory(), &provider, None)?;
    let root = volume(
        &provider,
        VolumeClass::Project,
        "wide-root".into(),
        VolumeOwner::Project("wide".into()),
    )?;
    let root_head = host.create_volume(&root).await?;
    let parent_authority = Authority {
        kind: AggregateKind::Conversation,
        id: "wide-parent".into(),
    };
    let parent_issuer = AuthorityIssuer::new("qualification", [19; 32], parent_authority.clone());
    let parent_agent = AgentId::from_bytes([31; 16]);
    let mut capabilities = vec![
        "conversation:bind".to_owned(),
        "fork:publish".to_owned(),
        "project:merge".to_owned(),
        root.capability(VolumeOperation::Read, &mut None)?,
        root.capability(VolumeOperation::Write, &mut None)?,
    ];
    for index in 1..=32_u8 {
        capabilities.push(
            volume(
                &provider,
                VolumeClass::Project,
                format!("wide-child-{index}"),
                VolumeOwner::Project("wide".into()),
            )?
            .capability(VolumeOperation::Read, &mut None)?,
        );
    }
    let parent_scope = parent_issuer
        .issue(
            "parent",
            &Capabilities::new(capabilities),
            Some(parent_agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let mut parent_reducer = Reducer::new(
        parent_authority.clone(),
        parent_issuer.verifier(&mut None).expect("verifier copy"),
        SchemaRegistry::new(),
    );
    parent_reducer.apply(&command(
        100,
        0,
        &parent_scope,
        Action::BindConversation {
            agent: parent_agent,
        },
    )?)?;
    let controller = ParentProjectController::new(
        &host,
        &parent_reducer,
        &parent_issuer.verifier(&mut None).expect("verifier copy"),
        &parent_scope,
        &root,
        None,
    )?;
    let workspaces = FilesystemProjectWorkspaces::new(
        &host,
        &parent_reducer,
        &parent_issuer.verifier(&mut None).expect("verifier copy"),
        &parent_scope,
        root.clone(),
    )?;
    let other_agent_scope = parent_issuer
        .issue(
            "other-agent",
            parent_scope.capabilities(),
            Some(AgentId::from_bytes([32; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    assert!(
        ParentProjectController::new(
            &host,
            &parent_reducer,
            &parent_issuer.verifier(&mut None).expect("verifier copy"),
            &other_agent_scope,
            &root,
            None,
        )
        .is_err()
    );
    let ungranted = parent_issuer
        .issue(
            "no-promotion",
            &Capabilities::new([
                "fork:publish".to_owned(),
                root.capability(VolumeOperation::Read, &mut None)?,
            ]),
            Some(parent_agent),
            None,
            &mut None,
        )
        .expect("scope proof");
    let ungranted_controller = ParentProjectController::new(
        &host,
        &parent_reducer,
        &parent_issuer.verifier(&mut None).expect("verifier copy"),
        &ungranted,
        &root,
        None,
    )?;
    let foreign_project = volume(
        &provider,
        VolumeClass::Project,
        "foreign-child".into(),
        VolumeOwner::Project("other-project".into()),
    )?;
    assert!(
        controller
            .fork_project(
                &root_head.generation,
                &foreign_project,
                &IdempotencyKey::new("foreign-fork")?,
            )
            .await
            .is_ok()
    );
    let mut siblings = Vec::new();
    for index in 1..=32_u8 {
        let child = volume(
            &provider,
            VolumeClass::Project,
            format!("wide-child-{index}"),
            VolumeOwner::Project("wide".into()),
        )?;
        if index == 1 {
            assert!(
                ungranted_controller
                    .prepare_project_import(&child, &root_head.generation, None)
                    .await
                    .is_err()
            );
            let child_authority = Authority {
                kind: AggregateKind::Conversation,
                id: "wide-child".into(),
            };
            let child_issuer = AuthorityIssuer::new("qualification", [19; 32], child_authority);
            let child_scope = child_issuer
                .issue("child", parent_scope.capabilities(), None, None, &mut None)
                .expect("scope proof");
            assert!(
                ParentProjectController::new(
                    &host,
                    &parent_reducer,
                    &parent_issuer.verifier(&mut None).expect("verifier copy"),
                    &child_scope,
                    &root,
                    None,
                )
                .is_err()
            );
        }
        let fork = controller
            .fork_project(
                &root_head.generation,
                &child,
                &IdempotencyKey::new(format!("wide-fork-{index}"))?,
            )
            .await?;
        host.apply(
            &fork.workspace,
            Some(&fork.generation),
            &[WorkspaceMutation::PutFile {
                path: "/shared.txt".into(),
                bytes: format!("sibling {index}").into_bytes().into(),
            }],
            &IdempotencyKey::new(format!("wide-write-{index}"))?,
        )
        .await?;
        siblings.push(child);
    }
    assert_eq!(
        host.resolve(&root_head.workspace, None).await?.generation,
        root_head.generation
    );
    let unpublished = Authority {
        kind: AggregateKind::Conversation,
        id: "unpublished-child".into(),
    };
    assert!(
        workspaces
            .prepare_project_import(
                &parent_scope,
                &parent_reducer,
                &unpublished,
                &siblings[0],
                &host
                    .resolve(
                        &workspace_ref(provider.clone(), &(siblings[0]).storage_name(&mut None)?)?,
                        None
                    )
                    .await?
                    .generation
            )
            .await
            .is_ok(),
        "a pinned readable branch can be imported without a published parent fork"
    );
    assert!(
        workspaces
            .prepare_project_import(
                &other_agent_scope,
                &parent_reducer,
                &unpublished,
                &siblings[0],
                &host
                    .resolve(
                        &workspace_ref(provider.clone(), &(siblings[0]).storage_name(&mut None)?)?,
                        None
                    )
                    .await?
                    .generation
            )
            .await
            .is_err()
    );
    assert_eq!(
        host.resolve(&root_head.workspace, None).await?.generation,
        root_head.generation
    );
    let first = controller
        .prepare_project_import(
            &siblings[0],
            &host
                .resolve(
                    &workspace_ref(provider.clone(), &(siblings[0]).storage_name(&mut None)?)?,
                    None,
                )
                .await?
                .generation,
            None,
        )
        .await?;
    let stale = controller
        .prepare_project_import(
            &siblings[1],
            &host
                .resolve(
                    &workspace_ref(provider.clone(), &(siblings[1]).storage_name(&mut None)?)?,
                    None,
                )
                .await?
                .generation,
            None,
        )
        .await?;
    assert!(
        ungranted_controller
            .apply_project_merge(&first, OperationId::from_bytes([100; 16]), BTreeMap::new())
            .await
            .is_err()
    );
    let first_outcome = controller
        .apply_project_merge(&first, OperationId::from_bytes([101; 16]), BTreeMap::new())
        .await?
        .0;
    assert!(matches!(first_outcome, JoinOutcome::Applied(_)));
    let replay = controller
        .prepare_project_import_at(
            &siblings[0],
            &host
                .resolve(
                    &workspace_ref(provider.clone(), &siblings[0].storage_name(&mut None)?)?,
                    None,
                )
                .await?
                .generation,
            &root_head.generation,
            OperationId::from_bytes([101; 16]),
        )
        .await?;
    let recovered = controller
        .apply_project_merge(&replay, OperationId::from_bytes([101; 16]), BTreeMap::new())
        .await?
        .0;
    assert!(matches!(recovered, JoinOutcome::AlreadyApplied(_)));
    let different_source = host
        .resolve(
            &workspace_ref(provider.clone(), &siblings[1].storage_name(&mut None)?)?,
            None,
        )
        .await?
        .generation;
    let mismatched = controller
        .prepare_project_import_at(
            &siblings[1],
            &different_source,
            &root_head.generation,
            OperationId::from_bytes([101; 16]),
        )
        .await?;
    assert!(matches!(
        controller
            .apply_project_merge(
                &mismatched,
                OperationId::from_bytes([101; 16]),
                BTreeMap::new()
            )
            .await?
            .0,
        JoinOutcome::IdempotencyConflict
    ));

    let fresh_operation = controller
        .apply_project_merge(&replay, OperationId::from_bytes([108; 16]), BTreeMap::new())
        .await?
        .0;
    assert!(matches!(fresh_operation, JoinOutcome::StaleTarget(_)));
    assert!(
        ungranted_controller
            .prepare_project_import_at(
                &siblings[0],
                &root_head.generation,
                &root_head.generation,
                OperationId::from_bytes([101; 16])
            )
            .await
            .is_err()
    );

    let stale_outcome = controller
        .apply_project_merge(&stale, OperationId::from_bytes([102; 16]), BTreeMap::new())
        .await?
        .0;
    assert!(matches!(stale_outcome, JoinOutcome::StaleTarget(_)));
    let inspected = controller
        .prepare_project_import(
            &siblings[1],
            &host
                .resolve(
                    &workspace_ref(provider.clone(), &(siblings[1]).storage_name(&mut None)?)?,
                    None,
                )
                .await?
                .generation,
            None,
        )
        .await?;
    let conflict = controller
        .apply_project_merge(
            &inspected,
            OperationId::from_bytes([103; 16]),
            BTreeMap::new(),
        )
        .await?
        .0;
    let JoinOutcome::Conflicted {
        conflicts,
        truncated,
    } = conflict
    else {
        return Err(acyclic_harness::Error::Invalid(
            "divergent sibling writes did not conflict".into(),
        ));
    };
    assert!(!truncated);
    let described = controller
        .describe_project_merge_conflicts(&inspected, &conflicts, truncated)
        .await?;
    assert_eq!(described.conflicts.len(), conflicts.len());
    assert!(
        ungranted_controller
            .describe_project_merge_conflicts(&inspected, &conflicts, truncated)
            .await
            .is_err()
    );
    assert_eq!(
        host.read(&root_head.workspace, None, "/shared.txt", 32, None)
            .await?,
        acyclic_stream::OwnedBytes::from(b"sibling 1".to_vec()),
    );
    let selections: BTreeMap<_, _> = conflicts
        .into_iter()
        .map(|conflict| (conflict, ConflictSide::Theirs))
        .collect();
    assert!(
        ungranted_controller
            .apply_project_merge(
                &inspected,
                OperationId::from_bytes([104; 16]),
                selections.clone()
            )
            .await
            .is_err()
    );
    let resolved_operation = OperationId::from_bytes([106; 16]);
    let resolved = controller
        .apply_project_merge(&inspected, resolved_operation, selections)
        .await?
        .0;
    assert!(matches!(&resolved, JoinOutcome::Applied(_)));
    let write = ContentGrant::verify(
        &parent_issuer.verifier(&mut None).expect("verifier copy"),
        &parent_scope,
        &root,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let notice_file = host
        .put_content(
            &root,
            &write,
            "notices/resolved-merge.txt",
            b"resolved merge",
            "text/plain",
            "resolved-merge.txt",
            1_024,
            &IdempotencyKey::new("wide-resolved-notice")?,
            None,
            acyclic_harness::conversation::ContentAccess::default(),
        )
        .await?;
    let receipt = controller
        .merge_receipt(
            &inspected,
            &resolved,
            &Authority {
                kind: AggregateKind::Conversation,
                id: "wide-child-2".into(),
            },
            resolved_operation,
            &ConversationMessage {
                id: Uuid::from_bytes([107; 16]),
                sequence: 1,
                kind: MessageKind::Merge,
                content: notice_file,
                attachments: ReferencedAttachments::Inline { items: Vec::new() },
                reply_to: None,
                tool_call_id: None,
                extensions: BTreeMap::new(),
            },
        )?
        .0;
    FilesystemProjectMergeVerifier::new(host.clone(), None)?
        .verify(&receipt)
        .await?;
    let recovered_receipt = controller
        .merge_receipt(
            &replay,
            &recovered,
            &Authority {
                kind: AggregateKind::Conversation,
                id: "wide-child-1".into(),
            },
            OperationId::from_bytes([101; 16]),
            &receipt.notice,
        )?
        .0;
    FilesystemProjectMergeVerifier::new(host.clone(), None)?
        .verify(&recovered_receipt)
        .await?;

    assert_eq!(
        host.read(&root_head.workspace, None, "/shared.txt", 32, None)
            .await?,
        acyclic_stream::OwnedBytes::from(b"sibling 2".to_vec()),
    );
    Ok(())
}
