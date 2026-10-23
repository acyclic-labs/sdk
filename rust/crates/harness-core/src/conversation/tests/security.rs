use super::*;

#[test]
fn owner_bound_private_write_rejects_an_attached_agent_with_a_copied_capability() -> Result<()> {
    let owner = AgentId::from_bytes([1; 16]);
    let attached = AgentId::from_bytes([2; 16]);
    let private = file(owner, "scratch/note.txt")?.volume().clone();
    let write = private.capability(VolumeOperation::Write, &mut None)?;
    let issuer = crate::core::AuthorityIssuer::new(
        "owner",
        [7; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Agent,
            id: owner.to_string(),
        },
    );
    let read = private.capability(VolumeOperation::Read, &mut None)?;
    let scope = issuer.issue(
        "write",
        &crate::Capabilities::new([write.clone(), read.clone()]),
        Some(owner),
        None,
        &mut None,
    )?;
    ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &scope,
        &private,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let unsigned = issuer.issue(
        "copied-capability",
        &crate::Capabilities::new([write.clone()]),
        None,
        None,
        &mut None,
    )?;
    assert!(
        ContentGrant::verify(
            &issuer.verifier(&mut None)?,
            &unsigned,
            &private,
            VolumeOperation::Write,
            u64::MAX,
            &mut None
        )
        .is_err()
    );
    let inherited =
        issuer.attenuate(&scope, "child", &crate::Capabilities::new([write.clone()]))?;
    assert_eq!(inherited.agent(), Some(owner));
    ContentGrant::verify(
        &issuer.verifier(&mut None)?,
        &inherited,
        &private,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    let foreign_issuer = crate::core::AuthorityIssuer::new(
        "attached",
        [8; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Agent,
            id: attached.to_string(),
        },
    );
    let copied = foreign_issuer.issue(
        "copied",
        &crate::Capabilities::new([write]),
        Some(attached),
        None,
        &mut None,
    )?;
    assert!(
        ContentGrant::verify(
            &foreign_issuer.verifier(&mut None)?,
            &copied,
            &private,
            VolumeOperation::Write,
            u64::MAX,
            &mut None
        )
        .is_err()
    );
    let exact_file = file(owner, "scratch/note.txt")?;
    let delegated =
        issuer.delegate_private_file_read(&scope, attached, "attached-read", &exact_file)?;
    assert_eq!(delegated.agent(), Some(attached));
    ContentGrant::verify_file_read(&issuer.verifier(&mut None)?, &delegated, &exact_file)?;
    let internal = file(owner, ".system/execution/state.json")?;
    assert!(matches!(
        ContentGrant::verify_read(&issuer.verifier(&mut None)?, &scope, &internal),
        Err(Error::Unauthorized(_))
    ));
    let internal_exact = issuer.issue(
        "internal-exact",
        &crate::Capabilities::new([internal.read_capability(&mut None)?]),
        Some(owner),
        None,
        &mut None,
    )?;
    assert!(matches!(
        ContentGrant::verify_file_read(&issuer.verifier(&mut None)?, &internal_exact, &internal),
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        issuer.delegate_private_file_read(&scope, attached, "internal", &internal),
        Err(Error::Unauthorized(_))
    ));
    let inherited_context = file(owner, ".system/inherited-conversation/prefix.json")?;
    ContentGrant::verify_read(&issuer.verifier(&mut None)?, &scope, &inherited_context)?;
    let directory = issuer.delegate_private_directory_read(
        &scope,
        attached,
        "attached-directory",
        &private,
        "scratch",
    )?;
    let directory_grant = ContentGrant::verify_directory_read(
        &issuer.verifier(&mut None)?,
        &directory,
        &private,
        "scratch",
        &mut None,
    )?;
    directory_grant.require_directory_path(&private, "scratch/deeper", None)?;
    directory_grant.require_file_read(&exact_file, None)?;
    assert!(
        directory_grant
            .require_directory_path(&private, "scratchpad", None)
            .is_err()
    );
    assert!(
        directory_grant
            .require_directory_path(&private, ".system", None)
            .is_err()
    );
    assert!(
        directory_grant
            .require(&private, VolumeOperation::Write, None)
            .is_err()
    );
    let foreign_owner_scope = foreign_issuer.issue(
        "copied-read",
        &crate::Capabilities::new([read]),
        Some(attached),
        None,
        &mut None,
    )?;
    assert!(
        foreign_issuer
            .delegate_private_file_read(&foreign_owner_scope, attached, "invalid", &exact_file)
            .is_err()
    );
    Ok(())
}

#[test]
fn descriptor_checks_exact_bytes() -> Result<()> {
    let descriptor = FileDescriptor::from_bytes(b"", "application/octet-stream", &mut None)?;
    descriptor.verify(b"", &mut None)?;
    assert!(descriptor.verify(b"x", &mut None).is_err());
    let encoded = serde_json::to_value(&descriptor).map_err(|e| Error::Invalid(e.to_string()))?;
    let decoded: FileDescriptor =
        serde_json::from_value(encoded).map_err(|e| Error::Invalid(e.to_string()))?;
    assert_eq!(decoded, descriptor);
    Ok(())
}

#[test]
fn configured_limits_reject_oversized_references() -> Result<()> {
    Limits::default().validate_file(&file(AgentId::new(), &"x".repeat(4_097))?, &mut None)?;
    let file = file(AgentId::new(), "message.txt")?;
    let mut limits = Limits {
        file_bytes: 4,
        ..Limits::default()
    };
    limits.validate(&mut None)?;
    assert!(limits.validate_file(&file, &mut None).is_err());
    limits.file_bytes = 5;
    limits.validate_file(&file, &mut None)?;
    limits.model_events_per_step = 1;
    limits.validate(&mut None)?;
    limits.attachments = 0;
    assert!(limits.validate(&mut None).is_err());
    Limits {
        attachments: 65_537,
        model_steps: 1_000_001,
        model_events_per_step: 1_000_001,
        tool_calls_per_step: 1_000_001,
        context_messages: 1_000_001,
        ..Limits::default()
    }
    .validate(&mut None)?;
    Ok(())
}

#[test]
fn paths_and_ownership_are_validated_on_decode() -> Result<()> {
    let agent = AgentId::new();
    for path in ["/absolute", "../escape", "a//b", "a/./b", "a\\b"] {
        assert!(file(agent, path).is_err(), "{path}");
    }
    let mut encoded = serde_json::to_value(file(agent, "dir/a.txt")?)
        .map_err(|e| Error::Invalid(e.to_string()))?;
    encoded["path"] = serde_json::json!("../escape");
    assert!(serde_json::from_value::<FileRef>(encoded).is_err());
    Ok(())
}

#[test]
fn conversation_enforces_binding_order_and_call_links() -> Result<()> {
    let agent = AgentId::new();
    let mut state = ConversationState::default();
    state.bind(agent)?;
    assert!(state.bind(agent).is_err());
    let call = ConversationMessage {
        id: Uuid::new_v4(),
        sequence: 1,
        kind: MessageKind::ToolCall,
        content: file(agent, "call.txt")?,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: None,
        tool_call_id: Some("call-1".into()),
        extensions: BTreeMap::new(),
    };
    state.append(call.clone(), &mut None)?;
    assert!(state.append(call.clone(), &mut None).is_err());
    let result = ConversationMessage {
        id: Uuid::new_v4(),
        sequence: 2,
        kind: MessageKind::ToolResult,
        content: file(agent, "result.txt")?,
        attachments: ReferencedAttachments::Inline { items: Vec::new() },
        reply_to: Some(call.id),
        tool_call_id: Some("call-1".into()),
        extensions: BTreeMap::new(),
    };
    state.append(result, &mut None)?;
    assert_eq!(state.messages.len(), 2);
    Ok(())
}

#[test]
fn retained_content_grants_reject_revoked_workload_ancestors() -> Result<()> {
    let owner = AgentId::from_bytes([1; 16]);
    let selected = file(owner, "scratch/note.txt")?;
    let volume = selected.volume();
    let issuer = crate::core::AuthorityIssuer::new(
        "retained",
        [71; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Agent,
            id: owner.to_string(),
        },
    );
    let scope = issuer.issue(
        "owner",
        &crate::Capabilities::new([
            volume.capability(VolumeOperation::Read, &mut None)?,
            volume.capability(VolumeOperation::Write, &mut None)?,
            selected.read_capability(&mut None)?,
            volume.directory_read_capability("scratch", &mut None)?,
        ]),
        Some(owner),
        None,
        &mut None,
    )?;
    let verifier = issuer.verifier(&mut None)?;
    let reads = [
        ContentGrant::verify_read(&verifier, &scope, &selected)?,
        ContentGrant::verify_file_read(&verifier, &scope, &selected)?,
        ContentGrant::verify_directory_read(&verifier, &scope, volume, "scratch", &mut None)?,
    ];
    let write = ContentGrant::verify(
        &verifier,
        &scope,
        volume,
        VolumeOperation::Write,
        u64::MAX,
        &mut None,
    )?;
    for grant in &reads {
        grant.require_file_read(&selected, None)?;
    }
    reads[2].require_directory_path(volume, "scratch", None)?;
    write.require(volume, VolumeOperation::Write, None)?;
    issuer.revoke_workload(&scope)?;
    for grant in &reads {
        assert!(matches!(
            grant.require_file_read(&selected, None),
            Err(Error::Unauthorized(_))
        ));
    }
    assert!(matches!(
        reads[2].require_directory_path(volume, "scratch", None),
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        write.require(volume, VolumeOperation::Write, None),
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}
