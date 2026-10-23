use super::*;
use crate::access::{
    AccessView, Action, Capability, Conditions, Context, Effect, Generations, Grant,
    POLICY_SCHEMA_VERSION, PathTrie, Policy, Principal, PrincipalKind, PrincipalPattern, Resource,
    ResourcePattern, Rule,
};

fn pattern(path: &str) -> ResourcePattern {
    ResourcePattern {
        lineage_root: None,
        volume: None,
        generations: Generations::All,
        path: path.into(),
    }
}

#[test]
fn projfs_rejects_each_restrictive_writable_mode() -> Result<(), Box<dyn std::error::Error>> {
    use crate::native_mount::actions::require_projfs_mode;
    let source = source(FilesystemProfile::Portable)?;
    let now = crate::native_mount::adapter::access::access_time(
        &crate::native_mount::clock::Clock::capture(),
    )?;
    require_projfs_mode(&compiled(&source, 1, vec![], None)?, true, now)?;
    for action in Action::ALL {
        let view = compiled(
            &source,
            2,
            vec![deny("restricted", &[action], "protected/**")],
            None,
        )?;
        assert_eq!(
            require_projfs_mode(&view, true, now).is_err(),
            matches!(
                action,
                Action::Create | Action::Write | Action::Append | Action::ReadHistory
            ),
            "unsupported writable mode for {action:?}",
        );
        assert!(matches!(
            require_projfs_mode(&view, false, now),
            Err(crate::native_mount::MountLifecycleError::NativePolicy(
                "ProjFS cannot prevent host mutations on read-only mounts"
            )),
        ));
    }
    Ok(())
}

pub(in crate::native_mount) fn deny(id: &str, actions: &[Action], path: &str) -> Rule {
    Rule {
        id: id.into(),
        effect: Effect::Deny,
        principal: PrincipalPattern::default(),
        actions: actions.iter().copied().collect(),
        resource: pattern(path),
        conditions: Conditions::default(),
    }
}

pub(in crate::native_mount) fn compiled<
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
>(
    source: &CheckoutMountSource<A, O>,
    revision: u64,
    rules: Vec<Rule>,
    capability: Option<Capability>,
) -> Result<PathTrie, Box<dyn std::error::Error>> {
    let mut layer = vec![Rule {
        id: "fixture".into(),
        effect: Effect::Allow,
        principal: PrincipalPattern::default(),
        actions: Action::ALL.into_iter().collect(),
        resource: pattern("**"),
        conditions: Conditions::default(),
    }];
    layer.extend(rules);
    let policy = Policy {
        schema_version: POLICY_SCHEMA_VERSION,
        version: revision,
        layers: vec![layer],
    };
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let (head, revision) = invoke(&source.runtime, &work, || async {
        let mut checkout = source
            .checkout
            .volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::read_only_pinned(),
                &work,
                work.cancellation(),
            )
            .await?;
        Ok::<_, MountSourceError>((
            checkout.generation_id(),
            checkout
                .access_policy_revision(&work, work.cancellation())
                .await?,
        ))
    })?;
    compile_policy(source, &policy, capability, head, revision).map_err(Into::into)
}

pub(super) fn compile_policy<A, O>(
    source: &CheckoutMountSource<A, O>,
    policy: &Policy,
    capability: Option<Capability>,
    head: crate::GenerationId,
    revision: crate::kernel::MetadataField<crate::ObjectId>,
) -> Result<PathTrie, MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
{
    let caller = Principal {
        kind: PrincipalKind::Service,
        id: "mount-test".into(),
        lineage: vec![],
        groups: Default::default(),
        roles: Default::default(),
        tags: Default::default(),
        attributes: Default::default(),
    };
    let cap = capability.unwrap_or_else(|| Capability {
        layers: vec![vec![Grant {
            actions: Action::ALL.into_iter().collect(),
            resource: pattern("**"),
        }]],
        expires_at: None,
    });
    let volume = uuid::Uuid::from_bytes(source.volume_id()?.into_bytes()).to_string();
    Ok(PathTrie::compile(
        policy,
        &caller,
        Resource {
            config: source.config,
            lineage: &[],
            volume: &volume,
            generation: 1,
            head: 1,
            path: "",
        },
        Context {
            now: crate::native_mount::adapter::access::access_time(
                &crate::native_mount::clock::Clock::capture(),
            )?,
            approvals: 0,
        },
        &cap,
    )
    .map_err(|_| MountSourceError::AccessDenied)?
    .with_policy_revision(head, revision))
}

pub(in crate::native_mount) fn bind_test_access<
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
>(
    source: CheckoutMountSource<A, O>,
    rules: Vec<Rule>,
    capability: Option<Capability>,
) -> Result<CheckoutMountSource<A, O>, Box<dyn std::error::Error>> {
    let view = Arc::new(AccessView::new(compiled(&source, 1, rules, capability)?));
    Ok(source.with_access_view(view)?)
}

pub(in crate::native_mount) fn publish_test_access<
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
>(
    source: &CheckoutMountSource<A, O>,
    rules: Vec<Rule>,
) -> Result<(), Box<dyn std::error::Error>> {
    let view = source
        .access_view
        .as_ref()
        .ok_or("fixture access missing")?;
    view.publish(compiled(
        source,
        view.current()?.policy_version() + 1,
        rules,
        None,
    )?)?;
    Ok(())
}

pub(super) fn seed(
    source: &MemorySource,
    name: &str,
    bytes: &'static [u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path(name);
    let created = source.create_file(&path, metadata(), None, &work)?;
    source
        .open_created(&path, &created)?
        .write_range(0, Bytes::from_static(bytes))?;
    Ok(())
}

#[test]
fn native_access_filters_hidden_pages_and_metadata() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "a-hidden", b"secret")?;
    seed(&source, "b-public", b"public")?;
    let source = bind_test_access(
        source,
        vec![deny("hidden", &[Action::List, Action::Read], "a-hidden")],
        None,
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    assert!(source.lookup(&native_test_path("a-hidden"))?.is_none());
    assert!(matches!(
        source.read_range(&native_test_path("a-hidden"), 0, 6),
        Err(MountSourceError::AccessDenied)
    ));
    let first = source.read_directory_with_meter(&MountPath::root(), None, 1, &work)?;
    assert!(first.entries.is_empty());
    let cursor = first
        .next_cursor
        .as_deref()
        .ok_or("hidden page lost its continuation")?;
    let next = source.read_directory_with_meter(&MountPath::root(), Some(cursor), 1, &work)?;
    assert_eq!(next.entries.len(), 1);
    assert_eq!(
        next.entries[0].name,
        native_test_path("b-public").components()[0]
    );
    Ok(())
}

#[test]
fn native_access_read_only_paths_reject_each_content_mutation()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "readonly", b"original")?;
    let source = bind_test_access(
        source,
        vec![deny(
            "readonly",
            &[
                Action::Create,
                Action::Write,
                Action::Append,
                Action::Delete,
                Action::Rename,
            ],
            "readonly",
        )],
        None,
    )?;
    let path = native_test_path("readonly");
    let file = source.open_file(&path)?;
    assert_eq!(file.read_range(0, 8)?.as_ref(), b"original");
    for result in [
        source.write_range(&path, 0, Bytes::from_static(b"bad")),
        file.write_range(0, Bytes::from_static(b"bad")),
        source.resize(&path, 1),
        file.resize(1),
    ] {
        assert!(matches!(result, Err(MountSourceError::ReadOnly)));
    }
    assert_eq!(file.read_range(0, 8)?.as_ref(), b"original");
    Ok(())
}

#[test]
fn native_access_inbox_creation_does_not_authorize_reopen_or_read()
-> Result<(), Box<dyn std::error::Error>> {
    let source = bind_test_access(
        source(FilesystemProfile::Portable)?,
        vec![deny(
            "inbox",
            &[
                Action::List,
                Action::Read,
                Action::Write,
                Action::Append,
                Action::Delete,
                Action::Rename,
            ],
            "message",
        )],
        None,
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("message");
    let created = source.create_file(&path, metadata(), None, &work)?;
    let file = source.open_created(&path, &created)?;
    file.write_range(0, Bytes::from_static(b"delivery"))?;
    assert!(matches!(
        file.read_range(0, 8),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(file.lookup(), Err(MountSourceError::AccessDenied)));
    assert!(matches!(
        source.open_file(&path),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(source.lookup(&path)?.is_none());
    assert!(matches!(
        source.create_file(&path, metadata(), None, &work),
        Err(MountSourceError::AlreadyExists)
    ));
    Ok(())
}

#[test]
fn native_access_append_is_atomic_and_cannot_overwrite() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "log", b"old")?;
    let source = bind_test_access(
        source,
        vec![deny("append-only", &[Action::Write], "log")],
        None,
    )?;
    let path = native_test_path("log");
    let first = source.open_file(&path)?;
    let second = source.open_file(&path)?;
    assert!(matches!(
        first.write_range(0, Bytes::from_static(b"bad")),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(first.resize(0).is_err());
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let tasks = [first, second].map(|file| {
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            file.write_range(3, Bytes::from_static(b"!"))
        })
    });
    barrier.wait();
    let mut successes = 0;
    for task in tasks {
        successes += usize::from(task.join().map_err(|_| "append thread panicked")?.is_ok());
    }
    assert_eq!(successes, 1);
    assert_eq!(source.read_range(&path, 0, 4)?.as_ref(), b"old!");
    Ok(())
}

#[test]
fn native_access_revokes_existing_and_new_handles() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "file", b"pinned")?;
    let old = source.open_file(&native_test_path("file"))?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    invoke(&source.runtime, &work, || async {
        let mut editor = source
            .checkout
            .volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                &work,
                work.cancellation(),
            )
            .await?;
        editor
            .write_access_policy(
                br#"{"schema_version":1,"version":2,"layers":[]}"#,
                crate::NamedAttributeWriteMode::Create,
                &work,
                work.cancellation(),
            )
            .await?;
        editor
            .commit(
                OperationId::new(),
                None,
                None,
                None,
                crate::PublicationPermit::Unrestricted,
                BOUNDARY_BUDGET,
                work.cancellation(),
                |_| async {
                    Ok::<_, crate::OperationFailure<FsError>>(crate::OperationReceipt {
                        value: (),
                        work: crate::WorkCounters::UNCHARGED,
                    })
                },
            )
            .await
            .map_err(|failure| MountSourceError::from(failure.error))?;
        Ok::<_, MountSourceError>(())
    })?;
    for result in [
        old.read_range(0, 6).map(|_| ()),
        old.read_range_with_meter(0, 6, &work).map(|_| ()),
        old.lookup().map(|_| ()),
        old.seek(0, MountSeekTarget::Data).map(|_| ()),
        old.write_range(0, Bytes::from_static(b"denied")),
        old.append_range(6, Bytes::from_static(b"denied")),
        old.resize_with_meter(0, &work),
        old.clone_source(&source.checkout, &work).map(|_| ()),
        old.clone_range_from(old.as_ref(), 0, 0, 6, &work),
    ] {
        assert!(matches!(result, Err(MountSourceError::AccessDenied)));
    }
    assert!(matches!(
        source.open_file(&native_test_path("file")),
        Err(MountSourceError::AccessDenied)
    ));
    Ok(())
}

#[test]
fn native_access_rename_checks_source_and_destination() -> Result<(), Box<dyn std::error::Error>> {
    for (from, to) in [("protected", "allowed-new"), ("allowed", "protected-new")] {
        let source = source(FilesystemProfile::Portable)?;
        seed(&source, "protected", b"protected")?;
        seed(&source, "allowed", b"allowed")?;
        let source = bind_test_access(
            source,
            vec![deny("protected", &[Action::Rename], "protected*")],
            None,
        )?;
        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
        assert!(
            source
                .rename(&native_test_path(from), &native_test_path(to), false, &work)
                .is_err()
        );
        assert!(source.lookup(&native_test_path(from))?.is_some());
        assert!(source.lookup(&native_test_path(to))?.is_none());
    }
    Ok(())
}

#[test]
fn native_access_pending_nested_mutations_cannot_publish_across_revocation()
-> Result<(), Box<dyn std::error::Error>> {
    for action in [
        Action::Create,
        Action::Write,
        Action::Append,
        Action::Rename,
        Action::Delete,
        Action::Admin,
    ] {
        let source = sources(
            FilesystemProfile::Portable,
            ConsistencyMode::TrackingSafe,
            MountPublication::Manual,
        )?
        .0;
        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
        source.create_directory(&native_test_path("nested"), metadata(), &work)?;
        seed(&source, "nested/from", b"retained")?;
        source.sync()?;
        match action {
            Action::Create => {
                source.create_file(&native_test_path("nested/new"), metadata(), None, &work)?;
            }
            Action::Write => source
                .open_file(&native_test_path("nested/from"))?
                .write_range(0, Bytes::from_static(b"changed"))?,
            Action::Append => source
                .open_file(&native_test_path("nested/from"))?
                .append_range(8, Bytes::from_static(b"!"))?,
            Action::Rename => source.rename(
                &native_test_path("nested/from"),
                &native_test_path("nested/to"),
                false,
                &work,
            )?,
            Action::Delete => source.remove(&native_test_path("nested/from"), None, &work)?,
            Action::Admin => source.hard_link(
                &native_test_path("nested/from"),
                &native_test_path("nested/link"),
                &work,
            )?,
            _ => unreachable!(),
        }
        let policy = crate::access::fixtures::denied(action, "**");
        super::policy_refresh::publish(&source, &policy)?;
        assert!(
            source.sync().is_err(),
            "native mutation published across policy revocation: {action:?}"
        );
    }
    Ok(())
}

#[test]
fn native_path_reads_and_enumeration_bind_their_actual_policy_snapshot()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "file", b"pinned")?;
    let path = native_test_path("file");
    let old = source.open_file(&path)?;
    let mut policy: Policy = serde_json::from_slice(crate::access::INITIAL_POLICY)?;
    policy.version += 1;
    super::policy_refresh::publish(&source, &policy)?;
    publish_test_access(&source, vec![])?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    // The pinned working checkout still has its original metadata. A new view
    // cannot authorize it, even when the new rules happen to allow the path.
    assert!(matches!(
        source.read_range(&path, 0, 6),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(
        source.lookup(&path),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(
        source.open_file(&path),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(
        source.directory_page(&MountPath::root(), None, 8, &work),
        Err(MountSourceError::AccessDenied)
    ));
    for result in [
        old.read_range(0, 6).map(|_| ()),
        old.read_range_with_meter(0, 6, &work).map(|_| ()),
        old.lookup().map(|_| ()),
        old.seek(0, MountSeekTarget::Data).map(|_| ()),
        old.write_range(0, Bytes::from_static(b"denied")),
        old.append_range(6, Bytes::from_static(b"denied")),
        old.resize_with_meter(0, &work),
        old.clone_source(&source.checkout, &work).map(|_| ()),
        old.clone_range_from(old.as_ref(), 0, 0, 6, &work),
    ] {
        assert!(matches!(result, Err(MountSourceError::AccessDenied)));
    }
    Ok(())
}

#[test]
fn native_access_read_capability_does_not_disclose_metadata()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "file", b"content")?;
    let source = bind_test_access(
        source,
        vec![],
        Some(Capability {
            layers: vec![vec![Grant {
                actions: [Action::Read].into_iter().collect(),
                resource: pattern("file"),
            }]],
            expires_at: None,
        }),
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let path = native_test_path("file");
    assert!(source.lookup(&path)?.is_none());
    assert!(matches!(
        source.open_file_with_lookup_with_meter(&path, MountOpenAccess::Metadata, &work),
        Err(MountSourceError::AccessDenied)
    ));
    let file = source.open_file(&path)?;
    assert_eq!(file.read_range(0, 7)?.as_ref(), b"content");
    assert!(matches!(file.lookup(), Err(MountSourceError::AccessDenied)));
    Ok(())
}

#[test]
fn native_access_full_fold_aliases_share_denials() -> Result<(), Box<dyn std::error::Error>> {
    let config = VolumeConfig {
        case_sensitivity: CaseSensitivity::ProfileFolded,
        ..VolumeConfig::portable(Lifecycle::Ephemeral)
    };
    let source = configured_sources(config, ConsistencyMode::Pinned, MountPublication::Manual)?.0;
    seed(&source, "ΟΣΑ", b"secret")?;
    let source = bind_test_access(
        source,
        vec![deny("sigma", &[Action::List, Action::Read], "ΟΣ*")],
        None,
    )?;
    for spelling in ["ΟΣΑ", "οσα", "οΣα"] {
        let path = native_test_path(spelling);
        assert!(source.lookup(&path)?.is_none());
        assert!(matches!(
            source.read_range(&path, 0, 6),
            Err(MountSourceError::AccessDenied)
        ));
    }
    Ok(())
}

#[test]
fn native_access_rejects_cross_volume_and_configuration_bindings()
-> Result<(), Box<dyn std::error::Error>> {
    let first = source(FilesystemProfile::Portable)?;
    let second = source(FilesystemProfile::Portable)?;
    let view = Arc::clone(first.access_view.as_ref().ok_or("missing fixture view")?);
    assert!(second.with_access_view(view).is_err());
    let mut wrong = first.config;
    wrong.case_sensitivity = CaseSensitivity::ProfileFolded;
    assert!(CheckoutMountSource::new(Arc::clone(&first.checkout), wrong).is_err());
    Ok(())
}

#[test]
fn native_access_clone_uses_both_handles_without_metadata_disclosure()
-> Result<(), Box<dyn std::error::Error>> {
    for (denied, allowed) in [
        (vec![deny("no-list", &[Action::List], "from")], true),
        (vec![deny("no-read", &[Action::Read], "from")], false),
        (vec![deny("no-write", &[Action::Write], "to")], false),
    ] {
        let source = source(FilesystemProfile::Portable)?;
        seed(&source, "from", b"contents")?;
        seed(&source, "to", b"original")?;
        let source = bind_test_access(source, denied, None)?;
        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
        let from = source.open_file(&native_test_path("from"))?;
        let to = source.open_file(&native_test_path("to"))?;
        assert_eq!(
            to.clone_range_from(from.as_ref(), 0, 0, 8, &work).is_ok(),
            allowed
        );
        assert_eq!(
            to.read_range(0, 8)?.as_ref(),
            if allowed { b"contents" } else { b"original" }
        );
    }
    let first = source(FilesystemProfile::Portable)?;
    let second = source(FilesystemProfile::Portable)?;
    seed(&first, "from", b"contents")?;
    seed(&second, "to", b"original")?;
    let work = capture_meter(BOUNDARY_BUDGET, second.cancellation())?;
    assert!(matches!(
        second.open_file(&native_test_path("to"))?.clone_range_from(
            first.open_file(&native_test_path("from"))?.as_ref(),
            0,
            0,
            8,
            &work,
        ),
        Err(MountSourceError::AccessDenied)
    ));
    assert_eq!(
        second.read_range(&native_test_path("to"), 0, 8)?.as_ref(),
        b"original"
    );
    Ok(())
}

#[test]
fn native_access_unbound_source_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
    let bound = source(FilesystemProfile::Portable)?;
    let unbound = CheckoutMountSource::new(Arc::clone(&bound.checkout), bound.config)?;
    let work = capture_meter(BOUNDARY_BUDGET, bound.cancellation())?;
    let path = native_test_path("unbound");
    assert!(matches!(
        unbound.lookup(&path),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(
        unbound.read_directory(&MountPath::root(), None, 1),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(
        unbound.create_file(&path, metadata(), None, &work),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(bound.lookup(&path)?.is_none());
    Ok(())
}

#[test]
fn native_access_list_only_never_reads_content_or_attribute_values()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "file", b"contents")?;
    let source = bind_test_access(
        source,
        vec![],
        Some(Capability {
            layers: vec![vec![Grant {
                actions: [Action::List].into_iter().collect(),
                resource: pattern("**"),
            }]],
            expires_at: None,
        }),
    )?;
    let path = native_test_path("file");
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    assert!(source.lookup(&path)?.is_some());
    assert_eq!(
        source
            .read_directory(&MountPath::root(), None, 8)?
            .entries
            .len(),
        1
    );
    let file = source.open_file(&path)?;
    assert!(file.lookup()?.node.logical_bytes > 0);
    assert!(matches!(
        file.read_range(0, 8),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(
        source.read_attribute(&path, b"user.secret", &work),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(
        file.read_attribute(b"user.secret", &work),
        Err(MountSourceError::AccessDenied)
    ));
    Ok(())
}

#[test]
fn native_access_open_checks_requested_content_rights() -> Result<(), Box<dyn std::error::Error>> {
    for (denied, access, allowed) in [
        (vec![Action::Read], MountOpenAccess::Read, false),
        (vec![Action::Read], MountOpenAccess::Write, true),
        (
            vec![Action::Write, Action::Append],
            MountOpenAccess::Write,
            false,
        ),
        (vec![Action::Write], MountOpenAccess::Write, true),
        (vec![Action::Read], MountOpenAccess::ReadWrite, false),
        (
            vec![Action::Read, Action::Write, Action::Append],
            MountOpenAccess::Metadata,
            true,
        ),
    ] {
        let source = source(FilesystemProfile::Portable)?;
        seed(&source, "file", b"data")?;
        let source = bind_test_access(source, vec![deny("restricted", &denied, "file")], None)?;
        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
        assert_eq!(
            source
                .open_file_with_lookup_with_meter(&native_test_path("file"), access, &work)
                .is_ok(),
            allowed,
            "{access:?} {denied:?}"
        );
    }
    Ok(())
}

#[test]
fn native_access_retarget_preserves_grants_and_checks_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "file", b"original")?;
    seed(&source, "other", b"other")?;
    publish_test_access(
        &source,
        vec![deny("read-only", &[Action::Write, Action::Append], "file")],
    )?;
    let path = native_test_path("file");
    let old = source.open_file(&path)?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    publish_test_access(&source, vec![])?;
    let detached = source.detach_file(&path)?;
    let rebound = retarget_file(old.as_ref(), detached.as_ref(), &work)?.0;
    assert!(
        rebound
            .write_range(0, Bytes::from_static(b"denied"))
            .is_err()
    );
    assert!(matches!(
        rebound.read_range(0, 8),
        Err(MountSourceError::AccessDenied)
    ));
    publish_test_access(&source, vec![deny("no-read", &[Action::Read], "file")])?;
    let detached = source.detach_file(&path)?;
    assert!(detached.read_range(0, 8).is_err());
    let rebound = retarget_file(old.as_ref(), detached.as_ref(), &work)?.0;
    assert!(matches!(
        rebound.read_range(0, 8),
        Err(MountSourceError::AccessDenied)
    ));
    let other = source.detach_file(&native_test_path("other"))?;
    assert!(matches!(
        retarget_file(old.as_ref(), other.as_ref(), &work),
        Err(MountSourceError::Stale)
    ));
    Ok(())
}

fn denial_without_probe(action: Action, path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let source = bind_test_access(
        source(FilesystemProfile::Portable)?,
        vec![deny("ungranted", &[action], path)],
        None,
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let before = work.receipt().backend_read_operations;
    assert!(
        source
            .authorize_with_meter(&[(action, &native_test_path(path))], &work)
            .is_err()
    );
    assert_eq!(work.receipt().backend_read_operations, before);
    Ok(())
}

#[test]
fn native_access_denials_precede_backend_and_host_probes() -> Result<(), Box<dyn std::error::Error>>
{
    for action in Action::ALL {
        denial_without_probe(action, "hidden/file")?;
    }
    Ok(())
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    // A published denial never probes storage or the native filesystem, for
    // every action and arbitrary bounded component spelling.
    #[test]
    fn native_access_denials_do_no_backend_io(
        action in 0..Action::ALL.len(), path in "[a-z]{1,20}(/[a-z]{1,20})?",
    ) {
        denial_without_probe(Action::ALL[action], &path)
            .map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()))?;
    }
}

#[test]
fn projection_admission_holds_policy_publication_until_retirement()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    assert!(source.projection_policy_lease(false, &work).is_err());
    let lease = source.projection_policy_lease(true, &work)?;
    let mut denied = crate::access::fixtures::denied(Action::Write, "**");
    denied.layers[0][1].actions = [Action::Write, Action::Append, Action::Create]
        .into_iter()
        .collect();
    assert!(super::policy_refresh::publish(&source, &denied).is_err());
    assert!(
        source
            .access_view
            .as_ref()
            .unwrap()
            .current()?
            .decide(
                Action::Write,
                "file",
                crate::native_mount::adapter::access::access_time(
                    &crate::native_mount::clock::Clock::capture()
                )?
            )
            .allowed
    );
    drop(lease);
    super::policy_refresh::publish(&source, &denied)?;
    assert!(source.projection_policy_lease(true, &work).is_err());
    Ok(())
}

#[test]
fn private_policy_staging_does_not_hold_a_publication_lease()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    let editor = invoke(&source.runtime, &work, || async {
        let mut editor = source
            .checkout
            .volume
            .checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
                &work,
                work.cancellation(),
            )
            .await?;
        editor
            .write_access_policy(
                crate::access::INITIAL_POLICY,
                NamedAttributeWriteMode::Create,
                &work,
                work.cancellation(),
            )
            .await?;
        Ok::<_, MountSourceError>(editor)
    })?;
    assert!(source.projection_policy_lease(true, &work).is_ok());
    drop(editor);
    assert!(source.projection_policy_lease(true, &work).is_ok());
    Ok(())
}

#[test]
fn native_relocations_cannot_disclose_denied_files_or_descendants()
-> Result<(), Box<dyn std::error::Error>> {
    for directory in [false, true] {
        for action in [Action::Read, Action::Write, Action::List] {
            let source = source(FilesystemProfile::Portable)?;
            let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
            let protected = if directory {
                source.create_directory(&native_test_path("private"), metadata(), &work)?;
                "private/key"
            } else {
                "private"
            };
            seed(&source, protected, b"secret")?;
            let source =
                bind_test_access(source, vec![deny("private", &[action], protected)], None)?;
            assert!(matches!(
                source.rename(
                    &native_test_path("private"),
                    &native_test_path("public"),
                    false,
                    &work
                ),
                Err(MountSourceError::AccessDenied)
            ));
            assert!(source.lookup(&native_test_path("public"))?.is_none());
        }
    }
    Ok(())
}

#[test]
fn native_hard_links_cannot_widen_source_permissions() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "private", b"secret")?;
    let source = bind_test_access(
        source,
        vec![deny("private", &[Action::Read], "private")],
        None,
    )?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    assert!(matches!(
        source.hard_link(
            &native_test_path("private"),
            &native_test_path("public"),
            &work
        ),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(source.lookup(&native_test_path("public"))?.is_none());
    Ok(())
}

#[test]
fn native_relocations_preserve_uniform_and_equivalent_suffix_permissions()
-> Result<(), Box<dyn std::error::Error>> {
    for restricted in [false, true] {
        let source = source(FilesystemProfile::Portable)?;
        let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
        source.create_directory(&native_test_path("from"), metadata(), &work)?;
        seed(&source, "from/key", b"retained")?;
        let rules = if restricted {
            vec![deny("elsewhere", &[Action::Read], "unrelated/**")]
        } else {
            vec![]
        };
        let source = bind_test_access(source, rules, None)?;
        source.rename(
            &native_test_path("from"),
            &native_test_path("to"),
            false,
            &work,
        )?;
        assert_eq!(
            source
                .read_range(&native_test_path("to/key"), 0, 8)?
                .as_ref(),
            b"retained"
        );
        source.hard_link(
            &native_test_path("to/key"),
            &native_test_path("alias"),
            &work,
        )?;
        assert_eq!(
            source
                .read_range(&native_test_path("alias"), 0, 8)?
                .as_ref(),
            b"retained"
        );
    }
    Ok(())
}

#[test]
fn native_relocations_intersect_capability_path_limits() -> Result<(), Box<dyn std::error::Error>> {
    let source = source(FilesystemProfile::Portable)?;
    seed(&source, "private", b"secret")?;
    let cap = Capability {
        layers: vec![vec![
            Grant {
                actions: Action::ALL
                    .into_iter()
                    .filter(|action| *action != Action::Read)
                    .collect(),
                resource: pattern("private"),
            },
            Grant {
                actions: Action::ALL.into_iter().collect(),
                resource: pattern("public"),
            },
        ]],
        expires_at: None,
    };
    let source = bind_test_access(source, vec![], Some(cap))?;
    let work = capture_meter(BOUNDARY_BUDGET, source.cancellation())?;
    assert!(matches!(
        source.rename(
            &native_test_path("private"),
            &native_test_path("public"),
            false,
            &work
        ),
        Err(MountSourceError::AccessDenied)
    ));
    assert!(matches!(
        source.hard_link(
            &native_test_path("private"),
            &native_test_path("public"),
            &work
        ),
        Err(MountSourceError::AccessDenied)
    ));
    Ok(())
}
