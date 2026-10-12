use super::*;

#[test]
pub(super) fn filtered_callback_error_replies_without_overwriting_parent_errno()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::native_mount::fuse::Errno;
    #[derive(Clone, Copy)]
    struct Reply<'a>(&'a std::cell::Cell<bool>);
    impl Reply<'_> {
        fn error(self, error: Errno) {
            assert_eq!(error, Errno::from_i32(libc::EIO));
            self.0.set(true);
        }
    }
    let (source, _) = shared_sources()?;
    let mut policy = FINITE_HANDLER_POLICY;
    policy.bytes_copied = 0;
    let core = crate::native_mount::fuse::ProjectionCore::new(
        Arc::new(source),
        empty_projection_state(),
        policy,
        &crate::CancellationToken::new(),
    )?;
    let replied = std::cell::Cell::new(false);
    let reply = Reply(&replied);
    crate::obs::assert_filtered_callback_isolation(|span| {
        respond!(
            core,
            reply,
            span,
            work,
            crate::native_mount::fuse::fuse_copy_bytes(b"refused before copy", work).map(|_| ()),
            |()| drop(reply)
        );
        libc::EIO
    });
    assert!(replied.get());
    Ok(())
}
#[test]
pub(super) fn administrative_helper_retains_actual_tree_and_original_payload_on_denial()
-> Result<(), Box<dyn std::error::Error>> {
    let cancellation = crate::CancellationToken::new();
    let creator =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
    let mut control = crate::native_mount::fuse::FuseRecoveryControl::default();
    acyclic_native_runtime::ProcessTree::spawn_closed_measured(
        c"/bin/sleep",
        &[c"60"],
        creator.runtime_meter().as_ref(),
        &mut control.helper,
    )
    .map_err(|error| creator.runtime_error(error))?;
    control.helper_work = Some(creator.clone());
    control.deadline = Some(std::time::Instant::now() + Duration::from_secs(30));
    let creator_receipt = creator.receipt();
    let denied = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_write_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    assert!(matches!(
        control.retire_controls(&denied),
        Err(crate::NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert!(control.controls_pending());
    assert!(
        !control
            .helper
            .as_ref()
            .ok_or("actual tree lost")?
            .is_reaped()
    );
    assert!(
        control
            .helper_work
            .as_ref()
            .ok_or("payload owner lost")?
            .shares_ledger(&creator)
    );
    assert!(!control.namespace_detach_confirmed());
    assert_eq!(denied.receipt().backend_write_operations, 0);

    let cancelled_token = crate::CancellationToken::new();
    let cancelled =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancelled_token)
            .map_err(|(cause, _)| cause)?;
    let cancelled_receipt = cancelled.receipt();
    cancelled_token.cancel();
    assert!(control.retire_controls(&cancelled).is_err());
    assert_eq!(cancelled.receipt(), cancelled_receipt);
    assert!(control.controls_pending());

    let original_admin = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 4096,
            backend_write_operations: 64,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    control.retire_controls(&original_admin)?;
    assert!(!control.controls_pending());
    assert!(control.helper_work.is_none());
    assert!(control.deadline.is_none());
    assert!(
        !control.namespace_detach_confirmed(),
        "process reap is not namespace evidence"
    );
    assert_eq!(
        creator.receipt(),
        creator_receipt,
        "retirement uses the supplied original administrative work"
    );
    assert!(original_admin.receipt().backend_write_operations > 0);
    assert!(original_admin.receipt().backend_read_operations > 0);
    Ok(())
}

#[test]
pub(super) fn administrative_recovery_retry_uses_its_own_finite_action_budget()
-> Result<(), Box<dyn std::error::Error>> {
    let temporary = tempfile::tempdir()?;
    let cancellation = crate::CancellationToken::new();
    let creator =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, _)| cause)?;
    let mut control = crate::native_mount::fuse::FuseRecoveryControl::default();
    acyclic_native_runtime::ProcessTree::spawn_closed_measured(
        c"/bin/sleep",
        &[c"60"],
        creator.runtime_meter().as_ref(),
        &mut control.helper,
    )
    .map_err(|error| creator.runtime_error(error))?;
    control.helper_work = Some(creator.clone());
    let denied = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    assert!(matches!(
        control.recover(temporary.path(), &denied),
        Err(crate::NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert!(control.controls_pending());
    assert!(!control.namespace_detach_confirmed());
    assert!(
        control
            .helper_work
            .as_ref()
            .ok_or("payload owner lost")?
            .shares_ledger(&creator)
    );
    let retry = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 4096,
            backend_write_operations: 64,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    let creator_receipt = creator.receipt();
    control.recover(temporary.path(), &retry)?;
    assert!(!control.controls_pending());
    assert!(
        control.namespace_detach_confirmed(),
        "only the real directory probe establishes absence"
    );
    assert!(control.helper_work.is_none());
    assert_eq!(creator.receipt(), creator_receipt);
    assert!(retry.receipt().backend_read_operations >= 2);
    assert!(retry.receipt().backend_write_operations > 0);
    Ok(())
}
pub(super) fn regular(file_id: FileId, logical_bytes: u64) -> MountLookup {
    MountLookup {
        node: MountNode {
            file_id,
            kind: MountNodeKind::Regular,
            logical_bytes,
            link_count: 1,
            device: None,
        },
        metadata: FileMetadata::default(),
    }
}

pub(super) fn directory(file_id: FileId) -> MountLookup {
    let mut lookup = regular(file_id, 0);
    lookup.node.kind = MountNodeKind::Directory;
    lookup
}

pub(super) fn name(value: &str) -> MountPath {
    MountPath::root().child(value.as_bytes().to_vec())
}

pub(super) fn empty_projection_state() -> crate::native_mount::fuse::ProjectionState {
    crate::native_mount::fuse::ProjectionState::new(crate::native_mount::fuse::AttributeDefaults {
        writable: true,
        mount_uid: 0,
        mount_gid: 0,
    })
}

#[test]
pub(super) fn kernel_forget_retires_only_an_inode_with_no_open_handle() -> Result<(), i32> {
    let mut state = empty_projection_state();
    let mut linked = regular(FileId::new(), 1);
    linked.node.link_count = 2;
    let intern = |state: &mut crate::native_mount::fuse::ProjectionState, path| {
        intern_projected(
            &mut state.next_inode,
            &mut state.by_inode,
            &mut state.inode_by_path,
            &mut state.inode_by_file,
            crate::native_mount::fuse::FuseOwned::empty(path),
            &linked,
            None,
            true,
            &fuse_fixture_work(),
        )
        .map_err(fuse_fixture_errno)
    };
    let forgotten = intern(&mut state, name("first"))?;
    state
        .release_lookup_reference(forgotten, 1, &fuse_fixture_work())
        .unwrap();
    assert!(!state.by_inode.contains_key(&forgotten));
    let current = intern(&mut state, name("alias"))?;
    assert_ne!(
        current, forgotten,
        "separate unheld lookups can get new inodes"
    );

    state
        .by_inode
        .get_mut(&current)
        .ok_or(libc::ESTALE)?
        .open_handles = 1;
    state
        .release_lookup_reference(current, 1, &fuse_fixture_work())
        .unwrap();
    assert_eq!(
        intern(&mut state, name("first"))?,
        current,
        "an open inode remains the same for its other hardlink name"
    );
    Ok(())
}

pub(super) fn positions<const N: usize>() -> [ViewStamp; N] {
    let slot = AtomicU64::new(0);
    std::array::from_fn(|_| ViewStamp::record(&slot))
}

pub(super) const FINITE_CACHE_BUDGET: crate::WorkBudget = crate::WorkBudget {
    bytes_copied: 1_000_000,
    source_path_components: 10_000,
    items_examined: 10_000,
    allocation_operations: 10_000,
    peak_allocation_bytes: 1_000_000,
    ..crate::performance::work_budget(0, 0)
};

pub(super) fn cache_work(
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> crate::NativeMaterializationWork {
    crate::NativeMaterializationWork::from_meter(Arc::new(crate::demand::NativeDemandReadMeter(
        Mutex::new(crate::demand::DemandWork::new(budget, cancellation)),
    )))
}

#[test]
pub(super) fn original_invalidation_tail_survives_refusal_without_copying_or_overtaking() {
    let [held, through] = positions();
    let work = cache_work(FINITE_CACHE_BUDGET, &crate::CancellationToken::new());
    let mut deferred =
        crate::native_mount::fuse::fuse_vec(2, &work).expect("original queued capacity");
    deferred.extend([KernelCacheItem::Inode(10), KernelCacheItem::Inode(11)]);
    let mut queue = crate::native_mount::fuse::InvalidationQueue {
        notified: Some(through),
        scanned: Some(through),
        processed: Some(held),
        deferred,
        ..Default::default()
    };
    let copied = work.native_read_meter().lock().work.bytes_copied;
    let mut in_flight = queue.take_deferred();
    assert_eq!(in_flight.next(), Some(KernelCacheItem::Inode(10)));
    let mut denied_budget = FINITE_CACHE_BUDGET;
    denied_budget.bytes_copied = 0;
    let denied = cache_work(denied_budget, &crate::CancellationToken::new());
    let failure = crate::native_mount::fuse::fuse_clone_path(&name("refused"), &denied)
        .expect_err("real copy refusal");
    queue.retain_pending_tail(in_flight);
    queue.finish(
        Some(through),
        Err((
            crate::NativeMountError::from(failure),
            denied.native_read_meter().lock().work,
        )),
    );
    assert_eq!(queue.processed, Some(held));
    assert!(queue.is_pending(Some(through)));
    queue.deferred = crate::native_mount::fuse::fuse_vec(1, &work).expect("later callback queue");
    queue.deferred.push(KernelCacheItem::Inode(12));
    let mut tail = queue.take_deferred();
    assert_eq!(tail.next(), Some(KernelCacheItem::Inode(11)));
    assert_eq!(tail.next(), None);
    assert_eq!(queue.deferred.as_slice(), &[KernelCacheItem::Inode(12)]);
    assert_eq!(work.native_read_meter().lock().work.bytes_copied, copied);
}

#[test]
pub(super) fn negative_entries_are_cacheable_only_while_tracked() {
    let [older, newer] = positions();
    let mut entry = InodeEntry::new(
        crate::native_mount::fuse::FuseOwned::empty(MountPath::root()),
        directory(FileId::new()),
        None,
        1,
        &fuse_fixture_work(),
    )
    .unwrap();
    let work = fuse_fixture_work();
    for index in 0..MAXIMUM_NEGATIVE_ENTRIES_PER_DIRECTORY {
        assert!(
            entry
                .remember_negative_child(format!("absent-{index}").as_bytes(), newer, &work)
                .expect("tracked name")
        );
    }
    assert!(
        entry
            .remember_negative_child(b"absent-0", older, &work)
            .expect("tracked name")
    );
    assert!(
        !entry
            .remember_negative_child(b"untracked", newer, &work)
            .expect("bounded name table")
    );
    // A tracked entry keeps the oldest position the kernel may hold.
    assert_eq!(entry.negative_children[b"absent-0".as_slice()], older);
    assert!(
        entry
            .remember_negative_child(b"absent-0", newer, &work)
            .expect("tracked name")
    );
    assert_eq!(entry.negative_children[b"absent-0".as_slice()], older);
}

#[test]
pub(super) fn each_name_keeps_its_own_verification() -> Result<(), i32> {
    let [first, second, third] = positions();
    let mut linked = regular(FileId::new(), 3);
    linked.node.link_count = 2;
    let mut next_inode = ROOT_INODE + 1;
    let mut by_inode = HashMap::new();
    let mut by_path = HashMap::new();
    let mut by_file = HashMap::new();
    let mut intern = |by_inode: &mut HashMap<u64, InodeEntry>, path: &str, stamp| {
        intern_projected(
            &mut next_inode,
            by_inode,
            &mut by_path,
            &mut by_file,
            crate::native_mount::fuse::FuseOwned::empty(name(path)),
            &linked,
            stamp,
            true,
            &fuse_fixture_work(),
        )
        .map_err(fuse_fixture_errno)
    };
    let inode = intern(&mut by_inode, "alias", Some(first))?;
    assert_eq!(intern(&mut by_inode, "original", Some(second))?, inode);
    // Refreshing one name re-verifies only that name.
    assert_eq!(intern(&mut by_inode, "original", Some(third))?, inode);
    let entry = &by_inode[&inode];
    assert_eq!(
        entry
            .binding(&name("alias"), &fuse_fixture_work())
            .unwrap()
            .and_then(|b| b.verified),
        Some(first)
    );
    assert_eq!(
        entry
            .binding(&name("original"), &fuse_fixture_work())
            .unwrap()
            .and_then(|b| b.verified),
        Some(third)
    );
    assert_eq!(entry.facts, Some(third));

    // Reuse through a name is bounded by that name's own verification.
    let mut sampled = Vec::new();
    for path in ["alias", "original"] {
        cached_projected_lookup(
            &mut by_inode,
            &by_path,
            &name(path),
            |_, stamp| {
                Ok({
                    sampled.push(stamp);
                    false
                })
            },
            &fuse_fixture_work(),
        )
        .unwrap();
    }
    assert_eq!(sampled, [first, third]);
    let entry = by_inode.get_mut(&inode).ok_or(libc::ESTALE)?;
    if let Some(binding) = entry
        .binding_mut(&name("alias"), &fuse_fixture_work())
        .unwrap()
    {
        binding.verified = None;
    }
    assert!(
        cached_projected_lookup(
            &mut by_inode,
            &by_path,
            &name("alias"),
            |_, _| Ok(true),
            &fuse_fixture_work()
        )
        .unwrap()
        .is_none(),
        "an unverified name is never reused"
    );
    Ok(())
}

#[test]
pub(super) fn a_name_joins_its_node_whatever_link_count_its_facts_carry() -> Result<(), i32> {
    let [before, after] = positions();
    let stale = regular(FileId::new(), 3);
    let mut next_inode = ROOT_INODE + 1;
    let mut by_inode = HashMap::from([(
        ROOT_INODE,
        InodeEntry::new(
            crate::native_mount::fuse::FuseOwned::empty(MountPath::root()),
            directory(FileId::new()),
            Some(before),
            1,
            &fuse_fixture_work(),
        )
        .unwrap(),
    )]);
    let mut by_path = HashMap::from([(
        crate::native_mount::fuse::FuseOwned::empty(MountPath::root()),
        ROOT_INODE,
    )]);
    let mut by_file = HashMap::new();
    let first = intern_projected(
        &mut next_inode,
        &mut by_inode,
        &mut by_path,
        &mut by_file,
        crate::native_mount::fuse::FuseOwned::empty(name("a")),
        &stale,
        Some(before),
        true,
        &fuse_fixture_work(),
    )
    .map_err(fuse_fixture_errno)?;
    let second = intern_projected(
        &mut next_inode,
        &mut by_inode,
        &mut by_path,
        &mut by_file,
        crate::native_mount::fuse::FuseOwned::empty(name("b")),
        &stale,
        Some(after),
        true,
        &fuse_fixture_work(),
    )
    .map_err(fuse_fixture_errno)?;
    assert_eq!(first, second);
    assert_eq!(by_inode[&first].bindings.len(), 2);
    Ok(())
}

#[test]
pub(super) fn a_reused_identity_gets_a_fresh_inode() -> Result<(), i32> {
    let [before, after] = positions();
    let reused = regular(FileId::new(), 3);
    let mut next_inode = ROOT_INODE + 1;
    let mut by_inode = HashMap::from([(
        ROOT_INODE,
        InodeEntry::new(
            crate::native_mount::fuse::FuseOwned::empty(MountPath::root()),
            directory(FileId::new()),
            Some(before),
            1,
            &fuse_fixture_work(),
        )
        .unwrap(),
    )]);
    let mut by_path = HashMap::from([(
        crate::native_mount::fuse::FuseOwned::empty(MountPath::root()),
        ROOT_INODE,
    )]);
    let mut by_file = HashMap::new();
    let old = intern_projected(
        &mut next_inode,
        &mut by_inode,
        &mut by_path,
        &mut by_file,
        crate::native_mount::fuse::FuseOwned::empty(name("removed")),
        &reused,
        Some(before),
        true,
        &fuse_fixture_work(),
    )
    .map_err(fuse_fixture_errno)?;
    if let Some(binding) = by_inode.get_mut(&old).and_then(|entry| {
        entry
            .binding_mut(&name("removed"), &fuse_fixture_work())
            .unwrap()
    }) {
        binding.kernel = Some(before);
    }
    let forgotten = {
        let mut deferred = crate::native_mount::fuse::FuseOwned::default();
        forget_unbound_names(
            &mut by_inode,
            &mut by_path,
            old,
            &name("created"),
            |path, _| Ok(*path != name("removed")),
            &mut deferred,
            &fuse_fixture_work(),
        )
        .unwrap();
        deferred
    };
    assert!(matches!(
        forgotten.as_slice(),
        [KernelCacheItem::Entry { parent: ROOT_INODE, name }] if name.as_slice() == b"removed"
    ));
    retire_reused_identity(&mut by_inode, &mut by_file, old);
    let created = intern_projected(
        &mut next_inode,
        &mut by_inode,
        &mut by_path,
        &mut by_file,
        crate::native_mount::fuse::FuseOwned::empty(name("created")),
        &reused,
        Some(after),
        true,
        &fuse_fixture_work(),
    )
    .map_err(fuse_fixture_errno)?;
    assert_ne!(created, old, "the new file gets a fresh inode");
    assert_eq!(by_inode[&created].anchor(), &name("created"));
    assert!(
        by_inode[&old].bindings.is_empty(),
        "the old inode names no file"
    );
    assert_eq!(by_path.get(&name("created")), Some(&created));
    assert_eq!(by_file.get(&reused.node.file_id), Some(&created));
    Ok(())
}

#[test]
pub(super) fn lookup_cache_requires_an_unchanged_view() {
    let path = name("cached");
    let lookup = regular(FileId::from_bytes([3; 16]), 7);
    let [stamp] = positions();
    let mut by_inode = HashMap::from([(
        2,
        InodeEntry::new(
            crate::native_mount::fuse::FuseOwned::empty(path.clone()),
            lookup,
            Some(stamp),
            0,
            &fuse_fixture_work(),
        )
        .unwrap(),
    )]);
    let by_path = HashMap::from([(crate::native_mount::fuse::FuseOwned::empty(path.clone()), 2)]);

    assert!(
        cached_projected_lookup(
            &mut by_inode,
            &by_path,
            &path,
            |_, _| Ok(false),
            &fuse_fixture_work()
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(by_inode[&2].lookup_references, 0);
    assert_eq!(
        cached_projected_lookup(
            &mut by_inode,
            &by_path,
            &path,
            |file_id, sampled| Ok({ file_id == lookup.node.file_id && sampled == stamp }),
            &fuse_fixture_work()
        )
        .unwrap(),
        Some((2, lookup, stamp))
    );
    assert_eq!(by_inode[&2].lookup_references, 1);
}

#[test]
pub(super) fn page_stores_and_content_claims_exclude_each_other() {
    let [before, target, after] = positions();
    let (stored, claimed) = (FileId::new(), FileId::new());
    let mut stores = PageStores::default();
    assert!(stores.may_begin(stored));
    stores.begin(stored, before, &fuse_fixture_work()).unwrap();
    assert!(!stores.may_begin(stored), "one store per file at a time");
    assert!(stores.precede(target), "the barrier waits for older reads");
    assert!(!stores.precede(before));

    stores.claim(claimed, &fuse_fixture_work()).unwrap();
    stores.claim(claimed, &fuse_fixture_work()).unwrap();
    assert!(!stores.may_begin(claimed));
    stores.release(claimed);
    assert!(!stores.may_begin(claimed), "every claim must be released");
    stores.release(claimed);
    assert!(stores.may_begin(claimed));

    stores.land(stored);
    assert!(!stores.precede(target));
    stores.begin(stored, after, &fuse_fixture_work()).unwrap();
    assert!(!stores.precede(target), "a newer read supersedes nothing");
    stores.land(stored);
    stores.claim(stored, &fuse_fixture_work()).unwrap();
    assert!(!stores.may_begin(stored));
}

#[test]
pub(super) fn only_content_changing_opens_claim() {
    assert!(!changes_content(libc::O_RDONLY));
    assert!(!changes_content(libc::O_RDONLY | libc::O_DIRECT));
    assert!(changes_content(libc::O_WRONLY));
    assert!(changes_content(libc::O_RDWR));
    assert!(changes_content(libc::O_RDONLY | libc::O_TRUNC));
}

#[test]
pub(super) fn invalidation_drops_exactly_the_superseded_kernel_items() -> Result<(), i32> {
    let [held, through] = positions();
    let mut next_inode = ROOT_INODE + 1;
    let mut by_inode = HashMap::from([(
        ROOT_INODE,
        InodeEntry::new(
            crate::native_mount::fuse::FuseOwned::empty(MountPath::root()),
            directory(FileId::new()),
            Some(held),
            1,
            &fuse_fixture_work(),
        )
        .unwrap(),
    )]);
    let mut by_path = HashMap::from([(
        crate::native_mount::fuse::FuseOwned::empty(MountPath::root()),
        ROOT_INODE,
    )]);
    let mut by_file = HashMap::new();
    let mut linked = regular(FileId::new(), 3);
    linked.node.link_count = 2;
    let mut intern = |by_inode: &mut HashMap<u64, InodeEntry>, path: &str| {
        intern_projected(
            &mut next_inode,
            by_inode,
            &mut by_path,
            &mut by_file,
            crate::native_mount::fuse::FuseOwned::empty(name(path)),
            &linked,
            Some(held),
            true,
            &fuse_fixture_work(),
        )
        .map_err(fuse_fixture_errno)
    };
    let file = intern(&mut by_inode, "changed")?;
    assert_eq!(intern(&mut by_inode, "kept")?, file);
    let root = by_inode.get_mut(&ROOT_INODE).ok_or(libc::ESTALE)?;
    root.kernel = Some(held);
    let work = fuse_fixture_work();
    assert!(
        root.remember_negative_child(b"appeared", held, &work)
            .expect("tracked name")
    );
    assert!(
        root.remember_negative_child(b"still-absent", held, &work)
            .expect("tracked name")
    );
    let entry = by_inode.get_mut(&file).ok_or(libc::ESTALE)?;
    entry.kernel = Some(held);
    for binding in entry.bindings.iter_mut() {
        binding.kernel = Some(held);
    }
    let changed = [name("changed"), name("appeared")];

    let mut items = stale_kernel_items(
        &mut by_inode,
        &by_path,
        |path, _, _| Ok(!changed.contains(path)),
        through,
        &fuse_fixture_work(),
    )
    .unwrap();

    let entry = |parent: u64, name: &str| KernelCacheItem::Entry {
        parent,
        name: crate::native_mount::fuse::FuseOwned::empty(name.as_bytes().to_vec()),
    };
    let mut expected = vec![
        // The file's anchor is its first name, which changed.
        KernelCacheItem::Inode(file),
        entry(ROOT_INODE, "changed"),
        entry(ROOT_INODE, "appeared"),
    ];
    let key = |item: &KernelCacheItem| format!("{item:?}");
    items.sort_by_key(key);
    expected.sort_by_key(key);
    assert_eq!(items.as_slice(), expected.as_slice());
    let root = &by_inode[&ROOT_INODE];
    assert_eq!(root.kernel, Some(held), "an unchanged inode stays held");
    assert!(
        root.negative_children
            .contains_key(b"still-absent".as_slice())
    );
    assert!(!root.negative_children.contains_key(b"appeared".as_slice()));
    let file = &by_inode[&file];
    // Pages an open handle reads after the drop postdate the check.
    assert_eq!(file.kernel, Some(through));
    assert_eq!(
        file.binding(&name("changed"), &fuse_fixture_work())
            .unwrap()
            .and_then(|b| b.kernel),
        None
    );
    assert_eq!(
        file.binding(&name("kept"), &fuse_fixture_work())
            .unwrap()
            .and_then(|b| b.kernel),
        Some(held)
    );
    Ok(())
}

#[test]
pub(super) fn directory_streams_wait_at_their_offset_and_evict_the_oldest() {
    let stream = |emitted| crate::native_mount::fuse::DirectoryStream {
        path: crate::native_mount::fuse::FuseOwned::empty(MountPath::root()),
        parent_inode: ROOT_INODE,
        binding_epoch: None,
        page: None,
        entry_position: 0,
        entries_stamp: None,
        confirmed: HashMap::new(),
        emitted,
        parked: 0,
        park_replacement: None,
        storage: None,
    };
    let mut streams = crate::native_mount::fuse::DirectoryStreams::default();
    let park = |streams: &mut crate::native_mount::fuse::DirectoryStreams,
                mut stream: crate::native_mount::fuse::DirectoryStream| {
        let work = fuse_fixture_work();
        streams
            .prepare_park(&mut stream, &work)
            .expect("park capacity");
        streams.park(ROOT_INODE, stream);
    };
    for offset in 0..crate::native_mount::fuse::MAXIMUM_DIRECTORY_STREAMS as u64 {
        park(&mut streams, stream(offset));
    }
    assert!(streams.take(ROOT_INODE, 3).is_some());
    assert!(
        streams.take(ROOT_INODE, 3).is_none(),
        "a stream resumes once"
    );
    park(&mut streams, stream(3));
    park(&mut streams, stream(1_000));
    assert!(
        streams.take(ROOT_INODE, 0).is_none(),
        "the longest-waiting stream makes room"
    );
    assert!(streams.take(ROOT_INODE, 3).is_some());
    assert!(streams.take(ROOT_INODE, 1_000).is_some());
    assert!(streams.take(ROOT_INODE + 1, 1).is_none());
}

#[test]
pub(super) fn revalidation_waits_for_deferred_notifications_in_flight() {
    let target = Some(ViewStamp::ORIGIN);
    let mut queue = crate::native_mount::fuse::InvalidationQueue {
        notified: target,
        scanned: target,
        processed: target,
        ..Default::default()
    };
    assert!(queue.is_idle());
    queue.deferred.push(KernelCacheItem::Inode(ROOT_INODE));
    assert!(queue.is_pending(target));
    let items = queue.take_deferred();
    assert_eq!(items.len(), 1);
    assert!(queue.deferred.is_empty());
    assert!(
        queue.is_pending(target),
        "kernel notifications have not finished"
    );
    assert!(!queue.is_idle());
    queue.deferred.push(KernelCacheItem::Inode(ROOT_INODE));
    queue.finish(target, Ok(()));
    assert!(
        queue.is_pending(target),
        "a later deferred item is still owed"
    );
    assert_eq!(queue.take_deferred().len(), 1);
    queue.finish(target, Ok(()));
    assert!(queue.is_idle());
}

#[test]
pub(super) fn releasing_deferred_items_wakes_the_invalidator()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::native_mount::fuse::ProjectionCore;
    use std::sync::PoisonError;
    for finishing in [false, true] {
        let (source, _) = shared_sources()?;
        let core = Arc::new(ProjectionCore::new(
            Arc::new(source),
            empty_projection_state(),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?);
        let waiting = core.state.lock().map_err(|_| "poisoned")?;
        let invalidator = std::thread::spawn({
            let core = Arc::clone(&core);
            move || {
                let state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
                let (state, waited) = core
                    .invalidation
                    .wait_timeout_while(state, std::time::Duration::from_secs(20), |state| {
                        state.invalidation.deferred.is_empty()
                    })
                    .unwrap_or_else(PoisonError::into_inner);
                (state.invalidation.deferred.len(), waited.timed_out())
            }
        });
        drop(waiting);
        // Let the invalidator sleep before the item is deferred.
        std::thread::sleep(std::time::Duration::from_millis(50));
        let mut state = if finishing {
            core.state_to_finish()
        } else {
            core.state().map_err(|_| "stopped")?
        };
        state
            .invalidation
            .deferred
            .push(KernelCacheItem::Inode(ROOT_INODE));
        drop(state);
        let (deferred, timed_out) = invalidator.join().map_err(|_| "invalidator panicked")?;
        assert_eq!((deferred, timed_out), (1, false), "finishing: {finishing}");
    }
    Ok(())
}

#[test]
pub(super) fn a_confirmation_at_a_position_lapses_when_its_name_changes()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources()?;
    let file = crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&name("a"), FileMetadata::default(), None, work)
    })?;
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.hard_link(&name("a"), &name("b"), work)
    })?;
    let file_id = file.node.file_id;
    let stamp = source.view_stamp().ok_or("a checkout carries stamps")?;

    let work = fuse_fixture_work();
    let mut confirmed = HashMap::new();
    confirm_name(
        &mut confirmed,
        file_id,
        crate::native_mount::fuse::FuseOwned::empty(name("b")),
        Some(stamp),
        &work,
    )
    .expect("confirmed name");
    confirm_name(
        &mut confirmed,
        file_id,
        crate::native_mount::fuse::FuseOwned::empty(name("a")),
        None,
        &work,
    )
    .expect("confirmed name");
    assert!(name_is_bound(&confirmed, &source, file_id, &name("b"), &work).expect("name history"));
    assert!(name_is_bound(&confirmed, &source, file_id, &name("a"), &work).expect("name history"));
    assert!(
        !name_is_bound(&confirmed, &source, FileId::new(), &name("b"), &work)
            .expect("name history")
    );
    assert!(!name_is_bound(&confirmed, &source, file_id, &name("c"), &work).expect("name history"));

    // The name changes after the page was read, before it is emitted.
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.remove(&name("b"), Some(file_id), work)
    })?;
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&name("b"), FileMetadata::default(), None, work)
    })?;
    assert!(!name_is_bound(&confirmed, &source, file_id, &name("b"), &work).expect("name history"));
    // A sibling changing leaves another name's confirmation standing.
    let mut sibling = HashMap::new();
    let stamp = source.view_stamp().ok_or("a checkout carries stamps")?;
    confirm_name(
        &mut sibling,
        file_id,
        crate::native_mount::fuse::FuseOwned::empty(name("a")),
        Some(stamp),
        &work,
    )
    .expect("confirmed name");
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&name("c"), FileMetadata::default(), None, work)
    })?;
    assert!(name_is_bound(&sibling, &source, file_id, &name("a"), &work).expect("name history"));
    Ok(())
}

#[test]
pub(super) fn node_facts_follow_the_node_and_directory_facts_its_listing()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources()?;
    let file = crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&name("file"), FileMetadata::default(), None, work)
    })?;
    let directory = crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_directory(&name("directory"), FileMetadata::default(), work)
    })?;
    let stamp = source.view_stamp().ok_or("a checkout carries stamps")?;
    let mut file_entry = InodeEntry::new(
        crate::native_mount::fuse::FuseOwned::empty(name("file")),
        file,
        Some(stamp),
        1,
        &fuse_fixture_work(),
    )
    .unwrap();
    let directory_entry = InodeEntry::new(
        crate::native_mount::fuse::FuseOwned::empty(name("directory")),
        directory,
        Some(stamp),
        1,
        &fuse_fixture_work(),
    )
    .unwrap();
    let work = fuse_fixture_work();
    assert_eq!(
        file_entry
            .current_facts(&source, &work)
            .expect("node history"),
        Some(stamp)
    );
    assert!(
        file_entry
            .lacks_named_attributes(&source, &work)
            .expect("node history")
    );

    // Names changing around a node leave its facts current...
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&name("sibling"), FileMetadata::default(), None, work)
    })?;
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.hard_link(&name("sibling"), &name("alias"), work)
    })?;
    assert_eq!(
        file_entry
            .current_facts(&source, &work)
            .expect("node history"),
        Some(stamp)
    );
    // ...but not a directory's, whose listing they change.
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(
            &name("directory").child(b"child".to_vec()),
            FileMetadata::default(),
            None,
            work,
        )
    })?;
    assert_eq!(
        directory_entry
            .current_facts(&source, &work)
            .expect("listing history"),
        None
    );

    // A change to the node supersedes its facts.
    source.write_range(&name("file"), 0, Bytes::from_static(b"changed"))?;
    assert_eq!(
        file_entry
            .current_facts(&source, &work)
            .expect("node history"),
        None
    );
    assert!(
        !file_entry
            .lacks_named_attributes(&source, &work)
            .expect("node history")
    );
    file_entry.facts = source.view_stamp();
    file_entry.lookup = source.lookup(&name("file"))?.ok_or("file")?;
    assert!(
        file_entry
            .lacks_named_attributes(&source, &work)
            .expect("node history")
    );
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.write_attribute(
            &name("file"),
            b"user.present",
            crate::kernel::OwnedSourceChunk::claimed(Bytes::from_static(b"value").into(), 0),
            MountAttributeWriteMode::Upsert,
            work,
        )
    })?;
    assert!(
        !file_entry
            .lacks_named_attributes(&source, &work)
            .expect("node history")
    );
    Ok(())
}

#[test]
pub(super) fn read_only_mount_rejects_truncating_open_before_source_mutation() {
    assert_eq!(admit_open(false, libc::O_TRUNC), Err(libc::EROFS));
    assert_eq!(admit_open(false, libc::O_RDONLY), Ok(()));
    assert_eq!(admit_open(true, libc::O_TRUNC), Ok(()));
}

#[test]
pub(super) fn root_inode_survives_checkout_identity_changes() -> Result<(), i32> {
    let old_file = FileId::from_bytes([1; 16]);
    let new_file = FileId::from_bytes([2; 16]);
    let mut by_inode = HashMap::from([(
        ROOT_INODE,
        InodeEntry::new(
            crate::native_mount::fuse::FuseOwned::empty(MountPath::root()),
            directory(old_file),
            None,
            1,
            &fuse_fixture_work(),
        )
        .unwrap(),
    )]);
    let mut inode_by_file = HashMap::from([(old_file, ROOT_INODE)]);

    replace_root_lookup(
        &mut by_inode,
        &mut inode_by_file,
        directory(new_file),
        &fuse_fixture_work(),
    )
    .map_err(fuse_fixture_errno)?;

    assert_eq!(by_inode[&ROOT_INODE].lookup.node.file_id, new_file);
    assert_eq!(by_inode[&ROOT_INODE].anchor(), &MountPath::root());
    assert_eq!(inode_by_file.get(&new_file), Some(&ROOT_INODE));
    assert!(!inode_by_file.contains_key(&old_file));
    assert_eq!(
        replace_root_lookup(
            &mut by_inode,
            &mut inode_by_file,
            regular(FileId::new(), 0),
            &fuse_fixture_work()
        )
        .map_err(fuse_fixture_errno),
        Err(libc::EIO),
        "the root stays a directory"
    );
    Ok(())
}

#[test]
pub(super) fn enumerated_entries_receive_nonzero_inodes_without_fake_lookup_references()
-> Result<(), i32> {
    let mut next_inode = ROOT_INODE + 1;
    let mut by_inode = HashMap::<u64, InodeEntry>::new();
    let mut inode_by_path = HashMap::new();
    let mut inode_by_file = HashMap::new();
    let lookup = regular(FileId::new(), 4);
    let path = name("preexisting.bin");

    let enumerated = intern_projected(
        &mut next_inode,
        &mut by_inode,
        &mut inode_by_path,
        &mut inode_by_file,
        crate::native_mount::fuse::FuseOwned::empty(path.clone()),
        &lookup,
        None,
        false,
        &fuse_fixture_work(),
    )
    .map_err(fuse_fixture_errno)?;
    assert_ne!(enumerated, 0);
    assert_eq!(by_inode[&enumerated].lookup_references, 0);

    let looked_up = intern_projected(
        &mut next_inode,
        &mut by_inode,
        &mut inode_by_path,
        &mut inode_by_file,
        crate::native_mount::fuse::FuseOwned::empty(path),
        &lookup,
        None,
        true,
        &fuse_fixture_work(),
    )
    .map_err(fuse_fixture_errno)?;
    assert_eq!(looked_up, enumerated);
    assert_eq!(by_inode[&enumerated].lookup_references, 1);

    let retained_by_inode_count = by_inode.len();
    let retained_inode_by_file_count = inode_by_file.len();
    next_inode = u64::MAX;
    let overflow = regular(FileId::new(), 4);
    assert_eq!(
        intern_projected(
            &mut next_inode,
            &mut by_inode,
            &mut inode_by_path,
            &mut inode_by_file,
            crate::native_mount::fuse::FuseOwned::empty(name("overflow.bin")),
            &overflow,
            None,
            false,
            &fuse_fixture_work()
        )
        .map_err(fuse_fixture_errno),
        Err(libc::EOVERFLOW)
    );
    assert_eq!(next_inode, u64::MAX);
    assert_eq!(by_inode.len(), retained_by_inode_count);
    assert_eq!(inode_by_file.len(), retained_inode_by_file_count);
    assert_eq!(by_inode[&enumerated].lookup_references, 1);
    assert_eq!(inode_by_file[&lookup.node.file_id], enumerated);
    Ok(())
}

pub(super) type MemoryCheckout =
    crate::Checkout<crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend>;
