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
    let (source, _) = shared_sources(posix_config())?;
    let mut policy = FINITE_HANDLER_POLICY;
    policy.bytes_copied = 0;
    let core = projection_core(Arc::new(source), policy, &crate::CancellationToken::new())?;
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
pub(super) fn administrative_recovery_retains_the_tree_and_retries_on_its_own_budget()
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
    assert!(!control.namespace_detach_confirmed());
    assert_eq!(denied.receipt().backend_write_operations, 0);

    let denied_probe = crate::NativeMaterializationWork::new(
        crate::WorkBudget {
            backend_read_operations: 0,
            ..crate::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .map_err(|(cause, _)| cause)?;
    assert!(matches!(
        control.recover(temporary.path(), &denied_probe),
        Err(crate::NativeMountError::Materialization(
            crate::NativeTreeMaterializationError::Work(_)
        ))
    ));
    assert!(control.controls_pending());
    assert!(!control.namespace_detach_confirmed());

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
    control.recover(temporary.path(), &original_admin)?;
    assert!(!control.controls_pending());
    assert!(
        control.namespace_detach_confirmed(),
        "the real directory probe establishes namespace absence"
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
    crate::native_mount::fuse::ProjectionState::new(true, 0, 0)
}

#[test]
pub(super) fn kernel_forget_retires_only_an_inode_with_no_open_handle() -> Result<(), i32> {
    let mut state = empty_projection_state();
    let mut linked = regular(FileId::new(), 1);
    linked.node.link_count = 2;
    let intern = |state: &mut crate::native_mount::fuse::ProjectionState, path| {
        state
            .intern_projected(
                crate::native_mount::fuse::FuseOwned::empty(path),
                &linked,
                None,
                true,
                &fuse_fixture_work(),
            )
            .map_err(native_callback_errno)
    };
    let forgotten = intern(&mut state, name("first"))?;
    state.release_lookup_reference(forgotten, 1);
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
    state.release_lookup_reference(current, 1);
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
    let copied = work.state.lock().work.bytes_copied;
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
            denied.state.lock().work,
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
    assert_eq!(work.state.lock().work.bytes_copied, copied);
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
    assert_eq!(entry.negative_children[b"absent-0".as_slice()], older);
    assert!(
        entry
            .remember_negative_child(b"absent-0", newer, &work)
            .expect("tracked name")
    );
    assert_eq!(entry.negative_children[b"absent-0".as_slice()], older);
}

#[test]
pub(super) fn a_name_joins_its_node_whatever_link_count_its_facts_carry() -> Result<(), i32> {
    let [before, after] = positions();
    let stale = regular(FileId::new(), 3);
    let mut state = empty_projection_state();
    let first = state
        .intern_projected(
            crate::native_mount::fuse::FuseOwned::empty(name("a")),
            &stale,
            Some(before),
            true,
            &fuse_fixture_work(),
        )
        .map_err(native_callback_errno)?;
    let second = state
        .intern_projected(
            crate::native_mount::fuse::FuseOwned::empty(name("b")),
            &stale,
            Some(after),
            true,
            &fuse_fixture_work(),
        )
        .map_err(native_callback_errno)?;
    assert_eq!(first, second);
    Ok(())
}

#[test]
pub(super) fn a_reused_identity_gets_a_fresh_inode() -> Result<(), i32> {
    let [before, after] = positions();
    let reused = regular(FileId::new(), 3);
    let mut state = empty_projection_state();
    let old = state
        .intern_projected(
            crate::native_mount::fuse::FuseOwned::empty(name("removed")),
            &reused,
            Some(before),
            true,
            &fuse_fixture_work(),
        )
        .map_err(native_callback_errno)?;
    forget_unbound_names(
        &mut state.by_inode,
        &mut state.inode_by_path,
        old,
        &name("created"),
        |path, _| Ok(*path != name("removed")),
        &mut crate::native_mount::fuse::FuseOwned::default(),
        &fuse_fixture_work(),
    )
    .map_err(native_callback_errno)?;
    retire_reused_identity(&mut state.by_inode, &mut state.inode_by_file, old);
    let created = state
        .intern_projected(
            crate::native_mount::fuse::FuseOwned::empty(name("created")),
            &reused,
            Some(after),
            true,
            &fuse_fixture_work(),
        )
        .map_err(native_callback_errno)?;
    assert_ne!(created, old, "the new file gets a fresh inode");
    assert!(
        state.by_inode[&old].bindings.is_empty(),
        "the old inode names no file"
    );
    Ok(())
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

    stores.in_flight.remove(&stored);
    assert!(!stores.precede(target));
    stores.begin(stored, after, &fuse_fixture_work()).unwrap();
    assert!(!stores.precede(target), "a newer read supersedes nothing");
    stores.in_flight.remove(&stored);
    stores.claim(stored, &fuse_fixture_work()).unwrap();
    assert!(!stores.may_begin(stored));
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
    };
    let mut streams = crate::native_mount::fuse::DirectoryStreams::default();
    let park = |streams: &mut crate::native_mount::fuse::DirectoryStreams,
                stream: crate::native_mount::fuse::DirectoryStream| {
        let work = fuse_fixture_work();
        let parking = streams.prepare_park(&work).expect("park capacity");
        streams.park(ROOT_INODE, stream, parking);
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
pub(super) fn releasing_deferred_items_wakes_the_invalidator()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::PoisonError;
    for finishing in [false, true] {
        let (source, _) = shared_sources(posix_config())?;
        let core = projection_core(
            Arc::new(source),
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )?;
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
    let (source, _) = shared_sources(posix_config())?;
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
    )?;
    confirm_name(
        &mut confirmed,
        file_id,
        crate::native_mount::fuse::FuseOwned::empty(name("a")),
        None,
        &work,
    )?;
    assert!(name_is_bound(
        &confirmed,
        &source,
        file_id,
        &name("b"),
        &work
    )?);
    assert!(name_is_bound(
        &confirmed,
        &source,
        file_id,
        &name("a"),
        &work
    )?);
    assert!(!name_is_bound(
        &confirmed,
        &source,
        FileId::new(),
        &name("b"),
        &work
    )?);
    assert!(!name_is_bound(
        &confirmed,
        &source,
        file_id,
        &name("c"),
        &work
    )?);

    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.remove(&name("b"), Some(file_id), work)
    })?;
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&name("b"), FileMetadata::default(), None, work)
    })?;
    assert!(!name_is_bound(
        &confirmed,
        &source,
        file_id,
        &name("b"),
        &work
    )?);
    let mut sibling = HashMap::new();
    let stamp = source.view_stamp().ok_or("a checkout carries stamps")?;
    confirm_name(
        &mut sibling,
        file_id,
        crate::native_mount::fuse::FuseOwned::empty(name("a")),
        Some(stamp),
        &work,
    )?;
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&name("c"), FileMetadata::default(), None, work)
    })?;
    assert!(name_is_bound(
        &sibling,
        &source,
        file_id,
        &name("a"),
        &work
    )?);
    Ok(())
}

#[test]
pub(super) fn node_facts_follow_the_node_and_directory_facts_its_listing()
-> Result<(), Box<dyn std::error::Error>> {
    let (source, _) = shared_sources(posix_config())?;
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
    )?;
    let directory_entry = InodeEntry::new(
        crate::native_mount::fuse::FuseOwned::empty(name("directory")),
        directory,
        Some(stamp),
        1,
        &fuse_fixture_work(),
    )?;
    let work = fuse_fixture_work();
    assert_eq!(file_entry.current_facts(&source, &work)?, Some(stamp));
    assert!(file_entry.lacks_named_attributes(&source, &work)?);

    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(&name("sibling"), FileMetadata::default(), None, work)
    })?;
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.hard_link(&name("sibling"), &name("alias"), work)
    })?;
    assert_eq!(file_entry.current_facts(&source, &work)?, Some(stamp));
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.create_file(
            &name("directory").child(b"child".to_vec()),
            FileMetadata::default(),
            None,
            work,
        )
    })?;
    assert_eq!(directory_entry.current_facts(&source, &work)?, None);

    source.write_range(&name("file"), 0, Bytes::from_static(b"changed"))?;
    assert_eq!(file_entry.current_facts(&source, &work)?, None);
    assert!(!file_entry.lacks_named_attributes(&source, &work)?);
    file_entry.facts = source.view_stamp();
    file_entry.lookup = source.lookup(&name("file"))?.ok_or("file")?;
    assert!(file_entry.lacks_named_attributes(&source, &work)?);
    crate::native_mount::adapter::boundary(source.cancellation(), |work| {
        source.write_attribute(
            &name("file"),
            b"user.present",
            crate::kernel::OwnedSourceChunk::claimed(Bytes::from_static(b"value").into(), 0),
            MountAttributeWriteMode::Upsert,
            work,
        )
    })?;
    assert!(!file_entry.lacks_named_attributes(&source, &work)?);
    Ok(())
}

#[test]
pub(super) fn enumerated_entries_receive_nonzero_inodes_without_fake_lookup_references()
-> Result<(), i32> {
    let mut state = empty_projection_state();
    let lookup = regular(FileId::new(), 4);
    let path = name("preexisting.bin");

    let enumerated = state
        .intern_projected(
            crate::native_mount::fuse::FuseOwned::empty(path.clone()),
            &lookup,
            None,
            false,
            &fuse_fixture_work(),
        )
        .map_err(native_callback_errno)?;
    assert_ne!(enumerated, 0);
    assert_eq!(state.by_inode[&enumerated].lookup_references, 0);

    let looked_up = state
        .intern_projected(
            crate::native_mount::fuse::FuseOwned::empty(path),
            &lookup,
            None,
            true,
            &fuse_fixture_work(),
        )
        .map_err(native_callback_errno)?;
    assert_eq!(looked_up, enumerated);
    assert_eq!(state.by_inode[&enumerated].lookup_references, 1);

    let retained_by_inode_count = state.by_inode.len();
    let retained_inode_by_file_count = state.inode_by_file.len();
    state.next_inode = u64::MAX;
    let overflow = regular(FileId::new(), 4);
    assert_eq!(
        state
            .intern_projected(
                crate::native_mount::fuse::FuseOwned::empty(name("overflow.bin")),
                &overflow,
                None,
                false,
                &fuse_fixture_work()
            )
            .map_err(native_callback_errno),
        Err(libc::EOVERFLOW)
    );
    assert_eq!(state.next_inode, u64::MAX);
    assert_eq!(state.by_inode.len(), retained_by_inode_count);
    assert_eq!(state.inode_by_file.len(), retained_inode_by_file_count);
    assert_eq!(state.by_inode[&enumerated].lookup_references, 1);
    assert_eq!(state.inode_by_file[&lookup.node.file_id].0, enumerated);
    Ok(())
}

pub(super) type MemoryCheckout =
    crate::Checkout<crate::facade::MemoryAuthorityBackend, crate::facade::MemoryObjectBackend>;
