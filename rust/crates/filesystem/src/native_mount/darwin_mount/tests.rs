
    #[test]
    fn filtered_callback_error_does_not_overwrite_parent_errno() {
        crate::obs::assert_filtered_callback_isolation(|span| {
            let status = super::ffi_status(0, span, |_| Ok(0));
            assert_eq!(status, -libc::ESTALE);
            -status
        });
    }
    use super::*;
    use crate::native_mount::MountPublication;

    unsafe extern "C" {
        fn nfs4_test_read_reply() -> c_int;
        fn nfs4_test_durable_writes() -> c_int;
        fn nfs4_test_change_attribute() -> c_int;
        fn nfs4_test_access_rights() -> c_int;
        fn nfs4_test_verify_attributes() -> c_int;
        fn nfs4_test_node_identity() -> c_int;
        fn nfs4_test_release_open_files() -> c_int;
    }

    type TestResult = Result<(), Box<dyn std::error::Error>>;
    type MemorySource = crate::native_mount::CheckoutMountSource<
        crate::facade::MemoryAuthorityBackend,
        crate::facade::MemoryObjectBackend,
    >;

    /// A writable context over a fresh POSIX checkout.
    fn checkout_context(
        publication: MountPublication,
    ) -> Result<(Arc<MemorySource>, DarwinMountContext), Box<dyn std::error::Error>> {
        use crate::Fs;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, Lifecycle};
        use crate::native_mount::SharedCheckout;

        let mut config = crate::model::VolumeConfig::portable(Lifecycle::Ephemeral);
        config.profile = FilesystemProfile::Posix;
        let fs = Fs::memory();
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let checkout = runtime.block_on(async {
            let cancellation = crate::CancellationToken::new();
            let volume = fs
                .create_volume(config, crate::WorkBudget::UNBOUNDED, &cancellation)
                .await?
                .value;
            volume
                .checkout(
                    GenerationSelector::Head,
                    CheckoutMode::tracking_transaction(),
                    crate::WorkBudget::UNBOUNDED,
                    &cancellation,
                )
                .await
                .map(|receipt| receipt.value)
        })?;
        let shared = Arc::new(SharedCheckout::with_publication(checkout, publication));
        let source = Arc::new(MemorySource::new(shared, config)?);
        let root_id = source
            .lookup(&MountPath::root())?
            .ok_or("root absent")?
            .node
            .file_id;
        let context = DarwinMountContext::new(
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
            true,
            &std::fs::metadata(".")?,
            root_id,
        );
        Ok((source, context))
    }

    fn os(error: i32) -> std::io::Error {
        std::io::Error::from_raw_os_error(error)
    }

    #[test]
    fn close_publishes_only_what_its_handle_wrote() -> TestResult {
        let (source, context) = checkout_context(MountPublication::CloseAndSync)?;
        let epoch = || source.generation_id();
        let path = MountPath::root().child(b"published".to_vec());
        context
            .mutate(|| source.create_file(&path, FileMetadata::default()))
            .map_err(os)?;
        let created = context
            .open(&path, InitialHandleState::Written)
            .map_err(os)?;
        let before = epoch()?;
        context.flush_on_close(created).map_err(os)?;
        assert_ne!(epoch()?, before, "closing the creating handle publishes");

        let reader = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        let before = epoch()?;
        context.flush_on_close(reader).map_err(os)?;
        context.sync().map_err(os)?;
        assert_eq!(epoch()?, before, "a clean close or sync publishes nothing");

        context
            .mutate_file(&path, reader, |file| {
                file.write_range(0, Bytes::from_static(b"through the handle"))
            })
            .map_err(os)?;
        let before = epoch()?;
        context.flush_on_close(reader).map_err(os)?;
        assert_ne!(epoch()?, before, "a written handle's close publishes");
        let before = epoch()?;
        context.flush_on_close(reader).map_err(os)?;
        context.sync().map_err(os)?;
        assert_eq!(epoch()?, before, "published writes are not published again");

        // NFS may write under a stateid that names no open handle: that
        // write does not dirty the handle, but a sync still covers it.
        context
            .mutate_file(&path, 0, |file| {
                file.write_range(0, Bytes::from_static(b"without a handle"))
            })
            .map_err(os)?;
        context.flush_on_close(reader).map_err(os)?;
        let before = epoch()?;
        context.sync().map_err(os)?;
        assert_ne!(
            epoch()?,
            before,
            "a sync publishes every acknowledged write"
        );
        Ok(())
    }

    #[test]
    fn manual_close_and_fsync_publish_nothing_until_sync() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let epoch = || source.generation_id();
        let path = MountPath::root().child(b"manual".to_vec());
        context
            .mutate(|| source.create_file(&path, FileMetadata::default()))
            .map_err(os)?;
        let created = context
            .open(&path, InitialHandleState::Written)
            .map_err(os)?;
        context
            .mutate_file(&path, created, |file| {
                file.write_range(0, Bytes::from_static(b"deferred"))
            })
            .map_err(os)?;
        let reader = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        let before = epoch()?;
        context.flush_on_close(reader).map_err(os)?;
        context.flush_on_close(created).map_err(os)?;
        context.sync().map_err(os)?;
        assert_eq!(epoch()?, before, "manual close and fsync publish nothing");
        source.sync()?;
        assert_ne!(epoch()?, before, "an explicit mount sync publishes");
        Ok(())
    }

    #[test]
    fn change_labels_advance_exactly_for_objects_that_changed() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let label = |path: &MountPath| {
            context
                .attributes(path, 0)
                .map(|attributes| attributes.change)
                .map_err(os)
        };
        let root = MountPath::root();
        let first = root.child(b"first".to_vec());
        let second = root.child(b"second".to_vec());
        for path in [&first, &second] {
            context
                .mutate(|| source.create_file(path, FileMetadata::default()))
                .map_err(os)?;
        }
        let (root_label, first_label, second_label) =
            (label(&root)?, label(&first)?, label(&second)?);
        assert_eq!(
            label(&first)?,
            first_label,
            "an unchanged file keeps its label"
        );
        assert_ne!(first_label, second_label, "objects never share a label");

        context
            .mutate_file(&first, 0, |file| {
                file.write_range(0, Bytes::from_static(b"written"))
            })
            .map_err(os)?;
        assert!(
            label(&first)? > first_label,
            "a written file gets a new label"
        );
        assert_eq!(label(&second)?, second_label, "other files keep theirs");
        assert_eq!(
            label(&root)?,
            root_label,
            "a content write keeps the listing"
        );

        source.remove(&second, None)?;
        assert!(
            label(&root)? > root_label,
            "a namespace change relabels its directory"
        );

        let first_label = label(&first)?;
        context.forget_view();
        assert!(
            label(&first)? > first_label,
            "an invalidation relabels everything"
        );

        let unstamped = ChangeLabels::new();
        let lookup = source.lookup(&first)?.ok_or("file absent")?;
        let observed = || Observed {
            path: first.clone(),
            lookup,
        };
        assert_ne!(
            unstamped.label(
                lookup.node.file_id,
                None,
                None,
                observed(),
                |_| true,
                |_| true
            ),
            unstamped.label(
                lookup.node.file_id,
                None,
                None,
                observed(),
                |_| true,
                |_| true
            ),
            "a source without stamps never repeats a label"
        );
        Ok(())
    }

    /// After a fence that could not confirm every change was reported, each
    /// labelled object is read again: one that reads the same keeps its
    /// label and a revalidation returns without waiting out the client's
    /// attribute timeout, while one that differs makes it wait.
    #[test]
    fn an_unconfirmed_fence_waits_only_for_what_changed() -> TestResult {
        use super::super::view_ledger::ViewChange;

        let (source, context) = checkout_context(MountPublication::Manual)?;
        let context = Arc::new(context);
        let observer = Arc::downgrade(&context);
        let observer: std::sync::Weak<dyn ViewObserver> = observer;
        source.observe_view(observer);
        let path = MountPath::root().child(b"kept".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        // The creation was made around the mount; take it as waited out.
        let made = context.around.made.load(Ordering::Acquire);
        context.around.settled.store(made, Ordering::Release);
        let label = context.attributes(&path, 0).map_err(os)?.change;

        source.record_projection_change(&ViewChange::Unconfirmed);
        assert!(
            !context.revalidate_with_wait(),
            "unchanged labels do not require waiting out the attribute timeout"
        );
        assert_eq!(context.attributes(&path, 0).map_err(os)?.change, label);

        // What the client holds no longer matches what the source reads.
        for issued in context
            .changes
            .labels
            .lock()
            .map_err(|_| "labels poisoned")?
            .values_mut()
        {
            issued.observed.lookup.node.logical_bytes += 1;
        }
        source.record_projection_change(&ViewChange::Unconfirmed);
        assert!(
            context.revalidate_with_wait(),
            "a change the fence could not confirm requires waiting out the attribute timeout"
        );
        Ok(())
    }

    #[test]
    fn attributes_observed_before_a_change_never_take_a_later_label() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"raced".to_vec());
        let created = source.create_file(&path, FileMetadata::default())?;
        let file_id = created.node.file_id;
        let binding = source.binding_epoch();
        let unchanged = |stamp| context.unchanged_since(&path, Some(file_id), stamp);
        let reported = |stamp| context.reported_unchanged_since(&path, Some(file_id), stamp);
        let before = context.cache_stamp().ok_or("checkout has no view stamp")?;
        source.write_range(&path, 0, Bytes::from_static(b"changed"))?;
        let after = context.cache_stamp().ok_or("checkout has no view stamp")?;
        let written = source.lookup(&path)?.ok_or("file absent")?;
        let observed = |lookup| Observed {
            path: path.clone(),
            lookup,
        };

        let current = context.changes.label(
            file_id,
            Some(after),
            binding,
            observed(written),
            unchanged,
            reported,
        );
        assert_eq!(
            context.changes.label(
                file_id,
                Some(after),
                binding,
                observed(written),
                unchanged,
                reported
            ),
            current,
            "attributes observed after the change share its label"
        );
        assert!(
            context.changes.label(
                file_id,
                Some(before),
                binding,
                observed(created),
                unchanged,
                reported
            ) > current,
            "attributes observed before it get a label of their own"
        );
        Ok(())
    }

    #[test]
    fn forgetting_the_view_rereads_what_the_source_did_not_record() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"unrecorded".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        let handle = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        let size = |handle| {
            context
                .attributes(&path, handle)
                .map(|attributes| attributes.logical_bytes)
                .map_err(os)
        };
        assert_eq!((size(0)?, size(handle)?), (0, 0));
        let label = context.attributes(&path, 0).map_err(os)?.change;

        // Stand in for a change the source reports to nobody: seed the
        // caches with a size the source no longer has.
        let stale = |lookup: &mut MountLookup| lookup.node.logical_bytes = 7;
        context
            .lookups
            .lock()
            .map_err(|_| "poisoned lookups")?
            .entries
            .values_mut()
            .filter_map(|(lookup, _)| lookup.as_mut())
            .for_each(stale);
        let files = context.files.read().map_err(|_| "poisoned handles")?;
        stale(
            &mut files
                .get(&handle)
                .ok_or("handle absent")?
                .observation
                .lock()
                .map_err(|_| "poisoned observation")?
                .lookup,
        );
        drop(files);
        assert_eq!((size(0)?, size(handle)?), (7, 7), "the caches were seeded");

        context.forget_view();
        assert_eq!(
            (size(0)?, size(handle)?),
            (0, 0),
            "neither the lookup cache nor a handle serves a forgotten fact"
        );
        let relabeled = context.attributes(&path, 0).map_err(os)?.change;
        assert!(
            relabeled > label,
            "the reread attributes take a fresh label"
        );
        assert_eq!(
            context.attributes(&path, handle).map_err(os)?.change,
            relabeled,
            "which then holds for the unchanged object"
        );
        Ok(())
    }

    #[test]
    fn revalidation_waits_out_the_attribute_timeout_for_changes_around_the_mount() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let context = context.observing_source();
        // SAFETY: the bridge reports a constant.
        let timeout = u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() });
        let barrier = || {
            let began = uptime();
            context.revalidate();
            (began, uptime())
        };
        let (began, ended) = barrier();
        assert!(ended - began < REPLY_DELIVERY_SLACK, "an unchanged view");

        // SAFETY: the name is NUL-terminated and `context` outlives the call.
        let status = unsafe {
            acyclic_fs_darwin_mount_mkdir(
                Arc::as_ptr(&context) as usize,
                c"/own".as_ptr(),
                0o755,
                0,
                0,
            )
        };
        assert_eq!(status, 0);
        let (began, ended) = barrier();
        assert!(
            ended - began < REPLY_DELIVERY_SLACK,
            "the client learns of the mount's own changes from their replies"
        );

        let waits_out_the_timeout = || {
            let (began, ended) = barrier();
            assert!(
                ended.as_secs() >= (began + REPLY_DELIVERY_SLACK).as_secs() + timeout,
                "every attribute cached before the barrier expired"
            );
            let (began, ended) = barrier();
            assert!(ended - began < REPLY_DELIVERY_SLACK, "once per change");
        };
        source.create_file(
            &MountPath::root().child(b"around".to_vec()),
            FileMetadata::default(),
        )?;
        waits_out_the_timeout();
        context.forget_view();
        waits_out_the_timeout();
        Ok(())
    }

    #[test]
    fn a_barrier_waits_only_for_callbacks_that_began_before_it() -> TestResult {
        let gate = Arc::new(CallbackGate::new());
        let earlier = gate.enter();
        let (drained, finished) = std::sync::mpsc::channel();
        let barrier = std::thread::spawn({
            let gate = Arc::clone(&gate);
            move || {
                gate.drain();
                let _ = drained.send(());
            }
        });
        assert!(
            finished.recv_timeout(Duration::from_millis(50)).is_err(),
            "an earlier callback holds the barrier"
        );
        let later = gate.enter();
        drop(earlier);
        finished.recv_timeout(Duration::from_secs(5))?;
        drop(later);
        barrier.join().map_err(|_| "barrier panicked")?;
        Ok(())
    }

    #[test]
    fn nfs_read_answers_short_reads_as_eof_in_place() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_read_reply() }, 0);
    }

    #[test]
    fn nfs_fileids_name_nodes_so_hard_links_share_one() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_node_identity() }, 0);
    }

    #[test]
    fn nfs_durable_writes_are_stable_without_a_sync() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_durable_writes() }, 0);
    }

    #[test]
    fn only_a_publishing_source_leaves_writes_unstable() -> TestResult {
        for (publication, durable) in [
            (MountPublication::Manual, 1),
            (MountPublication::CloseAndSync, 0),
        ] {
            let (_, context) = checkout_context(publication)?;
            // SAFETY: `context` outlives the call it is passed to.
            let answer = unsafe {
                acyclic_fs_darwin_mount_durable_writes(std::ptr::from_ref(&context) as usize)
            };
            assert_eq!(answer, durable, "{publication:?}");
        }
        Ok(())
    }

    #[test]
    fn handles_open_their_source_file_only_when_used() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"deferred".to_vec());
        let created = source.create_file(&path, FileMetadata::default())?;
        source.write_range(&path, 0, Bytes::from_static(b"kept"))?;
        let bound = |handle: u64| -> Result<bool, Box<dyn std::error::Error>> {
            let files = context.files.read().map_err(|_| "poisoned handles")?;
            Ok(files.get(&handle).ok_or("handle absent")?.file.is_some())
        };

        let closed_unused = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        assert!(!bound(closed_unused)?, "an open binds nothing");
        assert_eq!(
            context
                .lookup_handle(&path, closed_unused)
                .map_err(os)?
                .node
                .file_id,
            created.node.file_id,
            "attributes need no source file"
        );
        assert!(!bound(closed_unused)?);

        let read = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        let bytes = context
            .file_or_open(&path, read)
            .map_err(os)?
            .read_up_to(0, 16)?;
        assert_eq!(bytes.as_ref(), b"kept");
        assert!(bound(read)?, "the first read binds the source file");

        // A replacement outside this mount leaves an unbound handle stale,
        // as an NFS handle on a removed object is.
        let stale = context.open(&path, InitialHandleState::Clean).map_err(os)?;
        source.remove(&path, Some(created.node.file_id))?;
        source.create_file(&path, FileMetadata::default())?;
        assert_eq!(context.file_or_open(&path, stale).err(), Some(libc::ESTALE));
        Ok(())
    }

    #[test]
    fn renaming_over_an_open_file_keeps_it_for_its_handles() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let (replaced, replacement) = (
            MountPath::root().child(b"replaced".to_vec()),
            MountPath::root().child(b"replacement".to_vec()),
        );
        let original = source.create_file(&replaced, FileMetadata::default())?;
        source.write_range(&replaced, 0, Bytes::from_static(b"original"))?;
        source.create_file(&replacement, FileMetadata::default())?;
        let handle = context
            .open(&replaced, InitialHandleState::Clean)
            .map_err(os)?;
        let rename_over = |from: &CStr| {
            // SAFETY: both names are NUL-terminated and `context` outlives the call.
            unsafe {
                acyclic_fs_darwin_mount_rename(
                    std::ptr::from_ref(&context) as usize,
                    from.as_ptr(),
                    c"/replaced".as_ptr(),
                    0,
                )
            }
        };
        assert_eq!(rename_over(c"/missing"), -libc::ENOENT);
        assert!(
            context
                .files
                .read()
                .map_err(|_| "poisoned handles")?
                .get(&handle)
                .ok_or("handle absent")?
                .file
                .is_none(),
            "a failed rename leaves the handle as it was"
        );
        assert_eq!(rename_over(c"/replacement"), 0);
        let file = context.file_or_open(&replaced, handle).map_err(os)?;
        assert_eq!(file.lookup()?.node.file_id, original.node.file_id);
        assert_eq!(file.read_up_to(0, 16)?.as_ref(), b"original");
        Ok(())
    }

    #[test]
    fn named_attribute_probes_need_no_source_call_without_attributes() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"plain".to_vec());
        source.create_file(&path, FileMetadata::default())?;
        assert!(!context.may_have_named_attributes(&path).map_err(os)?);
        // SAFETY: the names are NUL-terminated and `context` outlives the call.
        let probe = || unsafe {
            acyclic_fs_darwin_mount_getxattr(
                std::ptr::from_ref(&context) as usize,
                c"/plain".as_ptr(),
                c"com.apple.provenance".as_ptr(),
                ptr::null_mut(),
                0,
            )
        };
        assert_eq!(probe(), -libc::ENOATTR);
        source.write_attribute(
            &path,
            b"com.apple.provenance",
            Bytes::from_static(b"origin"),
            MountAttributeWriteMode::Upsert,
        )?;
        assert!(context.may_have_named_attributes(&path).map_err(os)?);
        assert_eq!(probe(), 6, "a present attribute is read from the source");
        Ok(())
    }

    #[test]
    fn nfs_change_attribute_travels_with_its_attributes() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_change_attribute() }, 0);
    }

    #[test]
    fn nfs_access_applies_the_callers_identity_to_mode_bits() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_access_rights() }, 0);
    }

    #[test]
    fn nfs_verify_compares_current_attribute_values() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_verify_attributes() }, 0);
    }

    #[test]
    fn nfs_teardown_releases_every_open_handle_once() {
        // SAFETY: the test hook owns all callback state.
        assert_eq!(unsafe { nfs4_test_release_open_files() }, 0);
    }

    #[test]
    fn content_changes_stamp_modification_and_status_times() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let path = MountPath::root().child(b"stamped".to_vec());
        let created = FileMetadata {
            modified_ns: MetadataField::Value(1),
            changed_ns: MetadataField::Value(1),
            ..create_metadata(0o644, mode::IFREG, 0, 0)
        };
        context
            .mutate(|| source.create_file(&path, created))
            .map_err(os)?;
        let times = || -> Result<_, Box<dyn std::error::Error>> {
            let metadata = source.lookup(&path)?.ok_or("file absent")?.metadata;
            Ok((metadata.modified_ns, metadata.changed_ns))
        };
        context
            .mutate_file(&path, 0, |file| {
                file.write_range(0, Bytes::from_static(b"stamped"))
            })
            .map_err(os)?;
        let (MetadataField::Value(modified), MetadataField::Value(changed)) = times()? else {
            return Err("write dropped a time".into());
        };
        assert!(modified > 1 && changed > 1, "a write stamps both times");
        context
            .mutate_file(&path, 0, |file| file.resize(1))
            .map_err(os)?;
        let (MetadataField::Value(resized), _) = times()? else {
            return Err("resize dropped a time".into());
        };
        assert!(resized >= modified, "a truncation stamps the time again");
        Ok(())
    }

    /// Mounts a fresh checkout for one live macOS test.
    #[allow(
        clippy::type_complexity,
        reason = "the tuple keeps the temporary directory, source, and session alive together for one test"
    )]
    fn live_mount() -> Result<
        (
            tempfile::TempDir,
            Arc<MemorySource>,
            crate::NativeMountSession,
        ),
        Box<dyn std::error::Error>,
    > {
        let (source, _) = checkout_context(MountPublication::Manual)?;
        let temporary = tempfile::tempdir()?;
        let mount = crate::mount_native(
            crate::NativeMountRequest {
                mount_id: crate::MountId::new(),
                volume_id: source.volume_id()?,
                destination: temporary.path().to_path_buf(),
                writable: true,
            },
            Arc::clone(&source) as Arc<dyn MountFilesystem>,
        )?;
        Ok((temporary, source, mount))
    }

    /// The local socket a live mount's server listens on, from the mount
    /// table's `<socket>:/` source.
    fn mount_socket(destination: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let destination = destination.canonicalize()?;
        let mut mounts = std::ptr::null_mut::<libc::statfs>();
        // SAFETY: `getmntinfo` points `mounts` at an OS-owned array of the
        // returned length, valid until the next call.
        let count = unsafe { libc::getmntinfo(&raw mut mounts, libc::MNT_NOWAIT) };
        // SAFETY: as above.
        let mounts = unsafe { std::slice::from_raw_parts(mounts, usize::try_from(count)?) };
        mounts
            .iter()
            .find_map(|mount| {
                // SAFETY: both names are NUL-terminated by the OS.
                let (on, from) = unsafe {
                    (
                        CStr::from_ptr(mount.f_mntonname.as_ptr()),
                        CStr::from_ptr(mount.f_mntfromname.as_ptr()),
                    )
                };
                (on.to_bytes() == destination.as_os_str().as_bytes())
                    .then(|| from.to_str().ok())
                    .flatten()
                    .and_then(|from| from.strip_prefix('<'))
                    .and_then(|from| from.split_once(">:"))
                    .map(|(socket, _)| PathBuf::from(socket))
            })
            .ok_or_else(|| "mount has no local socket source".into())
    }

    #[test]
    #[ignore = "requires a live macOS NFS mount"]
    fn macos_revalidation_shows_changes_made_around_the_mount() -> TestResult {
        let (temporary, source, mut mount) = live_mount()?;
        let (changed, created) = (
            temporary.path().join("changed"),
            temporary.path().join("created"),
        );
        let names = || -> std::io::Result<Vec<_>> {
            std::fs::read_dir(temporary.path())?
                .map(|entry| entry.map(|entry| entry.file_name()))
                .collect()
        };
        // Cache the file's attributes, the listing, and the absent name.
        std::fs::write(&changed, b"old")?;
        assert_eq!(std::fs::metadata(&changed)?.len(), 3);
        assert_eq!(names()?, ["changed"]);
        assert_eq!(
            std::fs::metadata(&created).err().map(|error| error.kind()),
            Some(std::io::ErrorKind::NotFound)
        );
        let began = Instant::now();
        mount.revalidate()?;
        assert!(
            began.elapsed() < REPLY_DELIVERY_SLACK,
            "the mount's own changes need no wait"
        );

        source.write_range(
            &MountPath::root().child(b"changed".to_vec()),
            0,
            Bytes::from_static(b"newer"),
        )?;
        source.create_file(
            &MountPath::root().child(b"created".to_vec()),
            FileMetadata::default(),
        )?;
        mount.revalidate()?;
        assert_eq!(std::fs::metadata(&changed)?.len(), 5);
        assert!(created.is_file(), "the absent name is looked up again");
        let mut listed = names()?;
        listed.sort();
        assert_eq!(listed, ["changed", "created"]);
        assert_eq!(std::fs::read(&changed)?, b"newer");
        assert!(mount.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live macOS NFS mount"]
    fn macos_server_answers_only_the_kernel() -> TestResult {
        use std::io::{Read as _, Write as _};
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let (temporary, _, mut mount) = live_mount()?;
        let socket = mount_socket(temporary.path())?;
        let directory = std::fs::metadata(socket.parent().ok_or("socket has no directory")?)?;
        assert_eq!(directory.permissions().mode() & 0o777, 0o700);
        // SAFETY: `getuid` has no preconditions.
        assert_eq!(directory.uid(), unsafe { libc::getuid() });

        // An NFS NULL call, as any local process could send it.
        let mut stream = std::os::unix::net::UnixStream::connect(&socket)?;
        // macOS refuses socket options (EINVAL) once the peer has closed, as
        // this server may already have done; the read then ends at once.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
        let call: [u32; 11] = [0x8000_0028, 1, 0, 2, 100_003, 4, 0, 0, 0, 0, 0];
        let bytes = call
            .iter()
            .flat_map(|word| word.to_be_bytes())
            .collect::<Vec<_>>();
        let _ = stream.write_all(&bytes);
        let mut reply = [0_u8; 4];
        assert_eq!(
            stream.read(&mut reply).unwrap_or(0),
            0,
            "the server closes a user process's connection unanswered"
        );
        std::fs::write(temporary.path().join("still-served"), b"kernel")?;
        assert_eq!(
            std::fs::read(temporary.path().join("still-served"))?,
            b"kernel"
        );
        assert!(mount.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live macOS NFS mount"]
    fn macos_locks_exclude_other_processes() -> TestResult {
        use std::os::fd::AsRawFd as _;

        let (temporary, _, mut mount) = live_mount()?;
        let path = temporary.path().join("locked");
        std::fs::write(&path, b"")?;
        // The NFS client keys locks by process, so another process contends.
        let contender = |operation: &str| {
            acyclic_native_runtime::process_status(Command::new("/usr/bin/perl")
                .arg("-e")
                .arg(format!(
                    "use Fcntl qw(:flock :DEFAULT); open(F, '+<', $ARGV[0]) or die;                      exit({operation} ? 0 : 1)"
                ))
                .arg(&path), std::time::Duration::from_secs(120))
                .map(|status| status.success())
        };
        let flock = "flock(F, LOCK_EX | LOCK_NB)";
        let fcntl = "fcntl(F, F_SETLK, my $region = pack('q q l s s', 0, 0, 0, F_WRLCK, 0))";
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        // SAFETY: `file` owns a live descriptor for each call.
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) }, 0);
        assert!(!contender(flock)?, "a held flock excludes another process");
        // SAFETY: as above.
        assert_eq!(unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) }, 0);
        assert!(contender(flock)?, "a released flock admits another process");
        let region = libc::flock {
            l_start: 0,
            l_len: 0,
            l_pid: 0,
            l_type: libc::c_short::try_from(libc::F_WRLCK)?,
            l_whence: libc::c_short::try_from(libc::SEEK_SET)?,
        };
        assert_eq!(
            // SAFETY: `region` outlives the call on the live descriptor.
            unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &raw const region) },
            0
        );
        assert!(
            !contender(fcntl)?,
            "a held record lock excludes another process"
        );
        drop(file);
        assert!(contender(fcntl)?, "closing releases the record lock");
        assert!(mount.stop()?);
        Ok(())
    }

    #[test]
    #[ignore = "requires a live macOS NFS mount"]
    fn macos_mode_bits_deny_access() -> TestResult {
        use std::os::unix::fs::PermissionsExt as _;

        let (temporary, _, mut mount) = live_mount()?;
        let path = temporary.path().join("read-only");
        std::fs::write(&path, b"kept")?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444))?;
        assert_eq!(
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .err()
                .map(|error| error.kind()),
            Some(std::io::ErrorKind::PermissionDenied),
            "a read-only file refuses a writer"
        );
        let script = temporary.path().join("script");
        std::fs::write(&script, b"#!/bin/sh\nexit 0\n")?;
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o644))?;
        assert_eq!(
            acyclic_native_runtime::process_status(
                &mut Command::new(&script),
                std::time::Duration::from_secs(120)
            )
            .err()
            .map(|error| error.kind()),
            Some(std::io::ErrorKind::PermissionDenied),
            "a file without an execute bit does not run"
        );
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755))?;
        assert!(
            acyclic_native_runtime::process_status(
                &mut Command::new(&script),
                std::time::Duration::from_secs(120)
            )?
            .success()
        );
        assert_eq!(std::fs::read(&path)?, b"kept");
        assert!(mount.stop()?);
        Ok(())
    }

    #[test]
    fn directory_page_rejects_a_native_mutation_between_reads() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        source.create_file(
            &MountPath::root().child(b"in-page".to_vec()),
            FileMetadata::default(),
        )?;
        let root = MountPath::root();
        let mut directory =
            DirectoryHandle::new(root, source.binding_epoch(), context.cache_epochs());
        // `.`, `..`, and one entry delivered: a listing that handed out an
        // entry cannot continue in a later view.
        advance_directory_to_offset(&context, &mut directory, 3)
            .map_err(std::io::Error::from_raw_os_error)?;
        source.create_file(
            &MountPath::root().child(b"after-page".to_vec()),
            FileMetadata::default(),
        )?;
        assert_eq!(
            finish_directory_page(&context, &directory, false),
            Err(libc::ESTALE)
        );
        assert_eq!(
            ensure_directory_page(&context, &mut directory),
            Err(libc::ESTALE)
        );
        Ok(())
    }

    #[test]
    fn a_listing_that_delivered_nothing_restarts_in_the_new_view() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let root = MountPath::root();
        let mut directory =
            DirectoryHandle::new(root, source.binding_epoch(), context.cache_epochs());
        ensure_directory_page(&context, &mut directory)
            .map_err(std::io::Error::from_raw_os_error)?;
        source.create_file(
            &MountPath::root().child(b"after-page".to_vec()),
            FileMetadata::default(),
        )?;
        // Only `.` and `..` were delivered: the listing starts again.
        assert_eq!(finish_directory_page(&context, &directory, false), Ok(()));
        assert_eq!(ensure_directory_page(&context, &mut directory), Ok(()));
        Ok(())
    }

    #[test]
    fn directory_continuation_requires_its_listing_and_binding_unchanged() -> TestResult {
        let (source, context) = checkout_context(MountPublication::Manual)?;
        let nested = MountPath::root().child(b"nested".to_vec());
        source.create_directory(&nested, FileMetadata::default())?;
        let epochs = context.cache_epochs().ok_or("checkout has no view stamp")?;
        let root = DirectoryHandle::new(MountPath::root(), Some(epochs.binding), Some(epochs));
        let directory = DirectoryHandle::new(nested.clone(), Some(epochs.binding), Some(epochs));
        let rebound = DirectoryHandle::new(
            nested.clone(),
            Some(epochs.binding + 1),
            Some(CacheEpochs {
                binding: epochs.binding + 1,
                ..epochs
            }),
        );
        assert!(root.is_current(&context));
        assert!(directory.is_current(&context));
        assert!(!rebound.is_current(&context));
        assert!(directory.can_reuse_pages());

        source.create_file(
            &MountPath::root().child(b"sibling".to_vec()),
            FileMetadata::default(),
        )?;
        assert!(!root.is_current(&context));
        assert!(directory.is_current(&context));
        source.create_file(&nested.child(b"child".to_vec()), FileMetadata::default())?;
        assert!(!directory.is_current(&context));

        let unrecorded =
            DirectoryHandle::new(nested, source.binding_epoch(), context.cache_epochs());
        assert!(unrecorded.is_current(&context));
        context.forget_view();
        assert!(
            !unrecorded.is_current(&context),
            "a forgotten view invalidates every page"
        );

        let epochless = DirectoryHandle::new(MountPath::root(), None, None);
        assert!(!epochless.is_current(&context));
        assert!(!epochless.can_reuse_pages());
        Ok(())
    }

    #[test]
    fn stalled_mount_loop_keeps_destination_fenced_until_callbacks_finish() {
        let (release, waiting) = std::sync::mpsc::channel::<()>();
        let (exited, complete) = std::sync::mpsc::channel::<()>();
        let events = Arc::new(LoopEvents::default());
        let exit_report = ExitReport(Arc::clone(&events));
        let thread = std::thread::spawn(move || {
            let _exit_report = exit_report;
            let _ = waiting.recv();
            let _ = exited.send(());
            0
        });
        let mut session = DarwinMountSession {
            resources: None,
            destination: PathBuf::new(),
            thread: Some(thread),
            events,
            loop_finished_before_teardown: None,
            loop_result: None,
            teardown_complete: false,
        };
        let start = Instant::now();
        let result = session.finish_thread_within(Duration::from_millis(20));
        assert!(matches!(result, MountLoopResult::TimedOut));
        assert!(classify_detached_loop(&result, UnmountEvidence::Detached, false).is_err());
        assert!(start.elapsed() < Duration::from_secs(1));
        assert!(session.thread.is_some());
        let _ = release.send(());
        assert!(complete.recv_timeout(Duration::from_secs(1)).is_ok());
        let result = session.finish_thread_within(Duration::from_secs(1));
        assert!(matches!(result, MountLoopResult::Exited(0)));
        assert!(classify_detached_loop(&result, UnmountEvidence::Detached, false).is_ok());
        assert!(session.thread.is_none());
        // This synthetic session has no destination or native driver to stop.
        session.teardown_complete = true;
    }

    #[test]
    fn mount_loop_reports_wake_waiters_and_survive_an_unwinding_loop() {
        const LONG: Duration = Duration::from_secs(60);
        let events = Arc::new(LoopEvents::default());
        let silent = events.wait_until(Duration::from_millis(20), |state| state.mounted);
        assert!(!silent.mounted && !silent.exited, "nothing was reported");

        let (proceed, proceeding) = std::sync::mpsc::channel::<()>();
        let exit_report = ExitReport(Arc::clone(&events));
        let thread = std::thread::spawn(move || {
            // SAFETY: `exit_report` keeps the events alive across the call,
            // as the mount loop thread does.
            unsafe { report_mounted(Arc::as_ptr(&exit_report.0).cast_mut().cast()) };
            let _ = proceeding.recv();
            std::panic::resume_unwind(Box::new("the loop unwinds instead of returning"));
        });
        let began = Instant::now();
        let mounted = events.wait_until(LONG, |state| state.mounted);
        assert!(mounted.mounted && !mounted.exited);
        let _ = proceed.send(());
        let exited = events.wait_until(LONG, |state| state.exited);
        assert!(exited.exited, "an unwinding loop still reports its exit");
        assert!(
            began.elapsed() < LONG / 2,
            "reports wake waiters, not timeouts"
        );
        assert!(thread.join().is_err());
    }
