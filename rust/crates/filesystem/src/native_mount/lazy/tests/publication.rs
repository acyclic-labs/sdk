use super::*;

#[test]
    fn listed_names_are_decoded_by_their_declared_encoding()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{LogicalName, NameEncoding};

        let text = "dir-\u{e9}";
        let utf16 = text
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        let windows = LogicalName::new(NameEncoding::WindowsUtf16Le, utf16.clone(), 510)?;
        let portable = LogicalName::new(NameEncoding::Utf8, text.as_bytes().to_vec(), 255)?;
        for name in [&windows, &portable] {
            assert_eq!(
                logical_child_path("/parent", name).as_deref(),
                Some("/parent/dir-\u{e9}")
            );
        }
        if cfg!(windows) {
            for name in [&windows, &portable] {
                assert_eq!(crate::native_mount::adapter::native_mount_name(name)?, utf16);
            }
        } else {
            assert_eq!(
                crate::native_mount::adapter::native_mount_name(&portable)?,
                text.as_bytes()
            );
        }
        Ok(())
    }

#[test]
    fn rebound_prunes_only_unreferenced_removed_identities()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::InlineFileData;
        use crate::{Digest, ObjectId, ObjectKind};

        let first = FileId::from_bytes([1; 16]);
        let second = FileId::from_bytes([2; 16]);
        let payload = InlineFileData::new(b"x")?;
        let record = |file_id| {
            (
                FileRecord {
                    file_id,
                    kind: FileKind::Regular,
                    link_count: 1,
                    metadata: ObjectId {
                        kind: ObjectKind::Metadata,
                        digest: Digest::from_bytes([0; 32]),
                    },
                    payload: FilePayload::InlineRegular(payload),
                },
                FileMetadata::default(),
            )
        };
        let mut removals = MountedRemovals {
            paths: BTreeMap::from([
                ("/a".to_owned(), first),
                ("/b".to_owned(), first),
                ("/c".to_owned(), second),
            ]),
            records: BTreeMap::from([(first, record(first)), (second, record(second))]),
            epoch: 0,
        };
        assert!(removals.rebound("/a"));
        assert!(removals.records.contains_key(&first));
        assert!(removals.rebound("/b"));
        assert!(!removals.records.contains_key(&first));
        assert!(removals.records.contains_key(&second));
        assert!(removals.rebound("/c"));
        assert!(removals.records.is_empty());
        assert!(!removals.rebound("/c"));
        assert_eq!(removals.epoch, 3);
        Ok(())
    }

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn rebound_does_not_publish_a_stale_removed_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("a"), b"old-a")?;
        std::fs::write(source_root.path().join("b"), b"old-b")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                source_root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "mounted-rebound-identity",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .engine_checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned())?;
        let path = |name: &str| {
            MountPath::root().child(if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            })
        };
        let a = path("a");
        let b = path("b");
        let _open_a = view.open_file(&a)?;
        let _open_b = view.open_file(&b)?;
        let old_a = view.lookup(&a)?.expect("source a").node.file_id;
        let old_b = view.lookup(&b)?.expect("source b").node.file_id;
        view.remove(&a, Some(old_a))?;
        view.create_file(&a, FileMetadata::default())?;
        view.write_range(&a, 0, Bytes::from_static(b"new-a"))?;
        view.remove(&b, Some(old_b))?;
        {
            let removals = view.removals.lock().map_err(|_| "poisoned removals")?;
            assert_eq!(removals.paths.len(), 1);
            assert_eq!(removals.records.len(), 1);
            assert!(removals.records.contains_key(&old_b));
        }
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/a", 0, 5).await?.as_ref(), b"new-a");
        assert!(matches!(
            lazy.lookup("/b").await,
            Err(LazyWorkspaceError::NotFound)
        ));
        Ok(())
    }

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn an_advance_records_only_a_moved_head_or_source()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_root = tempfile::tempdir()?;
        let (demand, view, recorded) = recorded_view(source_root.path()).await?;
        let changes = || recorded.0.lock().map(|positions| positions.len());
        let binding = view.binding_epoch();

        view.advance_to_head_async().await?;
        assert_eq!(changes().map_err(|_| "poisoned observer")?, 0);
        assert_eq!(
            view.binding_epoch(),
            binding,
            "an advance that moved nothing keeps every binding"
        );

        demand.invalidate();
        view.advance_to_head_async().await?;
        assert!(
            changes().map_err(|_| "poisoned observer")? > 0,
            "a rebind to a new source epoch changes the view"
        );
        assert_ne!(view.binding_epoch(), binding);
        Ok(())
    }

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn removing_a_source_only_file_supersedes_its_cached_lookups()
    -> Result<(), Box<dyn std::error::Error>> {
        let source_root = tempfile::tempdir()?;
        let (_, view, recorded) = recorded_view(source_root.path()).await?;

        let path = MountPath::root().child(if cfg!(windows) {
            "source-only"
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect()
        } else {
            b"source-only".to_vec()
        });
        let stamp = view.view_stamp().ok_or("a lazy view carries stamps")?;
        let file_id = view.lookup(&path)?.ok_or("source-only file")?.node.file_id;
        assert!(view.unchanged_since(&path, Some(file_id), stamp));

        view.remove(&path, Some(file_id))?;

        assert_eq!(view.lookup(&path)?, None);
        assert!(!view.unchanged_since(&path, Some(file_id), stamp));
        assert!(!view.unchanged_since(&path, None, stamp));
        assert!(!view.unchanged_since(&MountPath::root(), None, stamp));
        let positions = recorded.0.lock().map_err(|_| "poisoned observer")?.clone();
        assert!(
            positions.iter().any(|position| *position > stamp),
            "observers learn of the removal"
        );
        Ok(())
    }

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn manual_fsync_publishes_nothing_until_sync() -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        let demand = Arc::new(
            NativeDemandSource::open(
                source_root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "manual-fsync",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .engine_checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned())?;
        let written = MountPath::root().child(if cfg!(windows) {
            "written"
                .encode_utf16()
                .flat_map(u16::to_le_bytes)
                .collect()
        } else {
            b"written".to_vec()
        });
        view.create_file(&written, FileMetadata::default())?;
        view.write_range(&written, 0, Bytes::from_static(b"fsynced"))?;
        view.flush()?;
        assert!(
            matches!(
                lazy.lookup("/written").await,
                Err(LazyWorkspaceError::NotFound)
            ),
            "a native fsync under manual publication publishes nothing"
        );
        view.sync_async().await?;
        assert_eq!(
            lazy.read_range("/written", 0, 7).await?.as_ref(),
            b"fsynced"
        );
        Ok(())
    }

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn replaced_source_identity_keeps_open_handle_and_surviving_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("a"), b"source")?;
        std::fs::write(source_root.path().join("c"), b"target")?;
        std::fs::hard_link(source_root.path().join("c"), source_root.path().join("d"))?;
        let demand = Arc::new(
            NativeDemandSource::open(
                source_root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "mounted-identity-replace",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .engine_checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned())?;
        let mount_path = |name: &str| {
            let bytes = if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            };
            MountPath::root().child(bytes)
        };
        let a = mount_path("a");
        let c = mount_path("c");
        let d = mount_path("d");
        let old = view.open_file(&c)?;
        view.rename(&a, &c, true)?;
        old.write_range(0, Bytes::from_static(b"update"))?;
        assert_eq!(view.read_range(&c, 0, 6)?.as_ref(), b"source");
        assert_eq!(view.read_range(&d, 0, 6)?.as_ref(), b"update");
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/c", 0, 6).await?.as_ref(), b"source");
        assert_eq!(lazy.read_range("/d", 0, 6).await?.as_ref(), b"update");
        old.write_range(0, Bytes::from_static(b"second!"))?;
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/c", 0, 6).await?.as_ref(), b"source");
        assert_eq!(lazy.read_range("/d", 0, 7).await?.as_ref(), b"second!");
        view.allocate_range(&d, 0, 1, MountRangeAllocation::PunchHole)?;
        view.sync_async().await?;
        assert_eq!(lazy.read_range("/d", 0, 7).await?.as_ref(), b"\0econd!");
        let current = view.open_file(&c)?;
        let current_id = current.lookup()?.node.file_id;
        view.remove(&c, Some(current_id))?;
        current.write_range(0, Bytes::from_static(b"orphan"))?;
        assert_eq!(current.read_range(0, 6)?.as_ref(), b"orphan");
        let mut external = lazy
            .workspace()
            .begin_transaction(IdempotencyKey::new())
            .await?;
        external
            .write("/external", Bytes::from_static(b"unrelated"))
            .await?;
        external.commit().await?;
        view.sync_async().await?;
        assert!(matches!(
            lazy.lookup("/c").await,
            Err(LazyWorkspaceError::NotFound)
        ));
        Ok(())
    }

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn conflicting_mount_removal_releases_publication_intent()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("file"), b"source")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                source_root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "conflicting-mount-removal",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .engine_checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned())?;
        let path = MountPath::root().child(if cfg!(windows) {
            "file".encode_utf16().flat_map(u16::to_le_bytes).collect()
        } else {
            b"file".to_vec()
        });
        let file = view.open_file(&path)?;
        view.remove(&path, Some(file.lookup()?.node.file_id))?;
        let mut conflicting = lazy
            .workspace()
            .begin_transaction(IdempotencyKey::new())
            .await?;
        conflicting
            .write("/file", Bytes::from_static(b"external"))
            .await?;
        conflicting.commit().await?;
        assert!(matches!(
            view.sync_async().await,
            Err(MountSourceError::Stale)
        ));
        assert!(
            !view
                .authored
                .shared_checkout()
                .lock()
                .await
                .has_retained_operation()
        );
        view.recover_pending_mount_change(crate::PublicationPermit::Unrestricted)
            .await?;
        assert!(lazy.mount_removal_publication().await?.is_none());
        Ok(())
    }

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn remembered_resolutions_follow_every_change_to_the_view()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::write(source_root.path().join("a"), b"source")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                source_root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "mounted-resolutions",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .engine_checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned())?;
        let mount_path = |name: &str| {
            let bytes = if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            };
            MountPath::root().child(bytes)
        };
        let (a, b) = (mount_path("a"), mount_path("b"));

        // Remembered answers: a source node, and an absence.
        let source = view.lookup(&a)?.ok_or("source file absent")?;
        assert_eq!(view.lookup(&a)?, Some(source));
        assert_eq!(view.lookup(&b)?, None);
        assert_eq!(view.lookup(&b)?, None);

        // A create through the view replaces the remembered absence.
        let created = view.create_file(&b, FileMetadata::default())?;
        assert_eq!(
            view.lookup(&b)?.map(|lookup| lookup.node.file_id),
            Some(created.node.file_id)
        );

        // A write through a handle promotes the file; its answer follows.
        view.open_file(&a)?
            .write_range(0, Bytes::from_static(b"authored-longer"))?;
        assert_eq!(
            view.lookup(&a)?.map(|lookup| lookup.node.logical_bytes),
            Some(15)
        );

        // A removal replaces the remembered answer with an absence.
        view.remove(&a, None)?;
        assert_eq!(view.lookup(&a)?, None);
        assert!(matches!(
            view.open_file(&a),
            Err(MountSourceError::NotFound)
        ));

        // Names beneath a directory the source lacks are absent until created.
        let fresh = mount_path("fresh");
        let inner = fresh.child(mount_path("inner").components()[0].clone());
        assert_eq!(view.lookup(&fresh)?, None);
        view.create_directory(&fresh, FileMetadata::default())?;
        assert_eq!(view.lookup(&inner)?, None);
        let created = view.create_file(&inner, FileMetadata::default())?;
        assert_eq!(
            view.lookup(&inner)?.map(|lookup| lookup.node.file_id),
            Some(created.node.file_id)
        );

        // A source file changed outside the view since it was remembered is
        // opened at its current version, never served stale.
        std::fs::write(source_root.path().join("c"), b"first")?;
        let c = mount_path("c");
        assert!(view.lookup(&c)?.is_some());
        std::fs::write(source_root.path().join("c"), b"second!")?;
        assert_eq!(view.open_file(&c)?.read_range(0, 7)?.as_ref(), b"second!");
        Ok(())
    }

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn promotion_and_attribute_reads_leave_cached_lookups_current()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        let source_root = tempfile::tempdir()?;
        std::fs::create_dir(source_root.path().join("d"))?;
        std::fs::write(source_root.path().join("d").join("f"), b"source")?;
        std::fs::write(source_root.path().join("g"), b"other")?;
        let demand = Arc::new(
            NativeDemandSource::open(
                source_root.path(),
                FilesystemProfile::Portable,
                VolumeLimits::default(),
            )
            .await?,
        );
        let fs = Fs::memory();
        let lazy = Arc::new(
            LazyWorkspace::attach(
                &fs,
                "mounted-promotion-effect",
                demand,
                MemoryLazyWorkspaceStore::default(),
            )
            .await?,
        );
        let checkout = lazy
            .workspace()
            .engine_checkout(
                GenerationSelector::Head,
                CheckoutMode::tracking_transaction(),
            )
            .await?;
        let config = checkout.volume_config();
        let authored = Arc::new(CheckoutMountSource::new(
            Arc::new(SharedCheckout::with_publication(
                checkout,
                MountPublication::Manual,
            )),
            config,
        )?);
        let view = LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned())?;
        let component = |name: &str| -> Vec<u8> {
            if cfg!(windows) {
                name.encode_utf16().flat_map(u16::to_le_bytes).collect()
            } else {
                name.as_bytes().to_vec()
            }
        };
        let directory = MountPath::root().child(component("d"));
        let file = directory.child(component("f"));
        let other = MountPath::root().child(component("g"));
        // NTFS reports a directory's write time, which creating an entry in
        // it changed, only once the directory is next read; let the source
        // report what reading it reports before the view is sampled.
        for path in [&directory, &file, &other] {
            view.lookup(path)?;
        }
        view.fence_changes()?;
        let before = [&directory, &file, &other].map(|path| view.lookup(path));
        let stamp = view.view_stamp().ok_or("lazy view has no stamp")?;

        // Reading attributes of a source file answers from the source.
        assert_eq!(view.read_attribute(&file, b"user.absent")?, None);
        assert!(view.list_attributes(&file, None, 8)?.names.is_empty());
        assert!(view.authored.lookup(&file)?.is_none());

        // A promotion records only the nodes whose answer changed. A source
        // watch may also report a conservative invalidation after the stamp
        // (FSEvents has no delivery barrier), so an unchanged answer may be
        // marked stale; the safety property is that a current answer never
        // survives with a different lookup.
        view.promote_locked(&file)?;
        assert!(view.authored.lookup(&file)?.is_some());
        let after = [&directory, &file, &other].map(|path| view.lookup(path));
        for ((path, before), after) in [&directory, &file, &other].iter().zip(before).zip(after) {
            let (before, after) = (before?, after?);
            let file_id = after.map(|lookup| lookup.node.file_id);
            assert!(
                !view.unchanged_since(path, file_id, stamp) || before == after,
                "a promotion left a current cache answer stale for {path:?}"
            );
        }
        Ok(())
    }

#[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
    #[tokio::test(flavor = "multi_thread")]
    async fn source_hard_link_open_unlink_write_preserves_surviving_alias()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::demand::native::NativeDemandSource;
        use crate::model::{CheckoutMode, FilesystemProfile, GenerationSelector, VolumeLimits};
        use crate::native_mount::{MountPublication, SharedCheckout};
        use crate::{Fs, MemoryLazyWorkspaceStore};

        for (write_before_unlink, write_before_first_sync, promote_survivor) in [
            (false, false, false),
            (false, true, false),
            (true, true, false),
            (true, true, true),
        ] {
            let source_root = tempfile::tempdir()?;
            std::fs::write(source_root.path().join("a"), b"source")?;
            std::fs::hard_link(source_root.path().join("a"), source_root.path().join("b"))?;
            let demand = Arc::new(
                NativeDemandSource::open(
                    source_root.path(),
                    FilesystemProfile::Portable,
                    VolumeLimits::default(),
                )
                .await?,
            );
            let fs = Fs::memory();
            let lazy = Arc::new(
                LazyWorkspace::attach(
                    &fs,
                    "mounted-identity-unlink",
                    demand,
                    MemoryLazyWorkspaceStore::default(),
                )
                .await?,
            );
            let checkout = lazy
                .workspace()
                .engine_checkout(
                    GenerationSelector::Head,
                    CheckoutMode::tracking_transaction(),
                )
                .await?;
            let config = checkout.volume_config();
            let authored = Arc::new(CheckoutMountSource::new(
                Arc::new(SharedCheckout::with_publication(
                    checkout,
                    MountPublication::Manual,
                )),
                config,
            )?);
            let view = LazyMountSource::new(Arc::clone(&lazy), authored, "/".to_owned())?;
            let mount_path = |name: &str| {
                let bytes = if cfg!(windows) {
                    name.encode_utf16().flat_map(u16::to_le_bytes).collect()
                } else {
                    name.as_bytes().to_vec()
                };
                MountPath::root().child(bytes)
            };
            let a = mount_path("a");
            let b = mount_path("b");
            let open = view.open_file(&a)?;
            let identity = open.lookup()?.node.file_id;
            assert!(
                view.prepare_removed_identity_candidate(
                    &BTreeMap::from([("/\0".to_owned(), identity,)]),
                    &[]
                )
                .await
                .is_err()
            );
            assert!(
                !view
                    .authored
                    .shared_checkout()
                    .lock()
                    .await
                    .has_retained_operation()
            );
            assert_eq!(
                view.lookup(&b)?.expect("surviving alias").node.file_id,
                identity
            );
            if write_before_unlink {
                open.write_range(0, Bytes::from_static(b"prime!"))
                    .expect("pre-unlink write");
            }
            let existing_alias = if promote_survivor {
                {
                    let _mutation = view.mutation_lease(None)?;
                    view.promote_locked(&b)?;
                }
                assert!(view.authored.lookup(&b)?.is_some());
                Some(view.open_file(&b)?)
            } else {
                None
            };
            view.remove(&a, Some(identity))
                .expect("unlink source alias");
            if write_before_first_sync {
                open.write_range(0, Bytes::from_static(b"update"))
                    .expect("post-unlink write");
            }
            let first = if write_before_first_sync {
                b"update"
            } else {
                b"source"
            };
            assert!(view.lookup(&a)?.is_none(), "removed name must stay absent");
            assert_eq!(view.open_file(&b)?.read_range(0, 6)?.as_ref(), first);
            if let Some(alias) = &existing_alias {
                assert_eq!(alias.read_range(0, 6)?.as_ref(), first);
            }
            view.sync_async().await?;
            assert!(
                view.lookup(&a)?.is_none(),
                "sync must not restore removed name"
            );
            assert_eq!(view.open_file(&b)?.read_range(0, 6)?.as_ref(), first);
            if promote_survivor {
                assert_eq!(view.authored.read_range(&b, 0, 6)?.as_ref(), first);
            }
            assert!(matches!(
                lazy.lookup("/a").await,
                Err(LazyWorkspaceError::NotFound)
            ));
            assert_eq!(lazy.read_range("/b", 0, 6).await?.as_ref(), first);
            open.write_range(0, Bytes::from_static(b"second!"))?;
            assert_eq!(view.lookup(&b)?.expect("alias").node.logical_bytes, 7);
            assert_eq!(view.read_range(&b, 0, 7)?.as_ref(), b"second!");
            assert_eq!(view.open_file(&b)?.read_range(0, 7)?.as_ref(), b"second!");
            if write_before_unlink {
                view.remove(&b, Some(identity))?;
                open.write_range(0, Bytes::from_static(b"orphan!"))?;
                assert_eq!(open.read_range(0, 7)?.as_ref(), b"orphan!");
                assert!(view.lookup(&b)?.is_none());
            }
            drop(existing_alias);
            drop(open);
            assert!(view.open_sources.lock().expect("open handles").is_empty());
            view.sync_async().await?;
            assert!(
                view.detached
                    .lock()
                    .expect("detached identities")
                    .is_empty()
            );
            if write_before_unlink {
                assert!(matches!(
                    lazy.lookup("/b").await,
                    Err(LazyWorkspaceError::NotFound)
                ));
            } else {
                assert_eq!(lazy.read_range("/b", 0, 7).await?.as_ref(), b"second!");
                assert_eq!(view.open_file(&b)?.read_range(0, 7)?.as_ref(), b"second!");
            }
        }
        Ok(())
    }

#[test]
    fn abandoned_directory_cursors_are_bounded_and_cleared() {
        let cursors = CursorTable::new(2);
        let first = cursors.remember((2, 1), 1_u8).expect("first cursor");
        let second = cursors.remember((2, 1), 2_u8).expect("second cursor");
        let third = cursors.remember((2, 1), 3_u8).expect("third cursor");

        assert!(matches!(
            cursors.take((2, 1), Some(&first)),
            Err(MountSourceError::Stale)
        ));
        assert_eq!(
            cursors.take((2, 1), Some(&second)).expect("second token"),
            Some(2)
        );
        assert_eq!(
            cursors.take((2, 1), Some(&third)).expect("third token"),
            Some(3)
        );

        let stale = cursors.remember((2, 1), 4_u8).expect("stale cursor");
        assert!(matches!(
            cursors.take((3, 1), Some(&stale)),
            Err(MountSourceError::Stale)
        ));
        let retained = cursors.remember((2, 1), 5_u8).expect("retained cursor");
        cursors.clear();
        assert!(matches!(
            cursors.take((2, 1), Some(&retained)),
            Err(MountSourceError::Stale)
        ));
    }

#[tokio::test]
    async fn source_view_gate_serializes_rebinds_and_fences_old_handles() {
        let gate = Arc::new(SourceViewGate::new());
        let initial = Arc::clone(&gate).read(None).await.expect("initial lease");
        assert_eq!(initial.generation, SourceViewGate::INITIAL_GENERATION);

        let waiting_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { waiting_gate.write().await });
        tokio::task::yield_now().await;
        assert!(
            !writer.is_finished(),
            "a rebind must wait for active readers"
        );
        drop(initial);

        let writer = writer.await.expect("writer task");
        gate.begin_transition();
        assert!(!gate.is_stable());
        gate.finish_transition();
        drop(writer);

        assert!(matches!(
            Arc::clone(&gate)
                .read(Some(SourceViewGate::INITIAL_GENERATION))
                .await,
            Err(MountSourceError::Stale)
        ));
        assert_eq!(
            Arc::clone(&gate)
                .read(None)
                .await
                .expect("new lease")
                .generation,
            SourceViewGate::INITIAL_GENERATION + 2
        );
    }

#[tokio::test]
    async fn source_view_gate_excludes_authored_mutations_from_read_callbacks() {
        let gate = Arc::new(SourceViewGate::new());
        let reader = Arc::clone(&gate).read(None).await.expect("reader lease");
        let mutation_gate = Arc::clone(&gate);
        let mutation = tokio::spawn(async move {
            mutation_gate
                .write_stable(Some(SourceViewGate::INITIAL_GENERATION))
                .await
        });
        tokio::task::yield_now().await;
        assert!(
            !mutation.is_finished(),
            "an authored mutation must wait for active read callbacks"
        );
        drop(reader);

        let mutation = mutation
            .await
            .expect("mutation task")
            .expect("mutation lease");
        assert_eq!(mutation.generation, SourceViewGate::INITIAL_GENERATION);
        let reader_gate = Arc::clone(&gate);
        let reader = tokio::spawn(async move { reader_gate.read(None).await });
        tokio::task::yield_now().await;
        assert!(
            !reader.is_finished(),
            "a read callback must wait for an authored mutation"
        );
        drop(mutation);
        assert_eq!(
            reader
                .await
                .expect("reader task")
                .expect("reader lease")
                .generation,
            SourceViewGate::INITIAL_GENERATION
        );
    }

#[tokio::test]
    async fn source_view_gate_allows_nested_callback_reads_while_writer_waits() {
        let gate = Arc::new(SourceViewGate::new());
        let owner = SourceViewGate::callback_owner();
        let outer = Arc::clone(&gate)
            .read_for_callback(owner, None)
            .await
            .expect("outer lease");
        let writer_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { writer_gate.write().await });
        tokio::task::yield_now().await;
        assert!(!writer.is_finished(), "writer must wait for outer callback");

        let nested = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            Arc::clone(&gate).read_for_callback(owner, Some(outer.generation)),
        )
        .await
        .expect("a callback must not deadlock when its source method reacquires the view")
        .expect("nested lease");
        drop(nested);
        drop(outer);
        drop(writer.await.expect("writer task"));
    }

#[tokio::test]
    async fn source_view_gate_gives_a_waiting_writer_priority_over_new_callbacks() {
        let gate = Arc::new(SourceViewGate::new());
        let active = Arc::clone(&gate).read(None).await.expect("active reader");
        let writer_gate = Arc::clone(&gate);
        let writer = tokio::spawn(async move { writer_gate.write().await });
        tokio::task::yield_now().await;
        assert!(!writer.is_finished(), "writer must wait for active reader");

        let later_gate = Arc::clone(&gate);
        let later = tokio::spawn(async move { later_gate.read(None).await });
        tokio::task::yield_now().await;
        assert!(
            !later.is_finished(),
            "a new callback bypassed a waiting writer"
        );

        drop(active);
        let writer = tokio::time::timeout(std::time::Duration::from_secs(1), writer)
            .await
            .expect("writer made no progress")
            .expect("writer task");
        assert!(!later.is_finished(), "reader crossed the active writer");
        drop(writer);
        drop(
            tokio::time::timeout(std::time::Duration::from_secs(1), later)
                .await
                .expect("later reader made no progress")
                .expect("reader task")
                .expect("reader lease"),
        );
    }
