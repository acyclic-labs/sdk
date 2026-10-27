//! Every reader-visible publication cut establishes the owner name's
//! durability before credentials escape. No threads or timing assumptions:
//! pause the filesystem publisher at each cut and invoke the real reader.
use super::*;
use std::io::Write as _;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tracing_subscriber::{layer::SubscriberExt as _, registry::LookupSpan};

#[derive(Clone, Default)]
struct Flushes(Arc<AtomicUsize>);

impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> tracing_subscriber::Layer<S> for Flushes {
    fn on_close(&self, id: tracing::span::Id, context: tracing_subscriber::layer::Context<'_, S>) {
        if let Some(span) = context.span(&id)
            && span.metadata().name() == "acyclic.runtime.sync_parent"
        {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Whether a reader can return credentials whose rename has not been
/// flushed. Enumerate create, partial write, file sync, rename, parent sync;
/// the writer takes no further steps while the reader runs.
fn every_owner_cut_is_durable(omit_reader_sync: bool) -> bool {
    let directory = tempfile::tempdir().unwrap();
    let project = LocalProject::Volume {
        directory: directory.path().to_path_buf(),
        volume: VolumeRef::new(
            crate::resources::ProviderRef::new(
                "local".into(),
                "filesystem".into(),
                "2".into(),
                &mut None,
            )
            .unwrap(),
            "project".into(),
            VolumeClass::Project,
            VolumeOwner::Project("project".into()),
            &mut None,
        )
        .unwrap(),
    };
    let owner = Owner::open(directory.path(), &project, None).unwrap();
    let plain =
        acyclic_native_runtime::owner_record::SecretBytes(serde_json::to_vec(&owner).unwrap());
    let mut bytes =
        acyclic_native_runtime::owner_record::encode_with(&plain.0, 65_536, None, |parts| {
            Ok::<_, std::io::Error>(parts.concat())
        })
        .unwrap();
    let mut safe = true;
    for cut in 0..5 {
        let directory = tempfile::tempdir().unwrap();
        let temporary = directory.path().join(".owner.next");
        let path = directory.path().join("owner.json");
        let mut file = acyclic_native_runtime::private_file::create(&temporary).unwrap();
        if cut == 1 {
            file.write_all(&bytes[..bytes.len() / 2]).unwrap();
        } else if cut >= 2 {
            file.write_all(&bytes).unwrap();
            file.sync_all().unwrap();
        }
        drop(file);
        if cut >= 3 {
            fs::rename(&temporary, &path).unwrap();
        }
        if cut == 4 {
            acyclic_native_runtime::sync_parent(
                directory.path(),
                acyclic_native_runtime::Durability::Full,
            )
            .unwrap();
        }
        let flushes = Flushes::default();
        let _subscriber =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(flushes.clone()));
        let read = if omit_reader_sync && cut >= 3 {
            // Negative control: independently decode a visible record while
            // omitting the shared private reader's mandatory parent sync.
            let bytes = fs::read(&path).unwrap();
            acyclic_native_runtime::owner_record::decode_with(&bytes, 65_536, None, |plain| {
                Ok::<_, std::io::Error>(serde_json::from_slice::<Owner>(plain).unwrap())
            })
            .map_err(storage)
        } else {
            Owner::load(&path)
        };
        match read {
            Err(Error::NotFound(_)) => assert!(cut < 3),
            Ok(read) => {
                assert!(cut >= 3);
                assert_eq!(read.key_ref, owner.key_ref);
                assert_eq!(read.task, owner.task);
                let synced = flushes.0.load(Ordering::Relaxed);
                assert_eq!(synced, usize::from(!omit_reader_sync));
                safe &= cut == 4 || synced == 1;
            }
            Err(error) => panic!("cut {cut}: {error}"),
        }
    }
    bytes.fill(0);
    safe
}

#[test]
fn every_owner_publication_cut_returns_only_durable_credentials() {
    assert!(every_owner_cut_is_durable(false));
}

#[test]
fn negative_control_an_owner_reader_can_outrun_the_publishers_flush() {
    assert!(!every_owner_cut_is_durable(true));
}

#[test]
fn private_legacy_owner_migrates_once_without_changing_its_original_identity() {
    let directory = tempfile::tempdir().unwrap();
    let private = directory.path().join("private");
    acyclic_native_runtime::private_file::create_directory_all(&private).unwrap();
    let path = private.join("owner.json");
    let key = vec![71_u8; 32];
    let owner: Owner = serde_json::from_value(serde_json::json!({
        "key": key, "agent": AgentId::from_bytes([72; 16]),
        "task": TaskId::from_bytes([73; 16]),
        "project": VolumeRef::new(
            crate::resources::ProviderRef::new("local".into(), "filesystem".into(), "2".into(), &mut None).unwrap(),
            "project".into(), VolumeClass::Project, VolumeOwner::Project("project".into()), &mut None).unwrap(),
        "workspace": null, "workspace_identity": null, "workspace_incarnation": null
    })).unwrap();
    #[cfg(not(windows))]
    let mut file = acyclic_native_runtime::private_file::create(&path).unwrap();
    // Reproduce the former writer's inherited Windows DACL, not the new
    // private writer. The strict reader is the old rejected-upgrade control.
    #[cfg(windows)]
    let mut file = fs::File::create(&path).unwrap();
    let mut legacy = serde_json::to_value(&owner).unwrap();
    legacy.as_object_mut().unwrap().remove("key_ref");
    legacy
        .as_object_mut()
        .unwrap()
        .insert("key".into(), serde_json::json!(vec![71_u8; 32]));
    serde_json::to_writer(&mut file, &legacy).unwrap();
    drop(file);
    assert_eq!(fs::read(&path).unwrap().first(), Some(&b'{')); // Old plaintext reader control.
    #[cfg(windows)]
    assert!(acyclic_native_runtime::private_file::read(&path, 65_536).is_err());
    let migrated = Owner::load(&path).unwrap();
    assert!(migrated.key.is_none());
    assert!(
        migrated
            .key_ref
            .unwrap()
            .with_seed(|seed| *seed == [71; 32])
            .unwrap()
    );
    assert_eq!(migrated.agent, owner.agent);
    assert_eq!(migrated.task, owner.task);
    let protected = fs::read(&path).unwrap();
    assert!(protected.starts_with(acyclic_native_runtime::owner_record::MAGIC));
    let plain =
        acyclic_native_runtime::owner_record::decode_with(&protected, 65_536, None, |bytes| {
            Ok::<_, std::io::Error>(bytes.to_vec())
        })
        .unwrap();
    let metadata: serde_json::Value = serde_json::from_slice(&plain).unwrap();
    assert!(
        metadata.get("key").is_none(),
        "migration retained a plaintext seed"
    );
    assert!(metadata.get("key_ref").is_some());
    let reopened = Owner::load(&path).unwrap();
    assert_eq!(reopened.key_ref, migrated.key_ref);
    assert_eq!(fs::read(&path).unwrap(), protected);
}

#[test]
fn concurrent_owner_migration_cannot_publish_another_callers_provisional_key() -> Result<()> {
    use acyclic_native_runtime::{
        FileLock, FileLockMode, owner_record, private_file, signing_key::SigningKeyReference,
    };
    // Enumerate the former interleaving and the serialized path, using real
    // credential operations and lock contention rather than thread timing.
    for serialized in [false, true] {
        let directory = tempfile::tempdir().map_err(storage)?;
        let path = directory.path().join("owner.json");
        let owner = Owner {
            key: Some(owner_record::new_key().map_err(storage)?),
            key_ref: None,
            agent: AgentId::new(),
            task: TaskId::new(),
            project: VolumeRef::new(
                crate::resources::ProviderRef::new(
                    "local".into(),
                    "filesystem".into(),
                    "2".into(),
                    &mut None,
                )?,
                "project".into(),
                VolumeClass::Project,
                VolumeOwner::Project("project".into()),
                &mut None,
            )?,
            workspace: None,
            workspace_identity: None,
            workspace_incarnation: None,
        };
        let seed = owner
            .key
            .as_ref()
            .ok_or_else(|| Error::Invalid("missing fixture seed".into()))?;
        let mut legacy =
            serde_json::to_value(&owner).map_err(|error| Error::Invalid(error.to_string()))?;
        legacy
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("fixture owner is not an object".into()))?
            .insert(
                "key".into(),
                serde_json::to_value(seed).map_err(|error| Error::Invalid(error.to_string()))?,
            );
        serde_json::to_writer(private_file::create(&path).map_err(storage)?, &legacy)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let lock = FileLock::acquire(
            private_file::open_or_create(&path.with_extension("lock")).map_err(storage)?,
            FileLockMode::Exclusive,
        )
        .map_err(storage)?;
        let first = SigningKeyReference::retain(seed).map_err(storage)?;
        let first_ref = first.reference();
        if serialized {
            TRY_OWNER_LOCK.set(true);
            let contender = Owner::load(&path);
            TRY_OWNER_LOCK.set(false);
            assert!(
                matches!(contender, Err(Error::Storage(_))),
                "contender bypassed publication ownership"
            );
            assert_eq!(fs::read(&path).map_err(storage)?.first(), Some(&b'{'));
        } else {
            // Actual former reader: reuse the first call's not-yet-published
            // credential and publish it while that call still owns cleanup.
            let contender = Owner::load_record(&path)?;
            assert_eq!(contender.key_ref, Some(first_ref));
        }
        drop(first);
        assert!(first_ref.with_seed(|_| ()).is_err());
        drop(lock);
        if serialized {
            let winner = Owner::load(&path)?;
            let reference = winner
                .key_ref
                .ok_or_else(|| Error::Invalid("missing published key".into()))?;
            assert!(
                reference
                    .with_seed(|stored| stored == seed)
                    .map_err(storage)?
            );
            assert_eq!(Owner::load(&path)?.key_ref, Some(reference));
        } else {
            assert!(
                Owner::load(&path).is_err(),
                "former interleaving did not lose published custody"
            );
        }
    }
    Ok(())
}
