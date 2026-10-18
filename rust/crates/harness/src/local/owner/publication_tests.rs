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
    let mut bytes = serde_json::to_vec(&owner).unwrap();
    let mut safe = true;
    for cut in 0..5 {
        let directory = tempfile::tempdir().unwrap();
        let temporary = directory.path().join(".owner.next");
        let path = directory.path().join("owner.json");
        let mut file = fs::OpenOptions::new();
        file.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            file.mode(0o600);
        }
        let mut file = file.open(&temporary).unwrap();
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
        publication::SKIP_READER_SYNC.set(omit_reader_sync);
        let read = Owner::load(&path);
        publication::SKIP_READER_SYNC.set(false);
        match read {
            Err(Error::NotFound(_)) => assert!(cut < 3),
            Ok(read) => {
                assert!(cut >= 3);
                assert_eq!(read.key, owner.key);
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
