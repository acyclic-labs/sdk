use super::*;
use crate::{
    client::{CommandCatalog, CommandSession},
    conversation::ContentResidencyVerifier,
    task_volume::{Snapshot, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;

struct NoReads;
impl ContentResidencyVerifier for NoReads {}

/// The checked property: headless input read at a snapshot of the task files
/// runs a command the TUI saved under a new name and rejects the builtin
/// name it replaced. `catalog` is how the resolver derives its catalog.
fn assert_headless_reads_the_saved_catalog(catalog: fn(&Snapshot) -> CommandCatalog) {
    futures::executor::block_on(async {
        let volume = Arc::new(
            TaskVolume::open(
                Arc::new(MemoryStream::default()),
                StreamPath::new("headless-catalog").unwrap(),
            )
            .await
            .unwrap(),
        );
        let mut tui =
            CommandSession::open(volume.clone(), uuid::Uuid::from_bytes([1; 16]), 65_536, 64)
                .await
                .unwrap();
        let mut saved: Vec<_> = tui.catalog().commands().cloned().collect();
        let shell = saved.iter_mut().find(|spec| spec.name == "shell").unwrap();
        shell.name = "saved-shell".into();
        shell.aliases.clear();
        tui.install_catalog(serde_json::to_vec(&saved).unwrap())
            .await
            .unwrap();
        let snapshot = volume
            .snapshot(&CommandCatalog::saved_query().unwrap(), 1)
            .await
            .unwrap();
        let catalog = catalog(&snapshot);
        let operation = OperationId::from_bytes([2; 16]);
        let saved = headless_input(&catalog, &NoReads, operation, "/saved-shell echo hi").await;
        assert!(
            saved.is_ok(),
            "a saved command is unknown under -p: {saved:?}"
        );
        let removed = headless_input(&catalog, &NoReads, operation, "/shell echo hi").await;
        assert!(removed.is_err(), "a removed builtin still runs under -p");
    });
}

#[test]
fn headless_input_resolves_from_the_catalog_file() {
    assert_headless_reads_the_saved_catalog(|snapshot| {
        CommandCatalog::saved(snapshot, super::super::local_headless::MAXIMUM_BYTES as u64).unwrap()
    });
}

/// The resolver before this change captured builtins and ignored the file.
#[test]
#[should_panic(expected = "a saved command is unknown under -p")]
fn a_builtin_capture_is_detected_negative_control() {
    assert_headless_reads_the_saved_catalog(|_| CommandCatalog::builtins().unwrap());
}

/// Before any catalog is saved, headless input sees the builtins.
#[test]
fn an_unsaved_catalog_is_the_builtins() {
    futures::executor::block_on(async {
        let volume = TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("headless-builtins").unwrap(),
        )
        .await
        .unwrap();
        let snapshot = volume
            .snapshot(&CommandCatalog::saved_query().unwrap(), 1)
            .await
            .unwrap();
        let catalog = CommandCatalog::saved(&snapshot, 65_536).unwrap();
        let operation = OperationId::from_bytes([3; 16]);
        assert!(
            headless_input(&catalog, &NoReads, operation, "/shell echo hi")
                .await
                .is_ok()
        );
    });
}
