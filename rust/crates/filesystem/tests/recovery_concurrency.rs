//! Deterministic recovery and concurrency conformance for durable filesystem protocols.
#![allow(
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::manual_let_else,
    clippy::panic,
    reason = "test fixtures use explicit fatal assertions to keep exhaustive state-machine cases readable"
)]

use acyclic_fs::{
    Digest, GenerationId, JournaledMaterializer, MaterializationBackend, MaterializationEdit,
    MaterializationError, MaterializationIntent, MaterializationJournal,
    MaterializationJournalStore, MaterializationPhase, MaterializationPlan,
    MaterializationPreimage, MaterializationRecovery, OperationId, OperationWindowCoordinator,
    OperationWindowError, OperationWindowFinish, OperationWindowPhase, OperationWindowSnapshot,
    OperationWindowStore, WorkspaceId,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use thiserror::Error;
use tokio::sync::Barrier;

fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

fn permit()
-> std::future::Ready<Result<(), MaterializationError<CrashStoreError, TreeBackendError>>> {
    std::future::ready(Ok(()))
}

use acyclic_fs as sdk;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_fs::{
    CancellationToken, NativeMaterializationWork, NativeTreeMaterializationBackend, WorkBudget,
};
#[path = "../src/materializer/memory.rs"]
mod memory;
use memory::{Store, StoreError};
#[path = "recovery_concurrency/backend.rs"]
mod backend;
#[path = "recovery_concurrency/capture.rs"]
mod capture;
#[path = "recovery_concurrency/journal.rs"]
mod journal;
#[path = "recovery_concurrency/mutation.rs"]
mod mutation;
#[cfg(not(target_arch = "wasm32"))]
#[path = "recovery_concurrency/native.rs"]
mod native;
#[path = "recovery_concurrency/recovery.rs"]
mod recovery;
#[path = "recovery_concurrency/window.rs"]
mod window;
use backend::{MutationCrashBackend, TreeBackend, TreeBackendError, source_tree, target_tree};
use journal::{CrashJournalStore, CrashStoreError};

fn plan(case: u8) -> MaterializationPlan {
    MaterializationPlan {
        operation_id: OperationId::from_bytes([case; 16]),
        intent: MaterializationIntent::GenerationTransition {
            from: generation(1),
            to: generation(2),
        },
        edits: vec![
            MaterializationEdit::Install {
                path: "a".to_owned(),
                image: b"new-a".to_vec(),
            },
            MaterializationEdit::Remove {
                path: "b".to_owned(),
            },
            MaterializationEdit::Rename {
                from: "from".to_owned(),
                to: "to".to_owned(),
            },
        ],
    }
}
