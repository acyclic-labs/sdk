//! Engine behaviour contracts: access, accounting, distribution and sessions.
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[cfg(not(loom))]
#[macro_use]
#[path = "support/unbounded.rs"]
mod unbounded;
#[cfg(not(loom))]
#[macro_use]
#[path = "support/provider_receipt.rs"]
mod provider_receipt;

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
mod computed_folds;
#[cfg(not(loom))]
mod core_group_commit;
#[cfg(not(loom))]
mod distributed;
#[cfg(not(loom))]
mod logical_objects;
#[cfg(not(loom))]
mod measured_fork;
#[cfg(not(loom))]
mod merge_lineage;

#[path = "cases/access.rs"]
#[cfg(not(loom))]
mod access;

#[path = "cases/path_properties.rs"]
#[cfg(not(loom))]
mod path_properties;

#[cfg(all(not(target_arch = "wasm32"), not(loom)))]
#[path = "cases/directory_cursor.rs"]
mod directory_cursor;

#[cfg(feature = "fuzzing")]
#[path = "cases/workspace_names.rs"]
#[cfg(not(loom))]
mod workspace_names;

#[path = "cases/cloud_contract.rs"]
#[cfg(not(loom))]
mod cloud_contract;

#[path = "cases/accounting_reconciliation.rs"]
#[cfg(not(loom))]
mod accounting_reconciliation;

#[path = "cases/accounting_layout.rs"]
#[cfg(not(loom))]
mod accounting_layout;

#[path = "cases/workspace_reads.rs"]
#[cfg(not(loom))]
mod workspace_reads;

#[path = "cases/session_fs.rs"]
#[cfg(not(loom))]
mod session_fs;

#[cfg(all(feature = "conformance", not(target_arch = "wasm32")))]
#[path = "cases/conformance.rs"]
#[cfg(not(loom))]
mod public_conformance;
