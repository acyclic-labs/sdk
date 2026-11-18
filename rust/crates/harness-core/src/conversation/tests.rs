use super::*;

#[cfg(not(target_arch = "wasm32"))]
fn native_conversation_budget() -> acyclic_fs_work::performance::WorkBudget {
    acyclic_fs_work::performance::WorkBudget {
        allocation_operations: 100_000,
        peak_allocation_bytes: 16_777_216,
        bytes_copied: 16_777_216,
        bytes_encoded: 16_777_216,
        bytes_hashed: 16_777_216,
        items_examined: 16_777_216,
        ..acyclic_fs_work::performance::WorkBudget::default()
    }
}

#[path = "../../tests/conversation_contract/fixture.rs"]
mod fixture;
pub(super) use fixture::file;
use fixture::message;

mod history;
mod native;
mod security;
mod wire;
