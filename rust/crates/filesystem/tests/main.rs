//! Integration behaviour tests.
//!
//! The behaviour suites live in their own binaries (`codec`, `native`, `engine`, `checkout`,
//! `workspace`, `git`, `wire`), so a test edit recompiles and relinks one small binary. This root
//! keeps the loom models, which the `--config=loom` leg names as `:main`, and the modules that
//! need the crate-root `native_sync` or `sync` shims.
#[cfg(loom)]
use acyclic_fs::NativeMountError;
#[cfg(not(target_arch = "wasm32"))]
#[cfg(not(loom))]
acyclic_test_support::counting_allocator!();

#[cfg(all(loom, not(target_arch = "wasm32")))]
#[path = "../src/sync.rs"]
mod sync;

#[cfg(all(loom, not(target_arch = "wasm32")))]
#[path = "../src/native_sync.rs"]
mod native_sync;

#[cfg(not(target_arch = "wasm32"))]
mod native_binding_model;
#[cfg(all(loom, not(target_arch = "wasm32"), feature = "model-checking"))]
mod native_view_table_loom;
#[cfg(feature = "model-checking")]
#[path = "operation_claim_model.rs"]
mod operation_claim_model;
#[cfg(all(loom, feature = "model-checking"))]
mod session_fs_model;

#[cfg(all(not(loom), any(target_os = "linux", target_os = "macos", windows)))]
#[path = "../src/native_sync/locks.rs"]
mod native_sync;

#[path = "native_view_table.rs"]
mod native_view_table;

#[cfg(feature = "browser")]
#[path = "browser_blob_custody.rs"]
mod browser_blob_custody;
#[cfg(feature = "browser")]
#[path = "browser_commit.rs"]
mod browser_commit;
