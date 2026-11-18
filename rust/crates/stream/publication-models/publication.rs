//! Loom models of stream publication. They exist only under `cfg(loom)`,
//! enabled for this package with
//! `bazel test --@rules_rust//rust/settings:per_crate_rustc_flag=//rust/crates/stream/publication-models:@--cfg=loom //rust/crates/stream/publication-models:publication`.
//! Without it the features stay additive and this target is empty; the
//! `conditional-watch-parity` proof's mutations fail if the models never run.
#![cfg(loom)]

mod error {
    use thiserror::Error;
    include!("../src/lib/error.rs");
}
pub use error::StreamError;
#[path = "../src/change.rs"]
mod change;
#[path = "../src/interest.rs"]
mod interest;
mod probe;
#[path = "../src/pull.rs"]
mod pull;
#[path = "../src/query_sync.rs"]
pub mod query_sync;
#[test]
fn lock_admission_and_cancellation_match_tokio() {
    loom::model(query_sync::parity::run);
}
#[test]
#[should_panic(expected = "granted reader excluded a compatible reader")]
fn lock_negative_control_retains_an_unpolled_reader_reservation() {
    loom::model(query_sync::parity::negative);
}
#[test]
fn watch_publication_cancellation_and_loss_match_tokio() {
    loom::model(query_sync::parity::watch_progress);
    for steps in 0u16..256 {
        loom::model(move || query_sync::parity::watch(steps, false));
    }
    for steps in 0u16..512 {
        loom::model(move || query_sync::parity::watch(steps, true));
    }
}
mod cursor {
    use super::{StreamError, pull};
    type FollowFrames = futures::stream::LocalBoxStream<'static, Result<Vec<u8>, StreamError>>;
    type FollowState = pull::PullCursor<Vec<u8>, StreamError>;
    include!("cursor.rs");
}
#[path = "../src/http/shared.rs"]
mod shared;
mod shared_models {
    use super::shared::*;
    include!("shared.rs");
}
