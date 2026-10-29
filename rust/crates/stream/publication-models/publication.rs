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
