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
pub use acyclic_stream::query_sync;
#[path = "../src/pull.rs"]
mod pull;
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
