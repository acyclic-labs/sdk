//! Shared bounded paging of one authorized file projection.
use super::{AgentPageRequest, AgentReply, Error, Result};
use crate::client::DiffView;
pub use acyclic_harness_client::DIFF_FILE_BYTES;

mod source;
pub use source::DiffSource;
pub(in crate::hosting) use source::frame;
