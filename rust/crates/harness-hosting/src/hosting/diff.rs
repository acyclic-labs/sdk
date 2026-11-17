//! Shared bounded paging of one authorized file projection.
use super::{AgentPageRequest, AgentReply};
use crate::client::DiffView;
use acyclic_harness_client::credit_error;
pub use acyclic_harness_client::DIFF_FILE_BYTES;

mod source;
pub use source::DiffSource;
pub(crate) use source::frame;
