//! Shared bounded paging of one authorized file projection.
use super::{AgentPageRequest, AgentReply, Error};
use crate::Result;
use crate::client::DiffView;
use acyclic_harness_client::credit_error;
pub use acyclic_harness_client::DIFF_FILE_BYTES;

mod source;
pub use source::DiffSource;
pub(in crate::) use source::frame;
