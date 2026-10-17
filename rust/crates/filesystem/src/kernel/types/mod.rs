//! Typed invariants shared by authenticated namespace algorithms.

use super::allocation::{KernelAccounting, KernelOwnedAllocation};
use super::codec::{CanonicalDecodeError, ReadAllocationStep};
use super::file_read::OriginalReadAccounting;
use super::namespace_path::{NamespacePathError, finish_owned};
use crate::foundation::{Digest, FileId};
use crate::kernel::Child;
use crate::model::FilesystemProfile;
use crate::storage::{ObjectId, ObjectKind};
use crate::{CancellationToken, WorkCounters};
use thiserror::Error;

mod name;
pub(crate) use name::utf16le_units;
pub use name::{LogicalName, NameEncoding};
mod case_fold;
pub(crate) use case_fold::case_fold_unicode;
mod tree;
pub(crate) use tree::digest_object;
pub use tree::{FileKind, TreeEntry, TreePage, TreePageError};
mod extent;
pub use extent::{Extent, ExtentChild, ExtentKind, ExtentPage, ExtentPageError};

#[cfg(test)]
#[path = "../tests/types.rs"]
mod tests;
