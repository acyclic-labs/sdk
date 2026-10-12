//! Original-work admission for the shared stock engine.
use super::*;
use crate::projection::ModelContextSelection;

mod allocation;
pub(crate) use allocation::*;
#[cfg(feature = "filesystem")]
mod diagnostics;
#[cfg(feature = "filesystem")]
pub(in crate::executor) use diagnostics::*;
mod journal;
pub(crate) use journal::*;
mod session;
pub(in crate::executor) use session::*;
mod tools;
pub(in crate::executor) use tools::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
mod composition;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub use composition::NativeStockExecutor;
