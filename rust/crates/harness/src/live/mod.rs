//! Live admission, cancellation and result observation.
use crate::{Admission, NativeWork, OperationId, Outcome};
use acyclic_stream::BoxProviderStream as BoxStream;
#[cfg(target_arch = "wasm32")]
use futures::future::AbortHandle;
use futures::{StreamExt, stream};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Semaphore, oneshot};
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::AbortHandle;
mod group;
pub use group::TaskGroup;
mod handle;
pub use handle::TaskHandle;
mod completion;
pub use completion::*;
mod timer;
pub(crate) use timer::{sleep_millis, timeout_until};
#[cfg(all(test, target_arch = "wasm32"))]
mod browser_tests;
#[cfg(test)]
mod tests;
