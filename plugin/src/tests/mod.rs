#![allow(
    clippy::indexing_slicing,
    reason = "tests index asserted fixture shapes"
)]

use super::*;
pub(crate) use support::configured_test_service_bootstrap;
use support::native_hook_request;
use support::*;

mod git;
mod hosts;
mod installation;
mod paths;
mod protocol;
mod recursive;
mod service;
mod state;
mod support;
