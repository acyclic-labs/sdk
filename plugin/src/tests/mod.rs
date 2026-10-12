#![allow(
    clippy::indexing_slicing,
    reason = "tests index asserted fixture shapes"
)]

use super::*;
use support::*;
pub(crate) use support::{configured_test_service_bootstrap, test_service_control};

mod hosts;
mod installation;
mod paths;
mod protocol;
mod recursive;
mod service;
mod state;
mod support;
