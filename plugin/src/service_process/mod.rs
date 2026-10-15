//! Service startup, diagnostics and control transport.

use crate::service::SessionCloseState;
use crate::*;

mod client;
mod diagnostics;
mod identity;
mod lifecycle;
#[cfg(target_os = "linux")]
mod linux;
mod locking;
mod reply;
mod rpc;
#[cfg(windows)]
mod windows;

pub(crate) use client::*;
pub(crate) use diagnostics::*;
pub(crate) use identity::*;
pub(crate) use lifecycle::*;
#[cfg(target_os = "linux")]
pub(crate) use linux::*;
pub(crate) use locking::*;
pub(crate) use reply::*;
pub(crate) use rpc::*;
#[cfg(windows)]
pub(crate) use windows::*;
