//! Service startup, diagnostics and control transport.

use crate::*;
use crate::service::SessionCloseState;

mod client;
mod diagnostics;
mod identity;
mod lifecycle;
mod locking;
mod reply;
mod rpc;
#[cfg(windows)]
mod windows;
#[cfg(target_os = "linux")]
mod linux;

pub(crate) use client::*;
pub(crate) use diagnostics::*;
pub(crate) use identity::*;
pub(crate) use lifecycle::*;
pub(crate) use locking::*;
pub(crate) use reply::*;
pub(crate) use rpc::*;
#[cfg(windows)]
pub(crate) use windows::*;
#[cfg(target_os = "linux")]
pub(crate) use linux::*;
