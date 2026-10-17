//! Host installation and uninstallation.

use super::*;

mod activation;
mod certification;
mod codex_config;
mod codex_plugin;
mod commands;
mod files;
mod hooks;
mod ownership;

pub(crate) use activation::activation;
pub(crate) use codex_config::*;
pub(crate) use codex_plugin::*;
pub(crate) use commands::*;
pub(crate) use files::*;
pub(crate) use hooks::*;
pub(crate) use ownership::*;
