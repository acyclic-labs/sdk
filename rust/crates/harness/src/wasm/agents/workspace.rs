//! Opaque ownership of the public admitted Machines workspace.
use crate::{machines::MachinesWorkspace, tool::ToolExecutor};
use std::sync::Arc;
use wasm_bindgen::prelude::*;

/// A platform's admitted Machine, selected volume and authenticated provider.
/// The trusted Rust opener constructs the public `MachinesWorkspace` using the
/// server's original caller admission, then returns this handle from the same
/// generated module. JavaScript never supplies a replacement shell callback,
/// credential, volume grant or signing authority.
#[wasm_bindgen]
pub struct WasmMachinesWorkspace {
    workspace: Arc<MachinesWorkspace>,
}

impl From<MachinesWorkspace> for WasmMachinesWorkspace {
    fn from(workspace: MachinesWorkspace) -> Self {
        Self {
            workspace: Arc::new(workspace),
        }
    }
}

impl WasmMachinesWorkspace {
    pub(super) fn executor(&self) -> Arc<dyn ToolExecutor> {
        self.workspace.clone()
    }
}
