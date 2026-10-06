#![allow(missing_docs)]

use acyclic_sdk_contract_wire::{BindingTransport, transport_control};
use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = PathBuf::from(env::var_os("OUT_DIR").ok_or("OUT_DIR is unset")?).join("control");
    transport_control::generate_control_bindings(
        &output,
        BindingTransport::Tonic {
            client: true,
            server: false,
        },
    )?;
    Ok(())
}
