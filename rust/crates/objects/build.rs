#![allow(missing_docs)]

use acyclic_sdk_contract_wire::{BindingTransport, transport_control};
use std::{env, path::PathBuf};

fn main() {
    let output = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR")).join("control");
    transport_control::generate_control_bindings(
        &output,
        BindingTransport::Tonic {
            client: true,
            server: false,
        },
    )
    .expect("generate Rust transport control bindings");
}
