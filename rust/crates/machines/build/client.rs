//! Project the client ownership contract from the canonical service descriptor.

use heck::{ToLowerCamelCase as _, ToSnakeCase as _};
use prost::Message as _;
use std::{fmt::Write as _, path::Path};

pub fn generate(output: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let descriptor = prost_types::FileDescriptorSet::decode(
        std::fs::read(output.join("acyclic-machines-v1.bin"))?.as_slice(),
    )?;
    let service = descriptor
        .file
        .iter()
        .flat_map(|file| &file.service)
        .find(|service| service.name.as_deref() == Some("MachinesService"))
        .ok_or("MachinesService descriptor is missing")?;
    let mut contract = String::from(
        "/// Public protobuf methods with platform-native future and stream ownership.\npub trait RpcClient: ProviderTask + 'static {\n",
    );
    let mut native = String::from(
        "#[cfg(not(target_arch = \"wasm32\"))]\nimpl RpcClient for super::native::Client {\n",
    );
    let mut javascript = String::from(
        "#[cfg(all(target_arch = \"wasm32\", feature = \"javascript\"))]\nimpl RpcClient for super::javascript::Client {\n",
    );
    for method in &service.method {
        let name = method.name.as_deref().ok_or("RPC name is missing")?;
        let rust = name.to_snake_case();
        let input = message(method.input_type.as_deref())?;
        let output = message(method.output_type.as_deref())?;
        let response = if method.server_streaming() {
            writeln!(
                contract,
                "/// Original {name} response producer.\ntype {name}Stream: Stream<Item = Result<wire::{output}, Status>> + Unpin + ProviderTask + 'static;"
            )?;
            writeln!(
                native,
                "type {name}Stream = tonic::Streaming<wire::{output}>;"
            )?;
            writeln!(
                javascript,
                "type {name}Stream = super::javascript::Output<wire::{output}>;"
            )?;
            format!("Self::{name}Stream")
        } else {
            format!("wire::{output}")
        };
        let (generic, request, bound) = if method.client_streaming() {
            (
                "<S>",
                "Request<S>".to_owned(),
                format!(" where S: Stream<Item = wire::{input}> + ProviderTask + 'static"),
            )
        } else {
            ("", format!("wire::{input}"), String::new())
        };
        let signature = format!(
            "fn {rust}{generic}(&mut self, request: {request}) -> impl Future<Output = Result<Response<{response}>, Status>> + ProviderTask{bound}"
        );
        writeln!(
            contract,
            "/// Invoke the original {name} RPC once.\n{signature};"
        )?;
        writeln!(
            native,
            "{signature} {{ async move {{ Self::{rust}(self, request).await }} }}"
        )?;
        let request = if method.client_streaming() {
            "request".to_owned()
        } else {
            "Request::new(futures::stream::once(std::future::ready(request)))".to_owned()
        };
        let call = if method.server_streaming() {
            "stream"
        } else {
            "unary"
        };
        let local = name.to_lower_camel_case();
        writeln!(
            javascript,
            "{signature} {{ async move {{ self.{call}(\"{local}\", {request}).await }} }}"
        )?;
    }
    contract.push_str("}\n");
    native.push_str("}\n");
    javascript.push_str("}\n");
    std::fs::write(
        output.join("machines-client.rs"),
        contract + &native + &javascript,
    )?;
    Ok(())
}

fn message(name: Option<&str>) -> Result<&str, Box<dyn std::error::Error>> {
    name.and_then(|name| name.strip_prefix(".acyclic.machines.v1."))
        .ok_or_else(|| "RPC message is outside the canonical Machines service".into())
}
