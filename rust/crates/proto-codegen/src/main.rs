//! Repository-local protobuf generator entry point.

use std::io::{self, Read, Write};

mod native_absence;
mod native_family;
mod readonly;
mod service;

use prost::Message;
use protoc_gen_prost::GeneratorResultExt;

fn main() -> io::Result<()> {
    let generator = std::env::args().nth(1);
    if generator.as_deref() == Some("readonly") {
        if std::env::args().len() != 2 {
            return Err(io::Error::other("readonly mode takes no arguments"));
        }
        return io::stdout().write_all(readonly::TYPESCRIPT_READONLY.as_bytes());
    }
    if generator.as_deref() == Some("native-absence") {
        let source = native_absence::render(&std::env::args().skip(2).collect::<Vec<_>>())
            .map_err(|error| io::Error::other(error.to_string()))?;
        return io::stdout().write_all(source.as_bytes());
    }
    if generator.as_deref() == Some("service") {
        let source = service::render(&std::env::args().skip(2).collect::<Vec<_>>())
            .map_err(|error| io::Error::other(error.to_string()))?;
        return io::stdout().write_all(source.as_bytes());
    }
    if generator.as_deref() == Some("native-family") {
        let args = std::env::args().skip(2).collect::<Vec<_>>();
        let [root, family] = args.as_slice() else {
            return Err(io::Error::other(
                "native-family expects workspace root and family",
            ));
        };
        let facts = native_family::render(std::path::Path::new(root), family)
            .map_err(|error| io::Error::other(error.to_string()))?;
        return serde_json::to_writer(io::stdout(), &facts).map_err(io::Error::other);
    }
    let mut request = Vec::new();
    io::stdin().read_to_end(&mut request)?;
    let response = match generator.as_deref() {
        Some("prost") => protoc_gen_prost::execute(&request),
        Some("tonic") => protoc_gen_tonic::execute(&request),
        _ => return Err(io::Error::other("expected prost or tonic generator")),
    }
    .unwrap_codegen_response();
    let mut encoded = Vec::new();
    response.encode(&mut encoded).map_err(io::Error::other)?;
    io::stdout().write_all(&encoded)
}
