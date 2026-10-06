//! Repository-local protobuf generator entry point.

use std::io::{self, Read, Write};

use prost::Message;
use prost_types::compiler::CodeGeneratorResponse;
use protoc_gen_prost::GeneratorResultExt;

fn normalize_tonic_response(mut response: CodeGeneratorResponse) -> CodeGeneratorResponse {
    for file in &mut response.file {
        let (Some(name), Some(content)) = (file.name.as_deref(), file.content.as_mut()) else {
            continue;
        };
        let Some(service) = [
            ("acyclic.objects.v1.rs", "acyclic.objects.v1"),
            ("acyclic.objects.v2.rs", "acyclic.objects.v2"),
            ("acyclic.machines.v1.rs", "acyclic.machines.v1"),
        ]
        .iter()
        .find_map(|(suffix, service)| name.ends_with(suffix).then_some(*service))
        else {
            continue;
        };
        let include = format!("include!(\"{service}.tonic.rs\");");
        if content.contains(&include) {
            *content = content.replace(
                &include,
                &format!("#[cfg(feature = \"grpc\")]\n{include}"),
            );
        }
    }
    response
}

fn main() -> io::Result<()> {
    let generator = std::env::args().nth(1);
    let mut request = Vec::new();
    io::stdin().read_to_end(&mut request)?;
    let response = match generator.as_deref() {
        Some("prost") => protoc_gen_prost::execute(&request).unwrap_codegen_response(),
        Some("tonic") => normalize_tonic_response(
            protoc_gen_tonic::execute(&request).unwrap_codegen_response(),
        ),
        _ => return Err(io::Error::other("expected prost or tonic generator")),
    };
    let mut encoded = Vec::new();
    response.encode(&mut encoded).map_err(io::Error::other)?;
    io::stdout().write_all(&encoded)
}

#[cfg(test)]
mod tests {
    use super::normalize_tonic_response;
    use prost_types::compiler::{code_generator_response::File, CodeGeneratorResponse};

    #[test]
    fn tonic_service_include_is_feature_gated_by_the_rust_generator() {
        let response = CodeGeneratorResponse {
            file: vec![File {
                name: Some("acyclic.objects.v2.rs".to_owned()),
                content: Some("include!(\"acyclic.objects.v2.tonic.rs\");\n".to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = normalize_tonic_response(response);
        assert_eq!(
            response.file[0].content.as_deref(),
            Some("#[cfg(feature = \"grpc\")]\ninclude!(\"acyclic.objects.v2.tonic.rs\");\n")
        );
    }
}
