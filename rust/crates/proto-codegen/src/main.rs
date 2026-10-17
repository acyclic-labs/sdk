//! Repository-local protobuf generator entry point.

use std::io::{self, Write};

mod native_absence;
mod native_family;
mod readonly;
mod service;

fn main() -> io::Result<()> {
    let generator = std::env::args().nth(1);
    if generator.as_deref() == Some("descriptors") {
        let args = std::env::args().skip(2).collect::<Vec<_>>();
        let [root, output] = args.as_slice() else {
            return Err(io::Error::other(
                "descriptors expects source root and output directory",
            ));
        };
        let root = std::path::Path::new(root);
        let output = std::path::Path::new(output);
        std::fs::create_dir_all(output)?;
        for (family, baseline, protos) in acyclic_proto_build::FAMILIES {
            acyclic_proto_build::descriptor(
                &root.join("rust/crates").join(family),
                protos,
                &output.join(baseline),
            )
            .map_err(|error| io::Error::other(error.to_string()))?;
        }
        return Ok(());
    }
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
    Err(io::Error::other(
        "expected descriptors, readonly, native-absence, service or native-family generator",
    ))
}
