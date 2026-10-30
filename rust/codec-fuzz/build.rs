use heck::{ToSnakeCase, ToUpperCamelCase};
use prost::Message;
use std::{fmt::Write as _, path::PathBuf};

fn messages(
    output: &mut String,
    package: &str,
    rust: &str,
    entries: &[prost_types::DescriptorProto],
    auth: bool,
) -> Result<(), std::fmt::Error> {
    for entry in entries {
        if entry
            .options
            .as_ref()
            .is_some_and(|options| options.map_entry())
        {
            continue;
        }
        let name = entry.name();
        let ty = format!(
            "{rust}::{}{}",
            name.to_upper_camel_case(),
            if auth { "Proto" } else { "" }
        );
        writeln!(
            output,
            "Decoder {{ name: \"{package}.{name}\", check: check::<{ty}>, seed: seed::<{ty}> }},"
        )?;
        messages(
            output,
            &format!("{package}.{name}"),
            &format!("{rust}::{}", name.to_snake_case()),
            &entry.nested_type,
            auth,
        )?;
    }
    Ok(())
}

fn generate() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let out = PathBuf::from(std::env::var_os("OUT_DIR").ok_or("missing OUT_DIR")?);
    let families = [
        ("auth", "acyclic.auth.v1", "acyclic_auth::contract", true),
        ("actors", "acyclic.actors.v1", "acyclic_actors::wire", false),
        (
            "filesystem-wire",
            "acyclic.filesystem.v2",
            "acyclic_fs_wire::wire::filesystem::v2",
            false,
        ),
        (
            "inference",
            "inference.customer.v1",
            "acyclic_inference::wire",
            false,
        ),
        (
            "machines",
            "acyclic.machines.v1",
            "acyclic_machines::wire",
            false,
        ),
        (
            "objects",
            "acyclic.objects.v1",
            "acyclic_objects::wire",
            false,
        ),
        ("stream", "acyclic.stream.v1", "acyclic_stream::wire", false),
    ];
    let mut table = String::from("pub static DECODERS: &[Decoder] = &[\n");
    for (family, package, rust, auth) in families {
        let (_, _, protos) = acyclic_proto_build::FAMILIES
            .iter()
            .find(|entry| entry.0 == family)
            .ok_or("unknown protocol family")?;
        let manifest = root.join("../crates").join(family);
        let descriptor = out.join(format!("{family}.bin"));
        acyclic_proto_build::descriptor(&manifest, protos, &descriptor)?;
        for proto in *protos {
            println!("cargo:rerun-if-changed={}", manifest.join(proto).display());
        }
        let set = prost_types::FileDescriptorSet::decode(std::fs::read(descriptor)?.as_slice())?;
        for file in set.file.iter().filter(|file| file.package() == package) {
            messages(&mut table, package, rust, &file.message_type, auth)?;
        }
    }
    table.push_str("];\n");
    std::fs::write(out.join("decoders.rs"), table)?;
    println!("cargo:rerun-if-changed=../../compatibility/proto");
    Ok(())
}

fn main() {
    if let Err(error) = generate() {
        eprintln!(
            "codec table generation failed: {error}; recovery: cargo check --manifest-path rust/codec-fuzz/Cargo.toml --all-targets"
        );
        std::process::exit(1);
    }
}
