use heck::{ToSnakeCase, ToUpperCamelCase};
use prost::Message;
use std::{fmt::Write as _, path::PathBuf};

fn messages(
    output: &mut String,
    package: &str,
    rust: &str,
    entries: &[prost_types::DescriptorProto],
    auth: bool,
    family: usize,
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
        let check = if auth {
            format!(
                "check_semantic::<{ty}, {rust}::{}>",
                name.to_upper_camel_case()
            )
        } else if package == "acyclic.actors.v1" {
            let semantic = if name == "Error" {
                "ServiceError"
            } else {
                name
            };
            format!("check_semantic::<{ty}, acyclic_actors::contract::domain::{semantic}>")
        } else {
            format!("check::<{ty}>")
        };
        writeln!(
            output,
            "Decoder {{ name: \"{package}.{name}\", family: {family}, check: {check}, seed: seed::<{ty}> }},"
        )?;
        messages(
            output,
            &format!("{package}.{name}"),
            &format!("{rust}::{}", name.to_snake_case()),
            &entry.nested_type,
            auth,
            family,
        )?;
    }
    Ok(())
}

fn generate() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let out = PathBuf::from(std::env::var_os("OUT_DIR").ok_or("missing OUT_DIR")?);
    let families: &[(&str, &[&str], &str, bool)] = &[
        ("auth", &["acyclic.auth.v1"], "acyclic_auth::contract", true),
        (
            "actors",
            &["acyclic.actors.v1"],
            "acyclic_actors::wire",
            false,
        ),
        (
            "filesystem-wire",
            &["acyclic.filesystem.v2"],
            "acyclic_fs_wire::wire::filesystem::v2",
            false,
        ),
        (
            "inference",
            &["inference.customer.v1"],
            "acyclic_inference::wire",
            false,
        ),
        (
            "machines",
            &["acyclic.machines.v1"],
            "acyclic_machines::wire",
            false,
        ),
        (
            "objects",
            &["acyclic.objects.v1"],
            "acyclic_objects::wire",
            false,
        ),
        (
            "stream",
            &["acyclic.stream.v1"],
            "acyclic_stream::wire",
            false,
        ),
        (
            "harness-core",
            &["acyclic.harness.v2", "acyclic.protocol.v1"],
            "acyclic_harness_core::wire",
            false,
        ),
    ];
    let mut table = String::from("pub static DECODERS: &[Decoder] = &[\n");
    let mut descriptors = String::from("static DESCRIPTORS: &[&[u8]] = &[\n");
    for (index, (family, packages, rust, auth)) in families.iter().copied().enumerate() {
        let (_, _, protos) = acyclic_proto_build::FAMILIES
            .iter()
            .find(|entry| entry.0 == family)
            .ok_or("unknown protocol family")?;
        let manifest = root.join("../crates").join(family);
        let descriptor = out.join(format!("{family}.bin"));
        acyclic_proto_build::descriptor(&manifest, protos, &descriptor)?;
        writeln!(
            descriptors,
            "include_bytes!(concat!(env!(\"OUT_DIR\"), \"/{family}.bin\")),"
        )?;
        for proto in *protos {
            println!("cargo:rerun-if-changed={}", manifest.join(proto).display());
        }
        let set = prost_types::FileDescriptorSet::decode(std::fs::read(descriptor)?.as_slice())?;
        for file in set
            .file
            .iter()
            .filter(|file| packages.contains(&file.package()))
        {
            messages(
                &mut table,
                file.package(),
                rust,
                &file.message_type,
                auth,
                index,
            )?;
        }
    }
    table.push_str("];\n");
    descriptors.push_str("];\n");
    table.push_str(&descriptors);
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
